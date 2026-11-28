"""Phase 6 gate: an alert fires on an injected failure, and clears when the failure is gone.

    python -m ops.alert_drill            # plan only
    python -m ops.alert_drill --run      # inject, run the real alert, check, clean up, run it again

The drill injects the same bad messages as the chaos drill (silver must quarantine every one),
then runs the real ``OPS.HEALTH_ALERT`` once with ``EXECUTE ALERT`` and checks that an incident was
opened in ``OPS.alert_log`` and that an email was attempted. It then removes the messages, runs the
alert again, and checks that the incident resolved and no business table changed.

If the alert is suspended (you have not run ``make demo-up``), the drill resumes it for the test and
suspends it again at the end.
"""

from __future__ import annotations

import argparse
import os
import time

from . import chaos_drill
from .verify_quality import WARN

ALERT = "OPS.HEALTH_ALERT"
SIGNAL = "quarantine_recent"


def _all(cursor, sql):
    cursor.execute(sql)
    return cursor.fetchall()


def alert_state(cursor, database: str) -> str:
    """'started' or 'suspended' (blank if the alert does not exist)."""
    cursor.execute(f"SHOW ALERTS LIKE 'HEALTH_ALERT' IN SCHEMA {database}.OPS")
    names = [d[0].lower() for d in cursor.description]
    rows = cursor.fetchall()
    return str(dict(zip(names, rows[0], strict=True)).get("state", "")).lower() if rows else ""


def open_incidents(cursor, database: str, signal: str | None = None) -> list[tuple]:
    where = "status = 'OPEN'" + (f" AND signal = '{signal}'" if signal else "")
    return _all(
        cursor,
        f"SELECT signal, severity, observed, email_status FROM {database}.OPS.alert_log "
        f"WHERE {where} ORDER BY signal",
    )


def last_run(cursor, database: str) -> tuple[str, str]:
    """State and error of the newest run of the alert, from its history."""
    rows = _all(
        cursor,
        "SELECT state, COALESCE(sql_error_message, '') FROM TABLE("
        f"{database}.INFORMATION_SCHEMA.ALERT_HISTORY("
        "SCHEDULED_TIME_RANGE_START => DATEADD('hour', -1, CURRENT_TIMESTAMP()), "
        "RESULT_LIMIT => 20, ALERT_NAME => 'HEALTH_ALERT')) "
        "WHERE state NOT IN ('SCHEDULED', 'EXECUTING') ORDER BY scheduled_time DESC LIMIT 1",
    )
    return (str(rows[0][0]), str(rows[0][1])) if rows else ("none", "")


def wait_for(check, timeout: float, pause: float = 3.0):
    """Call check() until it returns something truthy or the time is up. Returns the last value."""
    deadline = time.monotonic() + timeout
    value = check()
    while not value and time.monotonic() < deadline:
        time.sleep(pause)
        value = check()
    return value


def run(connection, database: str, timeout: float = 90.0, pause: float = 3.0) -> list[dict]:
    faults = chaos_drill.injectable()
    expected = len(faults)
    drill_id = time.strftime("%Y%m%d-%H%M%S")
    cursor = connection.cursor()
    results: list[dict] = []

    def record(name, want, actual, ok=None, severity="fail", detail=""):
        results.append(
            {
                "name": name,
                "expected": want,
                "actual": actual,
                "ok": actual == want if ok is None else ok,
                "severity": severity,
                "detail": detail,
            }
        )

    def execute_alert():
        cursor.execute(f"EXECUTE ALERT {database}.{ALERT}")

    was = alert_state(cursor, database)
    record(
        "the alert exists",
        "started or suspended",
        was or "missing",
        was in ("started", "suspended"),
    )
    if not results[-1]["ok"]:
        return results
    try:
        if was == "suspended":
            cursor.execute(f"ALTER ALERT {database}.{ALERT} RESUME")
        cursor.execute(f"CALL {database}.OPS.RAISE_ALERTS()")  # start from a settled log
        before = chaos_drill.snapshot(cursor, database)
        existing = open_incidents(cursor, database)
        record("before: no incidents are open", 0, len(existing), detail=str(existing[:3]))
        if existing:
            return results  # never mix the drill with a real incident
        print(f"drill id {drill_id}: injecting {expected} bad messages")
        try:
            for sql, params in chaos_drill.insert_statements(database, drill_id, faults):
                cursor.execute(sql, params)
            for sql in chaos_drill.refresh_statements(database):
                cursor.execute(sql)
            execute_alert()
            found = wait_for(lambda: open_incidents(cursor, database, SIGNAL), timeout, pause)
            record("an incident opened for the injected failure", 1, len(found))
            if found:
                _, severity, observed, _ = found[0]
                record("it has the expected severity", "warning", severity)
                record("it counts every injected message", f">= {expected}", observed,
                       observed is not None and int(observed) >= expected)  # fmt: skip
            others = [i for i in open_incidents(cursor, database) if i[0] != SIGNAL]
            record("nothing else fired (the rejects are accounted for)", 0, len(others),
                   detail=str(others[:3]))  # fmt: skip
            state, error = last_run(cursor, database)
            record("the alert ran and its action succeeded", "TRIGGERED", state, detail=error[:200])
            email = wait_for(
                lambda: (open_incidents(cursor, database, SIGNAL) or [("", "", 0, None)])[0][3],
                timeout, pause)  # fmt: skip
            record("an email was attempted", "sent, not configured or failed", email or "none yet",
                   bool(email))  # fmt: skip
            record("the email was delivered", "sent", email or "none yet", email == "sent",
                   WARN if email == "not configured" else "fail",
                   "run `make alerts-setup EMAIL=you@example.com` to receive these"
                   if email == "not configured" else str(email))  # fmt: skip
        finally:
            for sql in chaos_drill.cleanup_statements(database, drill_id):
                cursor.execute(sql)
            for sql in chaos_drill.refresh_statements(database):
                cursor.execute(sql)
        execute_alert()
        wait_for(lambda: not open_incidents(cursor, database), timeout, pause)
        record("after cleanup: the incident resolved", 0, len(open_incidents(cursor, database)))
        resolved = _all(
            cursor,
            f"SELECT COUNT(*) FROM {database}.OPS.alert_log "
            f"WHERE signal = '{SIGNAL}' AND status = 'RESOLVED' AND resolved_at IS NOT NULL",
        )[0][0]
        record("it is kept in the log as resolved", ">= 1", int(resolved), int(resolved) >= 1)
        after = chaos_drill.snapshot(cursor, database)
        record(
            "cleanup restored every table count", 0, sum(1 for t in before if before[t] != after[t])
        )
    finally:
        if was == "suspended":
            cursor.execute(f"ALTER ALERT {database}.{ALERT} SUSPEND")
        cursor.close()
    return results


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(prog="python -m ops.alert_drill")
    parser.add_argument("--run", action="store_true", help="really inject (default is a plan)")
    parser.add_argument(
        "--database", default=os.environ.get("SNOWFLAKE_DATABASE", "MERCHANT_DISBURSEMENT")
    )
    args = parser.parse_args(argv)
    if not args.run:
        print(f"Would inject {len(chaos_drill.injectable())} bad messages, run {ALERT} with")
        print("EXECUTE ALERT, expect one OPEN incident in OPS.alert_log (and an email), then")
        print("clean up and run the alert again, expecting the incident to be RESOLVED.")
        print("\nAdd --run to do it.")
        return 0
    from .connection import connect

    conn = connect(warehouse=os.environ.get("SNOWFLAKE_TRANSFORM_WAREHOUSE", "TRANSFORM_WH"))
    try:
        results = run(conn, args.database)
    finally:
        conn.close()
    for r in results:
        tag = "PASS" if r["ok"] else ("WARN" if r["severity"] == WARN else "FAIL")
        extra = f"  {r['detail']}" if r["detail"] and not r["ok"] else ""
        want, got = f"expected {r['expected']!s:>8}", f"actual {r['actual']!s:>8}"
        print(f"{tag}  {r['name']:<58} {want}  {got}{extra}")
    return 0 if all(r["ok"] or r["severity"] == WARN for r in results) else 1


if __name__ == "__main__":
    raise SystemExit(main())
