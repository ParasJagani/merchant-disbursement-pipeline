"""Check that monitoring is deployed and healthy, and show what is open right now.

    python -m ops.verify_monitoring

It fails if an object is missing or if the alert itself has been failing (an alert that fails
silently is worse than no alert). Open incidents and a missing email address are warnings.
"""

from __future__ import annotations

import argparse
import os

from .verify_quality import FAIL, WARN, _rows, result

RULES = {
    "quarantine_recent",
    "reconciliation_gap",
    "task_failures_recent",
    "dynamic_table_failures_recent",
    "bronze_stale_minutes",
}
BAD_STATES = ("FAILED", "CONDITION_FAILED", "ACTION_FAILED")


def check(connection, database: str, days: int = 1) -> list[dict]:
    out = []
    rules = {
        r["signal"] for r in _rows(connection, f"SELECT signal FROM {database}.OPS.alert_rules")
    }
    out.append(result("every rule is defined", len(RULES), len(RULES & rules), RULES <= rules,
                      detail=str(sorted(RULES - rules))))  # fmt: skip
    signals = _rows(connection, f"SELECT * FROM {database}.OPS.health_signals ORDER BY signal")
    out.append(result("the health signals can be measured", len(RULES), len(signals),
                      len(signals) == len(RULES)))  # fmt: skip
    for proc in ("RAISE_ALERTS", "SEND_ALERT_EMAILS"):
        found = _rows(connection, f"SHOW PROCEDURES LIKE '{proc}' IN SCHEMA {database}.OPS")
        out.append(result(f"procedure {proc} exists", "present", "present" if found else "missing",
                          bool(found)))  # fmt: skip
    alerts = _rows(connection, f"SHOW ALERTS LIKE 'HEALTH_ALERT' IN SCHEMA {database}.OPS")
    state = str(alerts[0]["state"]).lower() if alerts else "missing"
    out.append(result("OPS.HEALTH_ALERT exists", "started or suspended", state,
                      state in ("started", "suspended")))  # fmt: skip
    history = _rows(
        connection,
        "SELECT state, scheduled_time, COALESCE(sql_error_message, '') AS error FROM TABLE("
        f"{database}.INFORMATION_SCHEMA.ALERT_HISTORY("
        f"SCHEDULED_TIME_RANGE_START => DATEADD('day', -{days}, CURRENT_TIMESTAMP()), "
        "RESULT_LIMIT => 1000, ALERT_NAME => 'HEALTH_ALERT'))",
    )
    bad = [h for h in history if str(h["state"]).upper() in BAD_STATES]
    out.append(result(f"alert runs that failed (last {days} day)", 0, len(bad), not bad, FAIL,
                      str([(str(h["state"]), h["error"][:80]) for h in bad[:2]])))  # fmt: skip
    settings = {
        r["setting"]: r["setting_value"]
        for r in _rows(
            connection, f"SELECT setting, setting_value FROM {database}.OPS.alert_settings"
        )
    }
    out.append(result("an alert email address is set up", "configured",
                      "configured" if settings.get("email_recipient") else "not set", bool(
                          settings.get("email_recipient")), WARN,
                      "run `make alerts-setup EMAIL=you@example.com`"))  # fmt: skip
    failed_mail = _rows(
        connection,
        f"SELECT COUNT(*) AS n FROM {database}.OPS.alert_log WHERE email_status LIKE 'failed%' "
        "AND opened_at >= DATEADD('day', -7, CURRENT_TIMESTAMP())",
    )[0]["n"]
    out.append(result("alert emails that failed (last 7 days)", 0, int(failed_mail),
                      not failed_mail, WARN, "see OPS.alert_log.email_status"))  # fmt: skip
    opened = _rows(
        connection,
        f"SELECT signal, severity, observed, opened_at FROM {database}.OPS.alert_log "
        "WHERE status = 'OPEN' ORDER BY opened_at",
    )
    out.append(result("incidents open now", 0, len(opened), not opened, WARN,
                      str([(o["signal"], o["observed"]) for o in opened[:5]])))  # fmt: skip
    return out


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(prog="python -m ops.verify_monitoring")
    parser.add_argument(
        "--database", default=os.environ.get("SNOWFLAKE_DATABASE", "MERCHANT_DISBURSEMENT")
    )
    args = parser.parse_args(argv)
    from .connection import connect

    conn = connect(warehouse=os.environ.get("SNOWFLAKE_TRANSFORM_WAREHOUSE", "TRANSFORM_WH"))
    try:
        results = check(conn, args.database)
        rules = _rows(
            conn,
            "SELECT signal, severity, threshold, window_minutes, enabled "
            f"FROM {args.database}.OPS.alert_rules ORDER BY signal",
        )
    finally:
        conn.close()
    for r in results:
        tag = "PASS" if r["ok"] else ("WARN" if r["severity"] == WARN else "FAIL")
        extra = f"  {r['detail']}" if r["detail"] and not r["ok"] else ""
        want, got = f"expected {r['expected']!s:>8}", f"actual {r['actual']!s:>8}"
        print(f"{tag}  {r['name']:<44} {want}  {got}{extra}")
    print("\nRules (edit OPS.alert_rules to change them):")
    for r in rules:
        on = "on " if r["enabled"] else "off"
        print(f"  {on} {r['signal']:<32} {r['severity']:<9} above {r['threshold']}")
    return 0 if all(r["ok"] or r["severity"] == WARN for r in results) else 1


if __name__ == "__main__":
    raise SystemExit(main())
