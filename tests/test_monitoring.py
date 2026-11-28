"""Phase 6: the health signals and the alert log, run on the DuckDB stand-in for Snowflake.

What this proves: which conditions fire, that an incident is opened once and not once per run,
that it resolves when the condition clears, and that thresholds and switches are data, not SQL.
What it cannot prove (checked on Snowflake by ``ops/alert_drill.py``): the ALERT object, the
INFORMATION_SCHEMA history functions and the email.
"""

from collections import defaultdict
from datetime import datetime, timedelta, timezone

import pytest

from generator.chaos import deliver
from generator.config import GeneratorConfig
from generator.schema import validate_message
from generator.simulate import simulate
from ops.chaos_drill import injectable
from silver.faults import TABLES
from silver.local import LocalSilver
from silver.spec import EVENT_TOPICS, REFERENCE_TABLES

CFG = GeneratorConfig(days=5, tx_per_day=40)
BRONZE = {t.topic: t.bronze for t in (*REFERENCE_TABLES, *EVENT_TOPICS)}


def _now() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


@pytest.fixture()
def silver():
    messages, _ = deliver(CFG, simulate(CFG).events)
    local = LocalSilver()
    local.deploy()
    rows = defaultdict(list)
    for m in sorted(messages, key=lambda m: m.order):
        if validate_message(m.topic, m.value)[0]:
            rows[BRONZE[m.topic]].append(
                (m.value, "part.ndjson.gz", len(rows[BRONZE[m.topic]]) + 1)
            )
    for table, batch in rows.items():
        local.load_bronze(table, batch)
    local.run_reference()
    return local


def raise_alerts(silver):
    silver.call("OPS.RAISE_ALERTS")


def log(silver):
    return silver.query(
        "SELECT signal, status, observed FROM OPS.alert_log ORDER BY opened_at, signal"
    )


def inject(silver, count=6, age_minutes=0):
    """Load bad messages the way the chaos drill does, loaded `age_minutes` ago, then run the
    reference track."""
    silver._clock = _now() - timedelta(minutes=age_minutes)
    faults = [f for f in injectable() if f.topic in BRONZE][:count]
    for n, f in enumerate(faults, start=1):
        silver.load_bronze(TABLES[f.topic].bronze, [(f.raw, "chaos/t.ndjson", n)])
    silver.run_reference()
    return len(faults)


def remove_injected(silver):
    for table in BRONZE.values():
        silver.con.execute(f"DELETE FROM BRONZE.{table} WHERE source_file = 'chaos/t.ndjson'")
    for t in REFERENCE_TABLES:
        silver.con.execute(
            f"DELETE FROM QUARANTINE.rejected_{t.name} WHERE source_file = 'chaos/t.ndjson'"
        )


def firing(silver):
    return {r[0] for r in silver.query("SELECT signal FROM OPS.health_signals WHERE firing")}


def test_a_clean_pipeline_raises_nothing(silver):
    assert firing(silver) == set()
    raise_alerts(silver)
    assert log(silver) == []


def test_injected_bad_messages_open_one_quarantine_incident(silver):
    count = inject(silver)
    assert firing(silver) == {"quarantine_recent"}
    raise_alerts(silver)
    rows = silver.query(
        "SELECT signal, severity, status, observed, email_status FROM OPS.alert_log"
    )
    assert rows == [("quarantine_recent", "warning", "OPEN", count, None)]


def test_the_same_incident_is_not_opened_again_on_the_next_run(silver):
    inject(silver, count=3)
    raise_alerts(silver)
    inject(silver, count=3)  # more of the same arrive; the incident stays one
    silver.con.execute("DELETE FROM BRONZE.billing_fees WHERE source_file = 'chaos/t.ndjson'")
    raise_alerts(silver)
    raise_alerts(silver)
    assert [r[:2] for r in log(silver)] == [("quarantine_recent", "OPEN")]


def test_the_incident_resolves_when_the_condition_clears(silver):
    inject(silver)
    raise_alerts(silver)
    remove_injected(silver)
    raise_alerts(silver)
    rows = silver.query("SELECT status, resolved_at IS NOT NULL FROM OPS.alert_log")
    assert rows == [("RESOLVED", True)]
    assert firing(silver) == set()


def test_a_new_incident_opens_after_the_old_one_resolved(silver):
    inject(silver, count=2)
    raise_alerts(silver)
    remove_injected(silver)
    raise_alerts(silver)
    inject(silver, count=2)
    raise_alerts(silver)
    assert sorted(r[1] for r in log(silver)) == ["OPEN", "RESOLVED"]


def test_rows_that_vanish_between_bronze_and_silver_raise_a_critical_incident(silver):
    silver.con.execute("DELETE FROM SILVER.silver_merchants_versions WHERE sequence = 1")
    assert "reconciliation_gap" in firing(silver)
    raise_alerts(silver)
    row = silver.query(
        "SELECT severity, status FROM OPS.alert_log WHERE signal = 'reconciliation_gap'"
    )
    assert row == [("critical", "OPEN")]


def test_platform_failures_open_and_resolve_incidents(silver):
    def set_platform(tasks, tables):
        silver.con.execute("DELETE FROM OPS.platform_signals")
        silver.con.execute(
            "INSERT INTO OPS.platform_signals VALUES "
            f"('task_failures_recent', {tasks}, now()), "
            f"('dynamic_table_failures_recent', {tables}, now())"
        )

    set_platform(2, 0)
    assert firing(silver) == {"task_failures_recent"}
    raise_alerts(silver)
    assert silver.query("SELECT signal, severity, status FROM OPS.alert_log") == [
        ("task_failures_recent", "critical", "OPEN")
    ]
    set_platform(0, 0)
    raise_alerts(silver)
    assert [r[0] for r in silver.query("SELECT status FROM OPS.alert_log")] == ["RESOLVED"]


def test_a_disabled_rule_never_fires_and_an_enabled_one_does(silver):
    assert "bronze_stale_minutes" not in firing(silver)  # the demo data is old, but the rule is off
    silver.con.execute(
        "UPDATE OPS.alert_rules SET enabled = TRUE WHERE signal = 'bronze_stale_minutes'"
    )
    assert "bronze_stale_minutes" in firing(silver)
    silver._clock = _now()
    silver.load_bronze("billing_fees", [])  # nothing loaded: still stale
    assert "bronze_stale_minutes" in firing(silver)


def test_thresholds_are_data_not_code(silver):
    inject(silver, count=3)
    silver.con.execute(
        "UPDATE OPS.alert_rules SET threshold = 5 WHERE signal = 'quarantine_recent'"
    )
    assert firing(silver) == set()
    silver.con.execute(
        "UPDATE OPS.alert_rules SET threshold = 2 WHERE signal = 'quarantine_recent'"
    )
    assert firing(silver) == {"quarantine_recent"}


def test_messages_quarantined_before_the_window_do_not_fire(silver):
    inject(silver, count=3, age_minutes=120)  # loaded two hours ago; the window is one hour
    assert firing(silver) == set()
    silver.con.execute(
        "UPDATE OPS.alert_rules SET window_minutes = 240 WHERE signal = 'quarantine_recent'"
    )
    assert firing(silver) == {"quarantine_recent"}


def test_deploying_again_keeps_changed_thresholds_and_open_incidents(silver):
    inject(silver)
    raise_alerts(silver)
    silver.con.execute(
        "UPDATE OPS.alert_rules SET threshold = 1 WHERE signal = 'quarantine_recent'"
    )
    silver.deploy()
    assert silver.query(
        "SELECT threshold FROM OPS.alert_rules WHERE signal = 'quarantine_recent'"
    ) == [(1,)]
    assert silver.query("SELECT COUNT(*) FROM OPS.alert_rules")[0][0] == 5
    assert [r[1] for r in log(silver)] == ["OPEN"]


def test_every_rule_has_a_severity_and_a_description(silver):
    rows = silver.query("SELECT signal, severity, description FROM OPS.alert_rules")
    assert len(rows) == 5
    assert all(sev in ("warning", "critical") and desc for _, sev, desc in rows)


def test_the_alert_object_is_wired_to_the_procedure_and_is_serverless():
    from ops.deploy import SQL_DIR

    text = (SQL_DIR / "60_monitoring.sql").read_text()
    alert = text[text.index("CREATE OR REPLACE ALERT") :]
    assert "THEN CALL {db}.OPS.RAISE_ALERTS()" in alert
    assert "WAREHOUSE" not in alert
    assert "SCHEDULE = '10 MINUTE'" in alert
    assert "INFORMATION_SCHEMA.TASK_HISTORY" in alert and "DYNAMIC_TABLE_REFRESH_HISTORY" in alert


def test_email_failures_are_recorded_not_raised():
    from ops.deploy import SQL_DIR

    text = (SQL_DIR / "60_monitoring.sql").read_text()
    send = text[text.index("PROCEDURE {db}.OPS.SEND_ALERT_EMAILS") :]
    assert "EXCEPTION" in send and "'failed: '" in send and "'not configured'" in send
    # RAISE_ALERTS commits the log before it tries to send anything.
    raise_proc = text[
        text.index("PROCEDURE {db}.OPS.RAISE_ALERTS") : text.index("PROCEDURE {db}.OPS.SEND")
    ]
    assert raise_proc.index("COMMIT;") < raise_proc.index("SEND_ALERT_EMAILS()")


# --- the alert drill, against the DuckDB stand-in -------------------------------------------------


class AlertStandIn:
    """Answers the drill's Snowflake SQL with a LocalSilver, and plays the ALERT object."""

    def __init__(self, silver, state="started", email="sent", fires=True, history_delay=0):
        self.silver, self.state, self.email, self.fires = silver, state, email, fires
        self.history_delay, self.history_reads = history_delay, 0
        self.rows, self.description, self.log = [], None, []
        self.last_state = "none"
        silver._clock = _now()  # injected rows count as loaded "now"

    def cursor(self):
        return self

    def close(self):
        pass

    def fetchall(self):
        return self.rows

    def _raise(self):
        self.silver.call("OPS.RAISE_ALERTS")
        self.silver.con.execute(
            f"UPDATE OPS.alert_log SET email_status = '{self.email}' "
            "WHERE status = 'OPEN' AND email_status IS NULL"
        )

    def execute(self, sql, params=None):
        import re

        self.log.append(sql)
        text = re.sub(r"\bDB\.", "", sql)
        self.description = None
        if text.startswith("SHOW ALERTS"):
            self.description, self.rows = [("name",), ("state",)], [("HEALTH_ALERT", self.state)]
        elif text.startswith("ALTER ALERT"):
            self.state = "started" if text.endswith("RESUME") else "suspended"
        elif text.startswith("CALL OPS.RAISE_ALERTS"):
            self._raise()
        elif text.startswith("EXECUTE ALERT"):
            wanted = firing(self.silver) or self.silver.query(
                "SELECT 1 FROM OPS.alert_log WHERE status = 'OPEN'"
            )
            if self.fires and wanted:
                self._raise()
            self.last_state = "TRIGGERED" if self.fires and wanted else "CONDITION_FALSE"
        elif "ALERT_HISTORY" in text:
            self.history_reads += 1
            late = self.history_reads <= self.history_delay
            self.rows = [("SCHEDULED", "")] if late else [("SCHEDULED", ""), (self.last_state, "")]
        elif text.startswith("ALTER DYNAMIC TABLE"):
            return
        elif text.startswith("CALL "):
            self.silver.run_reference()
        elif text.startswith("INSERT INTO BRONZE."):
            import re as _re

            table = _re.match(r"INSERT INTO BRONZE\.(\w+)", text)[1]
            rows = [tuple(params[i : i + 3]) for i in range(0, len(params), 3)]
            self.silver.load_bronze(table, rows)
        else:
            from silver.local import translate

            self.rows = self.silver.query(translate(text))


def drill(silver, timeout=0, **kwargs):
    from ops import alert_drill

    conn = AlertStandIn(silver, **kwargs)
    results = alert_drill.run(conn, "DB", timeout=timeout, pause=0)
    return conn, {r["name"]: r for r in results}


def test_the_alert_drill_opens_an_incident_then_resolves_it(silver):
    conn, results = drill(silver)
    assert all(r["ok"] for r in results.values()), [r for r in results.values() if not r["ok"]]
    assert results["an incident opened for the injected failure"]["actual"] == 1
    assert results["it counts every injected message"]["actual"] >= 500
    assert silver.query("SELECT status FROM OPS.alert_log") == [("RESOLVED",)]


def test_a_run_that_reaches_the_history_a_little_late_still_passes(silver):
    conn, results = drill(silver, timeout=5, history_delay=3)
    assert results["the alert ran and its action succeeded"]["ok"]
    assert conn.history_reads > 3


def test_the_drill_fails_when_the_alert_does_not_open_an_incident(silver):
    conn, results = drill(silver, fires=False)
    assert not results["an incident opened for the injected failure"]["ok"]
    assert not results["the alert ran and its action succeeded"]["ok"]
    assert results["cleanup restored every table count"]["ok"]  # still cleans up


def test_a_suspended_alert_is_resumed_for_the_drill_and_suspended_again(silver):
    conn, results = drill(silver, state="suspended")
    assert all(r["ok"] for r in results.values())
    assert conn.state == "suspended"
    assert any(s.startswith("ALTER ALERT") and s.endswith("RESUME") for s in conn.log)


def test_an_unconfigured_email_is_a_warning_not_a_failure(silver):
    conn, results = drill(silver, email="not configured")
    delivered = results["the email was delivered"]
    assert not delivered["ok"] and delivered["severity"] == "warn"
    assert results["an email was attempted"]["ok"]


def test_the_drill_does_not_start_while_a_real_incident_is_open(silver):
    silver.con.execute("DELETE FROM SILVER.silver_merchants_versions WHERE sequence = 1")
    conn, results = drill(silver)
    assert not results["before: no incidents are open"]["ok"]
    assert not any(s.startswith("INSERT INTO DB.BRONZE.") for s in conn.log)


# --- set-up and verify tools ------------------------------------------------------------------


def test_setup_builds_the_integration_and_stores_the_address_in_snowflake_only():
    from ops import setup_alerts

    sql = setup_alerts.statements("DB", "paras@example.com")
    assert sql[0].startswith("CREATE OR REPLACE NOTIFICATION INTEGRATION MERCHANT_EMAIL_INT")
    assert "ALLOWED_RECIPIENTS = ('paras@example.com')" in sql[0]
    assert any("alert_settings" in s and "paras@example.com" in s for s in sql[1:])
    assert sql[-1].startswith("CALL SYSTEM$SEND_EMAIL(")
    assert not setup_alerts.statements("DB", "paras@example.com", send_test=False)[-1].startswith(
        "CALL"
    )


@pytest.mark.parametrize(
    "bad", ["", "nobody", "a@b", "a b@c.com", "x@y.com'); DROP TABLE t; --", "a@b.com,c@d.com"]
)
def test_setup_refuses_anything_that_is_not_one_plain_address(bad):
    from ops import setup_alerts

    with pytest.raises(ValueError):
        setup_alerts.statements("DB", bad)


def test_no_email_address_is_committed_to_the_repository():
    import re
    from pathlib import Path

    root = Path(__file__).resolve().parents[1]
    found = []
    for path in [*root.glob("sql/*.sql"), *root.glob("ops/*.py"), *root.glob("docs/*.md")]:
        found += re.findall(r"[\w.+-]+@(?!example\.com)[\w-]+\.[a-z]{2,}", path.read_text())
    assert not found, found


class Verify:
    """Scripted answers for verify_monitoring, keyed on a fragment of the SQL."""

    def __init__(self, answers):
        self.answers, self._rows, self.description = answers, [], []

    def cursor(self):
        return self

    def close(self):
        pass

    def execute(self, sql):
        for fragment, (names, rows) in self.answers.items():
            if fragment in sql:
                self.description = [(n,) for n in names]
                self._rows = rows
                return
        raise AssertionError(f"unexpected SQL: {sql[:80]}")

    def fetchall(self):
        return self._rows


def _healthy(**override):
    from ops.verify_monitoring import RULES

    answers = {
        "FROM DB.OPS.alert_rules": (["signal"], [(s,) for s in sorted(RULES)]),
        "FROM DB.OPS.health_signals": (["signal"], [(s,) for s in sorted(RULES)]),
        "SHOW PROCEDURES": (["name"], [("x",)]),
        "SHOW ALERTS": (["name", "state"], [("HEALTH_ALERT", "started")]),
        "ALERT_HISTORY": (["state", "scheduled_time", "error"], [("TRIGGERED", None, "")]),
        "FROM DB.OPS.alert_settings": (
            ["setting", "setting_value"],
            [("email_recipient", "a@example.com")],
        ),
        "email_status LIKE": (["n"], [(0,)]),
        "WHERE status = 'OPEN'": (["signal", "severity", "observed", "opened_at"], []),
    }
    answers.update(override)
    return Verify(answers)


def test_verify_monitoring_passes_when_everything_is_in_place():
    from ops import verify_monitoring

    results = verify_monitoring.check(_healthy(), "DB")
    assert all(r["ok"] for r in results), [r for r in results if not r["ok"]]


def test_verify_monitoring_fails_when_the_alert_has_been_failing():
    from ops import verify_monitoring

    conn = _healthy(
        ALERT_HISTORY=(
            ["state", "scheduled_time", "error"],
            [("ACTION_FAILED", None, "Insufficient privileges")],
        )
    )
    bad = [r for r in verify_monitoring.check(conn, "DB") if not r["ok"]]
    assert [r["name"] for r in bad] == ["alert runs that failed (last 1 day)"]
    assert bad[0]["severity"] == "fail"


def test_open_incidents_and_missing_email_are_warnings_not_failures():
    from ops import verify_monitoring

    conn = _healthy(
        **{
            "FROM DB.OPS.alert_settings": (["setting", "setting_value"], []),
            "WHERE status = 'OPEN'": (
                ["signal", "severity", "observed", "opened_at"],
                [("quarantine_recent", "warning", 3, None)],
            ),
        }
    )
    bad = [r for r in verify_monitoring.check(conn, "DB") if not r["ok"]]
    assert len(bad) == 2 and all(r["severity"] == "warn" for r in bad)
