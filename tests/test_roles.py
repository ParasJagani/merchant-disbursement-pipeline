"""The role gate: grants SQL is wired up, and the probe logic tells refused from broken."""

import re
from pathlib import Path

from ops.deploy import SQL_DIR, render
from ops.verify_roles import PROBES, check

ROOT = Path(__file__).resolve().parents[1]


class RoleCursor:
    """Stands in for Snowflake: ``allowed`` says which (role, text) the role may run."""

    def __init__(self, role, rules, log, secondary=lambda: '{"roles":"","value":""}'):
        self.role, self.rules, self.log, self.row = role, rules, log, None
        self.secondary = secondary

    def execute(self, sql):
        self.log.append((self.role, sql))
        if sql == "SELECT CURRENT_ROLE()":
            self.row = (self.role,)
            return
        if sql == "SELECT CURRENT_SECONDARY_ROLES()":
            self.row = (self.secondary(),)
            return
        verdict = self.rules(self.role, sql)
        if verdict != "ok":
            raise RuntimeError(verdict)

    def fetchone(self):
        return self.row

    def close(self):
        pass


class RoleConn:
    def __init__(self, role, rules, log):
        self.role, self.rules, self.log, self.closed = role, rules, log, False

    def cursor(self):
        return RoleCursor(self.role, self.rules, self.log)

    def close(self):
        self.closed = True


def run(rules):
    log, conns = [], []

    def connect_as(role):
        conns.append(RoleConn(role, rules, log))
        return conns[-1]

    return check(connect_as, "DB"), log, conns


def well_behaved(role, sql):
    """Roles exactly as 70_grants.sql defines them."""
    text = sql.upper()
    writes = text.startswith(("CREATE", "DELETE", "DROP"))
    if role == "TRANSFORMER":
        if "ALERT_SETTINGS" in text:
            return "SQL access control error: Insufficient privileges to operate on table"
        if writes and ".SILVER." in text:
            return "SQL access control error: Insufficient privileges to operate on schema"
    if role == "REPORTER":
        if ".BRONZE." in text or "ALERT_SETTINGS" in text:
            return "Object 'X' does not exist or not authorized."
        if writes and ".GOLD." in text:
            return "SQL access control error: Insufficient privileges to operate on schema"
    return "ok"


def test_a_correctly_granted_pair_of_roles_passes_every_probe():
    results, _, conns = run(well_behaved)
    assert all(r["ok"] for r in results), [r for r in results if not r["ok"]]
    assert len(results) == len(PROBES) + 4  # plus "running as" and "no secondary roles", per role
    assert all(c.closed for c in conns)


def test_each_role_connects_once_and_probes_run_as_the_right_role():
    _, log, conns = run(well_behaved)
    assert sorted(c.role for c in conns) == ["REPORTER", "TRANSFORMER"]
    assert {role for role, _ in log} == {"REPORTER", "TRANSFORMER"}


def test_a_session_that_keeps_secondary_roles_fails_the_gate():
    log, conns = [], []

    def connect_as(role):
        class Leaky(RoleConn):
            def cursor(self):
                return RoleCursor(
                    self.role, self.rules, self.log, lambda: '{"roles":"ACCOUNTADMIN","value":"x"}'
                )

        conns.append(Leaky(role, well_behaved, log))
        return conns[-1]

    results = check(connect_as, "DB")
    failed = [r["name"] for r in results if not r["ok"]]
    assert failed == ["TRANSFORMER: has no secondary roles", "REPORTER: has no secondary roles"]


def test_every_connection_turns_secondary_roles_off():
    text = (ROOT / "ops" / "connection.py").read_text()
    assert "USE SECONDARY ROLES NONE" in text
    assert any("DEFAULT_SECONDARY_ROLES = ()" in g for g in grants())


def test_a_role_with_too_much_access_fails_the_refusal_probe():
    results, _, _ = run(lambda role, sql: "ok")  # everyone may do everything
    failed = {r["name"] for r in results if not r["ok"]}
    assert "REPORTER: cannot change gold" in failed
    assert "TRANSFORMER: cannot read the alert settings" in failed
    assert "REPORTER: reads daily gold" not in failed


def test_a_role_with_too_little_access_fails_the_working_probe():
    def too_strict(role, sql):
        if role == "REPORTER" and "GOLD_DAILY" in sql.upper():
            return "SQL access control error: Insufficient privileges to operate on table"
        return well_behaved(role, sql)

    results, _, _ = run(too_strict)
    assert [r["name"] for r in results if not r["ok"]] == ["REPORTER: reads daily gold"]


def test_a_typo_is_not_mistaken_for_a_refusal():
    def broken(role, sql):
        if role == "REPORTER" and "DELETE" in sql.upper():
            return "SQL compilation error: syntax error line 1 at position 4 unexpected 'FORM'"
        return well_behaved(role, sql)

    results, _, _ = run(broken)
    assert [r["name"] for r in results if not r["ok"]] == ["REPORTER: cannot change gold"]


def test_writing_probes_cannot_change_data():
    for _, _, statements, _ in PROBES:
        for s in statements:
            head = s.upper()
            assert not head.startswith("INSERT") and not head.startswith("UPDATE")
            if head.startswith("DELETE"):
                assert "WHERE 1=0" in head
            if head.startswith(("CREATE", "DROP")):
                assert "_ROLE_PROBE" in head


def grants():
    return render(SQL_DIR / "70_grants.sql", "DB")


def test_grants_are_deployed_last_and_every_statement_is_a_grant():
    from ops.deploy import sql_files

    assert sql_files()[-1].name == "70_grants.sql"
    assert grants() and all(
        g.upper().startswith(("GRANT ", "ALTER USER IF EXISTS ")) for g in grants()
    )


def test_the_reporter_is_never_given_more_than_select_and_usage():
    for g in grants():
        if g.startswith("GRANT") and g.endswith("TO ROLE REPORTER"):
            privilege = g.split(" ON ")[0].removeprefix("GRANT ")
            assert privilege in {"USAGE", "SELECT"}, g


def test_the_reporter_cannot_reach_bronze_the_alert_settings_or_the_ledger_internals():
    text = " ".join(g for g in grants() if g.endswith("TO ROLE REPORTER"))
    assert "BRONZE" not in text and "alert_settings" not in text
    assert "OPS.alert_log" in text and "OPS.health_signals" in text


def test_the_transformer_writes_only_to_gold():
    for g in grants():
        if g.endswith("ROLE TRANSFORMER") and re.match(r"GRANT (USAGE, )?(CREATE|OWNERSHIP)", g):
            assert ".GOLD" in g, g


def test_dbt_and_the_dashboard_run_as_their_own_roles_not_the_admin():
    profile = (ROOT / "dbt" / "profiles.yml").read_text()
    assert "SNOWFLAKE_TRANSFORMER_ROLE', 'TRANSFORMER'" in profile
    assert "ACCOUNTADMIN" not in profile
    sources = (ROOT / "statements" / "sources.py").read_text()
    assert 'default_role="REPORTER"' in sources
