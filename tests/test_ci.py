"""The CI is code too: the free workflow stays free, the Snowflake one cannot run by accident."""

from pathlib import Path

import yaml

from ops.load_bronze import load

ROOT = Path(__file__).resolve().parents[1]


def workflow(name):
    return yaml.safe_load((ROOT / ".github" / "workflows" / f"{name}.yml").read_text())


def triggers(wf):
    return wf.get("on") or wf[True]  # YAML reads a bare `on` as True


def test_the_push_workflow_never_touches_snowflake():
    text = (ROOT / ".github" / "workflows" / "ci.yml").read_text()
    assert "SNOWFLAKE" not in text.replace("Snowflake account is touched", "")
    assert "make integration" not in text and "--target snowflake" not in text
    assert "push" in triggers(workflow("ci"))


def test_the_push_workflow_runs_the_whole_pipeline_offline():
    text = (ROOT / ".github" / "workflows" / "ci.yml").read_text()
    for step in ("make dbt-local", "make statements-verify", "dbt docs generate"):
        assert step in text


def test_the_snowflake_workflow_is_manual_and_asks_for_confirmation():
    wf = workflow("integration")
    on = triggers(wf)
    assert list(on) == ["workflow_dispatch"]
    job = wf["jobs"]["snowflake"]
    assert "'run'" in job["if"] and job["timeout-minutes"] <= 60
    assert "make integration" in [s.get("run") for s in job["steps"]]


def test_the_keys_come_from_secrets_and_are_removed_afterwards():
    text = (ROOT / ".github" / "workflows" / "integration.yml").read_text()
    assert "secrets.SNOWFLAKE_ADMIN_PRIVATE_KEY" in text and "-----BEGIN" not in text
    last = workflow("integration")["jobs"]["snowflake"]["steps"][-1]
    assert last["if"] == "always()" and "rm -rf .keys" in last["run"]
    assert ".keys/" in (ROOT / ".gitignore").read_text()


def test_the_integration_script_always_stops_the_meter():
    text = (ROOT / "scripts" / "integration.sh").read_text()
    assert "trap stop_meter EXIT" in text and "ops.task_control down" in text
    assert text.index("ops.deploy") < text.index("dbt build") < text.index("ops.verify_roles")
    assert text.index("ops.verify_roles") < text.index("statements.verify")


class Cursor:
    def __init__(self):
        self.sql = []

    def execute(self, sql):
        self.sql.append(sql)

    def fetchone(self):
        return ("loaded 148 files",)

    def close(self):
        pass


class Conn:
    def __init__(self):
        self.c = Cursor()

    def cursor(self):
        return self.c


def test_load_calls_the_same_procedure_the_task_calls():
    conn = Conn()
    assert load(conn, "DB") == "loaded 148 files"
    assert conn.c.sql == ["CALL DB.OPS.LOAD_BRONZE()"]
