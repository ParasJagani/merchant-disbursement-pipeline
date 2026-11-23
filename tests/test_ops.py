import re
from pathlib import Path

from file_writer.naming import topic_path
from generator.config import TOPICS
from ops.deploy import SQL_DIR, deploy, render, split_statements, sql_files
from ops.task_control import set_state
from ops.verify_bronze import check

ROOT = Path(__file__).resolve().parents[1]


class Cursor:
    def __init__(self, answers=None):
        self.executed, self.answers = [], answers or {}

    def execute(self, sql):
        self.executed.append(sql)
        self.last = sql

    def fetchone(self):
        for table, value in self.answers.items():
            if table in self.last:
                return value
        return (0, 0)

    def close(self):
        pass


class Conn:
    def __init__(self, answers=None):
        self.c = Cursor(answers)

    def cursor(self):
        return self.c


# --- SQL deployment -------------------------------------------------------------------------


def test_split_keeps_dollar_quoted_blocks_whole_and_drops_comments():
    text = (
        "-- a comment; with a semicolon\nSELECT 1;\nCREATE PROCEDURE p() AS\n$$\n"
        "BEGIN\n  SELECT 2;\nEND;\n$$;\nSELECT 3"
    )
    parts = split_statements(text)
    assert parts[0] == "SELECT 1"
    assert (
        parts[1].startswith("CREATE PROCEDURE")
        and "SELECT 2;" in parts[1]
        and parts[1].endswith("$$")
    )
    assert parts[2] == "SELECT 3" and len(parts) == 3


def test_deploy_skips_the_bootstrap_file_and_runs_in_order():
    names = [p.name for p in sql_files()]
    assert names == sorted(names) and not any(n.startswith("00_") for n in names)
    conn = Conn()
    count = deploy(conn, "MY_DB")
    assert count == len(conn.c.executed) > 15
    assert conn.c.executed[0] == "USE DATABASE MY_DB"
    assert not any("{db}" in s for s in conn.c.executed)


def test_dry_run_changes_nothing(capsys):
    assert deploy(None, "MY_DB", dry_run=True) > 15
    assert "CREATE TABLE IF NOT EXISTS ref_merchants" in capsys.readouterr().out


def test_every_statement_is_safe_to_repeat():
    for path in sql_files():
        for s in render(path, "DB"):
            head = s.lstrip().upper()
            assert (
                head.startswith(("USE ", "GRANT ", "ALTER STAGE "))  # ALTER ... SET is repeatable
                or " IF NOT EXISTS " in head[:80]
                or head.startswith("CREATE OR REPLACE ")
            ), s[:60]


def test_bronze_has_one_change_tracked_table_per_topic():
    text = (SQL_DIR / "10_bronze.sql").read_text()
    for topic in TOPICS:
        match = re.search(rf"CREATE TABLE IF NOT EXISTS {topic_path(topic)}\b[^;]*;", text)
        assert match, topic
        assert "CHANGE_TRACKING = TRUE" in match.group(0)
        assert "raw VARIANT" in match.group(0) and "source_file" in match.group(0)


def test_load_procedure_copies_every_topic_from_its_own_folder_with_skip_file():
    text = (SQL_DIR / "20_load_task.sql").read_text()
    for topic in TOPICS:
        p = topic_path(topic)
        assert f"COPY INTO {{db}}.BRONZE.{p} " in text
        assert f"@{{db}}.BRONZE.LANDING/{p}/" in text
    assert text.count("ON_ERROR = SKIP_FILE") == len(TOPICS)
    assert "SCHEDULE = '5 MINUTE'" in text and "ALTER TASK" not in text  # created suspended


def test_bootstrap_has_placeholders_and_no_secrets():
    text = (SQL_DIR / "00_bootstrap_once.sql").read_text()
    assert "<PASTE_TF_PUBLIC_KEY_HERE>" in text and "<PASTE_LOADER_PUBLIC_KEY_HERE>" in text
    assert "-----BEGIN" not in text and "PASSWORD" not in text.upper()


# --- operations ---------------------------------------------------------------------------


def test_verify_bronze_compares_counts_with_the_manifest():
    manifest = {"valid_messages_by_topic": {t: 10 for t in TOPICS}}
    good = {f"BRONZE.{topic_path(t)}": (10, 2) for t in TOPICS}
    assert all(r["ok"] for r in check(Conn(good), manifest, "DB"))
    bad = dict(good, **{"BRONZE.billing_fees": (11, 2)})
    results = check(Conn(bad), manifest, "DB")
    assert [r["topic"] for r in results if not r["ok"]] == ["billing.fees"]


def test_task_control_resumes_and_suspends():
    conn = Conn()
    assert set_state(conn, "DB", True) == ["ALTER TASK DB.OPS.LOAD_BRONZE_TASK RESUME"]
    assert set_state(conn, "DB", False) == ["ALTER TASK DB.OPS.LOAD_BRONZE_TASK SUSPEND"]


# --- Terraform (no binary here, so static checks) ----------------------------------------------


def test_terraform_files_are_balanced_and_define_the_expected_objects():
    tf = {p.name: p.read_text() for p in (ROOT / "terraform").glob("*.tf")}
    assert {
        "versions.tf",
        "variables.tf",
        "main.tf",
        "warehouses.tf",
        "roles.tf",
        "outputs.tf",
    } <= set(tf)
    for name, text in tf.items():
        assert text.count("{") == text.count("}"), name
    joined = "\n".join(tf.values())
    for needle in [
        '"LOADER"',
        '"TRANSFORMER"',
        '"REPORTER"',
        "INGEST_WH",
        "TRANSFORM_WH",
        "snowflake_resource_monitor",
        "BRONZE",
        "QUARANTINE",
        "OPS",
    ]:
        assert needle in joined, needle
    assert "terraform.tfvars" in (ROOT / ".gitignore").read_text()


def test_no_real_credentials_are_committed():
    for path in ROOT.rglob("*"):
        if (
            path.is_file()
            and ".git" not in path.parts
            and path.suffix in {".tf", ".sql", ".py", ".md", ".example"}
        ):
            text = path.read_text(errors="ignore")
            assert "-----BEGIN" + " PRIVATE KEY" not in text, path
            assert "-----BEGIN" + " RSA PRIVATE KEY" not in text, path


def test_stage_file_format_is_fully_qualified():
    """A load runs with no current schema, so the file format needs a full name."""
    text = (SQL_DIR / "10_bronze.sql").read_text()
    assert "FORMAT_NAME = '{db}.BRONZE.NDJSON_GZ'" in text
    assert "FILE_FORMAT = ndjson_gz" not in text
