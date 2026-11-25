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


def test_task_control_resumes_and_suspends_tasks_and_dynamic_tables():
    conn = Conn()
    up = set_state(conn, "DB", True)
    assert up[0] == "ALTER TASK DB.OPS.LOAD_BRONZE_TASK RESUME"
    assert "ALTER TASK DB.OPS.MERGE_MERCHANTS_TASK RESUME" in up
    assert "ALTER DYNAMIC TABLE DB.SILVER.silver_transactions RESUME" in up
    down = set_state(conn, "DB", False)
    assert [s.replace("SUSPEND", "RESUME") for s in down] == up
    assert conn.c.executed == up + down


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


def test_verify_connects_as_the_loader_with_the_loader_key(monkeypatch, tmp_path):
    """The loader user must sign in with the loader's own key, not the admin key."""
    import ops.connection
    from ops import verify_bronze

    seen = {}

    class Stop(Exception):
        pass

    def fake_connect(**kwargs):
        seen.update(kwargs)
        raise Stop

    monkeypatch.setattr(ops.connection, "connect", fake_connect)
    manifest = tmp_path / "manifest.json"
    manifest.write_text("{}")
    try:
        verify_bronze.main(["--manifest", str(manifest)])
    except Stop:
        pass
    assert seen["key_env"] == "SNOWFLAKE_LOADER_PRIVATE_KEY_PATH"
    assert seen["user_env"] == "SNOWFLAKE_LOADER_USER"
    assert seen["default_user"] == "LOADER_SERVICE_USER"


# --- silver operations --------------------------------------------------------------------


def test_reset_silver_never_touches_bronze_tables():
    from ops.reset_silver import statements

    todo = statements("DB")
    assert all(" IF EXISTS " in s for s in todo)
    dropped = [s for s in todo if s.startswith("DROP TABLE")]
    assert dropped and not any(".BRONZE." in s for s in dropped)
    assert any(s.startswith("DROP STREAM") and ".BRONZE.ref_merchants_stream" in s for s in todo)


def test_reset_silver_prints_by_default(capsys):
    from ops.reset_silver import main

    assert main([]) == 0
    assert "Dry run" in capsys.readouterr().out


class _SilverCursor:
    """Answers verify_silver's queries from a dict, keyed by a fragment of the SQL."""

    def __init__(self, counts, mode, ledger):
        self.counts, self.mode, self.ledger = counts, mode, ledger

    def execute(self, sql):
        self.sql = sql
        self.description = None

    @property
    def description(self):
        return self._description

    @description.setter
    def description(self, _):
        sql = self.sql
        if sql.startswith("SELECT COUNT(*)"):
            self._description, self._rows = (
                [("n",)],
                [(self.counts[sql.split(" FROM ")[1].split(".", 1)[1]],)],
            )
        elif sql.startswith("SHOW DYNAMIC"):
            from silver.spec import DYNAMIC_TABLES

            self._description = [("name",), ("refresh_mode",)]
            self._rows = [(t.split(".")[-1].upper(), self.mode) for t in DYNAMIC_TABLES]
        elif sql.startswith("SHOW TASKS"):
            from silver.spec import MERGE_TASKS

            self._description = [("name",), ("state",)]
            self._rows = [(t.split(".")[-1], "suspended") for t in MERGE_TASKS]
        else:
            self._description, self._rows = None, self.ledger

    def fetchall(self):
        return self._rows

    def close(self):
        pass


def _silver_conn(counts, mode="INCREMENTAL", ledger=()):
    class C:
        def cursor(self):
            return _SilverCursor(counts, mode, list(ledger))

    return C()


def test_verify_silver_passes_when_everything_agrees_and_flags_full_refresh():
    from ops.verify_silver import check
    from silver.spec import SILVER_TABLES

    counts = {t: 3 for t in SILVER_TABLES}
    ledger = [("m", "a", "EUR", "2026-01-01", "SALE", 100, "t1")]
    oracle = [
        dict(merchant_id="m", account_id="a", currency="EUR", posting_date="2026-01-01",
             sales_amount=100, refund_amount=0, dispute_amount=0, platform_fee_amount=0,
             pass_through_fee_amount=0, adjustment_amount=0, disbursed_total=100)
    ]  # fmt: skip
    good = check(_silver_conn(counts, ledger=ledger), "DB", counts, oracle)
    assert all(r["ok"] for r in good)
    full = check(_silver_conn(counts, mode="FULL", ledger=ledger), "DB", counts, oracle)
    assert [r["name"] for r in full if not r["ok"]] and all(
        "refresh mode" in r["name"] for r in full if not r["ok"]
    )
    off = check(_silver_conn(counts, ledger=[]), "DB", counts, oracle)
    assert [r["name"] for r in off if not r["ok"]] == ["ledger from silver equals the oracle"]


def test_verify_gold_flags_a_single_wrong_amount():
    from generator.oracle import COLUMNS
    from ops.verify_gold import check

    row = ("m", "a", "EUR", "2026-01-01", 100, 0, 0, 5, 0, 0, 95)
    oracle = [dict(zip(COLUMNS, row, strict=True))]

    class C:
        def __init__(self, daily, total, monthly, ledger):
            self.answers = [daily, [(total,)], [(monthly,)], [(ledger,)]]

        def cursor(self):
            outer = self

            class Cur:
                def execute(self, sql):
                    pass

                def fetchall(self):
                    return outer.answers.pop(0)

                def close(self):
                    pass

            return Cur()

    assert all(r["ok"] for r in check(C([row], 95, 95, 95), "DB", oracle))
    wrong = row[:-1] + (96,)
    bad = check(C([wrong], 96, 96, 96), "DB", oracle)
    assert [r["name"] for r in bad if not r["ok"]] == ["daily rows equal the oracle, every amount"]
    mismatch = check(C([row], 95, 90, 95), "DB", oracle)
    assert [r["name"] for r in mismatch if not r["ok"]] == ["monthly total equals daily total"]


def test_refresh_silver_runs_each_merge_then_refreshes_every_dynamic_table():
    from ops.refresh_silver import statements
    from silver.spec import DYNAMIC_TABLES

    todo = statements("DB")
    assert todo[:2] == ["CALL DB.OPS.MERGE_MERCHANTS()", "CALL DB.OPS.MERGE_ACCOUNTS()"]
    assert len(todo) == 2 + len(DYNAMIC_TABLES) == 17
    assert all(s.startswith("ALTER DYNAMIC TABLE DB.") and s.endswith(" REFRESH") for s in todo[2:])
