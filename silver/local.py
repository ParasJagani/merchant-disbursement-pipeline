"""Run the silver SQL on DuckDB, so the logic can be tested without a Snowflake account.

This is a test harness, not a second implementation: it reads the same generated files that are
deployed to Snowflake (``sql/30_silver_reference.sql`` and ``sql/40_silver_events.sql``) and
translates the handful of Snowflake-only constructs on the fly.

What is emulated, and how:

* dynamic tables           -> views (they are always fully refreshed here)
* streams                  -> a table refilled with the bronze rows not yet consumed
* stored procedures        -> the statements between BEGIN TRANSACTION and COMMIT, run in one
                              transaction; the stream offset advances only if they all succeed
* tasks                    -> ignored; ``run_reference()`` plays the task
* GET_PATH / TYPEOF / TRY_TO_TIMESTAMP_NTZ / IFF -> DuckDB JSON functions and macros

What is NOT proven here: that Snowflake accepts each statement, that dynamic tables refresh
incrementally, and cost. Those are checked on Snowflake by ``ops/verify_silver.py``.
"""

from __future__ import annotations

import re
from datetime import datetime, timedelta, timezone
from pathlib import Path

import duckdb

from file_writer.naming import topic_path
from generator.config import TOPICS
from generator.schema import validate_message
from ops.deploy import SQL_DIR, split_statements

from .check import LEDGER_SQL, oracle_from_rows
from .spec import EVENT_TOPICS, REFERENCE_TABLES

SILVER_FILES = ("30_silver_reference.sql", "40_silver_events.sql")
SCHEMAS = ("BRONZE", "SILVER", "QUARANTINE", "OPS")
BRONZE_DDL = (
    "CREATE TABLE IF NOT EXISTS BRONZE.{name} "
    "(raw JSON, source_file VARCHAR, source_row BIGINT, loaded_at TIMESTAMP)"
)
MACROS = (
    "CREATE OR REPLACE MACRO iff(c, a, b) AS CASE WHEN c THEN a ELSE b END",
    # Snowflake's format string is ignored; the generated SQL always uses the same ISO format.
    "CREATE OR REPLACE MACRO try_to_timestamp_ntz(x, fmt) AS "
    "TRY_STRPTIME(x, '%Y-%m-%dT%H:%M:%S.%gZ')",
)

_INT = re.compile(
    r"IFF\(TYPEOF\(GET_PATH\(raw, '([^']+)'\)\) = 'INTEGER', GET_PATH\(raw, '\1'\)::NUMBER, NULL\)"
)
_STR = re.compile(r"GET_PATH\(raw, '([^']+)'\)::STRING")
_DYNAMIC = re.compile(r"CREATE DYNAMIC TABLE (?:IF NOT EXISTS )?(\S+)\s.*?\nAS\n(.*)", re.DOTALL)
_STREAM = re.compile(r"CREATE STREAM IF NOT EXISTS (\S+) ON TABLE (\S+)")
_PROC = re.compile(r"CREATE OR REPLACE PROCEDURE (\S+?)\(\)")
_BODY = re.compile(r"\$\$\s*BEGIN(.*?)\n\s*EXCEPTION", re.DOTALL)
_CONTROL = re.compile(r"^\s*(BEGIN TRANSACTION|COMMIT|RETURN\b.*)\s*$", re.IGNORECASE)


def translate(statement: str) -> str:
    """Rewrite Snowflake SQL for DuckDB. Only the constructs the silver files use."""
    sql = statement.replace("{db}.", "")
    sql = _INT.sub(
        lambda m: (
            f"CASE WHEN json_type(raw, '$.{m[1]}') IN ('BIGINT', 'UBIGINT') "
            f"THEN json_extract(raw, '$.{m[1]}')::BIGINT END"
        ),
        sql,
    )
    sql = _STR.sub(lambda m: f"json_extract_string(raw, '$.{m[1]}')", sql)
    sql = sql.replace("CHANGE_TRACKING = TRUE", "")
    sql = re.sub(r"\bNUMBER\b", "BIGINT", sql)
    sql = re.sub(r"\bTIMESTAMP_(NTZ|LTZ)\b", "TIMESTAMP", sql)
    sql = re.sub(r"\bVARIANT\b", "JSON", sql)
    sql = re.sub(r"\bSTRING\b", "VARCHAR", sql)
    sql = sql.replace("CURRENT_TIMESTAMP()", "CURRENT_TIMESTAMP")
    return sql


class LocalSilver:
    """An in-memory stand-in for the BRONZE/SILVER/QUARANTINE schemas."""

    def __init__(self, path: str = ":memory:") -> None:
        self.con = duckdb.connect(path)
        for schema in SCHEMAS:
            self.con.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
        for macro in MACROS:
            self.con.execute(macro)
        for table in (*REFERENCE_TABLES, *EVENT_TOPICS):
            self.con.execute(BRONZE_DDL.format(name=table.bronze))
        self.procedures: dict[str, list[str]] = {}
        self.streams: dict[str, str] = {}  # stream table -> bronze table
        self.consumed: dict[str, int] = {}  # stream table -> bronze rows already read
        self._clock = datetime(2026, 2, 1, tzinfo=timezone.utc).replace(tzinfo=None)

    # -- deploy ---------------------------------------------------------------------------------
    def deploy(self) -> int:
        count = 0
        for name in SILVER_FILES:
            for statement in split_statements((SQL_DIR / name).read_text()):
                count += self._apply(statement)
        return count

    def _apply(self, statement: str) -> int:
        head = statement.lstrip().upper()
        if head.startswith("CREATE OR REPLACE TASK"):
            return 1
        if head.startswith("CREATE OR REPLACE PROCEDURE"):
            name = _PROC.search(statement)[1].replace("{db}.", "").upper()
            body = _BODY.search(statement)[1]
            lines = [s for s in split_statements(body) if not _CONTROL.match(s)]
            self.procedures[name] = [translate(s) for s in lines]
            return 1
        stream = _STREAM.search(statement)
        if stream:
            table = stream[1].replace("{db}.", "")
            source = stream[2].replace("{db}.", "")
            self.con.execute(
                f"CREATE TABLE IF NOT EXISTS {table} AS SELECT * FROM {source} WHERE false"
            )
            self.streams[table.upper()] = source.upper()
            self.consumed.setdefault(table.upper(), 0)
            return 1
        dynamic = _DYNAMIC.search(statement)
        if dynamic:
            sql = translate(dynamic[2])
            self.con.execute(f"CREATE OR REPLACE VIEW {dynamic[1].replace('{db}.', '')} AS {sql}")
            return 1
        self.con.execute(translate(statement))
        return 1

    # -- bronze ---------------------------------------------------------------------------------
    def load_bronze(self, table: str, rows: list[tuple[str, str, int]]) -> None:
        """Append (raw_json_text, source_file, source_row) rows, like one COPY INTO batch."""
        self._clock += timedelta(minutes=1)
        for start in range(0, len(rows), 2000):
            chunk = rows[start : start + 2000]
            marks = ", ".join(["(?::JSON, ?, ?, ?)"] * len(chunk))
            args = [v for raw, f, r in chunk for v in (raw, f, r, self._clock)]
            self.con.execute(f"INSERT INTO BRONZE.{table} VALUES {marks}", args)

    def load_golden(self, golden_dir: Path) -> int:
        """Load a golden dataset the way the file writer would: malformed messages stay out."""
        loaded = 0
        for topic_dir in sorted(p for p in Path(golden_dir).iterdir() if p.is_dir()):
            topic = topic_dir.name
            if topic not in TOPICS:
                continue
            table = topic_path(topic)
            for path in sorted(topic_dir.glob("*.ndjson")):
                rows = []
                for line in path.read_text().splitlines():
                    if line and validate_message(topic, line)[0]:
                        rows.append((line, f"{table}/{path.name}", len(rows) + 1))
                self.load_bronze(table, rows)
                loaded += len(rows)
        return loaded

    def ledger(self) -> list[dict]:
        """The daily totals derived from silver alone (see silver/check.py)."""
        return oracle_from_rows(self.query(translate(LEDGER_SQL)))

    # -- the reference track's task ---------------------------------------------------------------
    def run_reference(self) -> dict[str, str]:
        """Run each MERGE procedure once, the way its task would when the stream has data."""
        results = {}
        for stream, source in self.streams.items():
            proc = "OPS.MERGE_" + source.split(".")[-1].removeprefix("REF_") + ""
            results[proc] = self._call(proc, stream, source)
        return results

    def _call(self, proc: str, stream: str, source: str) -> str:
        offset = self.consumed[stream]
        total = self.con.execute(f"SELECT count(*) FROM {source}").fetchone()[0]
        if total == offset:  # SYSTEM$STREAM_HAS_DATA is false: the task does not run
            return "skipped"
        self.con.execute(f"DELETE FROM {stream}")
        self.con.execute(
            f"INSERT INTO {stream} SELECT * FROM {source} ORDER BY rowid OFFSET {offset}"
        )
        self.con.execute("BEGIN TRANSACTION")
        try:
            for statement in self.procedures[proc]:
                self.con.execute(statement)
            self.con.execute("COMMIT")
        except Exception:
            self.con.execute("ROLLBACK")
            raise
        self.consumed[stream] = total  # the offset moves only after the commit
        return "ok"

    def close(self) -> None:
        self.con.close()

    # -- queries --------------------------------------------------------------------------------
    def query(self, sql: str, params: list | None = None) -> list[tuple]:
        return self.con.execute(sql, params or []).fetchall()


def main(argv=None) -> int:
    """Write a DuckDB file holding bronze and silver for a golden dataset (dbt reads it locally)."""
    import argparse

    parser = argparse.ArgumentParser(prog="python -m silver.local")
    parser.add_argument("--golden", type=Path, default=Path("data/golden"))
    parser.add_argument("--out", type=Path, default=Path("dbt/target/local.duckdb"))
    args = parser.parse_args(argv)
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.unlink(missing_ok=True)
    local = LocalSilver(str(args.out))
    local.deploy()
    rows = local.load_golden(args.golden)
    local.run_reference()
    local.close()
    print(f"loaded {rows} messages and built silver in {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
