"""The chaos drill: inject every kind of bad message into real bronze tables, then prove that
silver quarantines each one with the expected reason, that no good data changed, and that the
drill leaves nothing behind.

    python -m ops.chaos_drill            # plan only
    python -m ops.chaos_drill --run      # inject, refresh, check, clean up

Only messages that silver must REJECT are injected (see silver/faults.py), so the drill cannot
add business data. The injected rows carry a source file named ``chaos/<drill id>.ndjson`` and are
deleted at the end, even when a check fails.
"""

from __future__ import annotations

import argparse
import os
import time
from collections import Counter
from pathlib import Path

from silver.check import LEDGER_SQL, differences, oracle_from_csv, oracle_from_rows
from silver.faults import TABLES, Fault, catalog
from silver.spec import REFERENCE_TABLES, SILVER_TABLES

from . import refresh_silver


def injectable() -> list[Fault]:
    """Faults that reach bronze and must be rejected: they cannot change any business table."""
    return [f for f in catalog() if f.loadable and f.silver_reason is not None]


def expected_rejects(faults: list[Fault]) -> Counter:
    return Counter((f.topic, f.silver_reason) for f in faults)


def insert_statements(database: str, drill_id: str, faults: list[Fault]) -> list[tuple[str, list]]:
    by_table: dict[str, list[Fault]] = {}
    for f in faults:
        by_table.setdefault(TABLES[f.topic].bronze, []).append(f)
    statements = []
    for table, items in by_table.items():
        values = ", ".join(["(%s, %s, %s)"] * len(items))
        params: list = []
        for n, f in enumerate(items, start=1):
            params += [f.raw, f"chaos/{drill_id}.ndjson", n]
        statements.append(
            (
                f"INSERT INTO {database}.BRONZE.{table} (raw, source_file, source_row) "
                f"SELECT PARSE_JSON(column1), column2, column3 FROM VALUES {values}",
                params,
            )
        )
    return statements


def cleanup_statements(database: str, drill_id: str) -> list[str]:
    marker = f"chaos/{drill_id}.ndjson"
    out = [
        f"DELETE FROM {database}.BRONZE.{TABLES[t].bronze} WHERE source_file = '{marker}'"
        for t in TABLES
    ]
    # The reference track copies rejects into plain tables; the event ones are dynamic tables.
    out += [
        f"DELETE FROM {database}.QUARANTINE.rejected_{t.name} WHERE source_file = '{marker}'"
        for t in REFERENCE_TABLES
    ]
    return out


def refresh_statements(database: str) -> list[str]:
    return refresh_silver.statements(database)


def _all(cursor, sql, params=None):
    cursor.execute(sql, params) if params else cursor.execute(sql)
    return cursor.fetchall()


def snapshot(cursor, database: str) -> dict[str, int]:
    return {
        t: int(_all(cursor, f"SELECT COUNT(*) FROM {database}.{t}")[0][0]) for t in SILVER_TABLES
    }


def ledger_differences(cursor, database: str, oracle: list[dict]) -> int:
    rows = _all(cursor, LEDGER_SQL.replace("{db}", database))
    return len(differences(oracle_from_rows(rows), oracle))


def unaccounted(cursor, database: str) -> int:
    rows = _all(
        cursor,
        f"SELECT SUM(ABS(unaccounted_rows)) + SUM(ABS(missing_entities)) "
        f"FROM {database}.OPS.silver_reconciliation",
    )
    return int(rows[0][0] or 0)


def rejects_by_reason(cursor, database: str, drill_id: str) -> Counter:
    marker = f"chaos/{drill_id}.ndjson"
    found: Counter = Counter()
    for t in TABLES.values():
        rows = _all(
            cursor,
            f"SELECT reject_reason, COUNT(*) FROM {database}.QUARANTINE.rejected_{t.name} "
            f"WHERE source_file = '{marker}' GROUP BY reject_reason",
        )
        for reason, n in rows:
            found[(t.topic, reason)] += int(n)
    return found


def run(connection, database: str, oracle: list[dict], drill_id: str | None = None) -> list[dict]:
    drill_id = drill_id or time.strftime("%Y%m%d-%H%M%S")
    faults = injectable()
    expected = expected_rejects(faults)
    cursor = connection.cursor()
    results: list[dict] = []

    def record(name, want, actual, ok=None):
        results.append(
            {
                "name": name,
                "expected": want,
                "actual": actual,
                "ok": actual == want if ok is None else ok,
            }
        )

    try:
        before = snapshot(cursor, database)
        record(
            "before: silver ledger equals the oracle",
            0,
            ledger_differences(cursor, database, oracle),
        )
        record("before: nothing unaccounted", 0, unaccounted(cursor, database))
        if not all(r["ok"] for r in results):
            return results  # never inject into a pipeline that is not clean to begin with
        try:
            for sql, params in insert_statements(database, drill_id, faults):
                cursor.execute(sql, params)
            for sql in refresh_statements(database):
                cursor.execute(sql)
            found = rejects_by_reason(cursor, database, drill_id)
            record(
                "every injected message quarantined", sum(expected.values()), sum(found.values())
            )
            wrong = sorted(
                (k, expected[k], found[k])
                for k in expected.keys() | found.keys()
                if expected[k] != found[k]
            )
            record("each one has the expected reason", 0, len(wrong))
            during = snapshot(cursor, database)
            changed = [
                t for t in SILVER_TABLES if t.startswith("SILVER.") and before[t] != during[t]
            ]
            record("no silver table changed", 0, len(changed))
            record("during: silver ledger still equals the oracle", 0,
                   ledger_differences(cursor, database, oracle))  # fmt: skip
            record("during: nothing unaccounted", 0, unaccounted(cursor, database))
        finally:
            for sql in cleanup_statements(database, drill_id):
                cursor.execute(sql)
            for sql in refresh_statements(database):
                cursor.execute(sql)
        after = snapshot(cursor, database)
        record("cleanup restored every table count", 0,
               sum(1 for t in SILVER_TABLES if before[t] != after[t]))  # fmt: skip
        record("after: nothing unaccounted", 0, unaccounted(cursor, database))
    finally:
        cursor.close()
    return results


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(prog="python -m ops.chaos_drill")
    parser.add_argument("--run", action="store_true", help="really inject (default is a plan)")
    parser.add_argument("--golden", type=Path, default=Path("data/golden"))
    parser.add_argument(
        "--database", default=os.environ.get("SNOWFLAKE_DATABASE", "MERCHANT_DISBURSEMENT")
    )
    args = parser.parse_args(argv)
    faults = injectable()
    expected = expected_rejects(faults)
    if not args.run:
        print(f"Would inject {len(faults)} bad messages across {len(expected)} topic/reason pairs.")
        for (topic, reason), n in sorted(expected.items()):
            print(f"  {topic:<24} {reason:<32} {n}")
        print("\nAdd --run to inject them, check the result and clean up.")
        return 0
    from .connection import connect

    oracle = oracle_from_csv(args.golden / "oracle" / "daily_disbursement.csv")
    conn = connect(warehouse=os.environ.get("SNOWFLAKE_TRANSFORM_WAREHOUSE", "TRANSFORM_WH"))
    try:
        results = run(conn, args.database, oracle)
    finally:
        conn.close()
    for r in results:
        tag = "PASS" if r["ok"] else "FAIL"
        print(f"{tag}  {r['name']:<52} expected {r['expected']!s:>6}  actual {r['actual']!s:>6}")
    return 0 if all(r["ok"] for r in results) else 1


if __name__ == "__main__":
    raise SystemExit(main())
