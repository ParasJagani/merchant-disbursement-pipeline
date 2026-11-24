"""Phase 3 gate: the silver tables on Snowflake agree with the golden dataset.

The expected row counts come from running the SAME generated SQL on DuckDB over the golden
files (``silver.local``), so a mismatch means Snowflake behaves differently from the tested logic.
The ledger built from the silver tables must equal the generator's oracle to the cent.
Dynamic tables must refresh incrementally, not by recomputing everything.

    python -m ops.verify_silver
"""

from __future__ import annotations

import argparse
import os
from pathlib import Path

from silver.check import LEDGER_SQL, differences, oracle_from_csv, oracle_from_rows
from silver.spec import DYNAMIC_TABLES, MERGE_TASKS, SILVER_TABLES


def expected_counts(golden_dir: Path) -> dict[str, int]:
    """Row counts per silver table, from the tested logic run over the golden files."""
    from silver.local import LocalSilver

    local = LocalSilver()
    local.deploy()
    local.load_golden(golden_dir)
    local.run_reference()
    return {t: local.query(f"SELECT COUNT(*) FROM {t}")[0][0] for t in SILVER_TABLES}


def _rows(connection, sql: str) -> list[dict]:
    cursor = connection.cursor()
    try:
        cursor.execute(sql)
        names = [d[0].lower() for d in cursor.description]
        return [dict(zip(names, row, strict=True)) for row in cursor.fetchall()]
    finally:
        cursor.close()


def check(connection, database: str, expected: dict[str, int], oracle: list[dict]) -> list[dict]:
    results = []

    for table in SILVER_TABLES:
        actual = int(_rows(connection, f"SELECT COUNT(*) AS n FROM {database}.{table}")[0]["n"])
        results.append(
            {"name": f"rows in {table}", "expected": expected[table], "actual": actual,
             "ok": actual == expected[table]}
        )  # fmt: skip

    cursor = connection.cursor()
    try:
        cursor.execute(LEDGER_SQL.replace("{db}", database))
        ledger = oracle_from_rows(cursor.fetchall())
    finally:
        cursor.close()
    off = differences(ledger, oracle)
    results.append(
        {"name": "ledger from silver equals the oracle", "expected": len(oracle),
         "actual": len(ledger), "ok": not off, "detail": f"{len(off)} differing rows"}
    )  # fmt: skip

    modes = {
        r["name"].upper(): r["refresh_mode"]
        for r in _rows(connection, f"SHOW DYNAMIC TABLES IN DATABASE {database}")
    }
    for table in DYNAMIC_TABLES:
        name = table.split(".")[-1].upper()
        mode = modes.get(name)
        results.append(
            {"name": f"{table} refresh mode", "expected": "INCREMENTAL", "actual": mode,
             "ok": mode == "INCREMENTAL"}
        )  # fmt: skip

    tasks = {
        r["name"].upper(): r["state"].lower()
        for r in _rows(connection, f"SHOW TASKS IN SCHEMA {database}.OPS")
    }
    for task in MERGE_TASKS:
        name = task.split(".")[-1].upper()
        results.append(
            {"name": f"{task} exists", "expected": "present", "actual": tasks.get(name, "missing"),
             "ok": name in tasks}
        )  # fmt: skip
    return results


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(prog="python -m ops.verify_silver")
    parser.add_argument("--golden", type=Path, default=Path("data/golden"))
    parser.add_argument(
        "--database", default=os.environ.get("SNOWFLAKE_DATABASE", "MERCHANT_DISBURSEMENT")
    )
    args = parser.parse_args(argv)
    from .connection import connect

    print("Running the silver logic locally on the golden files for the expected counts...")
    expected = expected_counts(args.golden)
    oracle = oracle_from_csv(args.golden / "oracle" / "daily_disbursement.csv")
    conn = connect()
    try:
        results = check(conn, args.database, expected, oracle)
    finally:
        conn.close()
    for r in results:
        extra = f"  {r['detail']}" if r.get("detail") and not r["ok"] else ""
        print(
            f"{'PASS' if r['ok'] else 'FAIL'}  {r['name']:<58} "
            f"expected {r['expected']!s:>10}  actual {r['actual']!s:>10}{extra}"
        )
    return 0 if all(r["ok"] for r in results) else 1


if __name__ == "__main__":
    raise SystemExit(main())
