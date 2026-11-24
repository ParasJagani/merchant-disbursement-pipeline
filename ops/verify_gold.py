"""Phase 4 gate: gold equals the generator's oracle to the cent.

This is an independent check of what `dbt build` already tests: it reads gold straight from
Snowflake and compares it with the oracle CSV, and checks that the monthly view adds up.

    python -m ops.verify_gold
"""

from __future__ import annotations

import argparse
import os
from pathlib import Path

from generator.oracle import COLUMNS
from silver.check import differences, oracle_from_csv


def _rows(connection, sql: str) -> list[tuple]:
    cursor = connection.cursor()
    try:
        cursor.execute(sql)
        return cursor.fetchall()
    finally:
        cursor.close()


def check(connection, database: str, oracle: list[dict]) -> list[dict]:
    daily = _rows(
        connection,
        f"SELECT {', '.join(COLUMNS)} FROM {database}.GOLD.gold_daily_disbursement",
    )
    gold = [
        {c: (str(v) if c in COLUMNS[:4] else int(v)) for c, v in zip(COLUMNS, row, strict=True)}
        for row in daily
    ]
    off = differences(gold, oracle)
    results = [
        {"name": "daily rows equal the oracle, every amount", "expected": len(oracle),
         "actual": len(gold), "ok": not off, "detail": f"{len(off)} differing rows"}
    ]  # fmt: skip

    gold_total = int(
        _rows(
            connection, f"SELECT SUM(disbursed_total) FROM {database}.GOLD.gold_daily_disbursement"
        )[0][0]
    )
    monthly_total = int(
        _rows(
            connection,
            f"SELECT SUM(disbursed_total) FROM {database}.GOLD.gold_monthly_disbursement",
        )[0][0]
    )
    results.append(
        {"name": "monthly total equals daily total", "expected": gold_total,
         "actual": monthly_total, "ok": gold_total == monthly_total}
    )  # fmt: skip

    ledger_total = int(
        _rows(connection, f"SELECT SUM(amount_signed) FROM {database}.GOLD.gold_ledger")[0][0]
    )
    results.append(
        {"name": "ledger total equals daily total", "expected": gold_total,
         "actual": ledger_total, "ok": gold_total == ledger_total}
    )  # fmt: skip
    return results


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(prog="python -m ops.verify_gold")
    parser.add_argument("--golden", type=Path, default=Path("data/golden"))
    parser.add_argument(
        "--database", default=os.environ.get("SNOWFLAKE_DATABASE", "MERCHANT_DISBURSEMENT")
    )
    args = parser.parse_args(argv)
    from .connection import connect

    oracle = oracle_from_csv(args.golden / "oracle" / "daily_disbursement.csv")
    conn = connect(warehouse=os.environ.get("SNOWFLAKE_TRANSFORM_WAREHOUSE", "TRANSFORM_WH"))
    try:
        results = check(conn, args.database, oracle)
    finally:
        conn.close()
    for r in results:
        extra = f"  {r['detail']}" if r.get("detail") and not r["ok"] else ""
        print(
            f"{'PASS' if r['ok'] else 'FAIL'}  {r['name']:<46} "
            f"expected {r['expected']!s:>12}  actual {r['actual']!s:>12}{extra}"
        )
    return 0 if all(r["ok"] for r in results) else 1


if __name__ == "__main__":
    raise SystemExit(main())
