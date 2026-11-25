"""Bring silver up to date now instead of waiting for the schedules.

Runs each reference MERGE once and refreshes every dynamic table. Resume them first with
`make demo-up` (a suspended dynamic table does not refresh). Then run `make demo-down`.

    python -m ops.refresh_silver
"""

from __future__ import annotations

import argparse
import os

from silver.spec import DYNAMIC_TABLES, REFERENCE_TABLES


def statements(database: str) -> list[str]:
    calls = [f"CALL {database}.OPS.MERGE_{t.name.upper()}()" for t in REFERENCE_TABLES]
    refresh = [f"ALTER DYNAMIC TABLE {database}.{t} REFRESH" for t in DYNAMIC_TABLES]
    return calls + refresh


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(prog="python -m ops.refresh_silver")
    parser.add_argument(
        "--database", default=os.environ.get("SNOWFLAKE_DATABASE", "MERCHANT_DISBURSEMENT")
    )
    args = parser.parse_args(argv)
    from .connection import connect

    conn = connect(warehouse=os.environ.get("SNOWFLAKE_TRANSFORM_WAREHOUSE", "TRANSFORM_WH"))
    try:
        cursor = conn.cursor()
        for sql in statements(args.database):
            cursor.execute(sql)
            print(sql)
        cursor.close()
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
