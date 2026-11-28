"""Load the staged files into bronze now instead of waiting for the five-minute task.

Calls the same procedure the task calls. Loading is idempotent: files already loaded are skipped.

    python -m ops.load_bronze
"""

from __future__ import annotations

import argparse
import os


def load(connection, database: str) -> str:
    cursor = connection.cursor()
    try:
        cursor.execute(f"CALL {database}.OPS.LOAD_BRONZE()")
        row = cursor.fetchone()
        return str(row[0]) if row else ""
    finally:
        cursor.close()


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(prog="python -m ops.load_bronze")
    parser.add_argument(
        "--database", default=os.environ.get("SNOWFLAKE_DATABASE", "MERCHANT_DISBURSEMENT")
    )
    args = parser.parse_args(argv)
    from .connection import connect

    conn = connect(warehouse=os.environ.get("SNOWFLAKE_INGEST_WAREHOUSE", "INGEST_WH"))
    try:
        print(load(conn, args.database))
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
