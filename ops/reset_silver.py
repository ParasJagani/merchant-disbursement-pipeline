"""Drop every silver object so the next deploy rebuilds it from bronze.

Use this after changing a silver rule: the deploy keeps existing dynamic tables (it never rebuilds
them by accident), so a changed definition needs a reset first. Bronze is never touched.

    python -m ops.reset_silver          # print what would be dropped
    python -m ops.reset_silver --yes    # drop it
"""

from __future__ import annotations

import argparse
import os

from silver.spec import DYNAMIC_TABLES, MERGE_TASKS, REFERENCE_TABLES


def statements(database: str) -> list[str]:
    out = [f"DROP TASK IF EXISTS {database}.{t}" for t in MERGE_TASKS]
    out += [f"DROP DYNAMIC TABLE IF EXISTS {database}.{t}" for t in DYNAMIC_TABLES]
    for t in REFERENCE_TABLES:
        out += [
            f"DROP VIEW IF EXISTS {database}.SILVER.silver_{t.name}_history",
            f"DROP TABLE IF EXISTS {database}.SILVER.silver_{t.name}_versions",
            f"DROP TABLE IF EXISTS {database}.SILVER.silver_{t.name}",
            f"DROP TABLE IF EXISTS {database}.QUARANTINE.rejected_{t.name}",
            f"DROP STREAM IF EXISTS {database}.BRONZE.{t.bronze}_stream",
            f"DROP PROCEDURE IF EXISTS {database}.OPS.MERGE_{t.name.upper()}()",
        ]
    return out


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(prog="python -m ops.reset_silver")
    parser.add_argument("--yes", action="store_true", help="really drop (default is to print)")
    parser.add_argument(
        "--database", default=os.environ.get("SNOWFLAKE_DATABASE", "MERCHANT_DISBURSEMENT")
    )
    args = parser.parse_args(argv)
    todo = statements(args.database)
    if not args.yes:
        print("\n".join(todo))
        print("\nDry run. Add --yes to drop these. Bronze is not touched.")
        return 0
    from .connection import connect

    conn = connect()
    try:
        cursor = conn.cursor()
        for statement in todo:
            cursor.execute(statement)
        cursor.close()
    finally:
        conn.close()
    print(f"Dropped {len(todo)} objects. Run `make deploy` to rebuild silver from bronze.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
