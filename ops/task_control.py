"""Demo mode: resume or suspend the tasks and the alert, so the meter only runs while you demo."""

from __future__ import annotations

import argparse
import os

from silver.spec import DYNAMIC_TABLES, MERGE_TASKS

TASKS = ["OPS.LOAD_BRONZE_TASK", *MERGE_TASKS]
ALERTS = ["OPS.HEALTH_ALERT"]


def set_state(connection, database: str, up: bool) -> list[str]:
    verb = "RESUME" if up else "SUSPEND"
    statements = [f"ALTER TASK {database}.{task} {verb}" for task in TASKS]
    statements += [f"ALTER ALERT {database}.{alert} {verb}" for alert in ALERTS]
    # Dynamic tables refresh on their own schedule and cost credits until suspended.
    statements += [f"ALTER DYNAMIC TABLE {database}.{table} {verb}" for table in DYNAMIC_TABLES]
    cursor = connection.cursor()
    try:
        for statement in statements:
            cursor.execute(statement)
    finally:
        cursor.close()
    return statements


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(prog="python -m ops.task_control")
    parser.add_argument("state", choices=["up", "down"])
    parser.add_argument(
        "--database", default=os.environ.get("SNOWFLAKE_DATABASE", "MERCHANT_DISBURSEMENT")
    )
    args = parser.parse_args(argv)
    from .connection import connect

    conn = connect()
    try:
        for s in set_state(conn, args.database, args.state == "up"):
            print(s)
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
