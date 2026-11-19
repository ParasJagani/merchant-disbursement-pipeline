"""Compare bronze row counts with what the generator's manifest says should have loaded."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

from file_writer.naming import topic_path
from generator.config import TOPICS


def check(connection, manifest: dict, database: str) -> list[dict]:
    expected = manifest["valid_messages_by_topic"]
    cursor = connection.cursor()
    results = []
    try:
        for topic in TOPICS:
            table = f"{database}.BRONZE.{topic_path(topic)}"
            cursor.execute(f"SELECT COUNT(*), COUNT(DISTINCT source_file) FROM {table}")
            rows, files = cursor.fetchone()
            results.append(
                {
                    "topic": topic,
                    "expected": expected[topic],
                    "actual": int(rows),
                    "files": int(files),
                    "ok": int(rows) == expected[topic],
                }
            )
    finally:
        cursor.close()
    return results


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(prog="python -m ops.verify_bronze")
    parser.add_argument("--manifest", type=Path, default=Path("data/golden/manifest.json"))
    parser.add_argument(
        "--database", default=os.environ.get("SNOWFLAKE_DATABASE", "MERCHANT_DISBURSEMENT")
    )
    args = parser.parse_args(argv)
    from .connection import connect

    conn = connect(
        role_env="SNOWFLAKE_LOADER_ROLE", default_role="LOADER", user_env="SNOWFLAKE_LOADER_USER"
    )
    try:
        results = check(conn, json.loads(args.manifest.read_text()), args.database)
    finally:
        conn.close()
    for r in results:
        print(
            f"{'PASS' if r['ok'] else 'FAIL'}  {r['topic']:<24} expected {r['expected']:>7}  "
            f"actual {r['actual']:>7}  files {r['files']}"
        )
    return 0 if all(r["ok"] for r in results) else 1


if __name__ == "__main__":
    raise SystemExit(main())
