"""Phase 5 gate: nothing is lost silently.

Checks, on a live account, that every message is accounted for from the file in the stage to the
silver table, and that nothing failed quietly on the way:

* the reconciliation view: every bronze row is rejected, a duplicate, or a unique event, and
  every valid entity is in silver (``unaccounted_rows`` and ``missing_entities`` are 0)
* every staged file was loaded, and no load reported a failure (``ON_ERROR = SKIP_FILE`` would
  otherwise skip a whole file without a word)
* the dead letters on disk equal the messages the writer should have rejected (when present)
* recent task and dynamic-table failures are listed as warnings

    python -m ops.verify_quality
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

from file_writer.naming import topic_path
from generator.config import TOPICS

FAIL, WARN = "fail", "warn"


def _rows(connection, sql: str) -> list[dict]:
    cursor = connection.cursor()
    try:
        cursor.execute(sql)
        names = [d[0].lower() for d in cursor.description]
        return [dict(zip(names, row, strict=True)) for row in cursor.fetchall()]
    finally:
        cursor.close()


def norm(path: str) -> str:
    """'@db.bronze.landing/ref_merchants/a.ndjson.gz' and 'landing/ref_merchants/a.ndjson.gz'
    name the same file: keep the topic folder and the file name."""
    parts = [p for p in str(path).lower().replace("\\", "/").split("/") if p]
    return "/".join(parts[-2:])


def result(name, expected, actual, ok, severity=FAIL, detail=""):
    return {"name": name, "expected": expected, "actual": actual, "ok": ok,
            "severity": severity, "detail": detail}  # fmt: skip


def check_reconciliation(connection, database: str) -> list[dict]:
    out = []
    for r in _rows(
        connection, f"SELECT * FROM {database}.OPS.silver_reconciliation ORDER BY topic"
    ):
        for column in ("unaccounted_rows", "missing_entities"):
            out.append(result(f"{r['topic']}: {column}", 0, int(r[column]), int(r[column]) == 0))
    return out


def check_loads(connection, database: str, days: int = 14) -> list[dict]:
    """Staged files vs load history vs bronze, so a skipped file cannot go unnoticed."""
    out = []
    staged = {norm(r["name"]) for r in _rows(connection, f"LIST @{database}.BRONZE.LANDING")}
    loaded_ok, failed = set(), []
    for topic in TOPICS:
        table = topic_path(topic).upper()
        sql = (
            "SELECT file_name, status, first_error_message FROM TABLE("
            f"{database}.INFORMATION_SCHEMA.COPY_HISTORY("
            f"TABLE_NAME => '{database}.BRONZE.{table}', "
            f"START_TIME => DATEADD('day', -{days}, CURRENT_TIMESTAMP())))"
        )
        for r in _rows(connection, sql):
            if str(r["status"]).lower() == "loaded":
                loaded_ok.add(norm(r["file_name"]))
            else:
                failed.append((norm(r["file_name"]), r["status"], r["first_error_message"]))
        source_files = _rows(
            connection, f"SELECT DISTINCT source_file AS f FROM {database}.BRONZE.{table}"
        )
        loaded_ok |= {norm(r["f"]) for r in source_files}
    # A file that failed once but was reloaded fine later is not a problem.
    still_failed = [f for f in failed if f[0] not in loaded_ok]
    never = sorted(staged - loaded_ok)
    out.append(result("staged files never loaded", 0, len(never), not never,
                      detail=", ".join(never[:5])))  # fmt: skip
    out.append(result("load failures with no later success", 0, len(still_failed),
                      not still_failed, detail=str(still_failed[:3])))  # fmt: skip
    out.append(result("files in the stage", ">0", len(staged), bool(staged)))
    return out


def check_dead_letters(manifest: dict, dlq_dir: Path) -> list[dict]:
    if not dlq_dir.exists():
        return [result("dead letters on disk", "n/a", "no folder", True, WARN,
                       f"{dlq_dir} not found; skipped")]  # fmt: skip
    out = []
    for topic in TOPICS:
        should = (
            manifest["delivered_messages_by_topic"][topic]
            - manifest["valid_messages_by_topic"][topic]
        )
        path = dlq_dir / f"{topic}.ndjson"
        values = set()
        if path.exists():
            values = {json.loads(line)["value"] for line in path.read_text().splitlines() if line}
        # Staging twice appends the same dead letters again, so count distinct messages.
        out.append(result(f"{topic}: dead letters equal rejected deliveries", should, len(values),
                          len(values) == should))  # fmt: skip
    return out


def check_failures(connection, database: str, days: int = 1) -> list[dict]:
    out = []
    queries = {
        "task runs that failed": (
            "SELECT name, error_message AS message FROM TABLE("
            f"{database}.INFORMATION_SCHEMA.TASK_HISTORY(SCHEDULED_TIME_RANGE_START => "
            f"DATEADD('day', -{days}, CURRENT_TIMESTAMP()), RESULT_LIMIT => 1000)) "
            "WHERE state = 'FAILED'"
        ),
        "dynamic table refreshes that failed": (
            "SELECT name, state_message AS message FROM TABLE("
            f"{database}.INFORMATION_SCHEMA.DYNAMIC_TABLE_REFRESH_HISTORY("
            f"DATA_TIMESTAMP_START => DATEADD('day', -{days}, CURRENT_TIMESTAMP()))) "
            "WHERE state = 'FAILED'"
        ),
    }
    for label, sql in queries.items():
        try:
            rows = _rows(connection, sql)
            out.append(result(f"{label} (last {days} day)", 0, len(rows), not rows, WARN,
                              str([(r["name"], r["message"]) for r in rows[:3]])))  # fmt: skip
        except Exception as error:  # a monitoring query must not hide the other checks
            out.append(result(label, "readable", "error", False, WARN, str(error)[:120]))
    return out


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(prog="python -m ops.verify_quality")
    parser.add_argument("--manifest", type=Path, default=Path("data/golden/manifest.json"))
    parser.add_argument("--dlq", type=Path, default=Path("data/dlq"))
    parser.add_argument(
        "--database", default=os.environ.get("SNOWFLAKE_DATABASE", "MERCHANT_DISBURSEMENT")
    )
    args = parser.parse_args(argv)
    from .connection import connect

    manifest = json.loads(args.manifest.read_text())
    conn = connect(warehouse=os.environ.get("SNOWFLAKE_TRANSFORM_WAREHOUSE", "TRANSFORM_WH"))
    try:
        results = check_reconciliation(conn, args.database)
        results += check_loads(conn, args.database)
        results += check_dead_letters(manifest, args.dlq)
        results += check_failures(conn, args.database)
        summary = _rows(
            conn, f"SELECT * FROM {args.database}.QUARANTINE.reject_summary ORDER BY 1, 2"
        )
    finally:
        conn.close()
    for r in results:
        tag = "PASS" if r["ok"] else ("WARN" if r["severity"] == WARN else "FAIL")
        extra = f"  {r['detail']}" if r["detail"] and not r["ok"] else ""
        want, got = f"expected {r['expected']!s:>8}", f"actual {r['actual']!s:>8}"
        print(f"{tag}  {r['name']:<66} {want}  {got}{extra}")
    print(f"\nQuarantine summary ({len(summary)} topic/reason rows):")
    for row in summary:
        print(f"  {row['topic']:<24} {row['reject_reason']:<32} {row['messages']}")
    return 0 if all(r["ok"] or r["severity"] == WARN for r in results) else 1


if __name__ == "__main__":
    raise SystemExit(main())
