"""Apply the versioned SQL files in sql/ in order. Every file is safe to run repeatedly."""

from __future__ import annotations

import argparse
import os
from pathlib import Path

SQL_DIR = Path(__file__).resolve().parents[1] / "sql"


def split_statements(text: str) -> list[str]:
    """Split on semicolons, except inside $$ ... $$ blocks. Full-line comments are dropped."""
    lines = [ln for ln in text.splitlines() if not ln.lstrip().startswith("--")]
    statements, current, in_dollar = [], [], False
    for line in lines:
        if line.count("$$") % 2 == 1:
            in_dollar = not in_dollar
        current.append(line)
        if not in_dollar and line.rstrip().endswith(";"):
            statements.append("\n".join(current).strip().rstrip(";").rstrip())
            current = []
    tail = "\n".join(current).strip()
    if tail:
        statements.append(tail)
    return [s for s in statements if s]


def sql_files(sql_dir: Path = SQL_DIR) -> list[Path]:
    # 00_* is the one-time manual bootstrap and is never run by the deploy.
    return [p for p in sorted(sql_dir.glob("*.sql")) if not p.name.startswith("00_")]


def render(path: Path, database: str) -> list[str]:
    return split_statements(path.read_text().replace("{db}", database))


def deploy(connection, database: str, dry_run: bool = False, sql_dir: Path = SQL_DIR) -> int:
    count = 0
    cursor = None if dry_run else connection.cursor()
    try:
        for path in sql_files(sql_dir):
            for statement in render(path, database):
                count += 1
                if dry_run:
                    print(f"-- {path.name}\n{statement};\n")
                else:
                    cursor.execute(statement)
    finally:
        if cursor is not None:
            cursor.close()
    return count


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(prog="python -m ops.deploy")
    parser.add_argument(
        "--database", default=os.environ.get("SNOWFLAKE_DATABASE", "MERCHANT_DISBURSEMENT")
    )
    parser.add_argument(
        "--dry-run", action="store_true", help="print the statements, change nothing"
    )
    args = parser.parse_args(argv)
    if args.dry_run:
        n = deploy(None, args.database, dry_run=True)
    else:
        from .connection import connect

        conn = connect()
        try:
            n = deploy(conn, args.database)
        finally:
            conn.close()
    print(f"{n} statements")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
