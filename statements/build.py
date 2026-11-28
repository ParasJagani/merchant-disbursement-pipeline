"""Build statement PDFs.

    python -m statements.build --merchant mch_001 --month 2026-01      # one statement
    python -m statements.build --all --out data/statements             # every merchant and month

Reads gold from the local DuckDB file by default; ``--source snowflake`` reads the real tables.
Run ``python -m statements.verify`` to prove the printed totals equal gold's.
"""

from __future__ import annotations

import argparse
from datetime import date
from pathlib import Path

from .model import build_statement
from .pdf import build_pdf
from .sources import month_start, open_source


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(prog="python -m statements.build")
    parser.add_argument("--source", choices=["duckdb", "snowflake"], default=None)
    parser.add_argument(
        "--path", default=None, help="DuckDB file (default dbt/target/local.duckdb)"
    )
    parser.add_argument("--merchant", help="merchant id, e.g. mch_001")
    parser.add_argument("--month", help="YYYY-MM, e.g. 2026-01")
    parser.add_argument("--all", action="store_true", help="every merchant and every month")
    parser.add_argument("--out", type=Path, default=Path("data/statements"))
    args = parser.parse_args(argv)
    if not args.all and not (args.merchant and args.month):
        parser.error("give --merchant and --month, or --all")
    source = open_source(args.source, path=args.path)
    args.out.mkdir(parents=True, exist_ok=True)
    written = 0
    try:
        merchants = [m["merchant_id"] for m in source.merchants()] if args.all else [args.merchant]
        for merchant_id in merchants:
            months = source.merchant_months(merchant_id) if args.all else [month_start(args.month)]
            for month in months:
                statement = build_statement(source, merchant_id, month)
                path = args.out / f"{merchant_id}_{month:%Y-%m}.pdf"
                path.write_bytes(build_pdf(statement, generated_on=date.today()))
                written += 1
                print(path)
    finally:
        source.close()
    print(f"{written} statement(s) written to {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
