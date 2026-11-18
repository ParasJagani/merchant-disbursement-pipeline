from __future__ import annotations

import argparse
import os
from dataclasses import replace
from pathlib import Path

from .chaos import deliver
from .config import GeneratorConfig
from .oracle import build_oracle
from .simulate import simulate
from .sinks import ensure_topics, make_kafka_producer, replay_to_producer, write_backfill


def _config(args: argparse.Namespace) -> GeneratorConfig:
    cfg = GeneratorConfig()
    overrides = {
        k: v
        for k, v in {
            "seed": args.seed,
            "days": args.days,
            "tx_per_day": args.tx_per_day,
            "start_date": args.start_date,
            "merchants": args.merchants,
        }.items()
        if v is not None
    }
    return replace(cfg, **overrides)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m generator", description=__doc__)
    sub = parser.add_subparsers(dest="mode", required=True)
    for name in ("backfill", "live"):
        p = sub.add_parser(name)
        p.add_argument("--seed", type=int)
        p.add_argument("--days", type=int)
        p.add_argument("--tx-per-day", type=int, dest="tx_per_day")
        p.add_argument("--start-date", dest="start_date")
        p.add_argument("--merchants", type=int)
        if name == "backfill":
            p.add_argument("--out", type=Path, default=Path("data/golden"))
        else:
            p.add_argument("--rate", type=float, default=50.0, help="events per second")
            p.add_argument(
                "--bootstrap", default=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
            )
    args = parser.parse_args(argv)

    cfg = _config(args)
    result = simulate(cfg)
    messages, stats = deliver(cfg, result.events)
    oracle_rows = build_oracle(result.ledger)

    if args.mode == "backfill":
        manifest = write_backfill(args.out, cfg, result, messages, stats, oracle_rows)
        print(
            f"wrote {manifest['file_count']} files, {len(messages)} messages, "
            f"{manifest['oracle_rows']} oracle rows to {args.out}"
        )
        print(f"dataset_sha256 {manifest['dataset_sha256']}")
    else:
        ensure_topics(args.bootstrap)
        sent = replay_to_producer(make_kafka_producer(args.bootstrap), messages, rate=args.rate)
        print(f"published {sent} messages to {args.bootstrap}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
