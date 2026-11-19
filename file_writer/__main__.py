from __future__ import annotations

import argparse
import os
import time
from pathlib import Path

from .dlq import KafkaDeadLetters, LocalDeadLetters
from .stagers import LocalStager, SnowflakeStager, connect_from_env
from .writer import FileWriter, Record, stage_backfill


def _stager(args):
    if args.stager == "local":
        return LocalStager(args.out)
    return SnowflakeStager(connect_from_env())


def run_kafka(args) -> int:
    from confluent_kafka import Consumer, Producer

    from generator.config import TOPICS

    bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap,
            "group.id": args.group,
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
        }
    )
    consumer.subscribe(TOPICS)

    def commit(offsets):
        from confluent_kafka import TopicPartition

        consumer.commit(
            offsets=[TopicPartition(t, p, o) for (t, p), o in offsets.items()], asynchronous=False
        )

    dlq = KafkaDeadLetters(Producer({"bootstrap.servers": bootstrap}))
    writer = FileWriter(
        _stager(args),
        dlq,
        commit=commit,
        max_messages=args.max_messages,
        max_age_seconds=args.max_age,
    )
    idle_since = time.monotonic()
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                writer.tick()
                if args.exit_when_idle and time.monotonic() - idle_since > args.exit_when_idle:
                    break
                continue
            if msg.error():
                continue
            idle_since = time.monotonic()
            _, ts = msg.timestamp()
            writer.handle(
                Record(
                    msg.topic(),
                    msg.partition(),
                    msg.offset(),
                    msg.key().decode() if msg.key() else None,
                    msg.value().decode(errors="replace"),
                    ts,
                )
            )
            writer.tick()
    finally:
        writer.close()
        consumer.close()
    print(writer.stats)
    return 0


def run_backfill(args) -> int:
    stats = stage_backfill(args.src, _stager(args), LocalDeadLetters(args.dlq))
    print(stats)
    return 0


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(prog="python -m file_writer")
    sub = parser.add_subparsers(dest="mode", required=True)
    for name in ("kafka", "backfill"):
        p = sub.add_parser(name)
        p.add_argument("--stager", choices=["local", "snowflake"], default="local")
        p.add_argument("--out", type=Path, default=Path("data/stage"))
        if name == "kafka":
            p.add_argument("--group", default="file-writer")
            p.add_argument("--max-messages", type=int, default=5000)
            p.add_argument("--max-age", type=float, default=60.0)
            p.add_argument("--exit-when-idle", type=float, default=0.0)
        else:
            p.add_argument("--src", type=Path, default=Path("data/golden"))
            p.add_argument("--dlq", type=Path, default=Path("data/dlq"))
    args = parser.parse_args(argv)
    return run_kafka(args) if args.mode == "kafka" else run_backfill(args)


if __name__ == "__main__":
    raise SystemExit(main())
