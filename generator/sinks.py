from __future__ import annotations

import hashlib
import json
import time
from collections import Counter, defaultdict
from collections.abc import Callable
from pathlib import Path

from .config import TOPICS, GeneratorConfig
from .events import Message, posting_date
from .oracle import write_oracle
from .simulate import SimulationResult


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def write_backfill(
    out_dir: Path,
    cfg: GeneratorConfig,
    result: SimulationResult,
    messages: list[Message],
    stats: dict,
    oracle_rows: list[dict],
) -> dict:
    """Write one NDJSON file per topic per emitted day, plus the oracle and a manifest."""
    out_dir.mkdir(parents=True, exist_ok=True)
    by_file: dict[tuple[str, str], list[str]] = defaultdict(list)
    for m in messages:
        by_file[(m.topic, posting_date(m.emitted_at_ms))].append(m.value)

    files: dict[str, str] = {}
    for (topic, day), lines in sorted(by_file.items()):
        path = out_dir / topic / f"{day}.ndjson"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("\n".join(lines) + "\n")
        files[f"{topic}/{day}.ndjson"] = _sha256(path)

    oracle_path = out_dir / "oracle" / "daily_disbursement.csv"
    write_oracle(oracle_path, oracle_rows)

    truth = Counter(e.topic for e in result.events)
    delivered = Counter(m.topic for m in messages)
    manifest = {
        "config": cfg.to_dict(),
        "clean_events_by_topic": {t: truth.get(t, 0) for t in TOPICS},
        "delivered_messages_by_topic": {t: delivered.get(t, 0) for t in TOPICS},
        "delivery_effects": stats,
        "oracle_rows": len(oracle_rows),
        "oracle_sha256": _sha256(oracle_path),
        "file_count": len(files),
        "files": files,
        "dataset_sha256": hashlib.sha256(json.dumps(files, sort_keys=True).encode()).hexdigest(),
    }
    (out_dir / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    return manifest


def replay_to_producer(
    producer,
    messages: list[Message],
    rate: float = 0.0,
    sleep: Callable[[float], None] = time.sleep,
) -> int:
    """Publish messages in emitted order. ``rate`` is events per second (0 means no throttle).

    ``producer`` needs ``produce(topic, key=, value=)``, ``poll(timeout)`` and ``flush()``,
    which is the confluent_kafka.Producer interface.
    """
    interval = 1.0 / rate if rate > 0 else 0.0
    sent = 0
    for m in messages:
        producer.produce(m.topic, key=m.key, value=m.value)
        producer.poll(0)
        sent += 1
        if interval:
            sleep(interval)
    producer.flush()
    return sent


def make_kafka_producer(bootstrap_servers: str):
    from confluent_kafka import Producer  # imported lazily so tests need no Kafka client

    return Producer({"bootstrap.servers": bootstrap_servers, "linger.ms": 20})


def ensure_topics(bootstrap_servers: str, partitions: int = 3) -> None:
    from confluent_kafka.admin import AdminClient, NewTopic

    admin = AdminClient({"bootstrap.servers": bootstrap_servers})
    existing = set(admin.list_topics(timeout=10).topics)
    wanted = [
        NewTopic(t, num_partitions=partitions, replication_factor=1)
        for t in TOPICS
        if t not in existing
    ]
    for future in admin.create_topics(wanted).values():
        future.result()
