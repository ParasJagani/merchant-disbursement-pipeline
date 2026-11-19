from __future__ import annotations

import json
from pathlib import Path
from typing import Protocol


class DeadLetters(Protocol):
    def send(self, topic: str, key: str | None, value: str, reason: str) -> None: ...


class LocalDeadLetters:
    """One NDJSON file per source topic: {reason, key, value}. Replayable after a fix."""

    def __init__(self, root: Path) -> None:
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)

    def send(self, topic: str, key: str | None, value: str, reason: str) -> None:
        with (self.root / f"{topic}.ndjson").open("a") as handle:
            handle.write(json.dumps({"reason": reason, "key": key, "value": value}) + "\n")


class KafkaDeadLetters:
    """Publishes the original bytes to ``dlq.<topic>`` with the reason in a header."""

    def __init__(self, producer) -> None:
        self.producer = producer

    def send(self, topic: str, key: str | None, value: str, reason: str) -> None:
        self.producer.produce(
            f"dlq.{topic}", key=key, value=value, headers=[("reject_reason", reason.encode())]
        )
        self.producer.poll(0)
