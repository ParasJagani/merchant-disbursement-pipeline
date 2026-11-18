from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from datetime import datetime, timezone


def iso_ms(ms: int) -> str:
    dt = datetime.fromtimestamp(ms / 1000, tz=timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.") + f"{ms % 1000:03d}Z"


def parse_iso_ms(text: str) -> int:
    dt = datetime.strptime(text, "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)


def posting_date(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d")


@dataclass
class Event:
    topic: str
    event_type: str
    entity_id: str
    event_time_ms: int
    sequence: int
    payload: dict
    event_id: str = ""
    op: str | None = None
    schema_version: int = 1

    def envelope(self, emitted_at_ms: int) -> dict:
        out = {
            "event_id": self.event_id,
            "event_type": self.event_type,
            "entity_id": self.entity_id,
            "event_time": iso_ms(self.event_time_ms),
            "emitted_at": iso_ms(emitted_at_ms),
            "schema_version": self.schema_version,
            "sequence": self.sequence,
        }
        if self.op is not None:
            out["op"] = self.op
        out["payload"] = self.payload
        return out


@dataclass(frozen=True)
class LedgerEntry:
    """One signed money movement; the oracle is a sum of these."""

    merchant_id: str
    account_id: str
    currency: str
    posting_date: str
    # SALE, REFUND, DISPUTE_DEBIT, DISPUTE_REVERSAL, PLATFORM_FEE, PASS_THROUGH_FEE, ADJUSTMENT
    entry_type: str
    amount_signed: int  # credit to the merchant is positive
    source_id: str


@dataclass(frozen=True)
class Message:
    """What the broker (or a staged file) actually carries."""

    topic: str
    key: str | None
    value: str
    emitted_at_ms: int
    order: int = 0


def make_event_id(seed: int, counter: int) -> str:
    return "evt_" + hashlib.sha1(f"{seed}:{counter}".encode()).hexdigest()[:20]


def dumps(obj: dict) -> str:
    return json.dumps(obj, separators=(",", ":"))


@dataclass
class SequenceBook:
    """Per-entity sequence numbers, starting at 1."""

    _next: dict[str, int] = field(default_factory=dict)

    def take(self, entity_id: str) -> int:
        n = self._next.get(entity_id, 0) + 1
        self._next[entity_id] = n
        return n
