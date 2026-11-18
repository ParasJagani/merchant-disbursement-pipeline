"""Delivery effects. They change how events arrive, never what the business truth is."""

from __future__ import annotations

import json
from collections import Counter

from .config import REFERENCE_TOPICS, GeneratorConfig
from .events import Event, Message, dumps
from .rng import Rng

POISON_KINDS = [
    "truncated_json",
    "missing_event_id",
    "amount_as_string",
    "unknown_schema_version",
    "unknown_event_type",
    "not_an_object",
]


def _poison_value(kind: str, envelope: dict) -> str:
    text = dumps(envelope)
    if kind == "truncated_json":
        return text[: len(text) // 2]
    if kind == "not_an_object":
        return "[1,2,3]"
    bad = json.loads(text)
    if kind == "missing_event_id":
        del bad["event_id"]
    elif kind == "amount_as_string":
        bad["payload"]["amount"] = "12.50"
    elif kind == "unknown_schema_version":
        bad["schema_version"] = 99
    elif kind == "unknown_event_type":
        bad["event_type"] = "teleported"
    return dumps(bad)


def deliver(cfg: GeneratorConfig, events: list[Event]) -> tuple[list[Message], dict]:
    rng = Rng(cfg.seed + 1_000_003)
    pending: list[tuple[int, int, Message]] = []
    stats: Counter = Counter()
    idx = 0

    for ev in sorted(events, key=lambda e: e.event_time_ms):
        emitted = ev.event_time_ms + rng.randint(200, 3000)
        if rng.chance(cfg.out_of_order_rate):
            emitted += rng.randint(60_000, 7_200_000)
            stats["out_of_order"] += 1
        pending.append(
            (emitted, idx, Message(ev.topic, ev.entity_id, dumps(ev.envelope(emitted)), emitted))
        )
        idx += 1
        if rng.chance(cfg.duplicate_rate):
            again = emitted + rng.randint(1_000, 300_000)
            pending.append(
                (again, idx, Message(ev.topic, ev.entity_id, dumps(ev.envelope(again)), again))
            )
            idx += 1
            stats["duplicates"] += 1

    sources = [e for e in events if e.topic not in REFERENCE_TOPICS]
    for n in range(round(cfg.poison_rate * len(events))):
        kind = POISON_KINDS[n % len(POISON_KINDS)]
        src = sources[rng.randint(0, len(sources) - 1)]
        at = cfg.start_ms + rng.randint(0, cfg.end_ms - cfg.start_ms - 1)
        value = _poison_value(kind, src.envelope(at))
        pending.append((at, idx, Message(src.topic, src.entity_id, value, at)))
        idx += 1
        stats[f"poison:{kind}"] += 1
        stats["poison"] += 1

    pending.sort(key=lambda p: (p[0], p[1]))
    messages = [
        Message(m.topic, m.key, m.value, m.emitted_at_ms, order=i)
        for i, (_, _, m) in enumerate(pending)
    ]
    return messages, dict(stats)
