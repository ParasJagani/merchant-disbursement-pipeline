"""Reference replay: rebuilds the oracle from the delivered messages alone.

This is a plain-Python version of the silver and gold logic. It is the yardstick the real
pipeline will be compared against, and it proves the oracle is reachable from what is delivered:
duplicates removed by event_id, malformed messages rejected, and money tied to posting date.
"""

from __future__ import annotations

import json

from .events import LedgerEntry, Message, parse_iso_ms, posting_date
from .oracle import build_oracle
from .schema import validate_message


def _entry(p: dict, day: str, kind: str, amount: int, source: str) -> LedgerEntry:
    return LedgerEntry(p["merchant_id"], p["account_id"], p["currency"], day, kind, amount, source)


def replay_to_ledger(messages: list[Message]) -> list[LedgerEntry]:
    seen: set[str] = set()
    entries: list[LedgerEntry] = []
    for msg in sorted(messages, key=lambda m: m.order):
        ok, _ = validate_message(msg.topic, msg.value)
        if not ok:
            continue
        doc = json.loads(msg.value)
        if doc["event_id"] in seen:
            continue
        seen.add(doc["event_id"])
        p, etype = doc["payload"], doc["event_type"]
        day = posting_date(parse_iso_ms(doc["event_time"]))

        if msg.topic == "payments.transactions" and etype == "captured":
            entries.append(_entry(p, day, "SALE", p["amount"], p["transaction_id"]))
        elif msg.topic == "payments.refunds":
            entries.append(_entry(p, day, "REFUND", -p["amount"], p["refund_id"]))
        elif msg.topic == "payments.disputes" and etype == "dispute_opened":
            entries.append(_entry(p, day, "DISPUTE_DEBIT", -p["amount"], p["dispute_id"]))
        elif msg.topic == "payments.disputes" and etype == "dispute_won":
            entries.append(_entry(p, day, "DISPUTE_REVERSAL", p["amount"], p["dispute_id"]))
        elif msg.topic == "billing.fees":
            kind = "PLATFORM_FEE" if p["fee_type"] == "platform" else "PASS_THROUGH_FEE"
            entries.append(_entry(p, day, kind, -p["amount"], p["fee_id"]))
        elif msg.topic == "ledger.adjustments":
            entries.append(_entry(p, day, "ADJUSTMENT", p["amount"], p["adjustment_id"]))
    return entries


def replay_to_oracle(messages: list[Message]) -> list[dict]:
    return build_oracle(replay_to_ledger(messages))
