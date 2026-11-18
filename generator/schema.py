"""Message validation shared by the file writer (dead-letter routing) and the tests."""

from __future__ import annotations

import json

from .config import CURRENCIES, REFERENCE_TOPICS, SCHEMA_VERSIONS

ENVELOPE_FIELDS = {
    "event_id": str,
    "event_type": str,
    "entity_id": str,
    "event_time": str,
    "emitted_at": str,
    "schema_version": int,
    "sequence": int,
    "payload": dict,
}

EVENT_TYPES = {
    "ref.merchants": {"merchant_upserted", "merchant_deleted"},
    "ref.accounts": {"account_upserted", "account_deleted"},
    "payments.transactions": {"authorized", "captured", "declined", "voided"},
    "payments.refunds": {"refund_issued"},
    "payments.disputes": {"dispute_opened", "dispute_won", "dispute_lost"},
    "billing.fees": {"fee_assessed"},
    "ledger.adjustments": {"adjustment_posted"},
}

# Required payload fields per topic, with types. Deletes on reference topics carry only the key.
PAYLOAD_FIELDS = {
    "ref.merchants": {"merchant_id": str, "name": str, "country": str, "risk_status": str},
    "ref.accounts": {
        "account_id": str,
        "merchant_id": str,
        "currency": str,
        "fee_plan": str,
        "status": str,
    },
    "payments.transactions": {
        "transaction_id": str,
        "account_id": str,
        "merchant_id": str,
        "amount": int,
        "currency": str,
    },
    "payments.refunds": {
        "refund_id": str,
        "transaction_id": str,
        "account_id": str,
        "merchant_id": str,
        "amount": int,
        "currency": str,
    },
    "payments.disputes": {
        "dispute_id": str,
        "transaction_id": str,
        "account_id": str,
        "merchant_id": str,
        "amount": int,
        "currency": str,
    },
    "billing.fees": {
        "fee_id": str,
        "transaction_id": str,
        "account_id": str,
        "merchant_id": str,
        "fee_type": str,
        "amount": int,
        "currency": str,
    },
    "ledger.adjustments": {
        "adjustment_id": str,
        "account_id": str,
        "merchant_id": str,
        "amount": int,
        "currency": str,
        "reason_code": str,
    },
}
DELETE_FIELDS = {"ref.merchants": {"merchant_id": str}, "ref.accounts": {"account_id": str}}


def _is(value, expected) -> bool:
    # bool is a subclass of int; a boolean amount is malformed.
    return isinstance(value, expected) and not isinstance(value, bool)


def validate_message(topic: str, raw: str) -> tuple[bool, str | None]:
    """Return (ok, reject_reason). The reason is a stable code usable in a quarantine table."""
    try:
        doc = json.loads(raw)
    except (TypeError, ValueError):
        return False, "invalid_json"
    if not isinstance(doc, dict):
        return False, "not_an_object"
    for name, expected in ENVELOPE_FIELDS.items():
        if name not in doc:
            return False, f"missing_{name}"
        if not _is(doc[name], expected):
            return False, f"bad_type_{name}"
    if doc["schema_version"] not in SCHEMA_VERSIONS:
        return False, "unknown_schema_version"
    if doc["event_type"] not in EVENT_TYPES.get(topic, set()):
        return False, "unknown_event_type"
    if topic in REFERENCE_TOPICS and doc.get("op") not in {"upsert", "delete"}:
        return False, "bad_op"
    required = PAYLOAD_FIELDS[topic]
    if topic in REFERENCE_TOPICS and doc.get("op") == "delete":
        required = DELETE_FIELDS[topic]
    for name, expected in required.items():
        if name not in doc["payload"]:
            return False, f"missing_payload_{name}"
        if not _is(doc["payload"][name], expected):
            return False, f"bad_type_payload_{name}"
    currency = doc["payload"].get("currency")
    if currency is not None and currency not in CURRENCIES:
        return False, "unknown_currency"
    return True, None
