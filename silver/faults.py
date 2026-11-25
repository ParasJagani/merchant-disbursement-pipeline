"""A catalog of malformed (and merely unusual) messages, with the verdict each one must get.

Every fault is one message built by breaking one thing in a valid template. The catalog is used
three ways: the tests prove each verdict on DuckDB, the chaos drill injects the same messages into
real bronze tables, and the quality guide lists them as the contract of what is rejected and why.

``layer`` says who is expected to catch it:

* ``both``        the file writer dead-letters it; silver would also quarantine it if it got in
* ``silver_only`` the writer lets it through; silver quarantines it (defence in depth)
* ``accepted``    unusual but valid; it must NOT be rejected
* ``writer_only`` not loadable into a VARIANT column (truncated JSON); the writer must stop it
"""

from __future__ import annotations

import copy
import json
from dataclasses import dataclass

from generator.schema import ENVELOPE_FIELDS, PAYLOAD_FIELDS

from .spec import EVENT_TOPICS, INT, REFERENCE_TABLES, EventTopic, ReferenceTable

TS = "2026-01-05T10:00:00.000Z"


@dataclass(frozen=True)
class Fault:
    topic: str
    name: str
    raw: str
    silver_reason: str | None  # None means silver must accept it
    layer: str

    @property
    def loadable(self) -> bool:
        return self.layer != "writer_only"


def _envelope(topic, event_type, entity, payload, op=None, sequence=1):
    doc = {
        "event_id": "evt_template",
        "event_type": event_type,
        "entity_id": entity,
        "event_time": TS,
        "emitted_at": "2026-01-05T10:00:01.000Z",
        "schema_version": 1,
        "sequence": sequence,
        "payload": payload,
    }
    if op:
        doc["op"] = op
    return doc


# One valid message per topic (reference topics also get a valid delete).
TEMPLATES: dict[str, dict] = {
    "ref.merchants": _envelope(
        "ref.merchants", "merchant_upserted", "mch_t",
        {"merchant_id": "mch_t", "name": "Template Co", "country": "DE", "vertical": "retail",
         "risk_status": "low"}, op="upsert",
    ),
    "ref.accounts": _envelope(
        "ref.accounts", "account_upserted", "acc_t",
        {"account_id": "acc_t", "merchant_id": "mch_t", "label": "Main", "currency": "EUR",
         "fee_plan": "standard", "payout_schedule": "daily", "status": "active"}, op="upsert",
    ),
    "payments.transactions": _envelope(
        "payments.transactions", "authorized", "txn_t",
        {"transaction_id": "txn_t", "account_id": "acc_t", "merchant_id": "mch_t", "amount": 1000,
         "currency": "EUR", "payment_method": "card", "status": "authorized"},
    ),
    "payments.refunds": _envelope(
        "payments.refunds", "refund_issued", "rfd_t",
        {"refund_id": "rfd_t", "transaction_id": "txn_t", "account_id": "acc_t",
         "merchant_id": "mch_t", "amount": 500, "currency": "EUR", "reason": "customer_request"},
    ),
    "payments.disputes": _envelope(
        "payments.disputes", "dispute_opened", "dsp_t",
        {"dispute_id": "dsp_t", "transaction_id": "txn_t", "account_id": "acc_t",
         "merchant_id": "mch_t", "amount": 700, "currency": "EUR", "reason_code": "fraud",
         "status": "opened"},
    ),
    "billing.fees": _envelope(
        "billing.fees", "fee_assessed", "fee_t",
        {"fee_id": "fee_t", "transaction_id": "txn_t", "account_id": "acc_t",
         "merchant_id": "mch_t", "fee_type": "platform", "fee_subtype": "standard", "amount": 30,
         "currency": "EUR"},
    ),
    "ledger.adjustments": _envelope(
        "ledger.adjustments", "adjustment_posted", "adj_t",
        {"adjustment_id": "adj_t", "account_id": "acc_t", "merchant_id": "mch_t", "amount": -2500,
         "currency": "EUR", "reason_code": "penalty", "approver": "ops.test"},
    ),
}  # fmt: skip
DELETE_TEMPLATES = {
    "ref.merchants": _envelope(
        "ref.merchants",
        "merchant_deleted",
        "mch_t",
        {"merchant_id": "mch_t"},
        op="delete",
        sequence=2,
    ),
    "ref.accounts": _envelope(
        "ref.accounts",
        "account_deleted",
        "acc_t",
        {"account_id": "acc_t"},
        op="delete",
        sequence=2,
    ),
}
TABLES = {t.topic: t for t in (*REFERENCE_TABLES, *EVENT_TOPICS)}
WRONG_STRING = 123  # a number where text belongs
WRONG_INT = [
    "12.50",
    12.5,
    True,
    None,
]  # text, a decimal, a boolean and null where an integer belongs


def _dump(doc) -> str:
    return json.dumps(doc, separators=(",", ":"))


def _with(doc: dict, path: tuple[str, ...], value=..., drop: bool = False) -> dict:
    out = copy.deepcopy(doc)
    node = out
    for key in path[:-1]:
        node = node[key]
    if drop:
        node.pop(path[-1], None)
    else:
        node[path[-1]] = value
    return out


def _envelope_faults(topic: str, table) -> list[Fault]:
    base = TEMPLATES[topic]
    out: list[Fault] = []

    def add(name, doc, reason, layer):
        out.append(Fault(topic, name, _dump(doc), reason, layer))

    reasons = {
        "event_id": "missing_event_id",
        "event_type": "missing_event_type",
        "entity_id": "missing_entity_id",
        "event_time": "bad_type_event_time",
        "emitted_at": "bad_type_emitted_at",
        "schema_version": "bad_type_schema_version",
        "sequence": "bad_type_sequence",
    }
    wrong = {"event_id": 5, "event_type": 5, "entity_id": 5, "event_time": 5, "emitted_at": 5,
             "schema_version": "1", "sequence": "1"}  # fmt: skip
    for field, reason in reasons.items():
        add(f"{field} missing", _with(base, (field,), drop=True), reason, "both")
        add(f"{field} wrong type", _with(base, (field,), wrong[field]), reason, "both")
        add(f"{field} is null", _with(base, (field,), None), reason, "both")
    add("sequence is a decimal", _with(base, ("sequence",), 1.5), "bad_type_sequence", "both")
    add("schema_version is a boolean", _with(base, ("schema_version",), True),
        "bad_type_schema_version", "both")  # fmt: skip
    add("event_time is not a timestamp", _with(base, ("event_time",), "yesterday"),
        "bad_type_event_time", "silver_only")  # fmt: skip
    add("emitted_at is not a timestamp", _with(base, ("emitted_at",), "garbage"),
        "bad_type_emitted_at", "silver_only")  # fmt: skip
    add("event_time without milliseconds", _with(base, ("event_time",), "2026-01-05T10:00:00Z"),
        "bad_type_event_time", "silver_only")  # fmt: skip
    for label, value in [
        ("fraction with 1 digit", "2026-01-05T10:00:00.5Z"),
        ("fraction with 6 digits", "2026-01-05T10:00:00.123456Z"),
        ("lower-case t", "2026-01-05t10:00:00.000Z"),
        ("with an offset instead of Z", "2026-01-05T10:00:00.000+00:00"),
        ("without Z", "2026-01-05T10:00:00.000"),
        ("with a space instead of T", "2026-01-05 10:00:00.000Z"),
        ("a date only", "2026-01-05"),
        ("an impossible date", "2026-02-30T10:00:00.000Z"),
        ("an impossible hour", "2026-01-05T25:00:00.000Z"),
        ("an empty string", ""),
    ]:
        add(f"event_time {label}", _with(base, ("event_time",), value), "bad_type_event_time",
            "silver_only")  # fmt: skip
        add(f"emitted_at {label}", _with(base, ("emitted_at",), value), "bad_type_emitted_at",
            "silver_only")  # fmt: skip
    add("event_time at the start of the epoch", _with(base, ("event_time",),
        "1970-01-01T00:00:00.000Z"), None, "accepted")  # fmt: skip
    add("schema_version 99", _with(base, ("schema_version",), 99), "unknown_schema_version", "both")
    add("schema_version 0", _with(base, ("schema_version",), 0), "unknown_schema_version", "both")
    add("unknown event_type", _with(base, ("event_type",), "teleported"), "unknown_event_type",
        "both")  # fmt: skip
    other = "refund_issued" if topic != "payments.refunds" else "fee_assessed"
    add("event_type from another topic", _with(base, ("event_type",), other),
        "unknown_event_type", "both")  # fmt: skip

    key = table.key
    add("payload key missing", _with(base, ("payload", key), drop=True), f"missing_payload_{key}",
        "both")  # fmt: skip
    add(
        "payload is missing", _with(base, ("payload",), drop=True), f"missing_payload_{key}", "both"
    )
    add("payload is text", _with(base, ("payload",), "oops"), f"missing_payload_{key}", "both")
    add("payload is a list", _with(base, ("payload",), [1]), f"missing_payload_{key}", "both")
    add("extra unknown envelope field", {**base, "unexpected": 1}, None, "accepted")
    add("extra unknown payload field", _with(base, ("payload", "unexpected"), "x"), None,
        "accepted")  # fmt: skip
    add("schema_version 2 is accepted", _with(base, ("schema_version",), 2), None, "accepted")
    add("the valid template itself", base, None, "accepted")
    # Whole-message problems.
    for name, raw, layer in [
        ("not an object: list", "[1,2,3]", "both"),
        ("not an object: text", '"text"', "both"),
        ("not an object: number", "42", "both"),
        ("not an object: null", "null", "both"),
        ("empty object", "{}", "both"),
    ]:
        out.append(Fault(topic, name, raw, "missing_event_id", layer))
    cut = _dump(base)[:40]
    out.append(Fault(topic, "truncated JSON", cut, None, "writer_only"))
    return out


def _payload_faults(topic: str, table) -> list[Fault]:
    base = TEMPLATES[topic]
    required_by_writer = set(PAYLOAD_FIELDS[topic])
    out: list[Fault] = []

    def add(name, doc, reason, layer):
        out.append(Fault(topic, name, _dump(doc), reason, layer))

    for f in table.fields:
        layer = "both" if f.name in required_by_writer else "silver_only"
        present = f.name in base["payload"]
        if not present:
            continue
        if f.required:
            reason = f"missing_payload_{f.name}" if f.kind != INT else f"bad_type_payload_{f.name}"
            add(f"payload.{f.name} missing", _with(base, ("payload", f.name), drop=True), reason,
                layer)  # fmt: skip
            add(f"payload.{f.name} is null", _with(base, ("payload", f.name), None), reason, layer)
        else:
            add(f"optional payload.{f.name} missing", _with(base, ("payload", f.name), drop=True),
                None, "accepted")  # fmt: skip
            add(f"optional payload.{f.name} is null", _with(base, ("payload", f.name), None),
                None, "accepted")  # fmt: skip
        if f.kind == INT:
            for bad in WRONG_INT:
                if bad is None:
                    continue
                add(f"payload.{f.name} = {bad!r}", _with(base, ("payload", f.name), bad),
                    f"bad_type_payload_{f.name}", layer)  # fmt: skip
        elif f.required:
            add(f"payload.{f.name} is a number", _with(base, ("payload", f.name), WRONG_STRING),
                f"missing_payload_{f.name}", layer)  # fmt: skip
        if f.allowed:
            reason = "unknown_currency" if f.name == "currency" else f"bad_value_{f.name}"
            writer_layer = "both" if f.name == "currency" else "silver_only"
            add(f"payload.{f.name} not allowed", _with(base, ("payload", f.name), "nope"), reason,
                writer_layer)  # fmt: skip
    if "currency" in {f.name for f in table.fields}:
        add("currency in lower case", _with(base, ("payload", "currency"), "eur"),
            "unknown_currency", "both")  # fmt: skip
    return out


def _amount_faults(topic: str, table: EventTopic) -> list[Fault]:
    base = TEMPLATES[topic]
    out: list[Fault] = []

    def add(name, value, reason, layer):
        doc = _with(base, ("payload", "amount"), value)
        out.append(Fault(topic, name, _dump(doc), reason, layer))

    if table.amount_rule == "positive":
        add("amount is zero", 0, "non_positive_amount", "silver_only")
        add("amount is negative", -1, "non_positive_amount", "silver_only")
        add("amount is 1 (smallest valid)", 1, None, "accepted")
    elif table.amount_rule == "non_negative":
        add("amount is negative", -1, "negative_amount", "silver_only")
        add("amount is zero (a fee can round to 0)", 0, None, "accepted")
    else:
        add("amount is zero", 0, "zero_amount", "silver_only")
        add("amount is negative (a debit)", -1, None, "accepted")
    add("amount larger than 32 bits", 5_000_000_000, None, "accepted")
    return out


def _reference_faults(topic: str, table: ReferenceTable) -> list[Fault]:
    base = TEMPLATES[topic]
    delete = DELETE_TEMPLATES[topic]
    key = table.key
    out = [
        Fault(topic, "op missing", _dump(_with(base, ("op",), drop=True)), "bad_op", "both"),
        Fault(topic, "op unknown", _dump(_with(base, ("op",), "merge")), "bad_op", "both"),
        Fault(topic, "op is null", _dump(_with(base, ("op",), None)), "bad_op", "both"),
        Fault(topic, "op wrong type", _dump(_with(base, ("op",), 1)), "bad_op", "both"),
        Fault(topic, "valid delete", _dump(delete), None, "accepted"),
        Fault(topic, "delete without key", _dump(_with(delete, ("payload", key), drop=True)),
              f"missing_payload_{key}", "both"),
        Fault(topic, "delete with an unknown event_type",
              _dump(_with(delete, ("event_type",), "teleported")), "unknown_event_type", "both"),
    ]  # fmt: skip
    return out


def catalog() -> list[Fault]:
    faults: list[Fault] = []
    for topic, table in TABLES.items():
        faults += _envelope_faults(topic, table)
        faults += _payload_faults(topic, table)
        if isinstance(table, EventTopic):
            faults += _amount_faults(topic, table)
        else:
            faults += _reference_faults(topic, table)
    return faults


assert set(ENVELOPE_FIELDS) >= {"event_id", "payload"}  # the catalog assumes the writer's envelope
