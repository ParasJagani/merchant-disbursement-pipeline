"""What each silver table looks like: its key, its payload fields and its validation rules."""

from __future__ import annotations

from dataclasses import dataclass

from generator.config import CURRENCIES, SCHEMA_VERSIONS
from generator.schema import EVENT_TYPES
from generator.simulate import ADJUSTMENT_REASONS
from generator.world import FEE_PLANS, RISK

STR = "str"
INT = "int"


@dataclass(frozen=True)
class Field:
    name: str
    kind: str = STR
    required: bool = True
    allowed: tuple[str, ...] | None = None


@dataclass(frozen=True)
class ReferenceTable:
    """A merchant or account table: changes arrive as upserts and deletes, history is kept."""

    name: str  # merchants
    bronze: str  # ref_merchants
    topic: str  # ref.merchants
    key: str  # merchant_id
    fields: tuple[Field, ...]

    @property
    def event_types(self) -> tuple[str, ...]:
        return tuple(sorted(EVENT_TYPES[self.topic]))


@dataclass(frozen=True)
class EventTopic:
    """An event topic: facts arrive as events and the silver table holds the latest state."""

    name: str  # transactions
    bronze: str  # payments_transactions
    topic: str  # payments.transactions
    key: str  # transaction_id
    fields: tuple[Field, ...]
    amount_rule: str = (
        "positive"  # "positive", "non_negative" (fees can round to 0) or "nonzero" (signed)
    )

    @property
    def event_types(self) -> tuple[str, ...]:
        return tuple(sorted(EVENT_TYPES[self.topic]))


_CURRENCY = Field("currency", STR, allowed=tuple(CURRENCIES))

MERCHANTS = ReferenceTable(
    "merchants",
    "ref_merchants",
    "ref.merchants",
    "merchant_id",
    (
        Field("name"),
        Field("country"),
        Field("vertical", required=False),
        Field("risk_status", allowed=tuple(RISK)),
    ),
)
ACCOUNTS = ReferenceTable(
    "accounts",
    "ref_accounts",
    "ref.accounts",
    "account_id",
    (
        Field("merchant_id"),
        Field("label", required=False),
        _CURRENCY,
        Field("fee_plan", allowed=tuple(FEE_PLANS)),
        Field("payout_schedule", required=False),
        Field("status", allowed=("active", "suspended", "closed")),
    ),
)

TRANSACTIONS = EventTopic(
    "transactions",
    "payments_transactions",
    "payments.transactions",
    "transaction_id",
    (
        Field("account_id"),
        Field("merchant_id"),
        Field("amount", INT),
        _CURRENCY,
        Field("payment_method", allowed=("card", "wallet", "bank_transfer")),
        Field("status", allowed=("authorized", "captured", "declined", "voided")),
        Field("channel", required=False),  # added by schema version 2
    ),
)
REFUNDS = EventTopic(
    "refunds",
    "payments_refunds",
    "payments.refunds",
    "refund_id",
    (
        Field("transaction_id"),
        Field("account_id"),
        Field("merchant_id"),
        Field("amount", INT),
        _CURRENCY,
        Field("reason", required=False),
    ),
)
DISPUTES = EventTopic(
    "disputes",
    "payments_disputes",
    "payments.disputes",
    "dispute_id",
    (
        Field("transaction_id"),
        Field("account_id"),
        Field("merchant_id"),
        Field("amount", INT),
        _CURRENCY,
        Field("reason_code", allowed=("fraud", "not_received", "not_as_described", "duplicate")),
        Field("status", allowed=("opened", "won", "lost")),
    ),
)
FEES = EventTopic(
    "fees",
    "billing_fees",
    "billing.fees",
    "fee_id",
    (
        Field("transaction_id"),
        Field("account_id"),
        Field("merchant_id"),
        Field("fee_type", allowed=("platform", "pass_through")),
        Field("fee_subtype", required=False),
        Field("amount", INT),
        _CURRENCY,
    ),
    amount_rule="non_negative",
)
ADJUSTMENTS = EventTopic(
    "adjustments",
    "ledger_adjustments",
    "ledger.adjustments",
    "adjustment_id",
    (
        Field("account_id"),
        Field("merchant_id"),
        Field("amount", INT),
        _CURRENCY,
        Field("reason_code", allowed=tuple(ADJUSTMENT_REASONS)),
        Field("approver", required=False),
    ),
    amount_rule="nonzero",
)

REFERENCE_TABLES = (MERCHANTS, ACCOUNTS)
EVENT_TOPICS = (TRANSACTIONS, REFUNDS, DISPUTES, FEES, ADJUSTMENTS)
VALID_SCHEMA_VERSIONS = tuple(sorted(SCHEMA_VERSIONS))


# Object names the operations scripts need (schema-qualified, without the database).
MERGE_TASKS = tuple(f"OPS.MERGE_{t.name.upper()}_TASK" for t in REFERENCE_TABLES)
DYNAMIC_TABLES = tuple(
    [f"SILVER.typed_{t.name}" for t in EVENT_TOPICS]
    + [f"SILVER.silver_{t.name}" for t in EVENT_TOPICS]
    + [f"QUARANTINE.rejected_{t.name}" for t in EVENT_TOPICS]
)
SILVER_TABLES = tuple(
    [f"SILVER.silver_{t.name}" for t in (*REFERENCE_TABLES, *EVENT_TOPICS)]
    + [f"QUARANTINE.rejected_{t.name}" for t in (*REFERENCE_TABLES, *EVENT_TOPICS)]
)
