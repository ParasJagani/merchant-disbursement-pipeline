from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone

DAY_MS = 86_400_000


@dataclass(frozen=True)
class GeneratorConfig:
    seed: int = 42
    start_date: str = "2026-01-01"
    days: int = 30
    merchants: int = 30
    tx_per_day: int = 1500

    # Business mix
    decline_rate: float = 0.07
    void_rate: float = 0.03
    refund_rate: float = 0.04
    dispute_rate: float = 0.004
    dispute_win_rate: float = 0.40
    offboard_count: int = 2

    # Realism switches (delivery side; they never change the business truth)
    duplicate_rate: float = 0.015
    out_of_order_rate: float = 0.02
    poison_rate: float = 0.001
    drift_day: int = 15  # day index from which transaction events carry the extra field

    @property
    def start_ms(self) -> int:
        d = date.fromisoformat(self.start_date)
        return int(datetime(d.year, d.month, d.day, tzinfo=timezone.utc).timestamp() * 1000)

    @property
    def end_ms(self) -> int:
        return self.start_ms + self.days * DAY_MS

    def to_dict(self) -> dict:
        return asdict(self)


TOPICS = [
    "ref.merchants",
    "ref.accounts",
    "payments.transactions",
    "payments.refunds",
    "payments.disputes",
    "billing.fees",
    "ledger.adjustments",
]
REFERENCE_TOPICS = {"ref.merchants", "ref.accounts"}
CURRENCIES = ["USD", "EUR", "GBP", "CAD", "AUD"]
SCHEMA_VERSIONS = {1, 2}
