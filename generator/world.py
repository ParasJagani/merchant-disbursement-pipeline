"""Fictional merchants and accounts. Nothing here resembles a real company."""

from __future__ import annotations

import math
from dataclasses import dataclass, field

from .config import GeneratorConfig
from .rng import Rng

FIRST = [
    "Vo",
    "Zen",
    "Lu",
    "Kes",
    "Bri",
    "Tan",
    "Nor",
    "Ar",
    "Mi",
    "Sol",
    "Cal",
    "Pri",
    "Quo",
    "Ven",
    "Hel",
    "Orb",
    "Fen",
    "Gal",
    "Ju",
    "Tor",
]
SECOND = [
    "yago",
    "tro",
    "mio",
    "trel",
    "ghtpath",
    "dem",
    "vale",
    "dent",
    "ra",
    "ace",
    "ora",
    "mo",
    "nta",
    "ture",
    "ios",
    "it",
    "ley",
    "axy",
    "no",
    "vex",
]

# vertical -> (average ticket in minor units, account labels)
VERTICALS = {
    "mobility": (2200, ["Rides", "Food Delivery", "Freight", "Scooters"]),
    "retail": (6500, ["Online", "Stores", "Marketplace", "Wholesale"]),
    "subscriptions": (1800, ["Monthly", "Annual", "Team", "Enterprise"]),
    "travel": (24000, ["Flights", "Stays", "Packages", "Cars"]),
    "gaming": (1200, ["Store", "In-App", "Passes", "Merch"]),
}
COUNTRIES = [
    ("US", "USD"),
    ("GB", "GBP"),
    ("DE", "EUR"),
    ("FR", "EUR"),
    ("NL", "EUR"),
    ("CA", "CAD"),
    ("AU", "AUD"),
]
RISK = ["low", "medium", "high"]

# fee plan -> (platform basis points, platform fixed minor units)
FEE_PLANS = {"standard": (80, 10), "growth": (60, 5), "enterprise": (45, 3)}
PASS_THROUGH = {"interchange": (170, 5), "scheme": (25, 0), "fx": (100, 0)}


@dataclass
class Account:
    account_id: str
    merchant_id: str
    label: str
    currency: str
    payout_schedule: str
    mu: float  # log-mean of ticket size
    weight: float
    plan_history: list[tuple[int, str]] = field(default_factory=list)  # (from_ms, plan)
    status: str = "active"

    def plan_at(self, ms: int) -> str:
        plan = self.plan_history[0][1]
        for from_ms, name in self.plan_history:
            if from_ms <= ms:
                plan = name
        return plan


@dataclass
class Merchant:
    merchant_id: str
    name: str
    country: str
    vertical: str
    risk_status: str
    weight: float
    accounts: list[Account] = field(default_factory=list)
    deleted_ms: int | None = None

    def active_at(self, ms: int) -> bool:
        return self.deleted_ms is None or ms < self.deleted_ms


def build_world(cfg: GeneratorConfig, rng: Rng) -> list[Merchant]:
    merchants: list[Merchant] = []
    verticals = list(VERTICALS)
    for i in range(cfg.merchants):
        name = FIRST[i % 20] + SECOND[(i * 7 + 3 + (i // 20) * 5) % 20]
        country, currency = COUNTRIES[rng.pick_weighted([40, 55, 70, 80, 85, 95, 100])]
        vertical = verticals[i % len(verticals)]
        # Zipf-like volume: the top four merchants carry roughly half the traffic.
        weight = 1.0 / math.pow(i + 1, 1.05)
        merchant = Merchant(
            merchant_id=f"mch_{i + 1:03d}",
            name=f"{name} {vertical.capitalize()}".replace("Subscriptions", "Plus"),
            country=country,
            vertical=vertical,
            risk_status=RISK[rng.pick_weighted([70, 90, 100])],
            weight=weight,
        )
        avg, labels = VERTICALS[vertical]
        n_accounts = 1 + rng.pick_weighted([20, 55, 85, 100])
        for j in range(n_accounts):
            acc_currency = currency
            if j > 0 and rng.chance(0.2):
                acc_currency = rng.choice(["USD", "EUR", "GBP"])
            plan = "enterprise" if i < 4 else rng.choice(["standard", "standard", "growth"])
            merchant.accounts.append(
                Account(
                    account_id=f"acc_{i + 1:03d}_{j + 1}",
                    merchant_id=merchant.merchant_id,
                    label=labels[j],
                    currency=acc_currency,
                    payout_schedule=rng.choice(["daily", "daily", "weekly"]),
                    mu=math.log(avg) + rng.uniform(-0.2, 0.2),
                    weight=1.0 / (j + 1),
                    plan_history=[(cfg.start_ms, plan)],
                )
            )
        merchants.append(merchant)
    return merchants
