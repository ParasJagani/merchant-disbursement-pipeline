"""What a statement says, as plain numbers, before any layout.

All amounts are integer minor units (cents). The totals printed on the statement are the sums of
the daily rows printed on it, so a reader can add the table up and get the total. Whether those
totals also equal gold's own monthly totals and the answer key is what ``statements.verify`` checks.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date

from .sources import AMOUNTS, Source, month_start

COMPONENTS = AMOUNTS[:-1]  # everything except disbursed_total
EXPONENT = {"USD": 2, "EUR": 2, "GBP": 2, "CAD": 2, "AUD": 2}


class StatementError(ValueError):
    """The data cannot be turned into a statement that is safe to send."""


def money(minor: int, currency: str) -> str:
    """12345 EUR -> '123.45'; -5 -> '-0.05'. Integer arithmetic only, no floats."""
    if currency not in EXPONENT:
        raise StatementError(f"no minor-unit rule for currency {currency!r}")
    scale = 10 ** EXPONENT[currency]
    whole, cents = divmod(abs(int(minor)), scale)
    text = f"{whole:,}.{cents:0{EXPONENT[currency]}d}"
    return f"-{text}" if minor < 0 else text


def disbursed(row: dict) -> int:
    """The rule every gold row must follow: sales minus deductions plus adjustments."""
    return (
        row["sales_amount"]
        - row["refund_amount"]
        - row["dispute_amount"]
        - row["platform_fee_amount"]
        - row["pass_through_fee_amount"]
        + row["adjustment_amount"]
    )


def add(rows: list[dict]) -> dict[str, int]:
    return {name: sum(int(r[name]) for r in rows) for name in AMOUNTS}


@dataclass(frozen=True)
class AccountStatement:
    account_id: str
    label: str
    currency: str
    days: list[dict]
    totals: dict[str, int]


@dataclass(frozen=True)
class CurrencySummary:
    currency: str
    accounts: int
    totals: dict[str, int]


@dataclass(frozen=True)
class MerchantStatement:
    merchant_id: str
    name: str
    country: str
    month: date
    accounts: list[AccountStatement] = field(default_factory=list)

    @property
    def currencies(self) -> list[CurrencySummary]:
        out = []
        for currency in sorted({a.currency for a in self.accounts}):
            members = [a for a in self.accounts if a.currency == currency]
            out.append(CurrencySummary(currency, len(members), add([a.totals for a in members])))
        return out


def build_statement(source: Source, merchant_id: str, month: str | date) -> MerchantStatement:
    """Gather one merchant's month from gold. Refuses to build from rows that do not add up."""
    first = month_start(month)
    merchants = {m["merchant_id"]: m for m in source.merchants()}
    if merchant_id not in merchants:
        raise StatementError(f"unknown merchant {merchant_id!r}")
    rows = source.daily(merchant_id, first)
    if not rows:
        raise StatementError(f"{merchant_id} has no activity in {first:%Y-%m}")
    labels = {a["account_id"]: a["label"] for a in source.accounts(merchant_id)}
    accounts = []
    for account_id in sorted({r["account_id"] for r in rows}):
        days = [r for r in rows if r["account_id"] == account_id]
        currencies = {r["currency"] for r in days}
        if len(currencies) != 1:
            raise StatementError(f"{account_id} has more than one currency: {sorted(currencies)}")
        for day in days:
            if disbursed(day) != day["disbursed_total"]:
                raise StatementError(
                    f"{account_id} on {day['posting_date']}: the parts add up to "
                    f"{disbursed(day)} but gold says {day['disbursed_total']}"
                )
        accounts.append(
            AccountStatement(
                account_id=account_id,
                label=labels.get(account_id, account_id),
                currency=currencies.pop(),
                days=days,
                totals=add(days),
            )
        )
    merchant = merchants[merchant_id]
    return MerchantStatement(
        merchant_id=merchant_id,
        name=merchant["name"],
        country=merchant["country"],
        month=first,
        accounts=accounts,
    )
