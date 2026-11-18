"""Expected daily disbursement per account, computed from the ledger entries only."""

from __future__ import annotations

import csv
from collections import defaultdict
from pathlib import Path

from .events import LedgerEntry

COLUMNS = [
    "merchant_id",
    "account_id",
    "currency",
    "posting_date",
    "sales_amount",
    "refund_amount",
    "dispute_amount",
    "platform_fee_amount",
    "pass_through_fee_amount",
    "adjustment_amount",
    "disbursed_total",
]


def build_oracle(entries: list[LedgerEntry]) -> list[dict]:
    totals: dict[tuple, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    for e in entries:
        totals[(e.merchant_id, e.account_id, e.currency, e.posting_date)][e.entry_type] += (
            e.amount_signed
        )
    rows = []
    for (merchant_id, account_id, currency, day), t in sorted(
        totals.items(), key=lambda kv: (kv[0][3], kv[0][1])
    ):
        sales = t["SALE"]
        refund = -t["REFUND"]
        dispute = -(t["DISPUTE_DEBIT"] + t["DISPUTE_REVERSAL"])
        platform = -t["PLATFORM_FEE"]
        passthrough = -t["PASS_THROUGH_FEE"]
        adjustment = t["ADJUSTMENT"]
        rows.append(
            {
                "merchant_id": merchant_id,
                "account_id": account_id,
                "currency": currency,
                "posting_date": day,
                "sales_amount": sales,
                "refund_amount": refund,
                "dispute_amount": dispute,
                "platform_fee_amount": platform,
                "pass_through_fee_amount": passthrough,
                "adjustment_amount": adjustment,
                "disbursed_total": sales - refund - dispute - platform - passthrough + adjustment,
            }
        )
    return rows


def write_oracle(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=COLUMNS, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)
