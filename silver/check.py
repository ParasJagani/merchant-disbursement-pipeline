"""A small ledger derived from the silver tables only, and the comparison with the oracle.

The same SQL runs on Snowflake (``ops/verify_silver.py``) and on DuckDB (the tests). It is a
preview of what Phase 4 turns into the real gold ledger: each silver table contributes entries,
the entries are summed per account and posting date, and the sums must equal the answer key.
"""

from __future__ import annotations

import csv
from pathlib import Path

from generator.events import LedgerEntry
from generator.oracle import COLUMNS, build_oracle

LEDGER_SQL = """
SELECT merchant_id, account_id, currency, captured_at::DATE::STRING, 'SALE', amount, transaction_id
  FROM {db}.SILVER.silver_transactions WHERE status = 'captured'
UNION ALL
SELECT merchant_id, account_id, currency, refunded_at::DATE::STRING, 'REFUND', -amount, refund_id
  FROM {db}.SILVER.silver_refunds
UNION ALL
SELECT merchant_id, account_id, currency, opened_at::DATE::STRING, 'DISPUTE_DEBIT', -opened_amount, dispute_id
  FROM {db}.SILVER.silver_disputes
UNION ALL
SELECT merchant_id, account_id, currency, resolved_at::DATE::STRING, 'DISPUTE_REVERSAL', resolved_amount, dispute_id
  FROM {db}.SILVER.silver_disputes WHERE status = 'won'
UNION ALL
SELECT merchant_id, account_id, currency, assessed_at::DATE::STRING,
       CASE fee_type WHEN 'platform' THEN 'PLATFORM_FEE' ELSE 'PASS_THROUGH_FEE' END, -amount, fee_id
  FROM {db}.SILVER.silver_fees
UNION ALL
SELECT merchant_id, account_id, currency, posted_at::DATE::STRING, 'ADJUSTMENT', amount, adjustment_id
  FROM {db}.SILVER.silver_adjustments
"""


def oracle_from_rows(rows) -> list[dict]:
    """Sum ledger rows exactly the way the generator sums its own ledger."""
    entries = [
        LedgerEntry(m, a, c, str(d), kind, int(amount), str(src))
        for m, a, c, d, kind, amount, src in rows
    ]
    return build_oracle(entries)


def oracle_from_csv(path: Path) -> list[dict]:
    with Path(path).open(newline="") as handle:
        return [
            {k: (v if k in COLUMNS[:4] else int(v)) for k, v in row.items()}
            for row in csv.DictReader(handle)
        ]


def differences(actual: list[dict], expected: list[dict]) -> list[tuple]:
    """Rows that are in one list and not the other (empty means they agree to the cent)."""
    key = lambda r: tuple(r[c] for c in COLUMNS)  # noqa: E731
    a, e = {key(r) for r in actual}, {key(r) for r in expected}
    return sorted(a ^ e)
