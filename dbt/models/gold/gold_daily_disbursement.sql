-- What each account is owed for each posting day: the report the business reads.
-- Amounts are integer minor units (cents). Deductions are shown as positive numbers and
-- subtracted in disbursed_total, which equals the plain sum of the signed ledger entries.

select
    merchant_id,
    account_id,
    currency,
    posting_date,
    sum(case when entry_type = 'SALE' then amount_signed else 0 end) as sales_amount,
    -sum(case when entry_type = 'REFUND' then amount_signed else 0 end) as refund_amount,
    -sum(case when entry_type in ('DISPUTE_DEBIT', 'DISPUTE_REVERSAL') then amount_signed else 0 end)
        as dispute_amount,
    -sum(case when entry_type = 'PLATFORM_FEE' then amount_signed else 0 end) as platform_fee_amount,
    -sum(case when entry_type = 'PASS_THROUGH_FEE' then amount_signed else 0 end)
        as pass_through_fee_amount,
    sum(case when entry_type = 'ADJUSTMENT' then amount_signed else 0 end) as adjustment_amount,
    sum(amount_signed) as disbursed_total
from {{ ref('gold_ledger') }}
group by merchant_id, account_id, currency, posting_date
