-- One signed money movement per row; a credit to the merchant is positive.
-- Every number in gold is a sum of these rows, so any total can be traced to the events behind it.
-- Posting date is the UTC date of the event that moved the money, not the day it arrived.

with sales as (
    select merchant_id, account_id, currency,
           cast(captured_at as date) as posting_date,
           captured_at as posted_at,
           'SALE' as entry_type,
           amount as amount_signed,
           transaction_id as source_id
    from {{ source('silver', 'silver_transactions') }}
    where status = 'captured'
),

refunds as (
    select merchant_id, account_id, currency,
           cast(refunded_at as date), refunded_at,
           'REFUND', -amount, refund_id
    from {{ source('silver', 'silver_refunds') }}
),

dispute_debits as (
    select merchant_id, account_id, currency,
           cast(opened_at as date), opened_at,
           'DISPUTE_DEBIT', -opened_amount, dispute_id
    from {{ source('silver', 'silver_disputes') }}
),

dispute_reversals as (
    select merchant_id, account_id, currency,
           cast(resolved_at as date), resolved_at,
           'DISPUTE_REVERSAL', resolved_amount, dispute_id
    from {{ source('silver', 'silver_disputes') }}
    where status = 'won'
),

fees as (
    select merchant_id, account_id, currency,
           cast(assessed_at as date), assessed_at,
           case fee_type when 'platform' then 'PLATFORM_FEE' else 'PASS_THROUGH_FEE' end,
           -amount, fee_id
    from {{ source('silver', 'silver_fees') }}
),

adjustments as (
    select merchant_id, account_id, currency,
           cast(posted_at as date), posted_at,
           'ADJUSTMENT', amount, adjustment_id
    from {{ source('silver', 'silver_adjustments') }}
),

entries as (
    select * from sales
    union all select * from refunds
    union all select * from dispute_debits
    union all select * from dispute_reversals
    union all select * from fees
    union all select * from adjustments
)

select
    entry_type || ':' || source_id as entry_id,
    merchant_id,
    account_id,
    currency,
    posting_date,
    posted_at,
    entry_type,
    amount_signed,
    source_id
from entries
