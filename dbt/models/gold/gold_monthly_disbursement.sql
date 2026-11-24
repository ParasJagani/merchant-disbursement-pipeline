-- The same report rolled up by calendar month (UTC), for statements and trend views.

select
    merchant_id,
    account_id,
    currency,
    date_trunc('month', posting_date) as posting_month,
    count(*) as active_days,
    sum(sales_amount) as sales_amount,
    sum(refund_amount) as refund_amount,
    sum(dispute_amount) as dispute_amount,
    sum(platform_fee_amount) as platform_fee_amount,
    sum(pass_through_fee_amount) as pass_through_fee_amount,
    sum(adjustment_amount) as adjustment_amount,
    sum(disbursed_total) as disbursed_total
from {{ ref('gold_daily_disbursement') }}
group by merchant_id, account_id, currency, date_trunc('month', posting_date)
