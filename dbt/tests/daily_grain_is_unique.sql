-- One row per merchant, account, currency and day.
select merchant_id, account_id, currency, posting_date, count(*) as n
from {{ ref('gold_daily_disbursement') }}
group by merchant_id, account_id, currency, posting_date
having count(*) > 1
