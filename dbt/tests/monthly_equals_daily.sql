-- Rolling up must not change any total.
with daily as (
    select account_id, currency, sum(disbursed_total) as total, count(*) as days
    from {{ ref('gold_daily_disbursement') }}
    group by account_id, currency
),
monthly as (
    select account_id, currency, sum(disbursed_total) as total, sum(active_days) as days
    from {{ ref('gold_monthly_disbursement') }}
    group by account_id, currency
)
select d.account_id, d.currency, d.total as daily_total, m.total as monthly_total
from daily d
full outer join monthly m on d.account_id = m.account_id and d.currency = m.currency
where d.total is distinct from m.total or d.days is distinct from m.days
