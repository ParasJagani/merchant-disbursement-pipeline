-- THE gate. Returns every row that is in gold but not in the generator's answer key, or the
-- other way round. An empty result means gold agrees with the oracle to the cent.

with gold as (
    select merchant_id, account_id, currency, posting_date, sales_amount, refund_amount,
           dispute_amount, platform_fee_amount, pass_through_fee_amount, adjustment_amount,
           disbursed_total
    from {{ ref('gold_daily_disbursement') }}
),

oracle as (
    select merchant_id, account_id, currency, posting_date, sales_amount, refund_amount,
           dispute_amount, platform_fee_amount, pass_through_fee_amount, adjustment_amount,
           disbursed_total
    from {{ ref('oracle_daily_disbursement') }}
),

missing_from_gold as (select 'missing_from_gold' as problem, * from (select * from oracle except select * from gold) a),
unexpected_in_gold as (select 'unexpected_in_gold' as problem, * from (select * from gold except select * from oracle) b)

select * from missing_from_gold
union all
select * from unexpected_in_gold
