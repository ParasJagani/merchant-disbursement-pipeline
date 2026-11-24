-- The total must equal its parts: sales minus deductions plus adjustments.
select *
from {{ ref('gold_daily_disbursement') }}
where disbursed_total
    != sales_amount - refund_amount - dispute_amount - platform_fee_amount
       - pass_through_fee_amount + adjustment_amount
