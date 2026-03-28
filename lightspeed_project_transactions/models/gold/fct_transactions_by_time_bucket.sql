with transactions_with_buckets as (
    select
        transaction_id,
        transaction_date,
        amount,
        errors,
        use_chip,
        hour(transaction_date) as transaction_hour,
        case
            when hour(transaction_date) >= 0 and hour(transaction_date) < 6 then '12 AM - 6 AM'
            when hour(transaction_date) >= 6 and hour(transaction_date) < 9 then '6 AM - 9 AM'
            when hour(transaction_date) >= 9 and hour(transaction_date) < 12 then '9 AM - 12 PM'
            when hour(transaction_date) >= 12 and hour(transaction_date) < 15 then '12 PM - 3 PM'
            when hour(transaction_date) >= 15 and hour(transaction_date) < 18 then '3 PM - 6 PM'
            when hour(transaction_date) >= 18 and hour(transaction_date) < 21 then '6 PM - 9 PM'
            else '9 PM - 12 AM'
        end as time_bucket,
        case
            when hour(transaction_date) >= 0 and hour(transaction_date) < 6 then 1
            when hour(transaction_date) >= 6 and hour(transaction_date) < 9 then 2
            when hour(transaction_date) >= 9 and hour(transaction_date) < 12 then 3
            when hour(transaction_date) >= 12 and hour(transaction_date) < 15 then 4
            when hour(transaction_date) >= 15 and hour(transaction_date) < 18 then 5
            when hour(transaction_date) >= 18 and hour(transaction_date) < 21 then 6
            else 7
        end as bucket_order
    from {{ ref('fact_transactions') }}
    where transaction_date is not null
)
select
    time_bucket,
    bucket_order,
    count(*) as transaction_count,
    sum(amount) as total_amount,
    avg(amount) as avg_transaction_amount,
    sum(case when errors is not null then 1 else 0 end) as errored_transaction_count,
    sum(case when upper(coalesce(use_chip, '')) like '%CHIP%' then 1 else 0 end) as chip_transaction_count,
    sum(case when upper(coalesce(use_chip, '')) not like '%CHIP%' then 1 else 0 end) as non_chip_transaction_count
from transactions_with_buckets
group by 1, 2
order by 2
