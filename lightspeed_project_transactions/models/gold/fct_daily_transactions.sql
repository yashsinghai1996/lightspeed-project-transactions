select
    cast(transaction_date as date) as transaction_day,
    count(*) as transaction_count,
    count(distinct user_id) as active_users,
    count(distinct card_id) as active_cards,
    sum(amount) as total_amount,
    avg(amount) as avg_transaction_amount,
    sum(case when errors is not null then 1 else 0 end) as errored_transaction_count,
    sum(case when upper(coalesce(use_chip, '')) like '%CHIP%' then 1 else 0 end) as chip_transaction_count,
    sum(case when upper(coalesce(use_chip, '')) not like '%CHIP%' then 1 else 0 end) as non_chip_transaction_count
from {{ ref('fact_transactions') }}
group by 1
