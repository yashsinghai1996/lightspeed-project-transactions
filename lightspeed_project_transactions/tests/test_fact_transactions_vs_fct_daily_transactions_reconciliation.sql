-- Reconciliation test:
-- Fails if daily transaction counts or total amounts do not match between fact_transactions and fct_daily_transactions.
with fact_daily as (
    select
        cast(transaction_date as date) as transaction_day,
        count(*) as transaction_count,
        sum(amount) as total_amount
    from {{ ref('fact_transactions') }}
    group by 1
),
mart_daily as (
    select
        transaction_day,
        transaction_count,
        total_amount
    from {{ ref('fct_daily_transactions') }}
)
select
    coalesce(f.transaction_day, m.transaction_day) as transaction_day,
    f.transaction_count as fact_transaction_count,
    m.transaction_count as mart_transaction_count,
    f.total_amount as fact_total_amount,
    m.total_amount as mart_total_amount
from fact_daily f
full outer join mart_daily m
    on f.transaction_day = m.transaction_day
where coalesce(f.transaction_count, -1) <> coalesce(m.transaction_count, -1)
   or coalesce(f.total_amount, cast(-1 as decimal(18, 2))) <> coalesce(m.total_amount, cast(-1 as decimal(18, 2)))
