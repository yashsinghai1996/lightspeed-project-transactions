-- Reconciliation test:
-- Fails if transaction counts or total amounts do not match between fact_transactions and fct_transactions_by_time_bucket.
with fact_totals as (
    select
        count(*) as transaction_count,
        sum(amount) as total_amount
    from {{ ref('fact_transactions') }}
    where transaction_date is not null
),
bucket_totals as (
    select
        sum(transaction_count) as transaction_count,
        sum(total_amount) as total_amount
    from {{ ref('fct_transactions_by_time_bucket') }}
)
select
    f.transaction_count as fact_transaction_count,
    b.transaction_count as bucket_transaction_count,
    f.total_amount as fact_total_amount,
    b.total_amount as bucket_total_amount
from fact_totals f
cross join bucket_totals b
where coalesce(f.transaction_count, -1) <> coalesce(b.transaction_count, -1)
   or coalesce(f.total_amount, cast(-1 as decimal(18, 2))) <> coalesce(b.total_amount, cast(-1 as decimal(18, 2)))
