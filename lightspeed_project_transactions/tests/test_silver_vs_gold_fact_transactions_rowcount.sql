-- Reconciliation test:
-- Fails if Silver transactions and Gold fact_transactions do not have the same row count.
with silver as (
    select count(*) as row_count
    from {{ ref('silver_transactions') }}
),
gold as (
    select count(*) as row_count
    from {{ ref('fact_transactions') }}
)
select
    silver.row_count as silver_row_count,
    gold.row_count as gold_row_count
from silver
cross join gold
where silver.row_count <> gold.row_count
