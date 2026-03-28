-- Reconciliation test:
-- Fails if Bronze transactions and Silver transactions do not have the same row count.
with bronze as (
    select count(*) as row_count
    from {{ ref('bronze_transactions') }}
),
silver as (
    select count(*) as row_count
    from {{ ref('silver_transactions') }}
)
select
    bronze.row_count as bronze_row_count,
    silver.row_count as silver_row_count
from bronze
cross join silver
where bronze.row_count <> silver.row_count
