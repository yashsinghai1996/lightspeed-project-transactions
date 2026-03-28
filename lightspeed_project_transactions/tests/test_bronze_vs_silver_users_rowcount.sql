-- Reconciliation test:
-- Fails if Bronze users and Silver users do not have the same row count.
with bronze as (
    select count(*) as row_count
    from {{ ref('bronze_users') }}
),
silver as (
    select count(*) as row_count
    from {{ ref('silver_users') }}
)
select
    bronze.row_count as bronze_row_count,
    silver.row_count as silver_row_count
from bronze
cross join silver
where bronze.row_count <> silver.row_count
