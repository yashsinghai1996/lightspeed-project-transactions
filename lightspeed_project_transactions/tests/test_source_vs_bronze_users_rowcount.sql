-- Reconciliation test:
-- Fails if source users_data and Bronze bronze_users do not have the same row count.
with source_data as (
    select count(*) as row_count
    from {{ source('source', 'users_data') }}
),
bronze as (
    select count(*) as row_count
    from {{ ref('bronze_users') }}
)
select
    source_data.row_count as source_row_count,
    bronze.row_count as bronze_row_count
from source_data
cross join bronze
where source_data.row_count <> bronze.row_count
