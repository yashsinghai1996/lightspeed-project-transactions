-- Reconciliation test:
-- Fails if Silver users and Gold dim_users do not have the same row count.
with silver as (
    select count(*) as row_count
    from {{ ref('silver_users') }}
),
gold as (
    select count(*) as row_count
    from {{ ref('dim_users') }}
)
select
    silver.row_count as silver_row_count,
    gold.row_count as gold_row_count
from silver
cross join gold
where silver.row_count <> gold.row_count
