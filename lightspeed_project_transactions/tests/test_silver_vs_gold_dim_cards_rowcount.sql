-- Reconciliation test:
-- Fails if Silver cards and Gold dim_cards do not have the same row count.
with silver as (
    select count(*) as row_count
    from {{ ref('silver_cards') }}
),
gold as (
    select count(*) as row_count
    from {{ ref('dim_cards') }}
)
select
    silver.row_count as silver_row_count,
    gold.row_count as gold_row_count
from silver
cross join gold
where silver.row_count <> gold.row_count
