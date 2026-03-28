{{ config(
    materialized='incremental',
    unique_key='transaction_id'
) }}

-- This model is incremental because transactions are the largest event-grain table
-- in the project and are naturally append-oriented. On incremental runs, dbt only
-- processes records with a transaction_date greater than or equal to the maximum
-- transaction_date already present in the target table. The unique_key ensures
-- transaction_id-level deduplication during incremental merges.
select
    id as transaction_id,
    date as transaction_date,
    client_id as user_id,
    card_id,
    merchant_id,
    merchant_city,
    merchant_state,
    zip as merchant_zip,
    mcc,
    amount,
    use_chip,
    errors
from {{ ref('silver_transactions') }}
{% if is_incremental() %}
where date >= (
    select max(transaction_date)
    from {{ this }}
)
{% endif %}
