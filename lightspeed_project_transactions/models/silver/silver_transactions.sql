select
    cast(id as bigint) as id,
    to_timestamp(date) as date,
    cast(client_id as bigint) as client_id,
    cast(card_id as bigint) as card_id,
    cast(nullif(regexp_replace(amount, '[^0-9.-]', ''), '') as decimal(18, 2)) as amount,
    trim(use_chip) as use_chip,
    cast(merchant_id as bigint) as merchant_id,
    trim(merchant_city) as merchant_city,
    upper(trim(merchant_state)) as merchant_state,
    trim(zip) as zip,
    cast(mcc as int) as mcc,
    nullif(trim(errors), '') as errors
from {{ ref('bronze_transactions') }}
