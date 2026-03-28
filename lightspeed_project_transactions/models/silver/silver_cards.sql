select
    cast(id as bigint) as id,
    cast(client_id as bigint) as client_id,
    trim(card_brand) as card_brand,
    trim(card_type) as card_type,
    cast(cast(card_number as decimal(19, 0)) as string) as card_number,
    to_date(expires, 'MM/yyyy') as expires,
    cast(cast(cvv as int) as string) as cvv,
    case
        when upper(trim(has_chip)) = 'YES' then true
        when upper(trim(has_chip)) = 'NO' then false
        else null
    end as has_chip,
    cast(num_cards_issued as int) as num_cards_issued,
    cast(nullif(regexp_replace(credit_limit, '[^0-9.-]', ''), '') as decimal(18, 2)) as credit_limit,
    to_date(acct_open_date, 'MM/yyyy') as acct_open_date,
    cast(year_pin_last_changed as int) as year_pin_last_changed,
    case
        when lower(trim(card_on_dark_web)) = 'yes' then true
        when lower(trim(card_on_dark_web)) = 'no' then false
        else null
    end as card_on_dark_web
from {{ ref('bronze_cards') }}
