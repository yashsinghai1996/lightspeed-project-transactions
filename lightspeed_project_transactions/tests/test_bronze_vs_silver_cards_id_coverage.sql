-- Reconciliation test:
-- Fails if any Bronze card id is missing from Silver cards.
select
    cast(b.id as bigint) as missing_card_id
from {{ ref('bronze_cards') }} b
left join {{ ref('silver_cards') }} s
    on cast(b.id as bigint) = s.id
where s.id is null
