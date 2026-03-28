-- Reconciliation test:
-- Fails if any Silver card id is missing from Gold dim_cards.
select
    s.id as missing_card_id
from {{ ref('silver_cards') }} s
left join {{ ref('dim_cards') }} g
    on s.id = g.card_id
where g.card_id is null
