-- Reconciliation test:
-- Fails if any Silver transaction id is missing from Gold fact_transactions.
select
    s.id as missing_transaction_id
from {{ ref('silver_transactions') }} s
left join {{ ref('fact_transactions') }} g
    on s.id = g.transaction_id
where g.transaction_id is null
