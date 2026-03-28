-- Reconciliation test:
-- Fails if any Bronze transaction id is missing from Silver transactions.
select
    cast(b.id as bigint) as missing_transaction_id
from {{ ref('bronze_transactions') }} b
left join {{ ref('silver_transactions') }} s
    on cast(b.id as bigint) = s.id
where s.id is null
