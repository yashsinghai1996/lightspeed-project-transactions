-- Reconciliation test:
-- Fails if any Bronze user id is missing from Silver users.
select
    cast(b.id as bigint) as missing_user_id
from {{ ref('bronze_users') }} b
left join {{ ref('silver_users') }} s
    on cast(b.id as bigint) = s.id
where s.id is null
