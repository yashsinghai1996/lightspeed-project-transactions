-- Reconciliation test:
-- Fails if any Silver user id is missing from Gold dim_users.
select
    s.id as missing_user_id
from {{ ref('silver_users') }} s
left join {{ ref('dim_users') }} g
    on s.id = g.user_id
where g.user_id is null
