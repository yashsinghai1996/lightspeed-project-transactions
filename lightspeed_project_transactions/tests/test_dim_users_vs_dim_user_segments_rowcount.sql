-- Reconciliation test:
-- Fails if Gold dim_users and Gold dim_user_segments do not have the same row count.
with dim_users as (
    select count(*) as row_count
    from {{ ref('dim_users') }}
),
dim_user_segments as (
    select count(*) as row_count
    from {{ ref('dim_user_segments') }}
)
select
    dim_users.row_count as dim_users_row_count,
    dim_user_segments.row_count as dim_user_segments_row_count
from dim_users
cross join dim_user_segments
where dim_users.row_count <> dim_user_segments.row_count
