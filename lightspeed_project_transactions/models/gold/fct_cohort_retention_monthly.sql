with user_month_activity as (
    select distinct
        user_id,
        cast(date_trunc('month', transaction_date) as date) as activity_month
    from {{ ref('fact_transactions') }}
    where transaction_date is not null
),
user_cohorts as (
    select
        user_id,
        min(activity_month) as cohort_month
    from user_month_activity
    group by 1
),
cohort_activity as (
    select
        c.cohort_month,
        a.activity_month,
        a.user_id,
        (
            (year(a.activity_month) - year(c.cohort_month)) * 12
            + (month(a.activity_month) - month(c.cohort_month))
        ) as months_since_cohort
    from user_month_activity a
    inner join user_cohorts c
        on a.user_id = c.user_id
    where a.activity_month >= c.cohort_month
),
cohort_sizes as (
    select
        cohort_month,
        count(distinct user_id) as cohort_size
    from user_cohorts
    group by 1
),
retention_counts as (
    select
        cohort_month,
        activity_month,
        months_since_cohort,
        count(distinct user_id) as retained_users
    from cohort_activity
    group by 1, 2, 3
)
select
    r.cohort_month,
    r.activity_month,
    r.months_since_cohort,
    s.cohort_size,
    r.retained_users,
    round(r.retained_users / s.cohort_size, 4) as retention_rate
from retention_counts r
inner join cohort_sizes s
    on r.cohort_month = s.cohort_month
order by 1, 3
