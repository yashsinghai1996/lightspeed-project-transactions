-- Sanity test:
-- Fails if cohort retention metrics violate expected boundaries or sequencing rules.
select
    cohort_month,
    activity_month,
    months_since_cohort,
    cohort_size,
    retained_users,
    retention_rate
from {{ ref('fct_cohort_retention_monthly') }}
where retained_users > cohort_size
   or retention_rate < 0
   or retention_rate > 1
   or months_since_cohort < 0
   or activity_month < cohort_month
