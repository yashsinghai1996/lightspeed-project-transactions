select
    cast(id as bigint) as id,
    cast(current_age as int) as current_age,
    cast(retirement_age as int) as retirement_age,
    cast(birth_year as int) as birth_year,
    cast(birth_month as int) as birth_month,
    trim(gender) as gender,
    trim(address) as address,
    cast(latitude as decimal(10, 6)) as latitude,
    cast(longitude as decimal(10, 6)) as longitude,
    cast(nullif(regexp_replace(per_capita_income, '[^0-9.-]', ''), '') as decimal(18, 2)) as per_capita_income,
    cast(nullif(regexp_replace(yearly_income, '[^0-9.-]', ''), '') as decimal(18, 2)) as yearly_income,
    cast(nullif(regexp_replace(total_debt, '[^0-9.-]', ''), '') as decimal(18, 2)) as total_debt,
    cast(credit_score as int) as credit_score,
    cast(num_credit_cards as int) as num_credit_cards
from {{ ref('bronze_users') }}
