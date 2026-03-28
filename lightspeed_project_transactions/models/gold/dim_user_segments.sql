with user_spend as (
    select
        user_id,
        sum(amount) as lifetime_total_amount_spent
    from {{ ref('fact_transactions') }}
    group by 1
)

select
    u.user_id,
    u.current_age,
    u.yearly_income,
    u.total_debt,
    u.credit_score,
    u.num_credit_cards,
    coalesce(s.lifetime_total_amount_spent, 0) as lifetime_total_amount_spent,
    case
        when u.current_age < 25 then '18-24'
        when u.current_age < 35 then '25-34'
        when u.current_age < 45 then '35-44'
        when u.current_age < 55 then '45-54'
        when u.current_age < 65 then '55-64'
        else '65+'
    end as age_band,
    case
        when u.yearly_income < 25000 then 'low_income'
        when u.yearly_income < 50000 then 'mid_income'
        when u.yearly_income < 100000 then 'upper_mid_income'
        else 'high_income'
    end as income_band,
    case
        when u.credit_score < 580 then 'poor'
        when u.credit_score < 670 then 'fair'
        when u.credit_score < 740 then 'good'
        when u.credit_score < 800 then 'very_good'
        else 'excellent'
    end as credit_score_band,
    case
        when u.total_debt < 5000 then 'low_debt'
        when u.total_debt < 20000 then 'medium_debt'
        else 'high_debt'
    end as debt_band,
    case
        when coalesce(s.lifetime_total_amount_spent, 0) < 1000 then 'low_spend'
        when coalesce(s.lifetime_total_amount_spent, 0) < 5000 then 'medium_spend'
        else 'high_spend'
    end as spend_band
from {{ ref('dim_users') }} as u
left join user_spend as s
    on u.user_id = s.user_id
