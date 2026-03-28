select
    user_id,
    current_age,
    yearly_income,
    total_debt,
    credit_score,
    num_credit_cards,
    case
        when current_age < 25 then '18-24'
        when current_age < 35 then '25-34'
        when current_age < 45 then '35-44'
        when current_age < 55 then '45-54'
        when current_age < 65 then '55-64'
        else '65+'
    end as age_band,
    case
        when yearly_income < 25000 then 'low_income'
        when yearly_income < 50000 then 'mid_income'
        when yearly_income < 100000 then 'upper_mid_income'
        else 'high_income'
    end as income_band,
    case
        when credit_score < 580 then 'poor'
        when credit_score < 670 then 'fair'
        when credit_score < 740 then 'good'
        when credit_score < 800 then 'very_good'
        else 'excellent'
    end as credit_score_band,
    case
        when total_debt < 5000 then 'low_debt'
        when total_debt < 20000 then 'medium_debt'
        else 'high_debt'
    end as debt_band
from {{ ref('dim_users') }}
