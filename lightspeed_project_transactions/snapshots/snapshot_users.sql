{% snapshot snapshot_users %}

{{
    config(
        target_schema='snapshots',
        unique_key='id',
        strategy='check',
        check_cols=[
            'current_age',
            'retirement_age',
            'birth_year',
            'birth_month',
            'gender',
            'address',
            'latitude',
            'longitude',
            'per_capita_income',
            'yearly_income',
            'total_debt',
            'credit_score',
            'num_credit_cards'
        ]
    )
}}

-- SCD Type 2 snapshot for users.
-- This snapshot preserves historical versions of user records whenever one of the
-- tracked descriptive attributes changes. dbt will manage dbt_valid_from and
-- dbt_valid_to so point-in-time user state can be reconstructed later.
select
    id,
    current_age,
    retirement_age,
    birth_year,
    birth_month,
    gender,
    address,
    latitude,
    longitude,
    per_capita_income,
    yearly_income,
    total_debt,
    credit_score,
    num_credit_cards
from {{ ref('silver_users') }}

{% endsnapshot %}
