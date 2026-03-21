-- marts/mart_monthly_trends.sql
-- Crime volume per month with month-over-month change

with base as (
    select * from {{ ref('stg_crimes') }}
),

monthly as (
    select
        crime_month,
        crime_month_str,
        crime_year,
        crime_month_num,
        count(*)                                        as total_crimes,
        count(distinct crime_category)                  as distinct_categories,
        sum(case when has_outcome then 1 else 0 end)    as crimes_with_outcome
    from base
    group by
        crime_month,
        crime_month_str,
        crime_year,
        crime_month_num
)

select
    crime_month,
    crime_month_str,
    crime_year,
    crime_month_num,
    total_crimes,
    distinct_categories,
    crimes_with_outcome,
    round(crimes_with_outcome::numeric / total_crimes * 100, 2) as outcome_rate_pct,
    lag(total_crimes) over (order by crime_month)               as prev_month_crimes,
    total_crimes - lag(total_crimes) over (order by crime_month) as mom_change
from monthly
order by crime_month
