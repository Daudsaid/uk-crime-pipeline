-- marts/mart_crimes_by_street.sql
-- Top streets by crime volume with category breakdown

with base as (
    select * from {{ ref('stg_crimes') }}
),

street_counts as (
    select
        street_id,
        street_name,
        round(avg(latitude)::numeric, 6)    as avg_latitude,
        round(avg(longitude)::numeric, 6)   as avg_longitude,
        count(*)                            as total_crimes,
        count(distinct crime_category)      as distinct_categories,
        mode() within group (
            order by crime_category
        )                                   as most_common_category,
        sum(case when has_outcome then 1 else 0 end) as crimes_with_outcome
    from base
    group by street_id, street_name
)

select
    street_id,
    street_name,
    avg_latitude,
    avg_longitude,
    total_crimes,
    distinct_categories,
    most_common_category,
    crimes_with_outcome,
    round(crimes_with_outcome::numeric / total_crimes * 100, 2) as outcome_rate_pct
from street_counts
order by total_crimes desc
