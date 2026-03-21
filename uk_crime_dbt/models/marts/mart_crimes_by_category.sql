-- marts/mart_crimes_by_category.sql
-- Total crimes grouped by category, with outcome rate

with base as (
    select * from {{ ref('stg_crimes') }}
)

select
    crime_category,
    count(*)                                            as total_crimes,
    sum(case when has_outcome then 1 else 0 end)        as crimes_with_outcome,
    round(
        sum(case when has_outcome then 1 else 0 end)::numeric
        / count(*) * 100, 2
    )                                                   as outcome_rate_pct
from base
group by crime_category
order by total_crimes desc
