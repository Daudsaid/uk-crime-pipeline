with enriched as (
    select * from {{ ref('int_crimes_enriched') }}
)

select
    crime_year,
    crime_month_num,
    month_name,
    crime_month,
    crime_category,
    severity_tier,
    location_type,
    count(*)                            as total_crimes,
    count(outcome_status)               as crimes_with_outcome
from enriched
group by
    crime_year,
    crime_month_num,
    month_name,
    crime_month,
    crime_category,
    severity_tier,
    location_type
order by
    crime_month,
    total_crimes desc
