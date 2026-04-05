with stg as (
    select * from {{ ref('stg_crimes') }}
)

select
    crime_id,
    crime_category,
    crime_month,
    to_char(crime_month, 'Month')       as month_name,
    date_part('year', crime_month)      as crime_year,
    date_part('month', crime_month)     as crime_month_num,
    location_type,
    location_subtype,
    latitude,
    longitude,
    street_id,
    street_name,
    outcome_status,
    case
        when crime_category in ('violent-crime', 'robbery', 'possession-of-weapons') then 'high'
        when crime_category in ('burglary', 'vehicle-crime', 'drugs') then 'medium'
        else 'low'
    end                                 as severity_tier
from stg
