-- staging/stg_crimes.sql
-- Cleans raw crimes table: renames columns, casts types, filters bad rows

with source as (
    select * from {{ source('crime_db', 'crimes') }}
),

cleaned as (
    select
        crime_id,

        -- treat 'NaN' strings as null
        nullif(persistent_id, 'NaN')    as persistent_id,

        lower(trim(category))           as crime_category,

        month                           as crime_month,
        to_char(month, 'YYYY-MM')       as crime_month_str,
        extract(year from month)::int   as crime_year,
        extract(month from month)::int  as crime_month_num,

        lower(trim(location_type))      as location_type,
        nullif(location_subtype, 'NaN') as location_subtype,

        latitude,
        longitude,

        street_id,
        initcap(
            regexp_replace(street_name, 'On or near ', '', 'i')
        )                               as street_name,

        nullif(outcome_status, 'NaN')   as outcome_status,

        case
            when nullif(outcome_status, 'NaN') is null then false
            else true
        end                             as has_outcome

    from source
    where
        latitude  is not null
        and longitude is not null
        and category  is not null
        and month     is not null
)

select * from cleaned
