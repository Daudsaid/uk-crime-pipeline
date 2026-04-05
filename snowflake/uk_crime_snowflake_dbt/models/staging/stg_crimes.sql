with source as (
    select * from {{ source('raw', 'crimes') }}
),

renamed as (
    select
        crime_id,
        persistent_id,
        category                          as crime_category,
        month                             as crime_month,
        location_type,
        nullif(location_subtype, '')      as location_subtype,
        nullif(context, '')               as context,
        latitude,
        longitude,
        street_id,
        street_name,
        outcome_status
    from source
)

select * from renamed
