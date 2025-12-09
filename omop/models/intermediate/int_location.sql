-- Deduplicate locations by location_id, keeping the most complete record
-- (prefer records with address_1 over those without)
with ranked_locations as (
    select
        location_id,
        address_1,
        address_2,
        city,
        state,
        zip,
        county,
        location_source_value,
        country_concept_id,
        country_source_value,
        latitude,
        longitude,
        row_number() over (
            partition by location_id
            order by
                -- Prefer records with address (facility locations over mailing-only)
                case when address_1 is not null and trim(address_1) != '' then 0 else 1 end,
                -- Then prefer records with more complete data
                case when city is not null and trim(city) != '' then 0 else 1 end,
                case when zip is not null and trim(zip) != '' then 0 else 1 end
        ) as rn
    from {{ ref('stg_location') }}
)
select
    cast(location_id as integer) as location_id,
    cast(address_1 as varchar) as address_1,
    cast(address_2 as varchar) as address_2,
    cast(city as varchar) as city,
    cast(state as varchar) as state,
    cast(zip as varchar) as zip,
    cast(county as varchar) as county,
    cast(location_source_value as varchar) as location_source_value,
    cast(country_concept_id as varchar) as country_concept_id,
    cast(country_source_value as varchar) as country_source_value,
    cast(latitude as float) as latitude,
    cast(longitude as float) as longitude
from ranked_locations
where rn = 1