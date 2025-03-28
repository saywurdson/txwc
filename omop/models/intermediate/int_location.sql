select
    cast(location_id as integer) as location_id,
    cast(address_1 as varchar) as address_1,
    cast(address_2 as varchar) as address_2,
    cast(city as varchar) as city,
    cast(state as varchar) as state,
    cast(zip as varchar) as zip,
    cast(county as varchar) as county,
    cast(location_source_value as varchar) as location_source_value,
    cast(country_concept_id as integer) as country_concept_id,
    cast(country_source_value as varchar) as country_source_value,
    cast(latitude as float) as latitude,
    cast(longitude as float) as longitude
from {{ ref('stg_location') }}