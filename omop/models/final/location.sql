select
    location_id,
    address_1,
    address_2,
    city,
    state,
    zip,
    county,
    location_source_value,
    cast(country_concept_id as integer) as country_concept_id,
    country_source_value,
    latitude,
    longitude
from {{ ref('int_location') }}