select
    cast(care_site_id as integer) as care_site_id,
    care_site_name,
    cast(place_of_service_concept_id as integer) as place_of_service_concept_id,
    cast(location_id as integer) as location_id,
    care_site_source_value,
    place_of_service_source_value
from {{ ref('int_care_site') }}