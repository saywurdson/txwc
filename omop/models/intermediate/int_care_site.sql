select distinct
    cast(care_site_id as integer) as care_site_id,
    cast(care_site_name as varchar) as care_site_name,
    cast(place_of_service_concept_id as integer) as place_of_service_concept_id,
    cast(location_id as integer) as location_id,
    cast(care_site_source_value as varchar) as care_site_source_value,
    cast(place_of_service_source_value as varchar) as place_of_service_source_value
from {{ ref('stg_care_site') }}