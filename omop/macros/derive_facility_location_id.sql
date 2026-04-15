{% macro derive_facility_location_id(alias='') %}
{# Facility-address location hash. Must match stg_location's facility CTEs.
   Uses canonical column order: name, address, city, state, postal, country. #}
{% set prefix = alias ~ '.' if alias else '' %}
cast(
  hash(
    concat_ws('||',
      {{ prefix }}facility_name,
      {{ prefix }}facility_primary_address,
      {{ prefix }}facility_city,
      {{ prefix }}facility_state_code,
      {{ prefix }}facility_postal_code,
      {{ prefix }}facility_country_code
    ),
    'xxhash64'
  ) % 1000000000
as varchar)
{% endmacro %}
