{% macro derive_employee_location_id(alias='') %}
{# Employee-mailing location hash. Must match stg_location's employee CTEs.
   Canonical column order: city, state, postal, country. #}
{% set prefix = alias ~ '.' if alias else '' %}
cast(
  hash(
    concat_ws('||',
      {{ prefix }}employee_mailing_city,
      {{ prefix }}employee_mailing_state_code,
      {{ prefix }}employee_mailing_postal_code,
      {{ prefix }}employee_mailing_country
    ),
    'xxhash64'
  ) % 1000000000
as varchar)
{% endmacro %}
