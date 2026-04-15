{% macro derive_care_site_id(claim_type, alias='') %}
{# Single source of truth for care_site_id hash. Must match stg_care_site exactly.
   claim_type: 'institutional' | 'professional' | 'pharmacy'
   alias: optional table alias prefix (e.g. 'h' for header) #}
{% set prefix = alias ~ '.' if alias else '' %}
{% if claim_type in ('institutional', 'professional') %}
cast(
  hash(
    concat_ws('||',
      {{ prefix }}billing_provider_last_name,
      {{ prefix }}facility_primary_address,
      {{ prefix }}facility_city,
      {{ prefix }}facility_state_code,
      {{ prefix }}facility_postal_code,
      {{ prefix }}facility_country_code
    ),
    'xxhash64'
  ) % 1000000000
as varchar)
{% elif claim_type == 'pharmacy' %}
cast(
  hash(
    concat_ws('||',
      {{ prefix }}billing_provider_last_name,
      {{ prefix }}billing_provider_fein,
      {{ prefix }}billing_provider_primary_1,
      {{ prefix }}billing_provider_city,
      {{ prefix }}billing_provider_state_code,
      {{ prefix }}billing_provider_postal_code
    ),
    'xxhash64'
  ) % 1000000000
as varchar)
{% else %}
{{ exceptions.raise_compiler_error("Unknown claim_type: " ~ claim_type) }}
{% endif %}
{% endmacro %}
