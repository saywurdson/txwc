{% macro derive_person_id(table_alias='') %}
{# Derives person_id as INTEGER by always hashing patient_account_number + demographics.
   This ensures person_id is always an integer (OMOP CDM conformant) and deterministic.
   The raw patient_account_number is preserved in person_source_value for traceability.
   Usage: {{ derive_person_id('h') }} or {{ derive_person_id() }} for unaliased tables. #}
{% set prefix = table_alias ~ '.' if table_alias else '' %}
cast(
  hash(
    concat_ws('||',
      coalesce({{ prefix }}patient_account_number, ''),
      coalesce({{ prefix }}employee_mailing_city, ''),
      coalesce({{ prefix }}employee_mailing_state_code, ''),
      coalesce({{ prefix }}employee_mailing_postal_code, ''),
      coalesce({{ prefix }}employee_mailing_country, ''),
      coalesce(cast({{ prefix }}employee_date_of_birth as varchar), ''),
      coalesce({{ prefix }}employee_gender_code, '')
    ), 'xxhash64'
  ) % 2000000000
as integer)
{% endmacro %}
