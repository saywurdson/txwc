{% macro derive_provider_id(table_alias='') %}
{# Derives provider_id from rendering provider fields using xxhash64.
   Must be used in a SELECT context where the source table has the required columns. #}
{% set prefix = table_alias ~ '.' if table_alias else '' %}
cast(
  hash(concat_ws('||',
    {{ prefix }}rendering_bill_provider_last,
    coalesce({{ prefix }}rendering_bill_provider_first, ''),
    {{ prefix }}rendering_bill_provider_state_1,
    {{ prefix }}rendering_bill_provider_4
  ), 'xxhash64') % 1000000000 as varchar
)
{% endmacro %}
