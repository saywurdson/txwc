{% set header_list = [
  ('institutional_header_current', 'raw', 'institutional'),
  ('institutional_header_historical', 'raw', 'institutional'),
  ('professional_header_current', 'raw', 'professional'),
  ('professional_header_historical', 'raw', 'professional'),
  ('pharmacy_header_current', 'raw', 'pharmacy'),
  ('pharmacy_header_historical', 'raw', 'pharmacy')
] %}

{% set cte_queries = [] %}
{% for table, schema, htype in header_list %}
  {% if check_table_exists(schema, table) %}
    {% if htype == 'institutional' %}
      {% set query %}
{{ table }} as (
  select distinct
    rendering_bill_provider_last as raw_last_name,
    rendering_bill_provider_first as raw_first_name,
    rendering_bill_provider_state_1 as raw_state,
    rendering_bill_provider_4 as raw_npi,
    concat(
      rendering_bill_provider_last,
      case
        when rendering_bill_provider_first is not null
          then concat(', ', rendering_bill_provider_first)
        else ''
      end
    ) as provider_name,
    cast(null as varchar) as dea,
    cast(null as integer) as specialty_concept_id,
    -- care_site_id hash must match stg_care_site
    cast(
      hash(
        concat_ws(
          '||',
          billing_provider_last_name,
          facility_primary_address,
          facility_city,
          facility_state_code,
          facility_postal_code,
          facility_country_code
        ),
        'xxhash64'
      ) % 1000000000
    as varchar) as care_site_id,
    cast(null as integer) as year_of_birth,
    cast(null as integer) as gender_concept_id,
    rendering_bill_provider_state_1 as provider_source_value,
    cast(null as varchar) as specialty_source_value,
    cast(null as integer) as specialty_source_concept_id,
    cast(null as varchar) as gender_source_value,
    cast(null as integer) as gender_source_concept_id
  from {{ source(schema, table) }}
)
      {% endset %}
    {% elif htype == 'professional' %}
      {% set query %}
{{ table }} as (
  select distinct
    -- Handle shifted columns when last_name='N' and NPI is invalid
    case when rendering_bill_provider_last = 'N'
              AND NOT regexp_matches(coalesce(rendering_bill_provider_4, ''), '^[0-9]{10}$')
         then rendering_bill_provider_first
         else rendering_bill_provider_last end as raw_last_name,
    case when rendering_bill_provider_last = 'N'
              AND NOT regexp_matches(coalesce(rendering_bill_provider_4, ''), '^[0-9]{10}$')
         then rendering_bill_provider_state_1
         else rendering_bill_provider_first end as raw_first_name,
    rendering_bill_provider_state_1 as raw_state,
    rendering_bill_provider_4 as raw_npi,
    -- Corrected provider name when columns are shifted
    concat(
      case when rendering_bill_provider_last = 'N'
                AND NOT regexp_matches(coalesce(rendering_bill_provider_4, ''), '^[0-9]{10}$')
           then rendering_bill_provider_first
           else rendering_bill_provider_last end,
      case
        when (case when rendering_bill_provider_last = 'N'
                        AND NOT regexp_matches(coalesce(rendering_bill_provider_4, ''), '^[0-9]{10}$')
                   then rendering_bill_provider_state_1
                   else rendering_bill_provider_first end) is not null
          then concat(', ',
            case when rendering_bill_provider_last = 'N'
                      AND NOT regexp_matches(coalesce(rendering_bill_provider_4, ''), '^[0-9]{10}$')
                 then rendering_bill_provider_state_1
                 else rendering_bill_provider_first end)
        else ''
      end
    ) as provider_name,
    cast(null as varchar) as dea,
    -- Map NUCC specialty codes to OMOP concepts
    {{ get_source_concept_ids(
        "referring_provider_specialty",
        domain_id='Provider',
        vocabulary_id='NUCC',
        required_value=0
    ) }} as specialty_concept_id,
    -- care_site_id hash must match stg_care_site
    cast(
      hash(
        concat_ws(
          '||',
          billing_provider_last_name,
          facility_primary_address,
          facility_city,
          facility_state_code,
          facility_postal_code,
          facility_country_code
        ),
        'xxhash64'
      ) % 1000000000
    as varchar) as care_site_id,
    cast(null as integer) as year_of_birth,
    cast(null as integer) as gender_concept_id,
    rendering_bill_provider_state_1 as provider_source_value,
    referring_provider_specialty as specialty_source_value,
    cast(null as integer) as specialty_source_concept_id,
    cast(null as varchar) as gender_source_value,
    cast(null as integer) as gender_source_concept_id
  from {{ source(schema, table) }}
)
      {% endset %}
    {% elif htype == 'pharmacy' %}
      {% set query %}
{{ table }} as (
  select distinct
    rendering_bill_provider_last as raw_last_name,
    rendering_bill_provider_first as raw_first_name,
    rendering_bill_provider_state_1 as raw_state,
    rendering_bill_provider_4 as raw_npi,
    concat(
      rendering_bill_provider_last,
      case
        when rendering_bill_provider_first is not null
          then concat(', ', rendering_bill_provider_first)
        else ''
      end
    ) as provider_name,
    cast(null as varchar) as dea,
    cast(null as integer) as specialty_concept_id,
    -- care_site_id hash must match stg_care_site
    cast(
      hash(
        concat_ws(
          '||',
          billing_provider_last_name,
          billing_provider_fein,
          billing_provider_primary_1,
          billing_provider_city,
          billing_provider_state_code,
          billing_provider_postal_code
        ),
        'xxhash64'
      ) % 1000000000
    as varchar) as care_site_id,
    cast(null as integer) as year_of_birth,
    cast(null as integer) as gender_concept_id,
    rendering_bill_provider_state_1 as provider_source_value,
    cast(null as varchar) as specialty_source_value,
    cast(null as integer) as specialty_source_concept_id,
    cast(null as varchar) as gender_source_value,
    cast(null as integer) as gender_source_concept_id
  from {{ source(schema, table) }}
)
      {% endset %}
    {% endif %}
    {% do cte_queries.append(query) %}
  {% endif %}
{% endfor %}

{% set valid_tables = [] %}
{% for table, schema, htype in header_list %}
  {% if check_table_exists(schema, table) %}
    {% do valid_tables.append(table) %}
  {% endif %}
{% endfor %}

{% if cte_queries | length > 0 %}
with {{ cte_queries | join(",\n") }},
-- Combine all raw provider data
all_providers_raw as (
  {% for table in valid_tables %}
    select * from {{ table }}
    {% if not loop.last %}
      union all
    {% endif %}
  {% endfor %}
),
-- Build a lookup of valid NPIs by provider name (last, first)
-- This allows us to find the correct NPI for providers with bad NPI data
valid_npi_lookup as (
  select distinct
    raw_last_name,
    raw_first_name,
    raw_npi as valid_npi
  from all_providers_raw
  where regexp_matches(raw_npi, '^[0-9]{10}$')
),
-- Deduplicate providers and look up valid NPIs
providers_with_npi as (
  select distinct
    cast(
      hash(
        concat_ws(
          '||',
          p.raw_last_name,
          coalesce(p.raw_first_name, ''),
          p.raw_state,
          p.raw_npi
        ),
        'xxhash64'
      ) % 1000000000
    as varchar) as provider_id,
    p.provider_name,
    -- Use valid NPI from current record if available, otherwise look up from other records
    coalesce(
      case when regexp_matches(p.raw_npi, '^[0-9]{10}$') then p.raw_npi else null end,
      npi_lookup.valid_npi
    ) as npi,
    p.dea,
    p.specialty_concept_id,
    p.care_site_id,
    p.year_of_birth,
    p.gender_concept_id,
    p.provider_source_value,
    p.specialty_source_value,
    p.specialty_source_concept_id,
    p.gender_source_value,
    p.gender_source_concept_id
  from all_providers_raw p
  left join valid_npi_lookup npi_lookup
    on p.raw_last_name = npi_lookup.raw_last_name
    and coalesce(p.raw_first_name, '') = coalesce(npi_lookup.raw_first_name, '')
)
{% endif %}

select *
from providers_with_npi
