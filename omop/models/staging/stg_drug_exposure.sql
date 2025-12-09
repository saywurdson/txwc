{% set detail_table_list = [
    ('pharmacy_detail_current', 'pharmacy_header_current', 'pharmacy'),
    ('pharmacy_detail_historical', 'pharmacy_header_historical', 'pharmacy'),
    ('institutional_detail_current', 'institutional_header_current', 'institutional'),
    ('institutional_detail_historical', 'institutional_header_historical', 'institutional'),
    ('professional_detail_current', 'professional_header_current', 'professional'),
    ('professional_detail_historical', 'professional_header_historical', 'professional')
] %}

{% set drug_type_mapping = {
    'pharmacy': 32869,
    'institutional': 32854,
    'professional': 32854
} %}

{% set cte_queries = [] %}

{% for table, header_table, detail_type in detail_table_list %}
  {% if check_table_exists('raw', table) and check_table_exists('raw', header_table) %}
    {% set drug_type_concept_id = drug_type_mapping[detail_type] %}
    
    {% if detail_type == 'pharmacy' %}
      {% set query %}
      {{ table }} as (
        select 
          cast(hash(concat_ws('||', pdc.bill_id, pdc.row_id), 'xxhash64') % 1000000000 as varchar) as drug_exposure_id,
          case 
            when phc.patient_account_number is null or trim(phc.patient_account_number) = '' then lpad(
              cast(
                hash(concat_ws('||',
                  coalesce(phc.employee_mailing_city, ''),
                  coalesce(phc.employee_mailing_state_code, ''),
                  coalesce(phc.employee_mailing_postal_code, ''),
                  coalesce(phc.employee_mailing_country, ''),
                  coalesce(cast(phc.employee_date_of_birth as varchar), ''),
                  coalesce(phc.employee_gender_code, '')
                ), 'xxhash64') % 1000000000 as varchar
              ),
              9,
              '0'
            )
            else phc.patient_account_number
          end as person_id,
          cast(null as integer) as drug_concept_id,
          coalesce(try_cast(pdc.service_line_from_date as date), try_cast(pdc.prescription_line_date as date)) as drug_exposure_start_date,
          coalesce(try_cast(pdc.service_line_from_date as timestamp), try_cast(pdc.prescription_line_date as timestamp)) as drug_exposure_start_datetime,
          -- Calculate end date: start_date + days_supply, fallback to visit_end_date
          COALESCE(
              coalesce(try_cast(pdc.service_line_from_date as date), try_cast(pdc.prescription_line_date as date)) + try_cast(pdc.drugs_supplies_number_of as integer),
              try_cast(phc.reporting_period_end_date as date)
          ) as drug_exposure_end_date,
          COALESCE(
              coalesce(try_cast(pdc.service_line_from_date as timestamp), try_cast(pdc.prescription_line_date as timestamp)) + (try_cast(pdc.drugs_supplies_number_of as integer) * INTERVAL '1' DAY),
              try_cast(phc.reporting_period_end_date as timestamp)
          ) as drug_exposure_end_datetime,
          cast(null as date) as verbatim_end_date,
          {{ drug_type_concept_id }} as drug_type_concept_id,
          cast(null as varchar) as stop_reason,
          0 as refills,
          try_cast(pdc.drugs_supplies_quantity as float) as quantity,
          try_cast(pdc.drugs_supplies_number_of as integer) as days_supply,
          cast(null as varchar) as sig,
          cast(null as integer) as route_concept_id,
          cast(null as integer) as lot_number,
          cast(hash(concat_ws('||',
            phc.rendering_bill_provider_last,
            coalesce(phc.rendering_bill_provider_first, ''),
            phc.rendering_bill_provider_state_1,
            phc.rendering_bill_provider_4
          ), 'xxhash64') % 1000000000 as varchar) as provider_id,
          cast(pdc.bill_id as varchar) as visit_occurrence_id,
          -- visit_detail_id uses same hash as visit_detail table
          cast(hash(concat_ws('||', pdc.bill_id, pdc.row_id), 'xxhash64') % 1000000000 as varchar) as visit_detail_id,
          pdc.ndc_billed_code as drug_source_value,
          cast(null as integer) as drug_source_concept_id,
          cast(null as varchar) as route_source_value,
          cast(null as varchar) as dose_unit_source_value
        from {{ source('raw', table) }} pdc
        join {{ source('raw', header_table) }} phc
          on cast(pdc.bill_id as varchar) = cast(phc.bill_id as varchar)
      )
      {% endset %}
    
    {% elif detail_type == 'institutional' %}
      {% set query %}
      {{ table }} as (
        select 
          cast(hash(concat_ws('||', idc.bill_id, idc.row_id), 'xxhash64') % 1000000000 as varchar) as drug_exposure_id,
          case 
            when ihc.patient_account_number is null or trim(ihc.patient_account_number) = '' then lpad(
              cast(
                hash(concat_ws('||',
                  coalesce(ihc.employee_mailing_city, ''),
                  coalesce(ihc.employee_mailing_state_code, ''),
                  coalesce(ihc.employee_mailing_postal_code, ''),
                  coalesce(ihc.employee_mailing_country, ''),
                  coalesce(cast(ihc.employee_date_of_birth as varchar), ''),
                  coalesce(ihc.employee_gender_code, '')
                ), 'xxhash64') % 1000000000 as varchar
              ),
              9,
              '0'
            )
            else ihc.patient_account_number
          end as person_id,
          cast(null as integer) as drug_concept_id,
          CASE WHEN idc.service_line_from_date = 'N' THEN NULL
              ELSE try_cast(idc.service_line_from_date as date) END as drug_exposure_start_date,
          CASE WHEN idc.service_line_from_date = 'N' THEN NULL
              ELSE try_cast(idc.service_line_from_date as timestamp) END as drug_exposure_start_datetime,
          -- Use GREATEST of: service_line_to_date vs (start_date + days_supply), then fallback to visit_end_date
          COALESCE(
              GREATEST(
                  CASE WHEN idc.service_line_to_date = 'N' THEN NULL ELSE try_cast(idc.service_line_to_date as date) END,
                  CASE WHEN idc.service_line_from_date != 'N' AND idc.service_line_from_date IS NOT NULL
                       AND try_cast(idc.days_units_billed as integer) IS NOT NULL
                       THEN try_cast(idc.service_line_from_date as date) + try_cast(idc.days_units_billed as integer)
                       ELSE NULL END
              ),
              try_cast(ihc.reporting_period_end_date as date)
          ) as drug_exposure_end_date,
          COALESCE(
              GREATEST(
                  CASE WHEN idc.service_line_to_date = 'N' THEN NULL ELSE try_cast(idc.service_line_to_date as timestamp) END,
                  CASE WHEN idc.service_line_from_date != 'N' AND idc.service_line_from_date IS NOT NULL
                       AND try_cast(idc.days_units_billed as integer) IS NOT NULL
                       THEN try_cast(idc.service_line_from_date as timestamp) + (try_cast(idc.days_units_billed as integer) * INTERVAL '1' DAY)
                       ELSE NULL END
              ),
              try_cast(ihc.reporting_period_end_date as timestamp)
          ) as drug_exposure_end_datetime,
          CASE WHEN idc.service_line_to_date = 'N' THEN NULL
              ELSE try_cast(idc.service_line_to_date as date) END as verbatim_end_date,
          {{ drug_type_concept_id }} as drug_type_concept_id,
          cast(null as varchar) as stop_reason,
          0 as refills,
          cast(null as integer) as quantity,
          try_cast(idc.days_units_billed as integer) as days_supply,
          cast(null as varchar) as sig,
          cast(null as integer) as route_concept_id,
          cast(null as integer) as lot_number,
          cast(hash(concat_ws('||',
            ihc.rendering_bill_provider_last,
            coalesce(ihc.rendering_bill_provider_first, ''),
            ihc.rendering_bill_provider_state_1,
            ihc.rendering_bill_provider_4
          ), 'xxhash64') % 1000000000 as varchar) as provider_id,
          cast(idc.bill_id as varchar) as visit_occurrence_id,
          -- visit_detail_id uses same hash as visit_detail table
          cast(hash(concat_ws('||', idc.bill_id, idc.row_id), 'xxhash64') % 1000000000 as varchar) as visit_detail_id,
          idc.hcpcs_line_procedure_billed as drug_source_value,
          cast(null as integer) as drug_source_concept_id,
          cast(null as varchar) as route_source_value,
          cast(null as varchar) as dose_unit_source_value
        from {{ source('raw', table) }} idc
        join {{ source('raw', header_table) }} ihc
          on cast(idc.bill_id as varchar) = cast(ihc.bill_id as varchar)
        join {{ source('omop', 'concept') }} as c
          on c.concept_code = idc.hcpcs_line_procedure_billed
        where c.domain_id = 'Drug'
          and c.vocabulary_id = 'HCPCS'
      )
      {% endset %}
    
    {% elif detail_type == 'professional' %}
      {% set query %}
      {{ table }} as (
        select 
          cast(hash(concat_ws('||', prdc.bill_id, prdc.row_id), 'xxhash64') % 1000000000 as varchar) as drug_exposure_id,
          case 
            when prhc.patient_account_number is null or trim(prhc.patient_account_number) = '' then lpad(
              cast(
                hash(concat_ws('||',
                  coalesce(prhc.employee_mailing_city, ''),
                  coalesce(prhc.employee_mailing_state_code, ''),
                  coalesce(prhc.employee_mailing_postal_code, ''),
                  coalesce(prhc.employee_mailing_country, ''),
                  coalesce(cast(prhc.employee_date_of_birth as varchar), ''),
                  coalesce(prhc.employee_gender_code, '')
                ), 'xxhash64') % 1000000000 as varchar
              ),
              9,
              '0'
            )
            else prhc.patient_account_number
          end as person_id,
          cast(null as integer) as drug_concept_id,
          try_cast(prdc.service_line_from_date as date) as drug_exposure_start_date,
          try_cast(prdc.service_line_from_date as timestamp) as drug_exposure_start_datetime,
          -- Use GREATEST of: service_line_to_date vs (start_date + days_supply), then fallback to visit_end_date
          COALESCE(
              GREATEST(
                  try_cast(prdc.service_line_to_date as date),
                  CASE WHEN try_cast(prdc.service_line_from_date as date) IS NOT NULL
                       AND try_cast(prdc.days_units_billed as integer) IS NOT NULL
                       THEN try_cast(prdc.service_line_from_date as date) + try_cast(prdc.days_units_billed as integer)
                       ELSE NULL END
              ),
              try_cast(prhc.reporting_period_end_date as date)
          ) as drug_exposure_end_date,
          COALESCE(
              GREATEST(
                  try_cast(prdc.service_line_to_date as timestamp),
                  CASE WHEN try_cast(prdc.service_line_from_date as timestamp) IS NOT NULL
                       AND try_cast(prdc.days_units_billed as integer) IS NOT NULL
                       THEN try_cast(prdc.service_line_from_date as timestamp) + (try_cast(prdc.days_units_billed as integer) * INTERVAL '1' DAY)
                       ELSE NULL END
              ),
              try_cast(prhc.reporting_period_end_date as timestamp)
          ) as drug_exposure_end_datetime,
          try_cast(prdc.service_line_to_date as date) as verbatim_end_date,
          {{ drug_type_concept_id }} as drug_type_concept_id,
          cast(null as varchar) as stop_reason,
          0 as refills,
          cast(null as integer) as quantity,
          try_cast(prdc.days_units_billed as integer) as days_supply,
          cast(null as varchar) as sig,
          cast(null as integer) as route_concept_id,
          cast(null as integer) as lot_number,
          cast(hash(concat_ws('||',
            prhc.rendering_bill_provider_last,
            coalesce(prhc.rendering_bill_provider_first, ''),
            prhc.rendering_bill_provider_state_1,
            prhc.rendering_bill_provider_4
          ), 'xxhash64') % 1000000000 as varchar) as provider_id,
          cast(prdc.bill_id as varchar) as visit_occurrence_id,
          -- visit_detail_id uses same hash as visit_detail table
          cast(hash(concat_ws('||', prdc.bill_id, prdc.row_id), 'xxhash64') % 1000000000 as varchar) as visit_detail_id,
          prdc.hcpcs_line_procedure_billed as drug_source_value,
          cast(null as integer) as drug_source_concept_id,
          cast(null as varchar) as route_source_value,
          cast(null as varchar) as dose_unit_source_value
        from {{ source('raw', table) }} prdc
        join {{ source('raw', header_table) }} prhc
          on cast(prdc.bill_id as varchar) = cast(prhc.bill_id as varchar)
        join {{ source('omop', 'concept') }} as c
          on c.concept_code = prdc.hcpcs_line_procedure_billed
        where c.domain_id = 'Drug'
          and c.vocabulary_id = 'HCPCS'
      )
      {% endset %}
    {% endif %}
    
    {% do cte_queries.append(query) %}
  {% endif %}
{% endfor %}

{% if cte_queries | length > 0 %}
with {{ cte_queries | join(",\n") }}
{% endif %}

{% set valid_tables = [] %}
{% for table, header_table, detail_type in detail_table_list %}
  {% if check_table_exists('raw', table) %}
    {% do valid_tables.append(table) %}
  {% endif %}
{% endfor %}

{% if valid_tables | length > 0 %}
select *
from (
  {% for table in valid_tables %}
    select * from {{ table }}
    {% if not loop.last %}
      union all
    {% endif %}
  {% endfor %}
) as final_result
{% else %}
select null as message
{% endif %}