{% set header_measurement_list = [
  ('final_ihc', 'institutional_header_current', 'institutional'),
  ('final_ihh', 'institutional_header_historical', 'institutional'),
  ('final_phc', 'professional_header_current', 'professional'),
  ('final_phh', 'professional_header_historical', 'professional')
] %}

{% set detail_measurement_list = [
  ('institutional_detail_current', 'institutional_detail_current', 'institutional_header_current', 'institutional'),
  ('institutional_detail_historical', 'institutional_detail_historical', 'institutional_header_historical', 'institutional'),
  ('professional_detail_current', 'professional_detail_current', 'professional_header_current', 'professional'),
  ('professional_detail_historical', 'professional_detail_historical', 'professional_header_historical', 'professional')
] %}

{% set measurement_ctes = [] %}

{% for alias, table, htype in header_measurement_list %}
  {% if check_table_exists('raw', table) %}
    {% if htype == 'institutional' %}
      {% set query %}
{{ alias }} as (
  with unpivot_cte as (
    select 
      ihc.bill_id,
      t.icd as measurement_source_value,
      t.source_column,
      t.priority,
      row_number() over (
         partition by ihc.bill_id, t.icd
         order by t.priority
      ) as rn
    from {{ source('raw', table) }} as ihc
    cross join lateral (
      values
        (first_icd_9cm_or_icd_10cm, 'first_icd_9cm_or_icd_10cm', 3),
        (second_icd_9cm_or_icd_10cm, 'second_icd_9cm_or_icd_10cm', 3),
        (third_icd_9cm_or_icd_10cm, 'third_icd_9cm_or_icd_10cm', 3),
        (fourth_icd_9cm_or_icd_10cm, 'fourth_icd_9cm_or_icd_10cm', 3),
        (fifth_icd_9cm_or_icd_10cm, 'fifth_icd_9cm_or_icd_10cm', 3),
        (principal_diagnosis_code, 'principal_diagnosis_code', 1),
        (admitting_diagnosis_code, 'admitting_diagnosis_code', 2)
    ) as t(icd, source_column, priority)
    join {{ source('omop','concept') }} as c
      on c.concept_code = t.icd
    where c.domain_id = 'Measurement'
      and c.vocabulary_id in ('ICD10CM','ICD9CM')
  ),
  unique_diag as (
    select bill_id, measurement_source_value, source_column, priority
    from unpivot_cte
    where rn = 1
  )
  select 
    cast(hash(concat_ws('||', ihc.row_id, ihc.bill_id), 'xxhash64') % 1000000000 as varchar) as measurement_id,
    case 
      when ihc.patient_account_number is null or trim(ihc.patient_account_number) = ''
      then lpad(
        cast(hash(concat_ws('||',
          coalesce(ihc.employee_mailing_city, ''),
          coalesce(ihc.employee_mailing_state_code, ''),
          coalesce(ihc.employee_mailing_postal_code, ''),
          coalesce(ihc.employee_mailing_country, ''),
          coalesce(cast(ihc.employee_date_of_birth as varchar), ''),
          coalesce(ihc.employee_gender_code, '')
        ), 'xxhash64') % 1000000000 as varchar),
        9,
        '0'
      )
      else ihc.patient_account_number
    end as person_id,
    cast(null as integer) as measurement_concept_id,
    cast(ihc.reporting_period_start_date as date) as measurement_date,
    cast(ihc.reporting_period_start_date as timestamp) as measurement_datetime,
    strftime(cast(ihc.reporting_period_start_date as timestamp), '%H:%M:%S') as measurement_time,
    32855 as measurement_type_concept_id,
    cast(null as integer) as operator_concept_id,
    cast(null as float) as value_as_number,
    cast(null as integer) as value_as_concept_id,
    cast(null as integer) as unit_concept_id,
    cast(null as integer) as range_low,
    cast(null as integer) as range_high,
    cast(hash(concat_ws('||',
      ihc.rendering_bill_provider_last,
      coalesce(ihc.rendering_bill_provider_first, ''),
      ihc.rendering_bill_provider_state_1,
      ihc.rendering_bill_provider_4
    ), 'xxhash64') % 1000000000 as varchar) as provider_id,
    cast(ihc.bill_id as varchar) as visit_occurrence_id,
    cast(null as integer) as visit_detail_id,
    unique_diag.measurement_source_value,
    cast(null as integer) as measurement_source_concept_id,
    cast(null as varchar) as unit_source_value,
    cast(null as integer) as unit_source_concept_id,
    cast(null as varchar) as value_source_value,
    cast(null as integer) as measurement_event_id,
    cast(null as integer) as meas_event_field_concept_id
  from {{ source('raw', table) }} as ihc
  join unique_diag on cast(ihc.bill_id as varchar) = cast(unique_diag.bill_id as varchar)
)
      {% endset %}
    {% elif htype == 'professional' %}
      {% set query %}
{{ alias }} as (
  with unpivot_cte as (
    select 
      phc.bill_id,
      t.icd as measurement_source_value,
      t.source_column
    from {{ source('raw', table) }} as phc
    cross join lateral (
      values
        (first_icd_9cm_or_icd_10cm, 'first_icd_9cm_or_icd_10cm'),
        (second_icd_9cm_or_icd_10cm, 'second_icd_9cm_or_icd_10cm'),
        (third_icd_9cm_or_icd_10cm, 'third_icd_9cm_or_icd_10cm'),
        (fourth_icd_9cm_or_icd_10cm, 'fourth_icd_9cm_or_icd_10cm'),
        (fifth_icd_9cm_or_icd_10cm, 'fifth_icd_9cm_or_icd_10cm')
    ) as t(icd, source_column)
    join {{ source('omop','concept') }} as c
      on c.concept_code = t.icd
    where c.domain_id = 'Measurement'
      and c.vocabulary_id in ('ICD10CM','ICD9CM')
  )
  select 
    cast(hash(concat_ws('||', phc.row_id, phc.bill_id), 'xxhash64') % 1000000000 as varchar) as measurement_id,
    case 
      when phc.patient_account_number is null or trim(phc.patient_account_number) = ''
      then lpad(
        cast(hash(concat_ws('||',
          coalesce(phc.employee_mailing_city, ''),
          coalesce(phc.employee_mailing_state_code, ''),
          coalesce(phc.employee_mailing_postal_code, ''),
          coalesce(phc.employee_mailing_country, ''),
          coalesce(cast(phc.employee_date_of_birth as varchar), ''),
          coalesce(phc.employee_gender_code, '')
        ), 'xxhash64') % 1000000000 as varchar),
        9,
        '0'
      )
      else phc.patient_account_number
    end as person_id,
    cast(null as integer) as measurement_concept_id,
    cast(phc.reporting_period_start_date as date) as measurement_date,
    cast(phc.reporting_period_start_date as timestamp) as measurement_datetime,
    strftime(cast(phc.reporting_period_start_date as timestamp), '%H:%M:%S') as measurement_time,
    32855 as measurement_type_concept_id,
    cast(null as integer) as operator_concept_id,
    cast(null as float) as value_as_number,
    cast(null as integer) as value_as_concept_id,
    cast(null as integer) as unit_concept_id,
    cast(null as integer) as range_low,
    cast(null as integer) as range_high,
    cast(hash(concat_ws('||',
      phc.rendering_bill_provider_last,
      coalesce(phc.rendering_bill_provider_first, ''),
      phc.rendering_bill_provider_state_1,
      phc.rendering_bill_provider_4
    ), 'xxhash64') % 1000000000 as varchar) as provider_id,
    cast(phc.bill_id as varchar) as visit_occurrence_id,
    cast(null as integer) as visit_detail_id,
    unpivot_cte.measurement_source_value,
    cast(null as integer) as measurement_source_concept_id,
    cast(null as varchar) as unit_source_value,
    cast(null as integer) as unit_source_concept_id,
    cast(null as varchar) as value_source_value,
    cast(null as integer) as measurement_event_id,
    cast(null as integer) as meas_event_field_concept_id
  from {{ source('raw', table) }} as phc
  join unpivot_cte on cast(phc.bill_id as varchar) = cast(unpivot_cte.bill_id as varchar)
)
      {% endset %}
    {% endif %}
    {% do measurement_ctes.append(query) %}
  {% endif %}
{% endfor %}

{% for alias, detail_table, header_table, dtype in detail_measurement_list %}
  {% if check_table_exists('raw', detail_table) and check_table_exists('raw', header_table) %}
    {% if dtype == 'institutional' %}
      {% set query %}
{{ alias }} as (
  select 
    cast(hash(concat_ws('||', idc.bill_id, idc.row_id), 'xxhash64') % 1000000000 as varchar) as measurement_id,
    case 
      when ihc.patient_account_number is null or trim(ihc.patient_account_number) = ''
      then lpad(
        cast(hash(concat_ws('||',
          coalesce(ihc.employee_mailing_city, ''),
          coalesce(ihc.employee_mailing_state_code, ''),
          coalesce(ihc.employee_mailing_postal_code, ''),
          coalesce(ihc.employee_mailing_country, ''),
          coalesce(cast(ihc.employee_date_of_birth as varchar), ''),
          coalesce(ihc.employee_gender_code, '')
        ), 'xxhash64') % 1000000000 as varchar),
        9,
        '0'
      )
      else ihc.patient_account_number
    end as person_id,
    cast(null as integer) as measurement_concept_id,
    cast(idc.service_line_from_date as date) as measurement_date,
    cast(idc.service_line_from_date as timestamp) as measurement_datetime,
    strftime(cast(idc.service_line_from_date as timestamp), '%H:%M:%S') as measurement_time,
    32854 as measurement_type_concept_id,
    cast(null as integer) as operator_concept_id,
    cast(null as float) as value_as_number,
    cast(null as integer) as value_as_concept_id,
    cast(null as integer) as unit_concept_id,
    cast(null as integer) as range_low,
    cast(null as integer) as range_high,
    cast(hash(concat_ws('||',
      ihc.rendering_bill_provider_last,
      coalesce(ihc.rendering_bill_provider_first, ''),
      ihc.rendering_bill_provider_state_1,
      ihc.rendering_bill_provider_4
    ), 'xxhash64') % 1000000000 as varchar) as provider_id,
    cast(idc.bill_id as varchar) as visit_occurrence_id,
    cast(null as integer) as visit_detail_id,
    idc.hcpcs_line_procedure_billed as measurement_source_value,
    cast(null as integer) as measurement_source_concept_id,
    cast(null as varchar) as unit_source_value,
    cast(null as integer) as unit_source_concept_id,
    cast(null as varchar) as value_source_value,
    cast(null as integer) as measurement_event_id,
    cast(null as integer) as meas_event_field_concept_id
  from {{ source('raw', detail_table) }} idc
  join {{ source('raw', header_table) }} ihc
    on cast(idc.bill_id as varchar) = cast(ihc.bill_id as varchar)
  join {{ source('omop','concept') }} as c
    on c.concept_code = idc.hcpcs_line_procedure_billed
  where c.domain_id = 'Measurement'
    and c.vocabulary_id = 'HCPCS'
)
      {% endset %}
    {% elif dtype == 'professional' %}
      {% set query %}
{{ alias }} as (
  select 
    cast(hash(concat_ws('||', prdc.bill_id, prdc.row_id), 'xxhash64') % 1000000000 as varchar) as measurement_id,
    case 
      when prhc.patient_account_number is null or trim(prhc.patient_account_number) = ''
      then lpad(
        cast(hash(concat_ws('||',
          coalesce(prhc.employee_mailing_city, ''),
          coalesce(prhc.employee_mailing_state_code, ''),
          coalesce(prhc.employee_mailing_postal_code, ''),
          coalesce(prhc.employee_mailing_country, ''),
          coalesce(cast(prhc.employee_date_of_birth as varchar), ''),
          coalesce(prhc.employee_gender_code, '')
        ), 'xxhash64') % 1000000000 as varchar),
        9,
        '0'
      )
      else prhc.patient_account_number
    end as person_id,
    cast(null as integer) as measurement_concept_id,
    cast(prdc.service_line_from_date as date) as measurement_date,
    cast(prdc.service_line_from_date as timestamp) as measurement_datetime,
    strftime(cast(prdc.service_line_from_date as timestamp), '%H:%M:%S') as measurement_time,
    32854 as measurement_type_concept_id,
    cast(null as integer) as operator_concept_id,
    cast(null as float) as value_as_number,
    cast(null as integer) as value_as_concept_id,
    cast(null as integer) as unit_concept_id,
    cast(null as integer) as range_low,
    cast(null as integer) as range_high,
    cast(hash(concat_ws('||',
      prhc.rendering_bill_provider_last,
      coalesce(prhc.rendering_bill_provider_first, ''),
      prhc.rendering_bill_provider_state_1,
      prhc.rendering_bill_provider_4
    ), 'xxhash64') % 1000000000 as varchar) as provider_id,
    cast(prdc.bill_id as varchar) as visit_occurrence_id,
    cast(null as integer) as visit_detail_id,
    prdc.hcpcs_line_procedure_billed as measurement_source_value,
    cast(null as integer) as measurement_source_concept_id,
    cast(null as varchar) as unit_source_value,
    cast(null as integer) as unit_source_concept_id,
    cast(null as varchar) as value_source_value,
    cast(null as integer) as measurement_event_id,
    cast(null as integer) as meas_event_field_concept_id
  from {{ source('raw', detail_table) }} prdc
  join {{ source('raw', header_table) }} prhc
    on cast(prdc.bill_id as varchar) = cast(prhc.bill_id as varchar)
  join {{ source('omop','concept') }} as c
    on c.concept_code = prdc.hcpcs_line_procedure_billed
  where c.domain_id = 'Measurement'
    and c.vocabulary_id = 'HCPCS'
)
      {% endset %}
    {% endif %}
    {% do measurement_ctes.append(query) %}
  {% endif %}
{% endfor %}

{% if measurement_ctes | length > 0 %}
with {{ measurement_ctes | join(",\n") }}
{% endif %}

{% set union_list = [] %}
{% for alias, table, htype in header_measurement_list %}
  {% if check_table_exists('raw', table) %}
    {% do union_list.append("select * from " ~ alias) %}
  {% endif %}
{% endfor %}
{% for alias, detail_table, header_table, dtype in detail_measurement_list %}
  {% if check_table_exists('raw', detail_table) and check_table_exists('raw', header_table) %}
    {% do union_list.append("select * from " ~ alias) %}
  {% endif %}
{% endfor %}

select *
from (
  {{ union_list | join(" union ") }}
) as final_result