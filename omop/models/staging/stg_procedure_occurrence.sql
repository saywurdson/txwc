{% set exists_i_current = check_table_exists('raw', 'institutional_header_current') %}
{% set exists_i_historical = check_table_exists('raw', 'institutional_header_historical') %}
{% set exists_id_current = check_table_exists('raw', 'institutional_detail_current') %}
{% set exists_id_historical = check_table_exists('raw', 'institutional_detail_historical') %}
{% set exists_prd_current = check_table_exists('raw', 'professional_detail_current') %}
{% set exists_prd_historical = check_table_exists('raw', 'professional_detail_historical') %}

{% set header_proc_list = [
  ('final_ihc', 'institutional_header_current', 'institutional'),
  ('final_ihh', 'institutional_header_historical', 'institutional')
] %}

{% set detail_proc_list = [
  ('final_id',  'institutional_detail_current',  'institutional_header_current',  'institutional'),
  ('final_idh', 'institutional_detail_historical', 'institutional_header_historical', 'institutional'),
  ('final_pdc', 'professional_detail_current',     'professional_header_current',     'professional'),
  ('final_pdh', 'professional_detail_historical',    'professional_header_historical',    'professional')
] %}

{% set header_ctes = [] %}
{% for alias, table, htype in header_proc_list %}
  {% if check_table_exists('raw', table) %}
    {% if htype == 'institutional' %}
      {% set icd_columns = [
          ('first_icd_9cm_or_icd_10cm_1', 'first_icd_9cm_or_icd_10cm'),
          ('second_icd_9cm_or_icd_10cm_1', 'second_icd_9cm_or_icd_10cm'),
          ('third_icd_9cm_or_icd_10cm_1', 'third_icd_9cm_or_icd_10cm'),
          ('fourth_icd_9cm_or_icd_10cm_1', 'fourth_icd_9cm_or_icd_10cm'),
          ('icd_9cm_or_icd_10cm_principal', 'icd_9cm_or_icd_10cm_principal')
      ] %}
      {% set unpivot_values = [] %}
      {% for col_source, col_alias in icd_columns %}
        {% do unpivot_values.append("(" ~ col_source ~ ", '" ~ col_alias ~ "')") %}
      {% endfor %}
      
      {% set query %}
{{ alias }} as (
  with unpivot_ihc_diagnoses as (
    select 
      ihc.bill_id,
      t.icd as procedure_source_value,
      t.source_column
    from {{ source('raw', table) }} as ihc
    cross join lateral (
      values
      {{ unpivot_values | join(",\n") }}
    ) as t(icd, source_column)
    join {{ source('omop','concept') }} as c
      on c.concept_code = t.icd
    where c.domain_id = 'Procedure'
      and c.vocabulary_id in ('ICD10PCS','ICD9Proc')
  )
  select 
    cast(hash(concat_ws('||', ihc.bill_id, ihc.row_id), 'xxhash64') % 1000000000 as varchar) as procedure_occurrence_id,
    case 
      when ihc.patient_account_number is null or trim(ihc.patient_account_number) = '' then
        lpad(
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
    cast(null as integer) as procedure_concept_id,
    case 
      when unpivot_ihc_diagnoses.source_column = 'icd_9cm_or_icd_10cm_principal' then cast(ihc.principal_procedure_date as date)
      when unpivot_ihc_diagnoses.source_column = 'first_icd_9cm_or_icd_10cm' then cast(ihc.first_procedure_date as date)
      when unpivot_ihc_diagnoses.source_column = 'second_icd_9cm_or_icd_10cm' then cast(ihc.second_procedure_date as date)
      when unpivot_ihc_diagnoses.source_column = 'third_icd_9cm_or_icd_10cm' then cast(ihc.third_procedure_date as date)
      when unpivot_ihc_diagnoses.source_column = 'fourth_icd_9cm_or_icd_10cm' then cast(ihc.fourth_procedure_date as date)
      else cast(ihc.principal_procedure_date as date)
    end as procedure_date,
    case 
      when unpivot_ihc_diagnoses.source_column = 'icd_9cm_or_icd_10cm_principal' then cast(ihc.principal_procedure_date as timestamp)
      when unpivot_ihc_diagnoses.source_column = 'first_icd_9cm_or_icd_10cm' then cast(ihc.first_procedure_date as timestamp)
      when unpivot_ihc_diagnoses.source_column = 'second_icd_9cm_or_icd_10cm' then cast(ihc.second_procedure_date as timestamp)
      when unpivot_ihc_diagnoses.source_column = 'third_icd_9cm_or_icd_10cm' then cast(ihc.third_procedure_date as timestamp)
      when unpivot_ihc_diagnoses.source_column = 'fourth_icd_9cm_or_icd_10cm' then cast(ihc.fourth_procedure_date as timestamp)
      else cast(ihc.principal_procedure_date as timestamp)
    end as procedure_datetime,
    cast(null as date) as procedure_end_date,
    cast(null as timestamp) as procedure_end_datetime,
    32855 as procedure_type_concept_id,
    cast(null as integer) as modifier_concept_id,
    1 as quantity,
    cast(hash(concat_ws('||',
      ihc.rendering_bill_provider_last,
      coalesce(ihc.rendering_bill_provider_first, ''),
      ihc.rendering_bill_provider_state_1,
      ihc.rendering_bill_provider_4
    ), 'xxhash64') % 1000000000 as varchar) as provider_id,
    cast(ihc.bill_id as varchar) as visit_occurrence_id,
    cast(null as integer) as visit_detail_id,
    unpivot_ihc_diagnoses.procedure_source_value,
    cast(null as integer) as procedure_source_concept_id,
    cast(null as varchar) as modifier_source_value
  from {{ source('raw', table) }} as ihc
  join unpivot_ihc_diagnoses on cast(ihc.bill_id as varchar) = cast(unpivot_ihc_diagnoses.bill_id as varchar)
)
      {% endset %}
    {% elif htype == 'professional' %}
      {% set icd_columns = [
          ('first_icd_9cm_or_icd_10cm', 'first_icd_9cm_or_icd_10cm'),
          ('second_icd_9cm_or_icd_10cm', 'second_icd_9cm_or_icd_10cm'),
          ('third_icd_9cm_or_icd_10cm', 'third_icd_9cm_or_icd_10cm'),
          ('fourth_icd_9cm_or_icd_10cm', 'fourth_icd_9cm_or_icd_10cm'),
          ('fifth_icd_9cm_or_icd_10cm', 'fifth_icd_9cm_or_icd_10cm')
      ] %}
      {% set unpivot_values = [] %}
      {% for col, alias_name in icd_columns %}
        {% do unpivot_values.append("(" ~ col ~ ", '" ~ alias_name ~ "')") %}
      {% endfor %}
      {% set query %}
{{ alias }} as (
  with unpivot_phc_diagnoses as (
    select 
      phc.bill_id,
      t.icd as procedure_source_value,
      t.source_column
    from {{ source('raw', table) }} as phc
    cross join lateral (
      values
      {{ unpivot_values | join(",\n") }}
    ) as t(icd, source_column)
    join {{ source('omop','concept') }} as c
      on c.concept_code = t.icd
    where c.domain_id = 'Procedure'
      and c.vocabulary_id in ('ICD10PCS','ICD9Proc')
  )
  select 
    cast(hash(concat_ws('||', phc.row_id, phc.bill_id), 'xxhash64') % 1000000000 as varchar) as procedure_occurrence_id,
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
    cast(null as integer) as procedure_concept_id,
    case 
      when unpivot_phc_diagnoses.source_column = 'first_icd_9cm_or_icd_10cm' then cast(phc.first_procedure_date as date)
      when unpivot_phc_diagnoses.source_column = 'second_icd_9cm_or_icd_10cm' then cast(phc.second_procedure_date as date)
      when unpivot_phc_diagnoses.source_column = 'third_icd_9cm_or_icd_10cm' then cast(phc.third_procedure_date as date)
      when unpivot_phc_diagnoses.source_column = 'fourth_icd_9cm_or_icd_10cm' then cast(phc.fourth_procedure_date as date)
      when unpivot_phc_diagnoses.source_column = 'fifth_icd_9cm_or_icd_10cm' then cast(phc.fourth_procedure_date as date)
      else cast(phc.first_procedure_date as date)
    end as procedure_date,
    case 
      when unpivot_phc_diagnoses.source_column = 'first_icd_9cm_or_icd_10cm' then cast(phc.first_procedure_date as timestamp)
      when unpivot_phc_diagnoses.source_column = 'second_icd_9cm_or_icd_10cm' then cast(phc.second_procedure_date as timestamp)
      when unpivot_phc_diagnoses.source_column = 'third_icd_9cm_or_icd_10cm' then cast(phc.third_procedure_date as timestamp)
      when unpivot_phc_diagnoses.source_column = 'fourth_icd_9cm_or_icd_10cm' then cast(phc.fourth_procedure_date as timestamp)
      when unpivot_phc_diagnoses.source_column = 'fifth_icd_9cm_or_icd_10cm' then cast(phc.fourth_procedure_date as timestamp)
      else cast(phc.first_procedure_date as timestamp)
    end as procedure_datetime,
    cast(null as date) as procedure_end_date,
    cast(null as timestamp) as procedure_end_datetime,
    32855 as procedure_type_concept_id,
    cast(null as integer) as modifier_concept_id,
    1 as quantity,
    cast(hash(concat_ws('||',
      phc.rendering_bill_provider_last,
      coalesce(phc.rendering_bill_provider_first, ''),
      phc.rendering_bill_provider_state_1,
      phc.rendering_bill_provider_4
    ), 'xxhash64') % 1000000000 as varchar) as provider_id,
    cast(phc.bill_id as varchar) as visit_occurrence_id,
    cast(null as integer) as visit_detail_id,
    unpivot_phc_diagnoses.procedure_source_value,
    cast(null as integer) as procedure_source_concept_id,
    cast(null as varchar) as modifier_source_value
  from {{ source('raw', table) }} as phc
  join unpivot_phc_diagnoses on cast(phc.bill_id as varchar) = cast(unpivot_phc_diagnoses.bill_id as varchar)
)
      {% endset %}
    {% endif %}
    {% do header_ctes.append(query) %}
  {% endif %}
{% endfor %}

{% set detail_ctes = [] %}
{% for alias, detail_table, header_table, dtype in detail_proc_list %}
  {% if check_table_exists('raw', detail_table) and check_table_exists('raw', header_table) %}
    {% if dtype == 'institutional' %}
      {% set query %}
{{ alias }} as (
  select 
    cast(hash(concat_ws('||', id.bill_id, id.row_id), 'xxhash64') % 1000000000 as varchar) as procedure_occurrence_id,
    case 
      when ihc.patient_account_number is null or trim(ihc.patient_account_number) = '' then
        lpad(
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
    cast(null as integer) as procedure_concept_id,
    cast(id.service_line_from_date as date) as procedure_date,
    cast(id.service_line_from_date as timestamp) as procedure_datetime,
    cast(id.service_line_to_date as date) as procedure_end_date,
    cast(id.service_line_to_date as timestamp) as procedure_end_datetime,
    32854 as procedure_type_concept_id,
    cast(null as integer) as modifier_concept_id,
    cast(id.days_units_billed as integer) as quantity,
    cast(hash(concat_ws('||',
      ihc.rendering_bill_provider_last,
      coalesce(ihc.rendering_bill_provider_first, ''),
      ihc.rendering_bill_provider_state_1,
      ihc.rendering_bill_provider_4
    ), 'xxhash64') % 1000000000 as varchar) as provider_id,
    cast(id.bill_id as varchar) as visit_occurrence_id,
    cast(null as integer) as visit_detail_id,
    id.hcpcs_line_procedure_billed as procedure_source_value,
    cast(null as integer) as procedure_source_concept_id,
    id.first_hcpcs_modifier_billed as modifier_source_value
  from {{ source('raw', detail_table) }} id
  join {{ source('raw', header_table) }} ihc
    on cast(id.bill_id as varchar) = cast(ihc.bill_id as varchar)
  join {{ source('omop','concept') }} as c
    on c.concept_code = id.hcpcs_line_procedure_billed
  where c.domain_id = 'Procedure'
    and c.vocabulary_id in ('CPT4','HCPCS')
)
      {% endset %}
    {% elif dtype == 'professional' %}
      {% set query %}
{{ alias }} as (
  select 
    cast(hash(concat_ws('||', prd.bill_id, prd.row_id), 'xxhash64') % 1000000000 as varchar) as procedure_occurrence_id,
    case 
      when prhc.patient_account_number is null or trim(prhc.patient_account_number) = '' then
        lpad(
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
    cast(null as integer) as procedure_concept_id,
    cast(prd.service_line_from_date as date) as procedure_date,
    cast(prd.service_line_from_date as timestamp) as procedure_datetime,
    cast(prd.service_line_to_date as date) as procedure_end_date,
    cast(prd.service_line_to_date as timestamp) as procedure_end_datetime,
    32854 as procedure_type_concept_id,
    cast(null as integer) as modifier_concept_id,
    cast(prd.days_units_billed as integer) as quantity,
    cast(hash(concat_ws('||',
      prhc.rendering_bill_provider_last,
      coalesce(prhc.rendering_bill_provider_first, ''),
      prhc.rendering_bill_provider_state_1,
      prhc.rendering_bill_provider_4
    ), 'xxhash64') % 1000000000 as varchar) as provider_id,
    cast(prd.bill_id as varchar) as visit_occurrence_id,
    cast(null as integer) as visit_detail_id,
    prd.hcpcs_line_procedure_billed as procedure_source_value,
    cast(null as integer) as procedure_source_concept_id,
    prd.first_hcpcs_modifier_billed as modifier_source_value
  from {{ source('raw', detail_table) }} prd
  join {{ source('raw', header_table) }} prhc
    on cast(prd.bill_id as varchar) = cast(prhc.bill_id as varchar)
  join {{ source('omop','concept') }} as c
    on c.concept_code = prd.hcpcs_line_procedure_billed
  where c.domain_id = 'Procedure'
    and c.vocabulary_id in ('CPT4','HCPCS')
)
      {% endset %}
    {% endif %}
    {% do detail_ctes.append(query) %}
  {% endif %}
{% endfor %}

{% set union_list = [] %}
{% for alias, table, htype in header_proc_list %}
  {% if check_table_exists('raw', table) %}
    {% do union_list.append("select * from " ~ alias) %}
  {% endif %}
{% endfor %}
{% for alias, detail_table, header_table, dtype in detail_proc_list %}
  {% if check_table_exists('raw', detail_table) and check_table_exists('raw', header_table) %}
    {% do union_list.append("select * from " ~ alias) %}
  {% endif %}
{% endfor %}

{% if (header_ctes | length) or (detail_ctes | length) %}
with
  {{ (header_ctes | join(",\n")) ~ ((detail_ctes | length > 0) and ",\n" or "") ~ (detail_ctes | join(",\n")) }}
{% endif %}

select *
from (
  {{ union_list | join(" union ") }}
) as final_result