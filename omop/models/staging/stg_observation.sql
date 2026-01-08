{% set has_current = check_table_exists('raw', 'institutional_header_current') %}
{% set has_historical = check_table_exists('raw', 'institutional_header_historical') %}

{% set header_ctes = [] %}
{% set detail_ctes = [] %}

{% if has_current %}
  {# Institutional header current - ICD observations #}
  {% set query %}
final_ihc as (
  with unpivot_cte as (
    select
      ihc.bill_id,
      t.icd as observation_source_value,
      t.source_column,
      t.priority,
      row_number() over (
        partition by ihc.bill_id, t.icd
        order by t.priority
      ) as rn
    from {{ source('raw', 'institutional_header_current') }} as ihc
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
    where c.domain_id = 'Observation'
      and c.vocabulary_id in ('ICD10CM','ICD9CM')
  ),
  unique_diag as (
    select bill_id, observation_source_value, source_column, priority
    from unpivot_cte
    where rn = 1
  ),
  -- RECOVERY: Extract ICD observation codes from billing_provider_last_name when columns are shifted
  recovered_observations as (
    select
      ihc.bill_id,
      ihc.billing_provider_last_name as observation_source_value,
      'billing_provider_last_name_recovered' as source_column,
      99 as priority  -- Lowest priority since recovered
    from {{ source('raw', 'institutional_header_current') }} as ihc
    join {{ source('omop','concept') }} as c
      on c.concept_code = ihc.billing_provider_last_name
    where c.domain_id = 'Observation'
      and c.vocabulary_id in ('ICD10CM','ICD9CM')
      and LENGTH(ihc.billing_provider_state_code) > 2  -- Indicates column shift
      -- Only recover if not already in unique_diag
      and ihc.bill_id not in (select bill_id from unique_diag where observation_source_value = ihc.billing_provider_last_name)
  ),
  -- Combine normal observations with recovered ones
  all_observations as (
    select bill_id, observation_source_value, source_column, priority
    from unique_diag
    union all
    select bill_id, observation_source_value, source_column, priority
    from recovered_observations
  )
  select
    cast(hash(concat_ws('||', ihc.row_id, ihc.bill_id), 'xxhash64') % 1000000000 as varchar) as observation_id,
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
    cast(null as integer) as observation_concept_id,
    cast(ihc.reporting_period_start_date as date) as observation_date,
    cast(ihc.reporting_period_start_date as timestamp) as observation_datetime,
    32855 as observation_type_concept_id,
    cast(null as float) as value_as_number,
    cast(null as varchar) as value_as_string,
    cast(null as integer) as value_as_concept_id,
    cast(null as integer) as qualifier_concept_id,
    cast(null as integer) as unit_concept_id,
    cast(hash(concat_ws('||',
      ihc.rendering_bill_provider_last,
      coalesce(ihc.rendering_bill_provider_first, ''),
      ihc.rendering_bill_provider_state_1,
      ihc.rendering_bill_provider_4
    ), 'xxhash64') % 1000000000 as varchar) as provider_id,
    cast(ihc.bill_id as varchar) as visit_occurrence_id,
    -- Header-based ICD observations don't have a corresponding detail line, so no visit_detail_id
    cast(null as varchar) as visit_detail_id,
    all_observations.observation_source_value,
    cast(null as integer) as observation_source_concept_id,
    cast(null as varchar) as unit_source_value,
    cast(null as varchar) as qualifier_source_value,
    cast(null as varchar) as value_source_value,
    cast(null as integer) as observation_event_id,
    cast(null as integer) as obs_event_field_concept_id
  from {{ source('raw', 'institutional_header_current') }} as ihc
  join all_observations on cast(ihc.bill_id as varchar) = cast(all_observations.bill_id as varchar)
)
  {% endset %}
  {% do header_ctes.append(query) %}

  {# Professional header current - ICD observations #}
  {% set query %}
final_phc as (
  with unpivot_cte as (
    select
      phc.bill_id,
      t.icd as observation_source_value,
      t.source_column
    from {{ source('raw', 'professional_header_current') }} as phc
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
    where c.domain_id = 'Observation'
      and c.vocabulary_id in ('ICD10CM','ICD9CM')
  )
  select
    cast(hash(concat_ws('||', phc.row_id, phc.bill_id), 'xxhash64') % 1000000000 as varchar) as observation_id,
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
    cast(null as integer) as observation_concept_id,
    cast(phc.reporting_period_start_date as date) as observation_date,
    cast(phc.reporting_period_start_date as timestamp) as observation_datetime,
    32855 as observation_type_concept_id,
    cast(null as float) as value_as_number,
    cast(null as varchar) as value_as_string,
    cast(null as integer) as value_as_concept_id,
    cast(null as integer) as qualifier_concept_id,
    cast(null as integer) as unit_concept_id,
    cast(hash(concat_ws('||',
      phc.rendering_bill_provider_last,
      coalesce(phc.rendering_bill_provider_first, ''),
      phc.rendering_bill_provider_state_1,
      phc.rendering_bill_provider_4
    ), 'xxhash64') % 1000000000 as varchar) as provider_id,
    cast(phc.bill_id as varchar) as visit_occurrence_id,
    -- Header-based ICD observations don't have a corresponding detail line, so no visit_detail_id
    cast(null as varchar) as visit_detail_id,
    unpivot_cte.observation_source_value,
    cast(null as integer) as observation_source_concept_id,
    cast(null as varchar) as unit_source_value,
    cast(null as varchar) as qualifier_source_value,
    cast(null as varchar) as value_source_value,
    cast(null as integer) as observation_event_id,
    cast(null as integer) as obs_event_field_concept_id
  from {{ source('raw', 'professional_header_current') }} as phc
  join unpivot_cte on cast(phc.bill_id as varchar) = cast(unpivot_cte.bill_id as varchar)
)
  {% endset %}
  {% do header_ctes.append(query) %}

  {# Institutional detail current - HCPCS observations #}
  {% set query %}
institutional_detail_current as (
  select
    cast(hash(concat_ws('||', idc.bill_id, idc.row_id), 'xxhash64') % 1000000000 as varchar) as observation_id,
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
    cast(null as integer) as observation_concept_id,
    CASE WHEN idc.service_line_from_date = 'N' THEN NULL
      ELSE cast(idc.service_line_from_date as date) END as observation_date,
    CASE WHEN idc.service_line_from_date = 'N' THEN NULL
      ELSE cast(idc.service_line_from_date as timestamp) END as observation_datetime,
    32854 as observation_type_concept_id,
    cast(null as float) as value_as_number,
    cast(null as varchar) as value_as_string,
    cast(null as integer) as value_as_concept_id,
    cast(null as integer) as qualifier_concept_id,
    cast(null as integer) as unit_concept_id,
    cast(hash(concat_ws('||',
      ihc.rendering_bill_provider_last,
      coalesce(ihc.rendering_bill_provider_first, ''),
      ihc.rendering_bill_provider_state_1,
      ihc.rendering_bill_provider_4
    ), 'xxhash64') % 1000000000 as varchar) as provider_id,
    cast(idc.bill_id as varchar) as visit_occurrence_id,
    -- Detail-based HCPCS observations link to visit_detail via bill_id + row_id hash
    cast(hash(concat_ws('||', idc.bill_id, idc.row_id), 'xxhash64') % 1000000000 as varchar) as visit_detail_id,
    idc.hcpcs_line_procedure_billed as observation_source_value,
    cast(null as integer) as observation_source_concept_id,
    cast(null as varchar) as unit_source_value,
    cast(null as varchar) as qualifier_source_value,
    cast(null as varchar) as value_source_value,
    cast(null as integer) as observation_event_id,
    cast(null as integer) as obs_event_field_concept_id
  from {{ source('raw', 'institutional_detail_current') }} idc
  join {{ source('raw', 'institutional_header_current') }} ihc
    on cast(idc.bill_id as varchar) = cast(ihc.bill_id as varchar)
  join {{ source('omop','concept') }} as c
    on c.concept_code = idc.hcpcs_line_procedure_billed
  where c.domain_id = 'Observation'
    and c.vocabulary_id = 'HCPCS'
)
  {% endset %}
  {% do detail_ctes.append(query) %}

  {# Professional detail current - HCPCS observations #}
  {% set query %}
professional_detail_current as (
  select
    cast(hash(concat_ws('||', prdc.bill_id, prdc.row_id), 'xxhash64') % 1000000000 as varchar) as observation_id,
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
    cast(null as integer) as observation_concept_id,
    cast(prdc.service_line_from_date as date) as observation_date,
    cast(prdc.service_line_from_date as timestamp) as observation_datetime,
    32854 as observation_type_concept_id,
    cast(null as float) as value_as_number,
    cast(null as varchar) as value_as_string,
    cast(null as integer) as value_as_concept_id,
    cast(null as integer) as qualifier_concept_id,
    cast(null as integer) as unit_concept_id,
    cast(hash(concat_ws('||',
      prhc.rendering_bill_provider_last,
      coalesce(prhc.rendering_bill_provider_first, ''),
      prhc.rendering_bill_provider_state_1,
      prhc.rendering_bill_provider_4
    ), 'xxhash64') % 1000000000 as varchar) as provider_id,
    cast(prdc.bill_id as varchar) as visit_occurrence_id,
    -- Detail-based HCPCS observations link to visit_detail via bill_id + row_id hash
    cast(hash(concat_ws('||', prdc.bill_id, prdc.row_id), 'xxhash64') % 1000000000 as varchar) as visit_detail_id,
    prdc.hcpcs_line_procedure_billed as observation_source_value,
    cast(null as integer) as observation_source_concept_id,
    cast(null as varchar) as unit_source_value,
    cast(null as varchar) as qualifier_source_value,
    cast(null as varchar) as value_source_value,
    cast(null as integer) as observation_event_id,
    cast(null as integer) as obs_event_field_concept_id
  from {{ source('raw', 'professional_detail_current') }} prdc
  join {{ source('raw', 'professional_header_current') }} prhc
    on cast(prdc.bill_id as varchar) = cast(prhc.bill_id as varchar)
  join {{ source('omop','concept') }} as c
    on c.concept_code = prdc.hcpcs_line_procedure_billed
  where c.domain_id = 'Observation'
    and c.vocabulary_id = 'HCPCS'
)
  {% endset %}
  {% do detail_ctes.append(query) %}
{% endif %}

{% if has_historical %}
  {# Institutional header historical - ICD observations #}
  {% set query %}
final_ihh as (
  with unpivot_cte as (
    select
      ihc.bill_id,
      t.icd as observation_source_value,
      t.source_column,
      t.priority,
      row_number() over (
        partition by ihc.bill_id, t.icd
        order by t.priority
      ) as rn
    from {{ source('raw', 'institutional_header_historical') }} as ihc
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
    where c.domain_id = 'Observation'
      and c.vocabulary_id in ('ICD10CM','ICD9CM')
  ),
  unique_diag as (
    select bill_id, observation_source_value, source_column, priority
    from unpivot_cte
    where rn = 1
  ),
  recovered_observations as (
    select
      ihc.bill_id,
      ihc.billing_provider_last_name as observation_source_value,
      'billing_provider_last_name_recovered' as source_column,
      99 as priority
    from {{ source('raw', 'institutional_header_historical') }} as ihc
    join {{ source('omop','concept') }} as c
      on c.concept_code = ihc.billing_provider_last_name
    where c.domain_id = 'Observation'
      and c.vocabulary_id in ('ICD10CM','ICD9CM')
      and LENGTH(ihc.billing_provider_state_code) > 2
      and ihc.bill_id not in (select bill_id from unique_diag where observation_source_value = ihc.billing_provider_last_name)
  ),
  all_observations as (
    select bill_id, observation_source_value, source_column, priority
    from unique_diag
    union all
    select bill_id, observation_source_value, source_column, priority
    from recovered_observations
  )
  select
    cast(hash(concat_ws('||', ihc.row_id, ihc.bill_id), 'xxhash64') % 1000000000 as varchar) as observation_id,
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
    cast(null as integer) as observation_concept_id,
    cast(ihc.reporting_period_start_date as date) as observation_date,
    cast(ihc.reporting_period_start_date as timestamp) as observation_datetime,
    32855 as observation_type_concept_id,
    cast(null as float) as value_as_number,
    cast(null as varchar) as value_as_string,
    cast(null as integer) as value_as_concept_id,
    cast(null as integer) as qualifier_concept_id,
    cast(null as integer) as unit_concept_id,
    cast(hash(concat_ws('||',
      ihc.rendering_bill_provider_last,
      coalesce(ihc.rendering_bill_provider_first, ''),
      ihc.rendering_bill_provider_state_1,
      ihc.rendering_bill_provider_4
    ), 'xxhash64') % 1000000000 as varchar) as provider_id,
    cast(ihc.bill_id as varchar) as visit_occurrence_id,
    cast(null as varchar) as visit_detail_id,
    all_observations.observation_source_value,
    cast(null as integer) as observation_source_concept_id,
    cast(null as varchar) as unit_source_value,
    cast(null as varchar) as qualifier_source_value,
    cast(null as varchar) as value_source_value,
    cast(null as integer) as observation_event_id,
    cast(null as integer) as obs_event_field_concept_id
  from {{ source('raw', 'institutional_header_historical') }} as ihc
  join all_observations on cast(ihc.bill_id as varchar) = cast(all_observations.bill_id as varchar)
)
  {% endset %}
  {% do header_ctes.append(query) %}

  {# Professional header historical - ICD observations #}
  {% set query %}
final_phh as (
  with unpivot_cte as (
    select
      phc.bill_id,
      t.icd as observation_source_value,
      t.source_column
    from {{ source('raw', 'professional_header_historical') }} as phc
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
    where c.domain_id = 'Observation'
      and c.vocabulary_id in ('ICD10CM','ICD9CM')
  )
  select
    cast(hash(concat_ws('||', phc.row_id, phc.bill_id), 'xxhash64') % 1000000000 as varchar) as observation_id,
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
    cast(null as integer) as observation_concept_id,
    cast(phc.reporting_period_start_date as date) as observation_date,
    cast(phc.reporting_period_start_date as timestamp) as observation_datetime,
    32855 as observation_type_concept_id,
    cast(null as float) as value_as_number,
    cast(null as varchar) as value_as_string,
    cast(null as integer) as value_as_concept_id,
    cast(null as integer) as qualifier_concept_id,
    cast(null as integer) as unit_concept_id,
    cast(hash(concat_ws('||',
      phc.rendering_bill_provider_last,
      coalesce(phc.rendering_bill_provider_first, ''),
      phc.rendering_bill_provider_state_1,
      phc.rendering_bill_provider_4
    ), 'xxhash64') % 1000000000 as varchar) as provider_id,
    cast(phc.bill_id as varchar) as visit_occurrence_id,
    cast(null as varchar) as visit_detail_id,
    unpivot_cte.observation_source_value,
    cast(null as integer) as observation_source_concept_id,
    cast(null as varchar) as unit_source_value,
    cast(null as varchar) as qualifier_source_value,
    cast(null as varchar) as value_source_value,
    cast(null as integer) as observation_event_id,
    cast(null as integer) as obs_event_field_concept_id
  from {{ source('raw', 'professional_header_historical') }} as phc
  join unpivot_cte on cast(phc.bill_id as varchar) = cast(unpivot_cte.bill_id as varchar)
)
  {% endset %}
  {% do header_ctes.append(query) %}

  {# Institutional detail historical - HCPCS observations #}
  {% set query %}
institutional_detail_historical as (
  select
    cast(hash(concat_ws('||', idc.bill_id, idc.row_id), 'xxhash64') % 1000000000 as varchar) as observation_id,
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
    cast(null as integer) as observation_concept_id,
    CASE WHEN idc.service_line_from_date = 'N' THEN NULL
      ELSE cast(idc.service_line_from_date as date) END as observation_date,
    CASE WHEN idc.service_line_from_date = 'N' THEN NULL
      ELSE cast(idc.service_line_from_date as timestamp) END as observation_datetime,
    32854 as observation_type_concept_id,
    cast(null as float) as value_as_number,
    cast(null as varchar) as value_as_string,
    cast(null as integer) as value_as_concept_id,
    cast(null as integer) as qualifier_concept_id,
    cast(null as integer) as unit_concept_id,
    cast(hash(concat_ws('||',
      ihc.rendering_bill_provider_last,
      coalesce(ihc.rendering_bill_provider_first, ''),
      ihc.rendering_bill_provider_state_1,
      ihc.rendering_bill_provider_4
    ), 'xxhash64') % 1000000000 as varchar) as provider_id,
    cast(idc.bill_id as varchar) as visit_occurrence_id,
    cast(hash(concat_ws('||', idc.bill_id, idc.row_id), 'xxhash64') % 1000000000 as varchar) as visit_detail_id,
    idc.hcpcs_line_procedure_billed as observation_source_value,
    cast(null as integer) as observation_source_concept_id,
    cast(null as varchar) as unit_source_value,
    cast(null as varchar) as qualifier_source_value,
    cast(null as varchar) as value_source_value,
    cast(null as integer) as observation_event_id,
    cast(null as integer) as obs_event_field_concept_id
  from {{ source('raw', 'institutional_detail_historical') }} idc
  join {{ source('raw', 'institutional_header_historical') }} ihc
    on cast(idc.bill_id as varchar) = cast(ihc.bill_id as varchar)
  join {{ source('omop','concept') }} as c
    on c.concept_code = idc.hcpcs_line_procedure_billed
  where c.domain_id = 'Observation'
    and c.vocabulary_id = 'HCPCS'
)
  {% endset %}
  {% do detail_ctes.append(query) %}

  {# Professional detail historical - HCPCS observations #}
  {% set query %}
professional_detail_historical as (
  select
    cast(hash(concat_ws('||', prdc.bill_id, prdc.row_id), 'xxhash64') % 1000000000 as varchar) as observation_id,
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
    cast(null as integer) as observation_concept_id,
    cast(prdc.service_line_from_date as date) as observation_date,
    cast(prdc.service_line_from_date as timestamp) as observation_datetime,
    32854 as observation_type_concept_id,
    cast(null as float) as value_as_number,
    cast(null as varchar) as value_as_string,
    cast(null as integer) as value_as_concept_id,
    cast(null as integer) as qualifier_concept_id,
    cast(null as integer) as unit_concept_id,
    cast(hash(concat_ws('||',
      prhc.rendering_bill_provider_last,
      coalesce(prhc.rendering_bill_provider_first, ''),
      prhc.rendering_bill_provider_state_1,
      prhc.rendering_bill_provider_4
    ), 'xxhash64') % 1000000000 as varchar) as provider_id,
    cast(prdc.bill_id as varchar) as visit_occurrence_id,
    cast(hash(concat_ws('||', prdc.bill_id, prdc.row_id), 'xxhash64') % 1000000000 as varchar) as visit_detail_id,
    prdc.hcpcs_line_procedure_billed as observation_source_value,
    cast(null as integer) as observation_source_concept_id,
    cast(null as varchar) as unit_source_value,
    cast(null as varchar) as qualifier_source_value,
    cast(null as varchar) as value_source_value,
    cast(null as integer) as observation_event_id,
    cast(null as integer) as obs_event_field_concept_id
  from {{ source('raw', 'professional_detail_historical') }} prdc
  join {{ source('raw', 'professional_header_historical') }} prhc
    on cast(prdc.bill_id as varchar) = cast(prhc.bill_id as varchar)
  join {{ source('omop','concept') }} as c
    on c.concept_code = prdc.hcpcs_line_procedure_billed
  where c.domain_id = 'Observation'
    and c.vocabulary_id = 'HCPCS'
)
  {% endset %}
  {% do detail_ctes.append(query) %}
{% endif %}

{% if has_current or has_historical %}
with
  {{ (header_ctes | join(",\n")) ~ (detail_ctes | length > 0 and ",\n" or "") ~ (detail_ctes | join(",\n")) }}

select *
from (
  {% if has_current %}
    select * from final_ihc
    union
    select * from final_phc
    union
    select * from institutional_detail_current
    union
    select * from professional_detail_current
  {% endif %}
  {% if has_current and has_historical %}
    union
  {% endif %}
  {% if has_historical %}
    select * from final_ihh
    union
    select * from final_phh
    union
    select * from institutional_detail_historical
    union
    select * from professional_detail_historical
  {% endif %}
) as final_result
{% else %}
-- No source tables available - return empty result set with OMOP observation schema
select
    cast(null as varchar) as observation_id,
    cast(null as varchar) as person_id,
    cast(null as integer) as observation_concept_id,
    cast(null as date) as observation_date,
    cast(null as timestamp) as observation_datetime,
    cast(null as integer) as observation_type_concept_id,
    cast(null as float) as value_as_number,
    cast(null as varchar) as value_as_string,
    cast(null as integer) as value_as_concept_id,
    cast(null as integer) as qualifier_concept_id,
    cast(null as integer) as unit_concept_id,
    cast(null as varchar) as provider_id,
    cast(null as varchar) as visit_occurrence_id,
    cast(null as varchar) as visit_detail_id,
    cast(null as varchar) as observation_source_value,
    cast(null as integer) as observation_source_concept_id,
    cast(null as varchar) as unit_source_value,
    cast(null as varchar) as qualifier_source_value,
    cast(null as varchar) as value_source_value,
    cast(null as integer) as observation_event_id,
    cast(null as integer) as obs_event_field_concept_id
where false
{% endif %}
