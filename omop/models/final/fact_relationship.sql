-- OMOP fact_relationship: links each procedure_occurrence to the condition
-- it was performed for, using the CMS-1500 Box 24E diagnosis_pointer on each
-- professional claim line. Only professional claims carry diagnosis pointers;
-- institutional and pharmacy claims don't expose this semantic at the line
-- level and are therefore not linked here.
--
-- Bidirectional edges are emitted per OMOP convention:
--   relationship_concept_id = 46233684  "Relevant condition of"  (procedure -> condition)
--   relationship_concept_id = 46233685  "Condition relevant to"  (condition -> procedure)
-- Domain concept IDs:
--   10 = Procedure, 19 = Condition
--
-- Limitation: when multiple detail lines on the same bill share the same
-- HCPCS code AND point to different diagnoses, the (visit, source_value)
-- join key fans out, and DISTINCT collapses it. 97% of diagnosis pointers
-- on this dataset are '1' (principal diagnosis), so this affects <3% of
-- rows in practice.

{% set has_current = check_table_exists('raw', 'professional_detail_current')
                     and check_table_exists('raw', 'professional_header_current') %}
{% set has_historical = check_table_exists('raw', 'professional_detail_historical')
                        and check_table_exists('raw', 'professional_header_historical') %}

with pointer_links as (
    {% if has_current %}
    select distinct
        cast(prd.bill_id as integer) as bill_id_int,
        prd.hcpcs_line_procedure_billed as proc_src,
        case prd.first_diagnosis_pointer
            when '1' then prh.first_icd_9cm_or_icd_10cm
            when '2' then prh.second_icd_9cm_or_icd_10cm
            when '3' then prh.third_icd_9cm_or_icd_10cm
            when '4' then prh.fourth_icd_9cm_or_icd_10cm
            when '5' then prh.fifth_icd_9cm_or_icd_10cm
        end as cond_src
    from {{ source('raw', 'professional_detail_current') }} prd
    join {{ source('raw', 'professional_header_current') }} prh
        on prd.bill_id = prh.bill_id
    where prd.first_diagnosis_pointer is not null
      and prd.hcpcs_line_procedure_billed is not null
    {% endif %}
    {% if has_current and has_historical %}
    union
    {% endif %}
    {% if has_historical %}
    select distinct
        cast(prd.bill_id as integer),
        prd.hcpcs_line_procedure_billed,
        case prd.first_diagnosis_pointer
            when '1' then prh.first_icd_9cm_or_icd_10cm
            when '2' then prh.second_icd_9cm_or_icd_10cm
            when '3' then prh.third_icd_9cm_or_icd_10cm
            when '4' then prh.fourth_icd_9cm_or_icd_10cm
            when '5' then prh.fifth_icd_9cm_or_icd_10cm
        end
    from {{ source('raw', 'professional_detail_historical') }} prd
    join {{ source('raw', 'professional_header_historical') }} prh
        on prd.bill_id = prh.bill_id
    where prd.first_diagnosis_pointer is not null
      and prd.hcpcs_line_procedure_billed is not null
    {% endif %}
    {% if not has_current and not has_historical %}
    select
        cast(null as integer) as bill_id_int,
        cast(null as varchar) as proc_src,
        cast(null as varchar) as cond_src
    where false
    {% endif %}
),
resolved as (
    -- Translate raw bill_id -> renumbered visit_occurrence_id via the map,
    -- then match procedures and conditions by (visit, source_value).
    select distinct
        p.procedure_occurrence_id,
        c.condition_occurrence_id
    from pointer_links pl
    join {{ ref('int_visit_occurrence_id_map') }} vm
        on pl.bill_id_int = vm.source_id
    join {{ ref('procedure_occurrence') }} p
        on p.visit_occurrence_id = vm.visit_occurrence_id
       and p.procedure_source_value = pl.proc_src
    join {{ ref('condition_occurrence') }} c
        on c.visit_occurrence_id = vm.visit_occurrence_id
       and c.condition_source_value = pl.cond_src
    where pl.cond_src is not null
)
select
    cast(procedure_occurrence_id as integer) as fact_id_1,
    cast(10 as integer) as domain_concept_id_1,
    cast(condition_occurrence_id as integer) as fact_id_2,
    cast(19 as integer) as domain_concept_id_2,
    cast(46233684 as integer) as relationship_concept_id
from resolved
union all
select
    cast(condition_occurrence_id as integer),
    cast(19 as integer),
    cast(procedure_occurrence_id as integer),
    cast(10 as integer),
    cast(46233685 as integer)
from resolved
