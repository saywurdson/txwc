-- Specimen table derived from measurement records
-- Uses OMOP vocabulary relationships via concept_ancestor to find specimen types
-- Traverses: measurement_concept -> descendant concepts -> "Has specimen" relationship -> specimen
with specimen_lookup as (
    -- Get specimen concepts for each measurement via concept hierarchy
    -- A measurement concept's descendants may have "Has specimen" relationships
    select distinct
        m.measurement_id,
        m.person_id,
        m.measurement_date as specimen_date,
        m.measurement_datetime as specimen_datetime,
        m.measurement_source_value,
        m.value_as_number as quantity,
        m.unit_concept_id,
        m.unit_source_value,
        c_specimen.concept_id as specimen_concept_id
    from {{ ref('measurement') }} m
    join {{ source('omop', 'concept_ancestor') }} ca
        on cast(m.measurement_concept_id as integer) = ca.ancestor_concept_id
    join {{ source('omop', 'concept_relationship') }} cr
        on ca.descendant_concept_id = cr.concept_id_1
    join {{ source('omop', 'concept') }} c_specimen
        on cr.concept_id_2 = c_specimen.concept_id
    where cr.relationship_id = 'Has specimen'
      and c_specimen.domain_id = 'Specimen'
      and c_specimen.standard_concept = 'S'
      and m.measurement_date is not null
),
-- Deduplicate: pick the most specific specimen (prefer urine > blood > serum for drug tests)
-- Use a priority ranking to pick the best specimen when multiple are found
ranked_specimens as (
    select
        *,
        row_number() over (
            partition by measurement_id
            order by
                -- Priority: prefer more specific specimen types
                case specimen_concept_id
                    when 4046280 then 1  -- Urine specimen (most specific for drug tests)
                    when 4001225 then 2  -- Blood specimen
                    when 4046368 then 3  -- Serum or plasma specimen
                    when 4001181 then 4  -- Serum specimen
                    when 4000626 then 5  -- Plasma specimen
                    else 10
                end
        ) as rn
    from specimen_lookup
)
select
    cast(row_number() over (order by measurement_id) as integer) as specimen_id,
    cast(person_id as varchar) as person_id,
    cast(specimen_concept_id as integer) as specimen_concept_id,
    cast(32856 as integer) as specimen_type_concept_id,  -- Lab derived
    cast(specimen_date as date) as specimen_date,
    cast(specimen_datetime as timestamp) as specimen_datetime,
    cast(quantity as numeric) as quantity,
    cast(unit_concept_id as integer) as unit_concept_id,
    cast(null as integer) as anatomic_site_concept_id,
    cast(null as integer) as disease_status_concept_id,
    cast(measurement_id as varchar(50)) as specimen_source_id,
    cast(measurement_source_value as varchar(50)) as specimen_source_value,
    cast(unit_source_value as varchar(50)) as unit_source_value,
    cast(null as varchar(50)) as anatomic_site_source_value,
    cast(null as varchar(50)) as disease_status_source_value
from ranked_specimens
where rn = 1  -- Take only the best specimen match per measurement
