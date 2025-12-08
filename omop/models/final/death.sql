-- Death table - empty with correct OMOP v5.4 schema
-- No death data available in the source claims data
select
    cast(null as integer) as person_id,
    cast(null as date) as death_date,
    cast(null as timestamp) as death_datetime,
    cast(null as integer) as death_type_concept_id,
    cast(null as integer) as cause_concept_id,
    cast(null as varchar) as cause_source_value,
    cast(null as integer) as cause_source_concept_id
where 1 = 0  -- Empty table
