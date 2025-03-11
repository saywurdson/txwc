-- adapted from https://github.com/OHDSI/ETL-CMS/blob/master/SQL/create_CDMv5_condition_era.sql
with cteconditiontarget (condition_occurrence_id, person_id, condition_concept_id, condition_start_date, condition_end_date) as (
    select
        co.condition_occurrence_id,
        co.person_id,
        co.condition_concept_id,
        co.condition_start_date,
        coalesce(nullif(co.condition_end_date, null), condition_start_date + interval '1 day') as condition_end_date
    from {{ ref('condition_occurrence') }} co
    where condition_concept_id != 0
),
-- cteenddates (the magic)
cteenddates (person_id, condition_concept_id, end_date) as (
    select
        person_id,
        condition_concept_id,
        event_date - interval '30 days' as end_date
    from (
        select
            person_id,
            condition_concept_id,
            event_date,
            event_type,
            max(start_ordinal) over (partition by person_id, condition_concept_id order by event_date, event_type rows unbounded preceding) as start_ordinal,
            row_number() over (partition by person_id, condition_concept_id order by event_date, event_type) as overall_ord
        from (
            -- select the start dates, assigning a row number to each
            select
                person_id,
                condition_concept_id,
                condition_start_date as event_date,
                -1 as event_type,
                row_number() over (partition by person_id, condition_concept_id order by condition_start_date) as start_ordinal
            from cteconditiontarget

            union all

            -- pad the end dates by 30 to allow a grace period for overlapping ranges.
            select
                person_id,
                condition_concept_id,
                condition_end_date + interval '30 days',
                1 as event_type,
                null
            from cteconditiontarget
        ) rawdata
    ) e
    where (2 * e.start_ordinal) - e.overall_ord = 0
),
cteconditionends (person_id, condition_concept_id, condition_start_date, era_end_date) as (
    select
        c.person_id,
        c.condition_concept_id,
        c.condition_start_date,
        min(e.end_date) as era_end_date
    from cteconditiontarget c
    join cteenddates e on c.person_id = e.person_id
        and c.condition_concept_id = e.condition_concept_id
        and e.end_date >= c.condition_start_date
    group by
        c.condition_occurrence_id,
        c.person_id,
        c.condition_concept_id,
        c.condition_start_date
),
final as (
    select
        row_number() over (order by person_id) as condition_era_id,
        person_id,
        condition_concept_id,
        min(condition_start_date) as condition_era_start_date,
        era_end_date as condition_era_end_date,
        count(*) as condition_occurrence_count
    from cteconditionends
    group by person_id, condition_concept_id, era_end_date
)
select * from final