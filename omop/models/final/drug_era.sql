-- adapted from https://github.com/OHDSI/ETL-CMS/blob/master/SQL/create_CDMv5_drug_era_non_stockpile.sql
with
-- normalize the drug exposure dates. we pick the first non-null value among:
--   1. drug_exposure_end_date,
--   2. drug_exposure_start_date plus days_supply (if it results in a different date),
--   3. otherwise, drug_exposure_start_date plus one day.
ctepredrugtarget as (
    select
        d.drug_exposure_id,
        d.person_id,
        c.concept_id as ingredient_concept_id,
        d.drug_exposure_start_date,
        d.days_supply,
        coalesce(
            d.drug_exposure_end_date,
            nullif(d.drug_exposure_start_date + (d.days_supply * interval '1 day'), d.drug_exposure_start_date),
            d.drug_exposure_start_date + interval '1 day'
        ) as drug_exposure_end_date
    from {{ ref('drug_exposure') }} d
    join {{ source('omop','concept_ancestor') }} ca 
      on ca.descendant_concept_id = d.drug_concept_id
    join {{ source('omop','concept') }} c 
      on ca.ancestor_concept_id = c.concept_id
    where c.vocabulary_id = 'RxNorm'
      and c.concept_class_id = 'Ingredient'
      and d.drug_concept_id != 0
      and d.days_supply >= 0
),
-- group overlapping exposures by generating start (-1) and end (1) events
ctesubexposureenddates as (
    select 
        person_id, 
        ingredient_concept_id, 
        event_date as end_date
    from (
        select 
            person_id, 
            ingredient_concept_id, 
            event_date, 
            event_type,
            max(start_ordinal) over (
                partition by person_id, ingredient_concept_id 
                order by event_date, event_type 
                rows unbounded preceding
            ) as start_ordinal,
            row_number() over (
                partition by person_id, ingredient_concept_id 
                order by event_date, event_type
            ) as overall_ord
        from (
            -- start events with a row number assigned as start_ordinal
            select 
                person_id, 
                ingredient_concept_id, 
                drug_exposure_start_date as event_date,
                -1 as event_type,
                row_number() over (
                    partition by person_id, ingredient_concept_id 
                    order by drug_exposure_start_date
                ) as start_ordinal
            from ctepredrugtarget

            union all

            -- end events
            select 
                person_id, 
                ingredient_concept_id, 
                drug_exposure_end_date, 
                1 as event_type,
                null
            from ctepredrugtarget
        ) rawdata
    ) e
    where (2 * e.start_ordinal) - e.overall_ord = 0
),
-- for each exposure, identify the earliest end date (on or after the start date) from the sub-exposures
ctedrugexposureends as (
    select 
        dt.person_id,
        dt.ingredient_concept_id as drug_concept_id,
        dt.drug_exposure_start_date,
        min(e.end_date) as drug_sub_exposure_end_date
    from ctepredrugtarget dt
    join ctesubexposureenddates e 
      on dt.person_id = e.person_id 
     and dt.ingredient_concept_id = e.ingredient_concept_id 
     and e.end_date >= dt.drug_exposure_start_date
    group by 
         dt.drug_exposure_id,
         dt.person_id,
         dt.ingredient_concept_id,
         dt.drug_exposure_start_date
),
-- aggregate overlapping exposures into sub-exposures
ctesubexposures as (
    select 
        row_number() over (
            partition by person_id, drug_concept_id, drug_sub_exposure_end_date 
            order by drug_sub_exposure_start_date
        ) as row_number,
        person_id,
        drug_concept_id,
        drug_sub_exposure_start_date,
        drug_sub_exposure_end_date,
        drug_exposure_count
    from (
        select 
            person_id,
            drug_concept_id,
            min(drug_exposure_start_date) as drug_sub_exposure_start_date,
            drug_sub_exposure_end_date,
            count(*) as drug_exposure_count
        from ctedrugexposureends
        group by person_id, drug_concept_id, drug_sub_exposure_end_date
    ) sub
),
-- calculate the total days exposed in each sub-exposure period
ctefinaltarget as (
    select 
        row_number,
        person_id,
        drug_concept_id as ingredient_concept_id,
        drug_sub_exposure_start_date,
        drug_sub_exposure_end_date,
        drug_exposure_count,
        datediff('day', drug_sub_exposure_start_date, drug_sub_exposure_end_date) as days_exposed
    from ctesubexposures
),
-- adjust the end dates by subtracting a 30-day grace period ("persistence window")
cteenddates as (
    select 
        person_id, 
        ingredient_concept_id, 
        event_date - interval '30 days' as end_date
    from (
        select 
            person_id, 
            ingredient_concept_id, 
            event_date, 
            event_type,
            max(start_ordinal) over (
                partition by person_id, ingredient_concept_id 
                order by event_date, event_type 
                rows unbounded preceding
            ) as start_ordinal,
            row_number() over (
                partition by person_id, ingredient_concept_id 
                order by event_date, event_type
            ) as overall_ord
        from (
            -- start events for sub-exposures
            select 
                person_id, 
                ingredient_concept_id, 
                drug_sub_exposure_start_date as event_date,
                -1 as event_type,
                row_number() over (
                    partition by person_id, ingredient_concept_id 
                    order by drug_sub_exposure_start_date
                ) as start_ordinal
            from ctefinaltarget

            union all

            -- end events padded by 30 days for the grace period
            select 
                person_id, 
                ingredient_concept_id, 
                drug_sub_exposure_end_date + interval '30 days' as event_date,
                1 as event_type,
                null
            from ctefinaltarget
        ) rawdata
    ) e
    where (2 * e.start_ordinal) - e.overall_ord = 0
),
-- determine the definitive end date for each drug era by joining with the padded end dates
ctedrugeraends as (
    select 
        ft.person_id,
        ft.ingredient_concept_id as drug_concept_id,
        ft.drug_sub_exposure_start_date,
        min(e.end_date) as drug_era_end_date,
        ft.drug_exposure_count,
        ft.days_exposed
    from ctefinaltarget ft
    join cteenddates e 
      on ft.person_id = e.person_id 
     and ft.ingredient_concept_id = e.ingredient_concept_id 
     and e.end_date >= ft.drug_sub_exposure_start_date
    group by 
         ft.person_id,
         ft.ingredient_concept_id,
         ft.drug_sub_exposure_start_date,
         ft.drug_exposure_count,
         ft.days_exposed
),
final as (
    select
        cast(row_number() over (order by person_id) as integer) as drug_era_id,
        cast(person_id as varchar) as person_id,
        cast(drug_concept_id as varchar) as drug_concept_id,
        cast(min(drug_sub_exposure_start_date) as date) as drug_era_start_date,
        cast(drug_era_end_date as date) as drug_era_end_date,
        cast(sum(drug_exposure_count) as integer) as drug_exposure_count,
        cast(datediff('day', min(drug_sub_exposure_start_date), drug_era_end_date) - sum(days_exposed) as integer) as gap_days
    from ctedrugeraends
    group by 
        person_id, 
        drug_concept_id, 
        drug_era_end_date
)
select * from final