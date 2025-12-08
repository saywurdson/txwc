-- Dose era table - spans of consistent drug dosing
-- Derived from drug_exposure records with quantity/dose information
-- Similar algorithm to drug_era but tracks specific dose levels
with drug_doses as (
    select
        d.drug_exposure_id,
        d.person_id,
        -- Get ingredient concept for grouping (same as drug_era)
        c.concept_id as drug_concept_id,
        d.drug_exposure_start_date,
        coalesce(
            d.drug_exposure_end_date,
            d.drug_exposure_start_date + (coalesce(d.days_supply, 1) * interval '1 day')
        ) as drug_exposure_end_date,
        -- Calculate daily dose from quantity and days_supply
        case
            when d.days_supply > 0 and d.quantity > 0
            then d.quantity / d.days_supply
            else d.quantity
        end as dose_value,
        -- Get unit from drug_strength if available
        coalesce(
            (
                select cast(ds.amount_unit_concept_id as integer)
                from {{ source('omop', 'drug_strength') }} ds
                where ds.drug_concept_id = d.drug_concept_id
                    and ds.amount_unit_concept_id is not null
                limit 1
            ),
            8576  -- milligram (default unit)
        ) as unit_concept_id
    from {{ ref('drug_exposure') }} d
    join {{ source('omop', 'concept_ancestor') }} ca
        on ca.descendant_concept_id = d.drug_concept_id
    join {{ source('omop', 'concept') }} c
        on ca.ancestor_concept_id = c.concept_id
    where c.vocabulary_id = 'RxNorm'
        and c.concept_class_id = 'Ingredient'
        and d.drug_concept_id != 0
        and d.quantity is not null
        and d.quantity > 0
),
-- Group consecutive exposures with same dose
dose_groups as (
    select
        person_id,
        drug_concept_id,
        unit_concept_id,
        dose_value,
        drug_exposure_start_date,
        drug_exposure_end_date,
        -- Identify gaps in dosing to create era boundaries
        sum(case
            when drug_exposure_start_date <= lag_end_date + interval '1 day' then 0
            else 1
        end) over (
            partition by person_id, drug_concept_id, unit_concept_id, dose_value
            order by drug_exposure_start_date
        ) as era_group
    from (
        select
            *,
            lag(drug_exposure_end_date) over (
                partition by person_id, drug_concept_id, unit_concept_id, dose_value
                order by drug_exposure_start_date
            ) as lag_end_date
        from drug_doses
    ) with_lag
),
dose_eras_grouped as (
    select
        person_id,
        drug_concept_id,
        unit_concept_id,
        dose_value,
        min(drug_exposure_start_date) as dose_era_start_date,
        max(drug_exposure_end_date) as dose_era_end_date
    from dose_groups
    group by person_id, drug_concept_id, unit_concept_id, dose_value, era_group
)
select
    cast(row_number() over (order by person_id, drug_concept_id, dose_era_start_date) as integer) as dose_era_id,
    cast(person_id as varchar) as person_id,
    cast(drug_concept_id as integer) as drug_concept_id,
    cast(unit_concept_id as integer) as unit_concept_id,
    cast(dose_value as numeric) as dose_value,
    cast(dose_era_start_date as date) as dose_era_start_date,
    cast(dose_era_end_date as date) as dose_era_end_date
from dose_eras_grouped
where dose_era_start_date is not null
