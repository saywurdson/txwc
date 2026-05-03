# omop — dbt project

The dbt project that transforms Texas workers' compensation medical billing data into [OMOP Common Data Model](https://ohdsi.github.io/CommonDataModel/) v5.4 tables. See the [parent README](../README.md) for the full ingestion pipeline (Socrata → DuckDB), reference-data loaders (RxNav, VSAC, OMOP vocab), and dashboard.

## Architecture

Three-layer model with materialization tuned per layer:

| Layer | Path | Materialization | Purpose |
|---|---|---|---|
| Staging | `models/staging/` (14 models) | `ephemeral` | Raw Socrata → typed columns. Unions `*_current` + `*_historical` source pairs across institutional / professional / pharmacy. |
| Intermediate | `models/intermediate/` (16 models) | `view` (`int_concept_map` is `table`) | Concept mapping, ID derivation, business logic. `int_concept_map` is materialized once because every downstream model joins it. |
| Final | `models/final/` (21 models) | `table` | OMOP CDM v5.4 tables — exact CDM names (no `fct_` / `dim_` prefix) so OHDSI tools can read the schema directly. |

Sources (`models/source.yml`):
- `raw` — 12 dlt-loaded tables (header + detail × current + historical × 3 claim types) with 7-day freshness on `*_current`.
- `omop` — vocabulary reference tables loaded from Athena (`concept`, `concept_relationship`, `concept_ancestor`, `drug_strength`).

## OMOP CDM tables produced

**Clinical:** `person`, `visit_occurrence`, `visit_detail`, `condition_occurrence`, `procedure_occurrence`, `drug_exposure`, `measurement`, `observation`, `device_exposure`, `specimen`, `death`

**Derived eras:** `drug_era`, `condition_era`, `dose_era`, `observation_period`

**Administrative:** `cost`, `payer_plan_period`, `care_site`, `location`, `provider`

**Relationships:** `fact_relationship` — links each `procedure_occurrence` to the `condition_occurrence` it was performed for, using the CMS-1500 Box 24E diagnosis pointer on professional claim lines.

## Workers'-comp-specific semantics

Several OMOP columns are populated with WC semantics that a generic claims ETL would leave empty:

- **Injury date observation** (`observation_concept_id = 40771952`, LOINC "Injury date") — one row per patient carrying `employee_date_of_injury`. Anchors `observation_period.observation_period_start_date` via `LEAST(injury_date, earliest_clinical_event)`.
- **Employer FEIN observation** (`observation_concept_id = 21492865`, LOINC "Employer name [Identifier]") — stores `employer_fein` in `value_as_string` for employer-level cohorts.
- **No-fault cost math** — `paid_by_patient`, `paid_patient_copay`, `paid_patient_coinsurance`, `paid_patient_deductible` hardcoded to `0` (WC has no patient responsibility). `amount_allowed = total_charge - service_adjustment_amount` surfaces ~$4M of contractual write-offs.

## Macros

`macros/` contains 14 reusable SQL macros, organized by purpose:

| Group | Macros | What they do |
|---|---|---|
| Concept mapping | `get_concept_ids`, `get_source_concept_ids`, `get_route_concept_id`, `get_route_source_value`, `get_dose_unit_source_value` | Two-step lookup against `int_concept_map` to resolve source codes (ICD, NDC, HCPCS) to standard OMOP concepts. Deterministic tie-breaking on `target_concept_id ASC`. |
| ID derivation | `derive_person_id`, `derive_provider_id`, `derive_care_site_id`, `derive_facility_location_id`, `derive_employee_location_id` | `xxhash64`-based deterministic surrogate keys so the same demographics / NPI / address resolves to the same id across claim types and reruns. |
| Validation | `check_table_exists`, `check_column_exists`, `check_domain_id` | Compile-time guards that fail fast when sources or vocabulary inputs are missing or wrong-domain. |
| Schema | `generate_schema_name` | Custom override that respects the `+schema:` config (used to isolate `dbt_project_evaluator` findings). |

## dbt_project_evaluator configuration

The project runs [`dbt_project_evaluator`](https://github.com/dbt-labs/dbt-project-evaluator) for governance, with OMOP-specific overrides in `dbt_project.yml` and exception entries in `seeds/dbt_project_evaluator_exceptions.csv`:

- **Findings isolated** to `dbt_project_evaluator` schema so they don't clutter `omop`.
- **Disabled checks** that conflict with the OMOP CDM:
  - `fct_model_naming_conventions` — CDM mandates exact final-table names.
  - `fct_source_directories` — single `source.yml` at `models/` root.
  - `fct_model_directories` — single source doesn't need nested `staging/<source>/` layout.
- **Tuned thresholds:**
  - `marts_folder_name: 'final'` so the lineage classifier puts CDM tables in the `marts` bucket.
  - `marts_prefixes: ['']` because CDM tables have no required prefix.
  - `too_many_joins_threshold: 13` because staging models union many source CTEs (claim type × current/historical × header/detail; up to 26 in `stg_observation`) and the package counts each CTE as a join.

## Running

From the repo root (the parent project owns the `uv` venv and the DuckDB file):

```bash
cd omop
uv run dbt deps          # install dbt_utils + dbt_project_evaluator
uv run dbt build         # run + test in topological order (recommended)

# Or separately:
uv run dbt run
uv run dbt test

# Targeted runs:
uv run dbt build --select +final.person       # person + all upstream deps
uv run dbt build --select state:modified+     # only changed models + downstream
```

### Profile

`profiles.yml` (in this directory, not `~/.dbt/`) targets DuckDB:

```yaml
omop:
  target: dev
  outputs:
    dev:
      type: duckdb
      threads: 4
      path: "{{ env_var('TXWC_DB_PATH', '../tx_workers_comp.db') }}"
      schema: omop
```

Override the database location with `TXWC_DB_PATH=/abs/path/to/file.db` if running outside the default repo layout.

## Prerequisites

- Raw data loaded: `uv run python ../load_data.py` from the repo root.
- OMOP vocabularies loaded: `uv run python ../load_vocab.py` (requires Athena CSVs in `../vocab/` — see parent README for the download steps).
- `dbt_packages/` populated: `uv run dbt deps`.

## References

- [OMOP CDM v5.4 spec](https://ohdsi.github.io/CommonDataModel/)
- [OHDSI Athena vocabularies](https://athena.ohdsi.org/)
- [dbt_project_evaluator](https://github.com/dbt-labs/dbt-project-evaluator)
