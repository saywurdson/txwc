# Texas Workers' Compensation Healthcare Analytics Pipeline

End-to-end data pipeline that extracts Texas workers' compensation medical billing data from the [Texas Open Data Portal](https://data.texas.gov), transforms it through a 3-layer dbt model architecture, and loads it into the [OMOP Common Data Model](https://ohdsi.github.io/CommonDataModel/) v5.4 using DuckDB.

![Python](https://img.shields.io/badge/Python-3.11-blue?logo=python)
![DuckDB](https://img.shields.io/badge/DuckDB-Database-FFC107?logo=duckdb)
![dbt](https://img.shields.io/badge/dbt-Transform-FF694B?logo=dbt)
![OMOP CDM](https://img.shields.io/badge/OMOP_CDM-v5.4-green)
![License](https://img.shields.io/badge/License-MIT-lightgrey)

---

## Overview

The Texas Department of Insurance, Division of Workers' Compensation (DWC) publishes medical billing data for injured employees as open data. This project builds an automated pipeline to:

1. **Extract** 12 datasets (professional, institutional, pharmacy claims) via the Socrata API
2. **Enrich** with clinical reference data from RxNav, VSAC/FHIR, and OMOP vocabularies
3. **Transform** raw billing records into 20+ standardized OMOP CDM tables using dbt
4. **Analyze** with interactive dashboards and Jupyter notebooks

### What is OMOP CDM?

The [OMOP Common Data Model](https://ohdsi.github.io/CommonDataModel/) is a universal standard for healthcare data. Just as USB-C lets any device connect to any charger, OMOP CDM lets healthcare data from any source be analyzed using the same tools and queries. Developed by the [OHDSI](https://ohdsi.org/) community and used by 100+ institutions worldwide, it maps proprietary billing codes (ICD-9/10, NDC, HCPCS/CPT) into standardized clinical concepts (SNOMED, RxNorm).

---

## Architecture

```
data.texas.gov (Socrata API)
    │
    │  load_data.py ── dlt pipeline with rate limiting,
    │                   patient-cohort sampling
    ▼
┌─────────────────────────────────────────────┐
│  DuckDB (tx_workers_comp.db)                │
│                                             │
│  ┌─────────┐    dbt (48 SQL models)         │
│  │ raw.*   │───────────────────────────────► │
│  │ 12 tbls │    staging → intermediate →    │
│  └─────────┘    final                       │
│                          ┌──────────┐       │
│  Reference Data ────────►│ omop.*   │       │
│  • OMOP Vocabularies     │ 20+ tbls │       │
│  • RxClass (RxNav API)   └──────────┘       │
│  • VSAC (FHIR API)                          │
└─────────────────────────────────────────────┘
    │
    ▼
Streamlit Dashboard & Jupyter Notebooks
```

---

## Data Sources

| Source | Description | Loader |
|--------|-------------|--------|
| [Texas Open Data Portal](https://data.texas.gov) | 12 workers' comp medical billing datasets (professional, institutional, pharmacy — headers & details, current & historical) | `load_data.py` |
| [RxNav / RxClass API](https://rxnav.nlm.nih.gov/) | Drug classification data (EPC, ATC, MOA) for all RxNorm concepts | `load_rxclass.py` |
| [VSAC FHIR API](https://cts.nlm.nih.gov/fhir) | Clinical value sets (SNOMED, ICD-10, LOINC, CPT groupings) | `load_vsac.py` |
| [OMOP Vocabularies (Athena)](https://athena.ohdsi.org/) | CONCEPT, CONCEPT_RELATIONSHIP, CONCEPT_ANCESTOR, DRUG_STRENGTH reference tables | `load_vocab.py` |

### Claim Types

| Type | Form | Description |
|------|------|-------------|
| Professional (SV1) | CMS-1500 | Doctor/provider office visits |
| Institutional (SV2) | CMS-1450/UB-04 | Hospital/facility bills |
| Pharmacy (SV4) | DWC Form-066 | Prescription drug bills |

---

## OMOP CDM Tables Produced

The dbt transformation layer maps raw billing data into these standardized clinical tables:

**Clinical:** `person`, `visit_occurrence`, `visit_detail`, `condition_occurrence`, `procedure_occurrence`, `drug_exposure`, `measurement`, `observation`, `device_exposure`, `specimen`, `death`

**Derived:** `drug_era`, `condition_era`, `dose_era`, `observation_period`

**Administrative:** `cost`, `payer_plan_period`, `care_site`, `location`, `provider`

---

## Quick Start

### Prerequisites

- Docker
- A Socrata app token ([register here](https://data.texas.gov/profile/edit/developer_settings))
- (Optional) UMLS API key for VSAC/RxClass enrichment
- (Required for OMOP) OMOP vocabulary files from [Athena](https://athena.ohdsi.org/) — see below

### Setup

```bash
# Clone the repo
git clone https://github.com/saywurdson/txwc.git
cd txwc

# Copy environment template and add your API keys
cp .env.example .env
# Edit .env with your tokens

# Build the Docker container
docker build -t txwc .
```

### Load Data

```bash
# Full load (all datasets, incremental for historical)
python load_data.py

# Load a specific claim type
python load_data.py --dataset professional --time_period current

# Patient-cohort sample (faster for development/testing)
python load_data.py --sample_patients 1000

# Sample by complexity (patients with most claims across types)
python load_data.py --sample_patients 500 --complex

# View database summary without downloading
python load_data.py --report_only
```

### Download OMOP Vocabularies

The dbt models require standard vocabulary files to map billing codes (ICD-10, NDC, HCPCS/CPT) to OMOP concepts. These are not included in the repo due to licensing.

1. Register at [Athena (athena.ohdsi.org)](https://athena.ohdsi.org/)
2. Click **Download** and select at minimum these vocabularies:
   - **SNOMED** — standard clinical concepts
   - **ICD10CM** — diagnosis codes
   - **ICD9CM** — legacy diagnosis codes
   - **RxNorm** / **RxNorm Extension** — drug concepts
   - **HCPCS** / **CPT4** — procedure codes (CPT4 requires a separate UMLS license)
   - **NDC** — pharmacy drug codes
   - **LOINC** — lab/observation codes
3. Download and extract the zip file
4. Copy the CSV files into `omop/seeds/`:
   ```bash
   cp /path/to/athena_download/*.csv omop/seeds/
   ```
5. Load into DuckDB:
   ```bash
   python load_vocab.py
   ```

The required files are: `CONCEPT.csv`, `CONCEPT_RELATIONSHIP.csv`, `CONCEPT_ANCESTOR.csv`, `DRUG_STRENGTH.csv`. The remaining files in the Athena download (`CONCEPT_SYNONYM`, `VOCABULARY`, `RELATIONSHIP`, `DOMAIN`, `CONCEPT_CLASS`) are not used by the dbt models but can be loaded for reference.

### Transform with dbt

```bash
cd omop
dbt deps
dbt run      # Build all OMOP CDM tables
dbt test     # Run data quality tests
```

### Launch Dashboard

```bash
streamlit run dashboard.py
```

---

## Project Structure

```
txwc/
├── load_data.py           # Socrata API ingestion via dlt pipeline
├── load_rxclass.py        # RxNav drug classification loader
├── load_vsac.py           # VSAC FHIR value set loader
├── load_vocab.py          # OMOP vocabulary CSV loader
├── dashboard.py           # Streamlit analytics dashboard
├── Dockerfile             # Container build
├── .env.example           # Environment variable template
│
├── omop/                  # dbt project
│   ├── dbt_project.yml
│   ├── profiles.yml
│   ├── models/
│   │   ├── staging/       # Raw data extraction (ephemeral)
│   │   ├── intermediate/  # Type casting & concept mapping (ephemeral)
│   │   └── final/         # OMOP CDM tables (materialized)
│   ├── macros/            # Reusable SQL (concept mapping, ID derivation)
│   ├── seeds/             # OMOP vocabulary CSVs (not tracked)
│   └── tests/
│
└── notebooks/             # Analysis notebooks
    ├── top10_analysis.ipynb
    ├── pdc.ipynb
    └── add_ccsr_value_sets.ipynb
```

---

## Key Engineering Features

- **dlt pipelines** — All loaders use [dlt](https://dlthub.com) for schema management, type handling, and idempotent loads into DuckDB
- **Patient-cohort sampling** — Development mode that selects N patients (random or by complexity score) and fetches only their complete records across all claim types
- **Full schema enforcement** — Post-load step queries Socrata metadata API to ensure all columns exist even when sampling small patient cohorts
- **Concept mapping** — Custom dbt macros map ICD-9/10 → SNOMED, NDC → RxNorm, HCPCS/CPT → standard concepts via OMOP vocabulary lookups
- **Hash-based patient identity** — Deterministic `person_id` derived from `xxhash64` of demographics for deduplication across claim types

---

## Technologies

| Category | Tools |
|----------|-------|
| **Language** | Python 3.11 |
| **Data Loading** | dlt (dlthub.com) |
| **Database** | DuckDB |
| **Transformation** | dbt (dbt-duckdb) |
| **Data Model** | OMOP CDM v5.4 |
| **APIs** | Socrata Open Data, VSAC FHIR, RxNav REST, UMLS |
| **Visualization** | Streamlit, Plotly, Jupyter |
| **Infrastructure** | Docker, VS Code Dev Containers |

---

## License

This project is licensed under the MIT License — see the [LICENSE](LICENSE) file for details.

The source data is public-use open data published by the [Texas Department of Insurance, Division of Workers' Compensation](https://www.tdi.texas.gov/wc/data.html).
