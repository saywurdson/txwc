"""
Texas Workers' Compensation Healthcare Analytics Dashboard

Interactive Streamlit dashboard for exploring OMOP CDM data derived from
Texas DWC medical billing records.
"""

import os
import streamlit as st
import duckdb
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import dlt

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

def _get_db_path():
    try:
        db = dlt.config["destination.duckdb.credentials"]
    except KeyError:
        db = "tx_workers_comp.db"
    if not os.path.isabs(db):
        db = os.path.join(os.path.dirname(os.path.abspath(__file__)), db)
    return db

DB_PATH = _get_db_path()

st.set_page_config(
    page_title="TX Workers' Comp Analytics",
    page_icon=":bar_chart:",
    layout="wide",
)


@st.cache_resource
def get_connection():
    return duckdb.connect(DB_PATH, read_only=True)


def query(sql: str):
    """Run a SQL query and return a pandas DataFrame."""
    return get_connection().execute(sql).fetchdf()


def safe_query(sql: str):
    """Run a SQL query, returning empty DataFrame on error."""
    try:
        return query(sql)
    except Exception:
        return pd.DataFrame()


def explain(text: str):
    """Show a plain-language explanation if the sidebar toggle is on."""
    if st.session_state.get("show_explanations", False):
        with st.expander("What does this mean?", expanded=True):
            st.markdown(text)


# Bill-level cost CTE using the OMOP cost table (replaces direct raw-header unions).
# Joins cost -> visit_occurrence on cost_event_id = visit_occurrence_id (for bill-level rows)
# to get visit_start_date, which is admission_date for institutional and
# reporting_period_start_date for professional/pharmacy.
BILL_COSTS_CTE = """
    bill_costs AS (
        SELECT
            c.total_charge AS charge,
            c.total_paid AS paid,
            v.visit_start_date AS bill_date,
            CASE c.cost_type_concept_id
                WHEN 32855 THEN 'Institutional'
                WHEN 32873 THEN 'Professional'
                WHEN 32869 THEN 'Pharmacy'
            END AS claim_type
        FROM omop.cost c
        JOIN omop.visit_occurrence v ON c.cost_event_id = v.visit_occurrence_id
        WHERE c.cost_domain_id = 'Visit'
    )
"""

# Line-level cost CTE exposing amount_allowed and paid_by_payer, which unlock
# WC-specific metrics (write-off rate, collection rate) that bill-level data can't show.
LINE_COSTS_CTE = """
    line_costs AS (
        SELECT
            c.total_charge AS charge,
            c.total_paid AS paid,
            c.amount_allowed AS allowed,
            c.paid_by_payer AS paid_by_payer,
            vd.visit_detail_start_date AS line_date,
            CASE c.cost_type_concept_id
                WHEN 32855 THEN 'Institutional'
                WHEN 32873 THEN 'Professional'
                WHEN 32869 THEN 'Pharmacy'
            END AS claim_type
        FROM omop.cost c
        JOIN omop.visit_detail vd ON c.cost_event_id = vd.visit_detail_id
        WHERE c.cost_domain_id = 'Visit Detail'
    )
"""

OPIOID_FILTER = """(c.concept_name ILIKE '%hydrocodone%'
    OR c.concept_name ILIKE '%oxycodone%'
    OR c.concept_name ILIKE '%morphine%'
    OR c.concept_name ILIKE '%fentanyl%'
    OR c.concept_name ILIKE '%tramadol%'
    OR c.concept_name ILIKE '%codeine%'
    OR c.concept_name ILIKE '%hydromorphone%'
    OR c.concept_name ILIKE '%methadone%'
    OR c.concept_name ILIKE '%buprenorphine%')"""


# ---------------------------------------------------------------------------
# Sidebar
# ---------------------------------------------------------------------------

st.sidebar.title("Filters")

_years_df = safe_query("""
    SELECT DISTINCT EXTRACT(YEAR FROM visit_start_date)::INT AS yr
    FROM omop.visit_occurrence
    WHERE visit_start_date IS NOT NULL
    ORDER BY yr
""")
years = _years_df["yr"].tolist() if not _years_df.empty and "yr" in _years_df.columns else []

if years:
    year_range = st.sidebar.slider(
        "Year Range",
        min_value=min(years),
        max_value=max(years),
        value=(min(years), max(years)),
    )
else:
    year_range = (2003, 2026)

yr_lo, yr_hi = year_range

st.sidebar.divider()
st.sidebar.checkbox(
    "Show explanations",
    value=False,
    key="show_explanations",
    help="Toggle plain-language explanations under each chart",
)

# ---------------------------------------------------------------------------
# Header
# ---------------------------------------------------------------------------

st.title("Texas Workers' Compensation Healthcare Analytics")
st.caption(
    "OMOP CDM v5.4  —  data sourced from the "
    "[Texas Open Data Portal](https://data.texas.gov)"
)

# ---------------------------------------------------------------------------
# Tabs
# ---------------------------------------------------------------------------

tab1, tab2, tab3, tab4, tab5, tab6, tab7 = st.tabs(
    ["Overview", "Injury Profile", "Condition Intelligence",
     "Cost & Payments", "Rx & Opioid Monitor", "Provider Analytics", "Geography"]
)

# ===========================  TAB 1: OVERVIEW  =============================
with tab1:
    kpis = safe_query(f"""
        SELECT
            (SELECT COUNT(*) FROM omop.person) AS patients,
            (SELECT COUNT(*) FROM omop.visit_occurrence
             WHERE EXTRACT(YEAR FROM visit_start_date) BETWEEN {yr_lo} AND {yr_hi}
            ) AS visits,
            (SELECT COUNT(*) FROM omop.condition_occurrence
             WHERE EXTRACT(YEAR FROM condition_start_date) BETWEEN {yr_lo} AND {yr_hi}
            ) AS conditions,
            (SELECT COUNT(DISTINCT provider_id) FROM omop.provider) AS providers,
            (SELECT COUNT(DISTINCT care_site_id) FROM omop.care_site) AS facilities,
            (SELECT COUNT(DISTINCT plan_source_value) FROM omop.payer_plan_period
             WHERE plan_source_value IS NOT NULL AND plan_source_value != ''
            ) AS carriers
    """)

    if not kpis.empty:
        c1, c2, c3, c4, c5, c6 = st.columns(6)
        c1.metric("Patients", f"{kpis['patients'][0]:,}")
        c2.metric("Visits", f"{kpis['visits'][0]:,}")
        c3.metric("Diagnoses", f"{kpis['conditions'][0]:,}")
        c4.metric("Providers", f"{kpis['providers'][0]:,}")
        c5.metric("Facilities", f"{kpis['facilities'][0]:,}")
        c6.metric("Carriers", f"{kpis['carriers'][0]:,}")

    explain(
        "These are the top-level counts for the dataset. **Patients** is the number of unique injured workers. "
        "**Visits** counts every medical encounter (doctor visit, hospital stay, pharmacy fill). "
        "**Diagnoses** is how many diagnosis codes were recorded. **Providers** are the doctors and clinicians "
        "who treated patients. **Facilities** are clinics, hospitals, and pharmacies. "
        "**Carriers** are the insurance companies paying the workers' comp claims."
    )

    st.divider()
    col_l, col_r = st.columns(2)

    with col_l:
        visits_yr = safe_query(f"""
            SELECT EXTRACT(YEAR FROM visit_start_date)::INT AS year,
                   COUNT(*) AS visits
            FROM omop.visit_occurrence
            WHERE visit_start_date IS NOT NULL
              AND EXTRACT(YEAR FROM visit_start_date) BETWEEN {yr_lo} AND {yr_hi}
            GROUP BY 1 ORDER BY 1
        """)
        if not visits_yr.empty:
            fig = px.area(
                visits_yr, x="year", y="visits",
                title="Visit Volume by Year",
                color_discrete_sequence=["#2196F3"],
            )
            fig.update_layout(xaxis_title="", yaxis_title="Visits")
            st.plotly_chart(fig, use_container_width=True)

        explain(
            "This shows how many medical visits happened each year. Rising trends may reflect "
            "more injuries being reported or more treatment per claim. Drops could mean fewer "
            "workplace injuries, policy changes, or data lag in recent years."
        )

    with col_r:
        gender = safe_query("""
            SELECT
                CASE gender_source_value
                    WHEN 'M' THEN 'Male'
                    WHEN 'F' THEN 'Female'
                    ELSE 'Unknown'
                END AS gender,
                COUNT(*) AS count
            FROM omop.person GROUP BY 1 ORDER BY 2 DESC
        """)
        if not gender.empty:
            fig = px.pie(
                gender, names="gender", values="count",
                title="Patient Gender Distribution",
                color_discrete_map={"Male": "#2196F3", "Female": "#E91E63", "Unknown": "#9E9E9E"},
                hole=0.4,
            )
            st.plotly_chart(fig, use_container_width=True)

        explain(
            "Gender breakdown of injured workers in the dataset. Workers' compensation claims "
            "skew male in industries like construction, manufacturing, and oil & gas, while "
            "healthcare and retail injuries tend to be more balanced."
        )

    col_l2, col_r2 = st.columns(2)

    with col_l2:
        age = safe_query("""
            SELECT (year_of_birth / 10) * 10 AS decade, COUNT(*) AS patients
            FROM omop.person
            WHERE year_of_birth BETWEEN 1930 AND 2005
            GROUP BY 1 ORDER BY 1
        """)
        if not age.empty:
            fig = px.bar(
                age, x="decade", y="patients",
                title="Patients by Birth Decade",
                color_discrete_sequence=["#26A69A"],
            )
            fig.update_layout(xaxis_title="Birth Decade", yaxis_title="Patients")
            st.plotly_chart(fig, use_container_width=True)

    with col_r2:
        repeat = safe_query("""
            SELECT
                CASE
                    WHEN cnt = 1 THEN '1 visit'
                    WHEN cnt BETWEEN 2 AND 5 THEN '2-5 visits'
                    WHEN cnt BETWEEN 6 AND 10 THEN '6-10'
                    WHEN cnt BETWEEN 11 AND 20 THEN '11-20'
                    ELSE '20+'
                END AS visit_bucket,
                COUNT(*) AS patients,
                MIN(cnt) AS sort_key
            FROM (SELECT person_id, COUNT(*) AS cnt FROM omop.visit_occurrence GROUP BY 1)
            GROUP BY 1 ORDER BY sort_key
        """)
        if not repeat.empty:
            fig = px.bar(
                repeat, x="visit_bucket", y="patients",
                title="Patient Utilization (Visits per Patient)",
                color_discrete_sequence=["#FF7043"],
            )
            fig.update_layout(xaxis_title="Visits per Patient", yaxis_title="Patients")
            st.plotly_chart(fig, use_container_width=True)

        explain(
            "This shows how many visits each patient had. Most workers' comp patients have only a few visits "
            "(e.g., a sprain that heals). Patients with 20+ visits often have chronic conditions, "
            "complex surgeries, or ongoing pain management — these are the high-cost claims that "
            "drive the majority of workers' comp spending."
        )

    # Monthly seasonality
    monthly = safe_query(f"""
        SELECT EXTRACT(MONTH FROM visit_start_date)::INT AS month, COUNT(*) AS visits
        FROM omop.visit_occurrence
        WHERE visit_start_date IS NOT NULL
          AND EXTRACT(YEAR FROM visit_start_date) BETWEEN {yr_lo} AND {yr_hi}
        GROUP BY 1 ORDER BY 1
    """)
    if not monthly.empty:
        month_names = {1: "Jan", 2: "Feb", 3: "Mar", 4: "Apr", 5: "May", 6: "Jun",
                       7: "Jul", 8: "Aug", 9: "Sep", 10: "Oct", 11: "Nov", 12: "Dec"}
        monthly["month_name"] = monthly["month"].map(month_names)
        fig = px.bar(
            monthly, x="month_name", y="visits",
            title="Seasonal Pattern: Visits by Month",
            color_discrete_sequence=["#5C6BC0"],
        )
        fig.update_layout(xaxis_title="", yaxis_title="Visits")
        st.plotly_chart(fig, use_container_width=True)

        explain(
            "Seasonal patterns in workplace injuries. Peaks in certain months may correlate "
            "with weather (heat-related injuries in summer), industry cycles (construction "
            "season), or holiday periods. Dips in December often reflect reduced work activity."
        )

    st.divider()
    st.subheader("Top Employers by Injured Worker Count")
    st.caption("Employers carrying the most workers' comp claims in this dataset")
    top_emp = safe_query("""
        SELECT
            value_as_string AS employer_fein,
            COUNT(DISTINCT person_id) AS patients,
            COUNT(*) AS bills
        FROM omop.observation
        WHERE observation_concept_id = 21492865  -- LOINC 'Employer name [Identifier]'
          AND value_as_string IS NOT NULL
        GROUP BY 1
        ORDER BY 2 DESC
        LIMIT 15
    """)
    if not top_emp.empty:
        fig = px.bar(
            top_emp.sort_values("patients"),
            x="patients", y="employer_fein", orientation="h",
            title="Top 15 Employers — Patient Count",
            color="patients", color_continuous_scale="Tealgrn",
            hover_data=["bills"],
        )
        fig.update_layout(
            xaxis_title="Distinct Injured Workers",
            yaxis_title="Employer FEIN",
            height=420,
        )
        st.plotly_chart(fig, use_container_width=True)
        explain(
            "Top employers ranked by how many of their workers filed workers' comp claims. "
            "The **Employer FEIN** is the Federal Employer Identification Number from the raw "
            "billing data (stored as `observation_concept_id = 21492865` in OMOP). High concentration "
            "of claims at a single employer may indicate specific workplace hazards, industry risk, "
            "or just that the employer is large. In real analytics you'd join this to industry / "
            "NAICS codes for safety benchmarking."
        )


# ========================  TAB 2: INJURY PROFILE  ==========================
with tab2:
    st.subheader("Injury Body Region Analysis")
    st.caption("Diagnoses classified by anatomical region based on SNOMED concept names")

    body = safe_query(f"""
        SELECT
            CASE
                WHEN c.concept_name ILIKE '%lumbar%' OR c.concept_name ILIKE '%low back%'
                     OR c.concept_name ILIKE '%lumbosacral%' THEN 'Low Back'
                WHEN c.concept_name ILIKE '%cervic%' OR c.concept_name ILIKE '%neck%' THEN 'Neck'
                WHEN c.concept_name ILIKE '%thorac%' OR c.concept_name ILIKE '%chest%'
                     OR c.concept_name ILIKE '%rib%' THEN 'Thorax'
                WHEN c.concept_name ILIKE '%knee%' THEN 'Knee'
                WHEN c.concept_name ILIKE '%shoulder%' OR c.concept_name ILIKE '%rotator%' THEN 'Shoulder'
                WHEN c.concept_name ILIKE '%wrist%' OR c.concept_name ILIKE '%hand%'
                     OR c.concept_name ILIKE '%carpal%' OR c.concept_name ILIKE '%finger%' THEN 'Wrist / Hand'
                WHEN c.concept_name ILIKE '%ankle%' OR c.concept_name ILIKE '%foot%'
                     OR c.concept_name ILIKE '%toe%' OR c.concept_name ILIKE '%plantar%' THEN 'Ankle / Foot'
                WHEN c.concept_name ILIKE '%hip%' OR c.concept_name ILIKE '%pelvi%' THEN 'Hip / Pelvis'
                WHEN c.concept_name ILIKE '%elbow%' THEN 'Elbow'
                WHEN c.concept_name ILIKE '%head%' OR c.concept_name ILIKE '%concuss%'
                     OR c.concept_name ILIKE '%brain%' OR c.concept_name ILIKE '%skull%' THEN 'Head / Brain'
                WHEN c.concept_name ILIKE '%spinal%' OR c.concept_name ILIKE '%spine%'
                     OR c.concept_name ILIKE '%vertebra%' THEN 'Spine (other)'
                WHEN c.concept_name ILIKE '%arm%' OR c.concept_name ILIKE '%humer%'
                     OR c.concept_name ILIKE '%forearm%' THEN 'Arm'
                WHEN c.concept_name ILIKE '%leg%' OR c.concept_name ILIKE '%tibia%'
                     OR c.concept_name ILIKE '%fibula%' OR c.concept_name ILIKE '%femur%' THEN 'Leg'
                ELSE 'Other / Systemic'
            END AS body_region,
            COUNT(*) AS diagnoses,
            COUNT(DISTINCT co.person_id) AS patients
        FROM omop.condition_occurrence co
        JOIN omop.concept c ON co.condition_concept_id = c.concept_id
        WHERE c.concept_name != 'No matching concept'
          AND EXTRACT(YEAR FROM co.condition_start_date) BETWEEN {yr_lo} AND {yr_hi}
        GROUP BY 1 ORDER BY 2 DESC
    """)

    if not body.empty:
        col_l, col_r = st.columns(2)
        with col_l:
            fig = px.treemap(
                body, path=["body_region"], values="diagnoses",
                title="Injury Distribution by Body Region (Diagnoses)",
                color="diagnoses",
                color_continuous_scale="Reds",
            )
            fig.update_traces(textinfo="label+value+percent root")
            st.plotly_chart(fig, use_container_width=True)

        with col_r:
            fig = px.bar(
                body.sort_values("patients", ascending=True),
                y="body_region", x="patients", orientation="h",
                title="Unique Patients Affected by Body Region",
                color_discrete_sequence=["#EF5350"],
            )
            fig.update_layout(yaxis_title="", xaxis_title="Patients")
            st.plotly_chart(fig, use_container_width=True)

        explain(
            "These charts show which body parts are most commonly injured. **Low back** injuries "
            "are typically the #1 workers' comp diagnosis — they're hard to disprove, often become "
            "chronic, and are the most expensive to treat. **Shoulder** and **knee** injuries are "
            "common in physical labor. The treemap shows total diagnoses (one patient can have multiple), "
            "while the bar chart shows unique patients affected."
        )

    # Injury-to-Treatment Delay
    st.subheader("Injury-to-First-Treatment Delay")
    st.caption("Time from employee date of injury to first recorded medical visit")
    delay = safe_query(f"""
        WITH injury_dates AS (
            SELECT person_id, MIN(observation_date) AS injury_date
            FROM omop.observation
            WHERE observation_concept_id = 40771952  -- LOINC 'Injury date'
              AND observation_date IS NOT NULL
            GROUP BY 1
        ),
        first_visits AS (
            SELECT person_id, MIN(visit_start_date) AS first_visit_date
            FROM omop.visit_occurrence
            WHERE EXTRACT(YEAR FROM visit_start_date) BETWEEN {yr_lo} AND {yr_hi}
            GROUP BY 1
        )
        SELECT
            CASE
                WHEN days_to_first BETWEEN 0 AND 1 THEN '0-1 days'
                WHEN days_to_first BETWEEN 2 AND 7 THEN '2-7 days'
                WHEN days_to_first BETWEEN 8 AND 14 THEN '8-14 days'
                WHEN days_to_first BETWEEN 15 AND 30 THEN '15-30 days'
                WHEN days_to_first BETWEEN 31 AND 90 THEN '1-3 months'
                WHEN days_to_first BETWEEN 91 AND 365 THEN '3-12 months'
                ELSE '1+ year'
            END AS delay_bucket,
            COUNT(*) AS patients,
            MIN(days_to_first) AS sort_key,
            ROUND(AVG(days_to_first), 1) AS avg_days
        FROM (
            SELECT i.person_id,
                   DATEDIFF('day', i.injury_date, f.first_visit_date) AS days_to_first
            FROM injury_dates i
            JOIN first_visits f ON i.person_id = f.person_id
            WHERE i.injury_date IS NOT NULL
              AND DATEDIFF('day', i.injury_date, f.first_visit_date) >= 0
        ) sub
        GROUP BY 1 ORDER BY sort_key
    """)
    if not delay.empty:
        col_l, col_r = st.columns([2, 1])
        with col_l:
            fig = px.bar(
                delay, x="delay_bucket", y="patients",
                title="Patients by Time to First Treatment",
                color="avg_days",
                color_continuous_scale="YlOrRd",
            )
            fig.update_layout(xaxis_title="Delay from Injury", yaxis_title="Patients")
            st.plotly_chart(fig, use_container_width=True)
        with col_r:
            total_pts = delay["patients"].sum()
            within_7 = delay[delay["sort_key"] <= 7]["patients"].sum()
            st.metric("Median Delay", f"{delay['avg_days'].median():.0f} days")
            st.metric("Treated within 7 days", f"{within_7 / total_pts * 100:.0f}%" if total_pts > 0 else "N/A")

    explain(
        "This measures how long injured workers wait before seeing a doctor after their injury. "
        "In workers' comp, **delayed treatment strongly correlates with worse outcomes** — longer "
        "disability, higher total claim costs, and lower return-to-work rates. Ideally most patients "
        "should be treated within 7 days. Large numbers in the 3-12 month bucket may indicate "
        "access barriers, claim disputes, or injuries that weren't immediately reported."
    )

    # Treatment Episode Duration
    st.subheader("Treatment Episode Duration")
    st.caption("Total span from first to last visit per patient")
    episodes = safe_query(f"""
        WITH ep AS (
            SELECT person_id,
                   MIN(visit_start_date) AS first_visit,
                   MAX(visit_start_date) AS last_visit,
                   COUNT(*) AS visit_count,
                   DATEDIFF('day', MIN(visit_start_date), MAX(visit_start_date)) AS episode_days
            FROM omop.visit_occurrence
            WHERE EXTRACT(YEAR FROM visit_start_date) BETWEEN {yr_lo} AND {yr_hi}
            GROUP BY 1
        )
        SELECT
            CASE
                WHEN episode_days = 0 THEN 'Single visit'
                WHEN episode_days BETWEEN 1 AND 30 THEN '< 1 month'
                WHEN episode_days BETWEEN 31 AND 90 THEN '1-3 months'
                WHEN episode_days BETWEEN 91 AND 180 THEN '3-6 months'
                WHEN episode_days BETWEEN 181 AND 365 THEN '6-12 months'
                ELSE '1+ year'
            END AS episode_length,
            COUNT(*) AS patients,
            ROUND(AVG(visit_count), 1) AS avg_visits,
            MIN(episode_days) AS sort_key
        FROM ep
        GROUP BY 1 ORDER BY sort_key
    """)
    if not episodes.empty:
        fig = px.bar(
            episodes, x="episode_length", y="patients",
            title="Patients by Treatment Episode Length",
            hover_data=["avg_visits"],
            color_discrete_sequence=["#26A69A"],
        )
        fig.update_layout(xaxis_title="Episode Duration", yaxis_title="Patients")
        st.plotly_chart(fig, use_container_width=True)

        explain(
            "How long each patient's treatment lasted from first to last visit. **Single visit** "
            "patients had a quick evaluation and were done. **1+ year** episodes are the expensive, "
            "complex claims — often involving surgery, chronic pain, or prolonged rehab. "
            "Hover over bars to see average visits per episode. High visit counts in short episodes "
            "suggest intensive treatment; low counts in long episodes suggest periodic check-ins."
        )

    # Visit type breakdown
    st.subheader("Visit Type & Duration")
    col_l, col_r = st.columns(2)

    with col_l:
        vtypes = safe_query(f"""
            SELECT COALESCE(c.concept_name, 'Unknown') AS visit_type, COUNT(*) AS visits
            FROM omop.visit_occurrence vo
            LEFT JOIN omop.concept c ON vo.visit_concept_id = c.concept_id
            WHERE EXTRACT(YEAR FROM vo.visit_start_date) BETWEEN {yr_lo} AND {yr_hi}
            GROUP BY 1 ORDER BY 2 DESC LIMIT 8
        """)
        if not vtypes.empty:
            fig = px.bar(
                vtypes.sort_values("visits"),
                y="visit_type", x="visits", orientation="h",
                title="Visit Types",
                color_discrete_sequence=["#42A5F5"],
            )
            fig.update_layout(yaxis_title="", xaxis_title="Visits")
            st.plotly_chart(fig, use_container_width=True)

    with col_r:
        duration = safe_query(f"""
            SELECT
                CASE
                    WHEN DATEDIFF('day', visit_start_date, visit_end_date) = 0 THEN 'Same day'
                    WHEN DATEDIFF('day', visit_start_date, visit_end_date) BETWEEN 1 AND 3 THEN '1-3 days'
                    WHEN DATEDIFF('day', visit_start_date, visit_end_date) BETWEEN 4 AND 7 THEN '4-7 days'
                    WHEN DATEDIFF('day', visit_start_date, visit_end_date) BETWEEN 8 AND 30 THEN '8-30 days'
                    ELSE '30+ days'
                END AS duration,
                COUNT(*) AS visits,
                MIN(DATEDIFF('day', visit_start_date, visit_end_date)) AS sort_key
            FROM omop.visit_occurrence
            WHERE visit_start_date IS NOT NULL AND visit_end_date IS NOT NULL
              AND EXTRACT(YEAR FROM visit_start_date) BETWEEN {yr_lo} AND {yr_hi}
            GROUP BY 1 ORDER BY sort_key
        """)
        if not duration.empty:
            fig = px.bar(
                duration, x="duration", y="visits",
                title="Visit Duration Distribution",
                color_discrete_sequence=["#26A69A"],
            )
            fig.update_layout(xaxis_title="Length of Stay", yaxis_title="Visits")
            st.plotly_chart(fig, use_container_width=True)

    # Treatment Pathway Sequencing
    st.subheader("Treatment Pathway: PT vs Imaging vs Injection")
    st.caption("For patients who received multiple treatment types, which came first?")
    explain(
        "Evidence-based workers' comp guidelines recommend **conservative treatment first**: "
        "physical therapy (PT) before imaging (MRI, X-ray) or injections. If most patients "
        "get imaging or injections *before* PT, it may indicate non-guideline-concordant care. "
        "This matters because early imaging often leads to unnecessary surgeries, and early "
        "injections can mask symptoms without addressing the underlying problem."
    )
    pathway = safe_query(f"""
        WITH categorized AS (
            SELECT po.person_id, po.procedure_date,
                CASE
                    WHEN c.concept_name ILIKE '%therapeutic%' OR c.concept_name ILIKE '%manual therapy%'
                         OR c.concept_name ILIKE '%exercise%' OR c.concept_name ILIKE '%neuromuscular%'
                         OR c.concept_name ILIKE '%work hardening%'
                         THEN 'Physical Therapy'
                    WHEN c.concept_name ILIKE '%radiologic%' OR c.concept_name ILIKE '%tomography%'
                         OR c.concept_name ILIKE '%MRI%' OR c.concept_name ILIKE '%imaging%'
                         OR c.concept_name ILIKE '%ultrasound%'
                         THEN 'Imaging'
                    WHEN c.concept_name ILIKE '%injection%' OR c.concept_name ILIKE '%block%'
                         THEN 'Injection'
                    ELSE NULL
                END AS tx_cat
            FROM omop.procedure_occurrence po
            JOIN omop.concept c ON po.procedure_concept_id = c.concept_id
            WHERE EXTRACT(YEAR FROM po.procedure_date) BETWEEN {yr_lo} AND {yr_hi}
        ),
        first_per_cat AS (
            SELECT person_id, tx_cat, MIN(procedure_date) AS first_date
            FROM categorized WHERE tx_cat IS NOT NULL
            GROUP BY 1, 2
        ),
        pt AS (SELECT person_id, first_date FROM first_per_cat WHERE tx_cat='Physical Therapy'),
        img AS (SELECT person_id, first_date FROM first_per_cat WHERE tx_cat='Imaging'),
        inj AS (SELECT person_id, first_date FROM first_per_cat WHERE tx_cat='Injection')
        SELECT 'Imaging vs PT' AS comparison,
               COUNT(*) AS patients_with_both,
               SUM(CASE WHEN img.first_date < pt.first_date THEN 1 ELSE 0 END) AS category_a_first,
               SUM(CASE WHEN pt.first_date <= img.first_date THEN 1 ELSE 0 END) AS category_b_first
        FROM pt JOIN img ON pt.person_id = img.person_id
        UNION ALL
        SELECT 'Injection vs PT',
               COUNT(*),
               SUM(CASE WHEN inj.first_date < pt.first_date THEN 1 ELSE 0 END),
               SUM(CASE WHEN pt.first_date <= inj.first_date THEN 1 ELSE 0 END)
        FROM pt JOIN inj ON pt.person_id = inj.person_id
    """)
    if not pathway.empty and pathway["patients_with_both"].sum() > 0:
        pathway_display = []
        for _, row in pathway.iterrows():
            parts = row["comparison"].split(" vs ")
            total = row["patients_with_both"]
            if total > 0:
                pathway_display.append({
                    "Comparison": row["comparison"],
                    f"{parts[0]} First": row["category_a_first"],
                    f"{parts[1]} First": row["category_b_first"],
                    "Total Patients": total,
                })
        if pathway_display:
            st.dataframe(pd.DataFrame(pathway_display), use_container_width=True, hide_index=True)
    else:
        st.info("Not enough procedure data to analyze treatment pathways.")

    st.divider()
    st.subheader("Injury-to-First-Procedure Delay")
    st.caption("Time from injury to the first recorded procedure (CPT / HCPCS)")
    proc_delay = safe_query(f"""
        WITH injury_dates AS (
            SELECT person_id, MIN(observation_date) AS injury_date
            FROM omop.observation
            WHERE observation_concept_id = 40771952
              AND observation_date IS NOT NULL
            GROUP BY 1
        ),
        first_procs AS (
            SELECT person_id, MIN(procedure_date) AS first_proc_date
            FROM omop.procedure_occurrence
            WHERE procedure_date IS NOT NULL
              AND EXTRACT(YEAR FROM procedure_date) BETWEEN {yr_lo} AND {yr_hi}
            GROUP BY 1
        )
        SELECT
            CASE
                WHEN gap BETWEEN 0 AND 7 THEN '0-7 days'
                WHEN gap BETWEEN 8 AND 30 THEN '8-30 days'
                WHEN gap BETWEEN 31 AND 90 THEN '1-3 months'
                WHEN gap BETWEEN 91 AND 365 THEN '3-12 months'
                ELSE '1+ year'
            END AS bucket,
            COUNT(*) AS patients,
            MIN(gap) AS sort_key,
            ROUND(AVG(gap), 1) AS avg_days
        FROM (
            SELECT i.person_id,
                   DATEDIFF('day', i.injury_date, f.first_proc_date) AS gap
            FROM injury_dates i
            JOIN first_procs f USING (person_id)
            WHERE DATEDIFF('day', i.injury_date, f.first_proc_date) >= 0
        ) sub
        GROUP BY 1 ORDER BY sort_key
    """)
    if not proc_delay.empty:
        col_l, col_r = st.columns([2, 1])
        with col_l:
            fig = px.bar(
                proc_delay, x="bucket", y="patients",
                title="Patients by Time from Injury to First Procedure",
                color="avg_days", color_continuous_scale="YlOrRd",
            )
            fig.update_layout(xaxis_title="Delay from Injury", yaxis_title="Patients")
            st.plotly_chart(fig, use_container_width=True)
        with col_r:
            total = proc_delay["patients"].sum()
            within_30 = proc_delay[proc_delay["sort_key"] <= 30]["patients"].sum()
            st.metric("Median Delay", f"{proc_delay['avg_days'].median():.0f} days")
            st.metric("Procedures within 30 days",
                      f"{within_30 / total * 100:.0f}%" if total > 0 else "N/A")
    explain(
        "Unlike the **visit** delay (first time patient saw a doctor), the **procedure** delay "
        "tracks the first interventional step — imaging, PT, injection, surgery. Longer gaps here "
        "typically mean conservative management was tried first (rest, meds) before escalating. "
        "Short gaps may signal acute injuries (fractures needing immediate setting) or providers "
        "using procedures as front-line treatment."
    )


# ====================  TAB 3: CONDITION INTELLIGENCE  ======================
with tab3:
    col_l, col_r = st.columns(2)

    with col_l:
        top_cond = safe_query(f"""
            SELECT
                co.condition_source_value AS code,
                COALESCE(c.concept_name, co.condition_source_value) AS condition_name,
                COUNT(*) AS occurrences
            FROM omop.condition_occurrence co
            LEFT JOIN omop.concept c ON co.condition_concept_id = c.concept_id
            WHERE EXTRACT(YEAR FROM co.condition_start_date) BETWEEN {yr_lo} AND {yr_hi}
            GROUP BY 1, 2 ORDER BY 3 DESC LIMIT 15
        """)
        if not top_cond.empty:
            fig = px.bar(
                top_cond.sort_values("occurrences"),
                y="condition_name", x="occurrences", orientation="h",
                title="Top 15 Diagnoses",
                color_discrete_sequence=["#EF5350"],
                hover_data=["code"],
            )
            fig.update_layout(yaxis_title="", xaxis_title="Occurrences")
            st.plotly_chart(fig, use_container_width=True)

    with col_r:
        icd_ver = safe_query(f"""
            SELECT
                CASE
                    WHEN condition_source_value ~ '[A-Z]' THEN 'ICD-10'
                    ELSE 'ICD-9'
                END AS version,
                COUNT(*) AS count
            FROM omop.condition_occurrence
            WHERE EXTRACT(YEAR FROM condition_start_date) BETWEEN {yr_lo} AND {yr_hi}
            GROUP BY 1
        """)
        if not icd_ver.empty:
            fig = px.pie(
                icd_ver, names="version", values="count",
                title="ICD-9 vs ICD-10 Diagnoses",
                color_discrete_map={"ICD-10": "#42A5F5", "ICD-9": "#FFA726"},
                hole=0.4,
            )
            st.plotly_chart(fig, use_container_width=True)

    explain(
        "**Left:** The most frequently recorded diagnosis codes. In workers' comp, you'll typically "
        "see musculoskeletal conditions (back pain, sprains, radiculopathy) dominating. "
        "**Right:** ICD-9 vs ICD-10 split. The US mandated ICD-10 in October 2015. Older claims "
        "use ICD-9 codes (numeric like '724.2'), newer claims use ICD-10 (alphanumeric like 'M54.5'). "
        "A mix of both is expected in historical data."
    )

    # ICD transition timeline
    st.subheader("ICD-9 to ICD-10 Transition Timeline")
    icd_timeline = safe_query(f"""
        SELECT
            EXTRACT(YEAR FROM condition_start_date)::INT AS year,
            CASE
                WHEN condition_source_value ~ '[A-Z]' THEN 'ICD-10'
                ELSE 'ICD-9'
            END AS version,
            COUNT(*) AS diagnoses
        FROM omop.condition_occurrence
        WHERE condition_start_date IS NOT NULL
          AND EXTRACT(YEAR FROM condition_start_date) BETWEEN {yr_lo} AND {yr_hi}
        GROUP BY 1, 2 ORDER BY 1
    """)
    if not icd_timeline.empty:
        fig = px.area(
            icd_timeline, x="year", y="diagnoses", color="version",
            title="ICD-9 to ICD-10 Transition (US mandate: Oct 2015)",
            color_discrete_map={"ICD-10": "#42A5F5", "ICD-9": "#FFA726"},
        )
        fig.add_vline(x=2015, line_dash="dash", line_color="red",
                      annotation_text="ICD-10 Mandate (Oct 2015)")
        fig.update_layout(xaxis_title="", yaxis_title="Diagnoses")
        st.plotly_chart(fig, use_container_width=True)

        explain(
            "This shows how the US healthcare system transitioned from ICD-9 to ICD-10 coding. "
            "The red dashed line marks the October 2015 federal mandate. You should see ICD-9 "
            "codes dropping to zero after 2015 and ICD-10 taking over. Any ICD-9 codes after "
            "2015 may indicate data quality issues or late-filed claims."
        )

    # Condition co-occurrence
    st.subheader("Condition Co-occurrence")
    st.caption("Most common diagnosis pairs seen in the same patient")
    cooccur = safe_query(f"""
        WITH person_conditions AS (
            SELECT DISTINCT co.person_id, c.concept_name
            FROM omop.condition_occurrence co
            JOIN omop.concept c ON co.condition_concept_id = c.concept_id
            WHERE c.concept_name != 'No matching concept'
              AND EXTRACT(YEAR FROM co.condition_start_date) BETWEEN {yr_lo} AND {yr_hi}
        )
        SELECT a.concept_name AS condition_a, b.concept_name AS condition_b,
               COUNT(*) AS shared_patients
        FROM person_conditions a
        JOIN person_conditions b
            ON a.person_id = b.person_id AND a.concept_name < b.concept_name
        GROUP BY 1, 2 ORDER BY 3 DESC LIMIT 12
    """)
    if not cooccur.empty:
        cooccur["pair"] = cooccur["condition_a"].str[:30] + "  +  " + cooccur["condition_b"].str[:30]
        fig = px.bar(
            cooccur.sort_values("shared_patients"),
            y="pair", x="shared_patients", orientation="h",
            title="Top Diagnosis Co-occurrences (Same Patient)",
            color_discrete_sequence=["#AB47BC"],
        )
        fig.update_layout(yaxis_title="", xaxis_title="Shared Patients")
        st.plotly_chart(fig, use_container_width=True)

        explain(
            "This shows which diagnoses frequently appear together in the same patient. "
            "Common pairs in workers' comp include **back pain + radiculopathy** (nerve pain "
            "radiating from a spinal injury) and **sprain + disc disorder**. Co-occurring "
            "conditions typically mean higher complexity, longer treatment, and more expensive claims."
        )

    # Patient complexity
    st.subheader("Patient Complexity Distribution")
    complexity = safe_query("""
        SELECT
            CASE
                WHEN cnt = 1 THEN '1'
                WHEN cnt BETWEEN 2 AND 5 THEN '2-5'
                WHEN cnt BETWEEN 6 AND 15 THEN '6-15'
                WHEN cnt BETWEEN 16 AND 50 THEN '16-50'
                ELSE '50+'
            END AS distinct_diagnoses,
            COUNT(*) AS patients,
            MIN(cnt) AS sort_key
        FROM (SELECT person_id, COUNT(DISTINCT condition_concept_id) AS cnt
              FROM omop.condition_occurrence GROUP BY 1)
        GROUP BY 1 ORDER BY sort_key
    """)
    if not complexity.empty:
        fig = px.bar(
            complexity, x="distinct_diagnoses", y="patients",
            title="Patient Complexity (Distinct Diagnosis Count per Patient)",
            color_discrete_sequence=["#26A69A"],
        )
        fig.update_layout(xaxis_title="Distinct Diagnoses", yaxis_title="Patients")
        st.plotly_chart(fig, use_container_width=True)

        explain(
            "Patient complexity measured by how many different diagnoses each patient has. "
            "Patients with just 1 diagnosis had a straightforward injury. Those with 6+ distinct "
            "diagnoses have complex, multi-system injuries — these patients consume disproportionate "
            "resources and are the hardest to return to work."
        )

    st.divider()
    st.subheader("Top Procedure → Diagnosis Treatment Pairs")
    st.caption(
        "Derived from the OMOP `fact_relationship` table, which links each procedure on a "
        "professional claim line to the specific diagnosis it treated (via the CMS-1500 Box 24E "
        "diagnosis pointer)."
    )
    top_pairs = safe_query("""
        SELECT
            p.procedure_source_value AS procedure_code,
            COALESCE(pc.concept_name, p.procedure_source_value) AS procedure_name,
            c.condition_source_value AS diagnosis_code,
            COALESCE(cc.concept_name, c.condition_source_value) AS diagnosis_name,
            COUNT(*) AS pair_count
        FROM omop.fact_relationship fr
        JOIN omop.procedure_occurrence p
          ON fr.fact_id_1 = p.procedure_occurrence_id AND fr.domain_concept_id_1 = 10
        JOIN omop.condition_occurrence c
          ON fr.fact_id_2 = c.condition_occurrence_id AND fr.domain_concept_id_2 = 19
        LEFT JOIN omop.concept pc ON pc.concept_code = p.procedure_source_value
            AND pc.vocabulary_id IN ('CPT4', 'HCPCS')
        LEFT JOIN omop.concept cc ON cc.concept_code = c.condition_source_value
            AND cc.vocabulary_id IN ('ICD10CM', 'ICD9CM')
        WHERE fr.relationship_concept_id = 46233684  -- 'Relevant condition of'
        GROUP BY 1, 2, 3, 4
        ORDER BY 5 DESC
        LIMIT 20
    """)
    if not top_pairs.empty:
        top_pairs["pair_label"] = (
            top_pairs["procedure_code"] + " → " + top_pairs["diagnosis_code"]
        )
        fig = px.bar(
            top_pairs.sort_values("pair_count").tail(20),
            x="pair_count", y="pair_label", orientation="h",
            title="Top 20 Treatment Pairs (Procedure → Diagnosis)",
            color="pair_count", color_continuous_scale="Viridis",
            hover_data=["procedure_name", "diagnosis_name"],
        )
        fig.update_layout(
            xaxis_title="Times Linked",
            yaxis_title="",
            height=600,
        )
        st.plotly_chart(fig, use_container_width=True)
        explain(
            "Each row shows which **procedure** was performed for which **specific diagnosis**. "
            "This is a richer signal than just \"what procedures were on the same bill as what "
            "diagnoses\" — the diagnosis pointer on each line tells us exactly which condition the "
            "procedure is treating. `97110` (Therapeutic Exercise) paired with multiple back and "
            "shoulder ICD codes is a classic workers' comp signature: repetitive physical therapy "
            "for musculoskeletal injuries. `97530` (Therapeutic Activities) appearing alongside "
            "fracture aftercare codes (`S*A`) is the post-op rehab pattern."
        )


# ======================  TAB 4: COST & PAYMENTS  ==========================
with tab4:
    st.caption(
        "Cost data sourced from the OMOP `cost` table — bill-level charges, paid amounts, "
        "WC-allowed amounts, and write-offs."
    )

    cost_summary = safe_query(f"""
        WITH {BILL_COSTS_CTE}
        SELECT
            COUNT(*) AS bills,
            SUM(charge) AS total_charges,
            SUM(paid) AS total_paid,
            AVG(charge) AS avg_charge,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY charge) AS median_charge,
            CASE WHEN SUM(charge) > 0 THEN SUM(paid) / SUM(charge) * 100 ELSE 0 END AS payment_rate
        FROM bill_costs
        WHERE charge IS NOT NULL AND charge > 0
          AND EXTRACT(YEAR FROM bill_date) BETWEEN {yr_lo} AND {yr_hi}
    """)

    if not cost_summary.empty and cost_summary["bills"][0] > 0:
        c1, c2, c3, c4, c5 = st.columns(5)
        c1.metric("Total Bills", f"{cost_summary['bills'][0]:,}")
        c2.metric("Total Charges", f"${cost_summary['total_charges'][0]:,.0f}")
        c3.metric("Total Paid", f"${cost_summary['total_paid'][0]:,.0f}")
        c4.metric("Avg Charge/Bill", f"${cost_summary['avg_charge'][0]:,.0f}")
        c5.metric("Payment Rate", f"{cost_summary['payment_rate'][0]:.1f}%")

        explain(
            "**Total Charges** is what providers billed. **Total Paid** is what insurance actually paid — "
            "these are always lower because Texas workers' comp has a **fee schedule** that caps what "
            "providers can charge. The **Payment Rate** shows the ratio: a 40% rate means providers "
            "received 40 cents per dollar billed. Low rates mean the fee schedule heavily discounts charges."
        )
        st.divider()

    col_l, col_r = st.columns(2)

    with col_l:
        charge_dist = safe_query(f"""
            WITH {BILL_COSTS_CTE}
            SELECT
                CASE
                    WHEN charge < 100 THEN '$0-99'
                    WHEN charge < 500 THEN '$100-499'
                    WHEN charge < 1000 THEN '$500-999'
                    WHEN charge < 5000 THEN '$1K-5K'
                    WHEN charge < 10000 THEN '$5K-10K'
                    WHEN charge < 50000 THEN '$10K-50K'
                    ELSE '$50K+'
                END AS charge_range,
                COUNT(*) AS bills,
                MIN(charge) AS sort_key
            FROM bill_costs
            WHERE charge IS NOT NULL AND charge > 0
              AND EXTRACT(YEAR FROM bill_date) BETWEEN {yr_lo} AND {yr_hi}
            GROUP BY 1 ORDER BY sort_key
        """)
        if not charge_dist.empty:
            fig = px.bar(
                charge_dist, x="charge_range", y="bills",
                title="Bill Charge Distribution",
                color_discrete_sequence=["#26A69A"],
            )
            fig.update_layout(xaxis_title="Charge Amount", yaxis_title="Number of Bills")
            st.plotly_chart(fig, use_container_width=True)

    with col_r:
        cost_by_type = safe_query(f"""
            WITH {BILL_COSTS_CTE}
            SELECT claim_type,
                   COUNT(*) AS bills,
                   SUM(charge) AS total_charges,
                   AVG(charge) AS avg_charge
            FROM bill_costs
            WHERE charge IS NOT NULL AND charge > 0
              AND EXTRACT(YEAR FROM bill_date) BETWEEN {yr_lo} AND {yr_hi}
            GROUP BY 1 ORDER BY 3 DESC
        """)
        if not cost_by_type.empty:
            fig = px.pie(
                cost_by_type, names="claim_type", values="total_charges",
                title="Total Charges by Claim Type",
                color_discrete_sequence=["#42A5F5", "#FF7043", "#66BB6A"],
                hole=0.4,
            )
            st.plotly_chart(fig, use_container_width=True)

    # Charges vs Payments over time
    cost_yr = safe_query(f"""
        WITH {BILL_COSTS_CTE}
        SELECT EXTRACT(YEAR FROM bill_date)::INT AS year,
               SUM(charge) AS total_charges,
               SUM(paid) AS total_paid
        FROM bill_costs
        WHERE charge IS NOT NULL AND charge > 0
          AND EXTRACT(YEAR FROM bill_date) BETWEEN {yr_lo} AND {yr_hi}
        GROUP BY 1 ORDER BY 1
    """)
    if not cost_yr.empty:
        fig = go.Figure()
        fig.add_trace(go.Bar(x=cost_yr["year"], y=cost_yr["total_charges"],
                             name="Charges", marker_color="#42A5F5"))
        fig.add_trace(go.Bar(x=cost_yr["year"], y=cost_yr["total_paid"],
                             name="Paid", marker_color="#66BB6A"))
        fig.update_layout(
            title="Charges vs. Payments by Year",
            barmode="group",
            xaxis_title="", yaxis_title="Amount ($)",
        )
        st.plotly_chart(fig, use_container_width=True)

    # --- WC Revenue Cycle: write-off and collection rates ---
    st.divider()
    st.subheader("Workers' Comp Revenue Cycle")
    st.caption("Line-level charges, allowed amounts, and collected payments from `omop.cost`")

    wc_cycle = safe_query(f"""
        WITH {LINE_COSTS_CTE}
        SELECT
            claim_type,
            COUNT(*) AS lines,
            SUM(charge) AS total_charged,
            SUM(allowed) AS total_allowed,
            SUM(paid_by_payer) AS total_paid,
            CASE WHEN SUM(charge) > 0
                 THEN (SUM(charge) - SUM(allowed)) / SUM(charge) * 100
                 ELSE 0 END AS writeoff_pct,
            CASE WHEN SUM(allowed) > 0
                 THEN SUM(paid_by_payer) / SUM(allowed) * 100
                 ELSE 0 END AS collection_pct
        FROM line_costs
        WHERE charge IS NOT NULL AND charge > 0
          AND line_date IS NOT NULL
          AND EXTRACT(YEAR FROM line_date) BETWEEN {yr_lo} AND {yr_hi}
        GROUP BY 1 ORDER BY 3 DESC
    """)

    if not wc_cycle.empty:
        totals = safe_query(f"""
            WITH {LINE_COSTS_CTE}
            SELECT
                SUM(charge) AS total_charged,
                SUM(allowed) AS total_allowed,
                SUM(paid_by_payer) AS total_paid,
                (SUM(charge) - SUM(allowed)) AS total_writeoff,
                CASE WHEN SUM(charge) > 0
                     THEN (SUM(charge) - SUM(allowed)) / SUM(charge) * 100 ELSE 0 END AS writeoff_pct,
                CASE WHEN SUM(allowed) > 0
                     THEN SUM(paid_by_payer) / SUM(allowed) * 100 ELSE 0 END AS collection_pct
            FROM line_costs
            WHERE charge IS NOT NULL AND charge > 0
              AND line_date IS NOT NULL
              AND EXTRACT(YEAR FROM line_date) BETWEEN {yr_lo} AND {yr_hi}
        """)
        if not totals.empty:
            k1, k2, k3, k4 = st.columns(4)
            k1.metric("Total Charged (lines)", f"${totals['total_charged'][0]:,.0f}")
            k2.metric("Fee-Schedule Allowed", f"${totals['total_allowed'][0]:,.0f}")
            k3.metric("Contract Write-off", f"${totals['total_writeoff'][0]:,.0f}",
                      delta=f"{totals['writeoff_pct'][0]:.1f}% of billed",
                      delta_color="off")
            k4.metric("Collection Rate", f"{totals['collection_pct'][0]:.1f}%",
                      help="Of the fee-schedule-allowed amount, what share was actually paid.")

        col_l, col_r = st.columns(2)
        with col_l:
            fig = px.bar(
                wc_cycle, x="claim_type", y=["total_charged", "total_allowed", "total_paid"],
                title="Charged → Allowed → Paid by Claim Type",
                barmode="group",
                labels={"value": "Amount ($)", "variable": "Stage"},
                color_discrete_sequence=["#42A5F5", "#FFB74D", "#66BB6A"],
            )
            fig.update_layout(xaxis_title="", yaxis_title="Amount ($)", legend_title="")
            st.plotly_chart(fig, use_container_width=True)

        with col_r:
            fig = go.Figure()
            fig.add_trace(go.Bar(
                x=wc_cycle["claim_type"], y=wc_cycle["writeoff_pct"],
                name="Write-off %", marker_color="#EF5350",
            ))
            fig.add_trace(go.Bar(
                x=wc_cycle["claim_type"], y=wc_cycle["collection_pct"],
                name="Collection %", marker_color="#26A69A",
            ))
            fig.update_layout(
                title="Write-off & Collection Rates by Claim Type",
                xaxis_title="", yaxis_title="Percent",
                barmode="group",
            )
            st.plotly_chart(fig, use_container_width=True)

        explain(
            "**Write-off** = (billed − allowed) / billed. For Texas workers' comp this is the "
            "contractual discount mandated by the DWC fee schedule — providers can't collect the "
            "full charge. A 60%+ write-off is normal; institutional (facility) claims typically "
            "have the highest write-off because facility charges are most discounted. "
            "**Collection rate** = paid / allowed. This is what *actually* got collected "
            "out of the already-discounted allowed amount. A 100% collection rate means the "
            "payer paid the full allowed; anything less is pending, denied, or disputed."
        )

    st.divider()
    st.subheader("Employer Cost Burden (Top 15)")
    st.caption("Total amount paid per employer across all their workers' claims")
    emp_cost = safe_query("""
        WITH employers AS (
            SELECT DISTINCT person_id, value_as_string AS employer_fein
            FROM omop.observation
            WHERE observation_concept_id = 21492865
              AND value_as_string IS NOT NULL
        ),
        patient_paid AS (
            SELECT v.person_id, SUM(c.total_paid) AS paid, SUM(c.total_charge) AS charged
            FROM omop.cost c
            JOIN omop.visit_occurrence v ON c.cost_event_id = v.visit_occurrence_id
            WHERE c.cost_domain_id = 'Visit' AND c.total_paid IS NOT NULL
            GROUP BY 1
        )
        SELECT
            e.employer_fein,
            COUNT(DISTINCT e.person_id) AS workers,
            SUM(pp.charged) AS total_charged,
            SUM(pp.paid) AS total_paid
        FROM employers e
        LEFT JOIN patient_paid pp ON e.person_id = pp.person_id
        GROUP BY 1
        ORDER BY 4 DESC NULLS LAST
        LIMIT 15
    """)
    if not emp_cost.empty:
        fig = px.bar(
            emp_cost.sort_values("total_paid"),
            x="total_paid", y="employer_fein", orientation="h",
            title="Top 15 Employers by Total Paid",
            color="workers", color_continuous_scale="Oranges",
            hover_data=["total_charged", "workers"],
        )
        fig.update_layout(
            xaxis_title="Total Paid ($)",
            yaxis_title="Employer FEIN",
            height=520,
        )
        st.plotly_chart(fig, use_container_width=True)
        explain(
            "Sum of all bill-level payments tied to each employer's injured workers. Combined "
            "with worker count, this shows **per-employer claim exposure**. A small employer with "
            "one catastrophic claim can dominate the top ranking; a large employer with many small "
            "claims is a different risk profile. For real benchmarking, normalize by employer "
            "payroll or workforce size (not available in this dataset)."
        )

    st.divider()

    # Procedure Cost Efficiency
    st.subheader("Procedure Cost Efficiency by Treatment Category")
    st.caption("Average charges vs. payments by treatment category from OMOP cost table")
    explain(
        "This breaks down what different types of treatment cost and how much actually gets paid. "
        "**Physical Therapy** often has the lowest payment rate because PT charges are heavily "
        "discounted by the fee schedule. **Imaging** (MRI, X-ray) and **Injections** tend to have "
        "higher payment rates. The gap between blue (charged) and green (paid) bars shows the "
        "discount for each category. Large gaps may discourage providers from offering those services."
    )
    proc_cost = safe_query(f"""
        SELECT
            CASE
                WHEN pc.concept_name ILIKE '%therapeutic%' OR pc.concept_name ILIKE '%manual therapy%'
                     OR pc.concept_name ILIKE '%exercise%' OR pc.concept_name ILIKE '%work hardening%'
                     OR pc.concept_name ILIKE '%neuromuscular%'
                     THEN 'Physical Therapy / Rehab'
                WHEN pc.concept_name ILIKE '%radiologic%' OR pc.concept_name ILIKE '%tomography%'
                     OR pc.concept_name ILIKE '%MRI%' OR pc.concept_name ILIKE '%imaging%'
                     OR pc.concept_name ILIKE '%ultrasound%'
                     THEN 'Imaging / Radiology'
                WHEN pc.concept_name ILIKE '%injection%' OR pc.concept_name ILIKE '%block%'
                     THEN 'Injection / Block'
                WHEN pc.concept_name ILIKE '%stimulation%' OR pc.concept_name ILIKE '%electro%'
                     THEN 'Electrical Stimulation'
                WHEN pc.concept_name ILIKE '%evaluation%' OR pc.concept_name ILIKE '%examination%'
                     OR pc.concept_name ILIKE '%disability%'
                     THEN 'Evaluation / Exam'
                ELSE 'Other Procedures'
            END AS proc_category,
            COUNT(*) AS line_items,
            ROUND(AVG(co.total_charge), 2) AS avg_charge,
            ROUND(AVG(co.total_paid), 2) AS avg_paid,
            ROUND(CASE WHEN SUM(co.total_charge) > 0
                  THEN SUM(co.total_paid) / SUM(co.total_charge) * 100
                  ELSE 0 END, 1) AS payment_rate_pct
        FROM omop.procedure_occurrence po
        JOIN omop.concept pc ON po.procedure_concept_id = pc.concept_id
        JOIN omop.cost co ON po.visit_detail_id = co.cost_event_id
            AND co.cost_domain_id = 'Visit Detail'
        WHERE EXTRACT(YEAR FROM po.procedure_date) BETWEEN {yr_lo} AND {yr_hi}
        GROUP BY 1 ORDER BY line_items DESC
    """)
    if not proc_cost.empty:
        fig = go.Figure()
        fig.add_trace(go.Bar(
            y=proc_cost["proc_category"], x=proc_cost["avg_charge"],
            name="Avg Charge", marker_color="#42A5F5", orientation="h"))
        fig.add_trace(go.Bar(
            y=proc_cost["proc_category"], x=proc_cost["avg_paid"],
            name="Avg Paid", marker_color="#66BB6A", orientation="h"))
        fig.update_layout(
            title="Average Charge vs. Paid by Procedure Category",
            barmode="group", yaxis_title="", xaxis_title="Amount ($)",
        )
        st.plotly_chart(fig, use_container_width=True)

    # Charges by claim type over time
    cost_yr_type = safe_query(f"""
        WITH {BILL_COSTS_CTE}
        SELECT EXTRACT(YEAR FROM bill_date)::INT AS year,
               claim_type,
               SUM(charge) AS total_charges
        FROM bill_costs
        WHERE charge IS NOT NULL AND charge > 0
          AND EXTRACT(YEAR FROM bill_date) BETWEEN {yr_lo} AND {yr_hi}
        GROUP BY 1, 2 ORDER BY 1
    """)
    if not cost_yr_type.empty:
        fig = px.bar(
            cost_yr_type, x="year", y="total_charges", color="claim_type",
            title="Total Charges by Year and Claim Type",
            color_discrete_sequence=["#42A5F5", "#FF7043", "#66BB6A"],
            barmode="stack",
        )
        fig.update_layout(xaxis_title="", yaxis_title="Total Charges ($)",
                          legend_title="Claim Type")
        st.plotly_chart(fig, use_container_width=True)

    explain(
        "This shows total charges broken down by claim type (professional = doctor visits, "
        "institutional = hospital stays, pharmacy = prescriptions) across years. "
        "**Institutional** claims are typically the most expensive per claim. Shifts in the mix "
        "over time can indicate changes in treatment patterns or injury severity."
    )

    # Insurance carriers
    st.subheader("Insurance Carriers")
    carriers = safe_query("""
        SELECT plan_source_value AS carrier,
               COUNT(DISTINCT person_id) AS patients,
               COUNT(*) AS policies
        FROM omop.payer_plan_period
        WHERE plan_source_value IS NOT NULL AND plan_source_value != ''
        GROUP BY 1 ORDER BY 2 DESC LIMIT 15
    """)
    if not carriers.empty:
        fig = px.bar(
            carriers.sort_values("patients"),
            y="carrier", x="patients", orientation="h",
            title="Top 15 Insurance Carriers by Patient Count",
            color_discrete_sequence=["#5C6BC0"],
        )
        fig.update_layout(yaxis_title="", xaxis_title="Patients")
        st.plotly_chart(fig, use_container_width=True)

        explain(
            "Insurance carriers (companies) ranked by how many injured workers they cover. "
            "In Texas workers' comp, employers choose their carrier. High concentration in a few "
            "carriers can indicate market dominance. Carriers with many patients have more "
            "negotiating leverage with providers and may impose stricter utilization review."
        )


# ==================  TAB 5: RX & OPIOID MONITOR  =========================
with tab5:
    st.subheader("Injury-to-First-Prescription Delay")
    st.caption("Time from injury to the first recorded drug fill — a proxy for pain-medication onset")
    rx_delay = safe_query(f"""
        WITH injury_dates AS (
            SELECT person_id, MIN(observation_date) AS injury_date
            FROM omop.observation
            WHERE observation_concept_id = 40771952
              AND observation_date IS NOT NULL
            GROUP BY 1
        ),
        first_drugs AS (
            SELECT person_id, MIN(drug_exposure_start_date) AS first_rx_date
            FROM omop.drug_exposure
            WHERE drug_exposure_start_date IS NOT NULL
              AND EXTRACT(YEAR FROM drug_exposure_start_date) BETWEEN {yr_lo} AND {yr_hi}
            GROUP BY 1
        )
        SELECT
            CASE
                WHEN gap BETWEEN 0 AND 7 THEN '0-7 days'
                WHEN gap BETWEEN 8 AND 30 THEN '8-30 days'
                WHEN gap BETWEEN 31 AND 90 THEN '1-3 months'
                WHEN gap BETWEEN 91 AND 365 THEN '3-12 months'
                ELSE '1+ year'
            END AS bucket,
            COUNT(*) AS patients,
            MIN(gap) AS sort_key,
            ROUND(AVG(gap), 1) AS avg_days
        FROM (
            SELECT i.person_id,
                   DATEDIFF('day', i.injury_date, f.first_rx_date) AS gap
            FROM injury_dates i
            JOIN first_drugs f USING (person_id)
            WHERE DATEDIFF('day', i.injury_date, f.first_rx_date) >= 0
        ) sub
        GROUP BY 1 ORDER BY sort_key
    """)
    if not rx_delay.empty:
        col_l, col_r = st.columns([2, 1])
        with col_l:
            fig = px.bar(
                rx_delay, x="bucket", y="patients",
                title="Patients by Time from Injury to First Prescription",
                color="avg_days", color_continuous_scale="Magma",
            )
            fig.update_layout(xaxis_title="Delay from Injury", yaxis_title="Patients")
            st.plotly_chart(fig, use_container_width=True)
        with col_r:
            total = rx_delay["patients"].sum()
            within_30 = rx_delay[rx_delay["sort_key"] <= 30]["patients"].sum()
            st.metric("Median Delay", f"{rx_delay['avg_days'].median():.0f} days")
            st.metric("Filled within 30 days",
                      f"{within_30 / total * 100:.0f}%" if total > 0 else "N/A")
    explain(
        "How quickly injured workers get prescription drugs after an injury. A tight 0-7 day "
        "peak is typical for acute injuries where pain meds are prescribed immediately. Later "
        "spikes in the 1-3 month or 3-12 month buckets may indicate **chronic pain management** "
        "or delayed escalation — both strongly correlate with long-term opioid risk in workers' "
        "comp research."
    )

    st.divider()
    st.subheader("Opioid Prescribing Rate Over Time")
    st.caption("Percentage of all drug prescriptions that are opioids, by year")

    explain(
        "**Opioid over-prescribing is the signature issue in workers' comp.** Injured workers "
        "are at high risk for opioid dependency because their injuries cause real pain and they "
        "often can't work while recovering. Texas enacted a **closed formulary** in 2011 and "
        "tightened it in 2017, restricting which drugs can be prescribed in workers' comp claims. "
        "This chart tracks whether those reforms are working. A declining opioid percentage is "
        "a positive sign. The red dashed line marks the 2017 reform."
    )

    opioid_trend = safe_query(f"""
        WITH all_drugs AS (
            SELECT EXTRACT(YEAR FROM de.drug_exposure_start_date)::INT AS year,
                   COUNT(*) AS total_rx
            FROM omop.drug_exposure de
            WHERE EXTRACT(YEAR FROM de.drug_exposure_start_date) BETWEEN {yr_lo} AND {yr_hi}
            GROUP BY 1
        ),
        opioid_drugs AS (
            SELECT EXTRACT(YEAR FROM de.drug_exposure_start_date)::INT AS year,
                   COUNT(*) AS opioid_rx
            FROM omop.drug_exposure de
            JOIN omop.concept c ON de.drug_concept_id = c.concept_id
            WHERE {OPIOID_FILTER}
              AND EXTRACT(YEAR FROM de.drug_exposure_start_date) BETWEEN {yr_lo} AND {yr_hi}
            GROUP BY 1
        )
        SELECT a.year, a.total_rx, COALESCE(o.opioid_rx, 0) AS opioid_rx,
               ROUND(COALESCE(o.opioid_rx, 0) * 100.0 / a.total_rx, 1) AS opioid_pct
        FROM all_drugs a
        LEFT JOIN opioid_drugs o ON a.year = o.year
        ORDER BY 1
    """)
    if not opioid_trend.empty:
        col_l, col_r = st.columns([3, 1])
        with col_l:
            fig = go.Figure()
            fig.add_trace(go.Bar(x=opioid_trend["year"], y=opioid_trend["total_rx"],
                                 name="Total Rx", marker_color="#90CAF9"))
            fig.add_trace(go.Bar(x=opioid_trend["year"], y=opioid_trend["opioid_rx"],
                                 name="Opioid Rx", marker_color="#EF5350"))
            fig.add_trace(go.Scatter(x=opioid_trend["year"], y=opioid_trend["opioid_pct"],
                                     name="Opioid %", yaxis="y2", mode="lines+markers",
                                     line=dict(color="#FF6F00", width=3)))
            fig.update_layout(
                title="Opioid Prescribing Trend",
                barmode="group",
                yaxis=dict(title="Prescriptions"),
                yaxis2=dict(title="Opioid %", overlaying="y", side="right", range=[0, 100]),
            )
            fig.add_vline(x=2017, line_dash="dash", line_color="red",
                          annotation_text="TX Closed Formulary (2017)")
            st.plotly_chart(fig, use_container_width=True)
        with col_r:
            if len(opioid_trend) > 0:
                latest = opioid_trend.iloc[-1]
                st.metric("Latest Opioid Rate", f"{latest['opioid_pct']}%")
                st.metric("Total Opioid Rx (all years)", f"{opioid_trend['opioid_rx'].sum():,}")
    else:
        st.info("No drug exposure data available.")

    # Opioid Escalation Tracker
    st.subheader("Opioid Escalation Tracker")
    st.caption("Patients by number of opioid prescriptions and duration of opioid eras")
    explain(
        "**Left chart:** How many opioid prescriptions each patient received. A patient with 1 Rx "
        "likely had short-term post-injury pain relief. Patients with 7+ prescriptions may be on "
        "a path toward chronic opioid use or dependency — these are red flags for case managers. "
        "**Right chart:** How long each patient's opioid treatment lasted (\"era\" = continuous "
        "period on that drug). Eras over 90 days are considered **chronic opioid use** by most "
        "guidelines. Eras over 1 year indicate serious dependency risk."
    )

    col_l, col_r = st.columns(2)
    with col_l:
        repeat_rx = safe_query(f"""
            SELECT
                CASE
                    WHEN opioid_rx_count = 1 THEN '1 Rx'
                    WHEN opioid_rx_count BETWEEN 2 AND 3 THEN '2-3 Rx'
                    WHEN opioid_rx_count BETWEEN 4 AND 6 THEN '4-6 Rx'
                    WHEN opioid_rx_count BETWEEN 7 AND 12 THEN '7-12 Rx'
                    ELSE '13+ Rx'
                END AS rx_bucket,
                COUNT(*) AS patients,
                MIN(opioid_rx_count) AS sort_key
            FROM (
                SELECT de.person_id, COUNT(*) AS opioid_rx_count
                FROM omop.drug_exposure de
                JOIN omop.concept c ON de.drug_concept_id = c.concept_id
                WHERE {OPIOID_FILTER}
                  AND EXTRACT(YEAR FROM de.drug_exposure_start_date) BETWEEN {yr_lo} AND {yr_hi}
                GROUP BY 1
            ) sub
            GROUP BY 1 ORDER BY sort_key
        """)
        if not repeat_rx.empty:
            fig = px.bar(
                repeat_rx, x="rx_bucket", y="patients",
                title="Patients by Opioid Prescription Count",
                color_discrete_sequence=["#EF5350"],
            )
            fig.update_layout(xaxis_title="", yaxis_title="Patients")
            st.plotly_chart(fig, use_container_width=True)

    with col_r:
        era_dur = safe_query("""
            SELECT c.concept_name AS opioid,
                   COUNT(*) AS eras,
                   ROUND(AVG(DATEDIFF('day', drug_era_start_date, drug_era_end_date)), 1) AS avg_days,
                   MAX(DATEDIFF('day', drug_era_start_date, drug_era_end_date)) AS max_days
            FROM omop.drug_era de
            JOIN omop.concept c ON de.drug_concept_id = c.concept_id
            WHERE c.concept_name ILIKE '%hydrocodone%'
               OR c.concept_name ILIKE '%oxycodone%'
               OR c.concept_name ILIKE '%morphine%'
               OR c.concept_name ILIKE '%fentanyl%'
               OR c.concept_name ILIKE '%tramadol%'
               OR c.concept_name ILIKE '%codeine%'
               OR c.concept_name ILIKE '%hydromorphone%'
               OR c.concept_name ILIKE '%methadone%'
               OR c.concept_name ILIKE '%buprenorphine%'
            GROUP BY 1 ORDER BY eras DESC
        """)
        if not era_dur.empty:
            fig = px.bar(
                era_dur.sort_values("avg_days"),
                y="opioid", x="avg_days", orientation="h",
                title="Average Opioid Era Duration (days)",
                color_discrete_sequence=["#FF7043"],
                hover_data=["max_days", "eras"],
            )
            fig.update_layout(yaxis_title="", xaxis_title="Average Days")
            st.plotly_chart(fig, use_container_width=True)


# ==================  TAB 6: PROVIDER ANALYTICS  ===========================
with tab6:
    st.subheader("Provider Caseload Concentration")
    st.caption("How patients are distributed across providers")

    explain(
        "Provider concentration reveals the structure of the workers' comp care network. "
        "In workers' comp, a few high-volume providers often treat a large share of patients — "
        "this can be normal (occupational medicine specialists) or concerning (potential "
        "over-treatment or referral mills). **If the top 5 providers treat over 50% of patients**, "
        "it suggests a highly concentrated network. Regulators and carriers monitor this for "
        "fraud detection and network adequacy."
    )

    provider_vol = safe_query(f"""
        WITH pv AS (
            SELECT vo.provider_id,
                   p.provider_name,
                   COUNT(DISTINCT vo.person_id) AS patients,
                   COUNT(*) AS total_visits
            FROM omop.visit_occurrence vo
            JOIN omop.provider p ON vo.provider_id = p.provider_id
            WHERE vo.provider_id IS NOT NULL
              AND EXTRACT(YEAR FROM vo.visit_start_date) BETWEEN {yr_lo} AND {yr_hi}
            GROUP BY 1, 2
        )
        SELECT provider_name, patients, total_visits,
               ROUND(total_visits * 1.0 / patients, 1) AS visits_per_patient,
               ROUND(patients * 100.0 / (SELECT COUNT(*) FROM omop.person), 1) AS pct_of_all_patients
        FROM pv
        ORDER BY patients DESC
        LIMIT 20
    """)
    if not provider_vol.empty:
        col_l, col_r = st.columns([2, 1])
        with col_l:
            fig = px.bar(
                provider_vol.head(15).sort_values("patients"),
                y="provider_name", x="patients", orientation="h",
                title="Top 15 Providers by Patient Count",
                color_discrete_sequence=["#5C6BC0"],
                hover_data=["total_visits", "visits_per_patient"],
            )
            fig.update_layout(yaxis_title="", xaxis_title="Patients")
            st.plotly_chart(fig, use_container_width=True)
        with col_r:
            top5_pts = provider_vol.head(5)["patients"].sum()
            total_pts = safe_query("SELECT COUNT(*) AS n FROM omop.person")
            if not total_pts.empty:
                pct = top5_pts / total_pts["n"][0] * 100
                st.metric("Top 5 Providers Treat", f"{pct:.0f}% of patients")
            st.metric("Total Providers", f"{len(provider_vol):,}+")

    # Provider concentration distribution
    provider_dist = safe_query(f"""
        WITH pv AS (
            SELECT vo.provider_id, COUNT(DISTINCT vo.person_id) AS patients
            FROM omop.visit_occurrence vo
            WHERE vo.provider_id IS NOT NULL
              AND EXTRACT(YEAR FROM vo.visit_start_date) BETWEEN {yr_lo} AND {yr_hi}
            GROUP BY 1
        )
        SELECT
            CASE
                WHEN patients = 1 THEN '1 patient'
                WHEN patients BETWEEN 2 AND 5 THEN '2-5 patients'
                WHEN patients BETWEEN 6 AND 20 THEN '6-20 patients'
                WHEN patients BETWEEN 21 AND 100 THEN '21-100 patients'
                ELSE '100+ patients'
            END AS caseload,
            COUNT(*) AS providers,
            MIN(patients) AS sort_key
        FROM pv GROUP BY 1 ORDER BY sort_key
    """)
    if not provider_dist.empty:
        fig = px.bar(
            provider_dist, x="caseload", y="providers",
            title="Provider Distribution by Caseload Size",
            color_discrete_sequence=["#26A69A"],
        )
        fig.update_layout(xaxis_title="Caseload Size", yaxis_title="Number of Providers")
        st.plotly_chart(fig, use_container_width=True)

        explain(
            "This shows how many providers fall into each caseload bracket. A healthy network "
            "has a long tail of low-volume providers (specialists who see a few WC patients) "
            "and a small number of high-volume providers (occupational medicine practices). "
            "If many providers treat only 1 patient, it may indicate fragmented care with "
            "poor continuity."
        )


# =======================  TAB 7: GEOGRAPHY  ===============================
with tab7:
    col_l, col_r = st.columns(2)

    with col_l:
        cities = safe_query("""
            SELECT city, state, COUNT(*) AS patients
            FROM omop.location
            WHERE city IS NOT NULL AND state IS NOT NULL
            GROUP BY 1, 2 ORDER BY 3 DESC LIMIT 20
        """)
        if not cities.empty:
            fig = px.bar(
                cities.sort_values("patients"),
                y="city", x="patients", orientation="h",
                title="Top 20 Cities by Patient Count",
                color_discrete_sequence=["#5C6BC0"],
                hover_data=["state"],
            )
            fig.update_layout(yaxis_title="", xaxis_title="Patients")
            st.plotly_chart(fig, use_container_width=True)

    with col_r:
        states = safe_query("""
            SELECT COALESCE(state, 'Unknown') AS state, COUNT(*) AS patients
            FROM omop.location WHERE state IS NOT NULL
            GROUP BY 1 ORDER BY 2 DESC LIMIT 10
        """)
        if not states.empty:
            fig = px.pie(
                states, names="state", values="patients",
                title="Patient Distribution by State",
                color_discrete_sequence=px.colors.qualitative.Set2,
            )
            st.plotly_chart(fig, use_container_width=True)

    explain(
        "Geographic distribution of injured workers. Texas workers' comp data is statewide, "
        "so you'll see major metro areas (Houston, Dallas, San Antonio, Austin) dominating. "
        "Out-of-state patients may have been injured in Texas or transferred for specialized care."
    )

    # Care facility distribution
    st.subheader("Healthcare Facility Concentration")
    fac_cities = safe_query("""
        SELECT l.city, l.state, COUNT(DISTINCT cs.care_site_id) AS facilities
        FROM omop.care_site cs
        JOIN omop.location l ON cs.location_id = l.location_id
        WHERE l.city IS NOT NULL
        GROUP BY 1, 2 ORDER BY 3 DESC LIMIT 15
    """)
    if not fac_cities.empty:
        fig = px.bar(
            fac_cities.sort_values("facilities"),
            y="city", x="facilities", orientation="h",
            title="Top 15 Cities by Care Facility Count",
            color_discrete_sequence=["#FF7043"],
            hover_data=["state"],
        )
        fig.update_layout(yaxis_title="", xaxis_title="Facilities")
        st.plotly_chart(fig, use_container_width=True)

    # Patient-to-facility ratio
    st.subheader("Patient-to-Facility Ratio by City")
    st.caption("Higher ratios may indicate underserved areas")
    explain(
        "This ratio divides the number of injured workers by the number of care facilities in each city. "
        "**High ratios** (many patients per facility) may indicate underserved areas where injured "
        "workers have limited access to care. **Low ratios** indicate well-served areas with plenty "
        "of provider options. Cities with 0 facilities but many patients are especially concerning — "
        "those workers must travel to other cities for treatment."
    )
    ratio = safe_query("""
        WITH city_patients AS (
            SELECT city, state, COUNT(*) AS patients
            FROM omop.location WHERE city IS NOT NULL
            GROUP BY 1, 2
        ),
        city_facilities AS (
            SELECT l.city, l.state, COUNT(DISTINCT cs.care_site_id) AS facilities
            FROM omop.care_site cs
            JOIN omop.location l ON cs.location_id = l.location_id
            WHERE l.city IS NOT NULL
            GROUP BY 1, 2
        )
        SELECT p.city, p.state, p.patients, COALESCE(f.facilities, 0) AS facilities,
               CASE WHEN COALESCE(f.facilities, 0) > 0
                    THEN ROUND(p.patients * 1.0 / f.facilities, 1)
                    ELSE NULL END AS patients_per_facility
        FROM city_patients p
        LEFT JOIN city_facilities f ON p.city = f.city AND p.state = f.state
        WHERE p.patients >= 50
        ORDER BY patients_per_facility DESC NULLS LAST
        LIMIT 15
    """)
    if not ratio.empty:
        fig = px.bar(
            ratio.sort_values("patients_per_facility"),
            y="city", x="patients_per_facility", orientation="h",
            title="Patients per Facility (cities with 50+ patients)",
            color="patients_per_facility",
            color_continuous_scale="YlOrRd",
        )
        fig.update_layout(yaxis_title="", xaxis_title="Patients per Facility")
        st.plotly_chart(fig, use_container_width=True)


# ---------------------------------------------------------------------------
# Footer
# ---------------------------------------------------------------------------

st.divider()
st.caption(
    "Data: [Texas Dept of Insurance, Division of Workers' Compensation]"
    "(https://www.tdi.texas.gov/wc/data.html) | "
    "Model: [OMOP CDM v5.4](https://ohdsi.github.io/CommonDataModel/) | "
    "Built with Streamlit + DuckDB + dbt + dlt"
)
