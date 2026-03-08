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

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

DB_PATH = os.environ.get(
    "TXWC_DB_PATH",
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "tx_workers_comp.db"),
)

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


# Reusable CTE that unions all raw header tables with charge and paid amounts.
# Every raw header table has: total_charge_per_bill, total_amount_paid_per_bill,
# reporting_period_start_date.
ALL_BILLS_CTE = """
    all_bills AS (
        SELECT TRY_CAST(total_charge_per_bill AS FLOAT) AS charge,
               TRY_CAST(total_amount_paid_per_bill AS FLOAT) AS paid,
               TRY_CAST(reporting_period_start_date AS DATE) AS bill_date,
               'Professional' AS claim_type
        FROM raw.professional_header_current
        UNION ALL
        SELECT TRY_CAST(total_charge_per_bill AS FLOAT),
               TRY_CAST(total_amount_paid_per_bill AS FLOAT),
               TRY_CAST(reporting_period_start_date AS DATE), 'Professional'
        FROM raw.professional_header_historical
        UNION ALL
        SELECT TRY_CAST(total_charge_per_bill AS FLOAT),
               TRY_CAST(total_amount_paid_per_bill AS FLOAT),
               TRY_CAST(reporting_period_start_date AS DATE), 'Institutional'
        FROM raw.institutional_header_current
        UNION ALL
        SELECT TRY_CAST(total_charge_per_bill AS FLOAT),
               TRY_CAST(total_amount_paid_per_bill AS FLOAT),
               TRY_CAST(reporting_period_start_date AS DATE), 'Institutional'
        FROM raw.institutional_header_historical
        UNION ALL
        SELECT TRY_CAST(total_charge_per_bill AS FLOAT),
               TRY_CAST(total_amount_paid_per_bill AS FLOAT),
               TRY_CAST(reporting_period_start_date AS DATE), 'Pharmacy'
        FROM raw.pharmacy_header_current
        UNION ALL
        SELECT TRY_CAST(total_charge_per_bill AS FLOAT),
               TRY_CAST(total_amount_paid_per_bill AS FLOAT),
               TRY_CAST(reporting_period_start_date AS DATE), 'Pharmacy'
        FROM raw.pharmacy_header_historical
    )
"""


# ---------------------------------------------------------------------------
# Sidebar
# ---------------------------------------------------------------------------

st.sidebar.title("Filters")

years = query("""
    SELECT DISTINCT EXTRACT(YEAR FROM visit_start_date)::INT AS yr
    FROM omop.visit_occurrence
    WHERE visit_start_date IS NOT NULL
    ORDER BY yr
""")["yr"].tolist()

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

tab1, tab2, tab3, tab4, tab5 = st.tabs(
    ["Overview", "Injury Profile", "Condition Intelligence", "Cost & Payments", "Geography"]
)

# ===========================  TAB 1: OVERVIEW  =============================
with tab1:
    kpis = query(f"""
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

    c1, c2, c3, c4, c5, c6 = st.columns(6)
    c1.metric("Patients", f"{kpis['patients'][0]:,}")
    c2.metric("Visits", f"{kpis['visits'][0]:,}")
    c3.metric("Diagnoses", f"{kpis['conditions'][0]:,}")
    c4.metric("Providers", f"{kpis['providers'][0]:,}")
    c5.metric("Facilities", f"{kpis['facilities'][0]:,}")
    c6.metric("Carriers", f"{kpis['carriers'][0]:,}")

    st.divider()
    col_l, col_r = st.columns(2)

    with col_l:
        visits_yr = query(f"""
            SELECT EXTRACT(YEAR FROM visit_start_date)::INT AS year,
                   COUNT(*) AS visits
            FROM omop.visit_occurrence
            WHERE visit_start_date IS NOT NULL
              AND EXTRACT(YEAR FROM visit_start_date) BETWEEN {yr_lo} AND {yr_hi}
            GROUP BY 1 ORDER BY 1
        """)
        fig = px.area(
            visits_yr, x="year", y="visits",
            title="Visit Volume by Year",
            color_discrete_sequence=["#2196F3"],
        )
        fig.update_layout(xaxis_title="", yaxis_title="Visits")
        st.plotly_chart(fig, use_container_width=True)

    with col_r:
        gender = query("""
            SELECT
                CASE gender_source_value
                    WHEN 'M' THEN 'Male'
                    WHEN 'F' THEN 'Female'
                    ELSE 'Unknown'
                END AS gender,
                COUNT(*) AS count
            FROM omop.person GROUP BY 1 ORDER BY 2 DESC
        """)
        fig = px.pie(
            gender, names="gender", values="count",
            title="Patient Gender Distribution",
            color_discrete_map={"Male": "#2196F3", "Female": "#E91E63", "Unknown": "#9E9E9E"},
            hole=0.4,
        )
        st.plotly_chart(fig, use_container_width=True)

    col_l2, col_r2 = st.columns(2)

    with col_l2:
        age = query("""
            SELECT (year_of_birth / 10) * 10 AS decade, COUNT(*) AS patients
            FROM omop.person
            WHERE year_of_birth BETWEEN 1930 AND 2005
            GROUP BY 1 ORDER BY 1
        """)
        fig = px.bar(
            age, x="decade", y="patients",
            title="Patients by Birth Decade",
            color_discrete_sequence=["#26A69A"],
        )
        fig.update_layout(xaxis_title="Birth Decade", yaxis_title="Patients")
        st.plotly_chart(fig, use_container_width=True)

    with col_r2:
        repeat = query("""
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
        fig = px.bar(
            repeat, x="visit_bucket", y="patients",
            title="Patient Utilization (Visits per Patient)",
            color_discrete_sequence=["#FF7043"],
        )
        fig.update_layout(xaxis_title="Visits per Patient", yaxis_title="Patients")
        st.plotly_chart(fig, use_container_width=True)

    # Monthly seasonality
    monthly = query(f"""
        SELECT EXTRACT(MONTH FROM visit_start_date)::INT AS month, COUNT(*) AS visits
        FROM omop.visit_occurrence
        WHERE visit_start_date IS NOT NULL
          AND EXTRACT(YEAR FROM visit_start_date) BETWEEN {yr_lo} AND {yr_hi}
        GROUP BY 1 ORDER BY 1
    """)
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


# ========================  TAB 2: INJURY PROFILE  ==========================
with tab2:
    st.subheader("Injury Body Region Analysis")
    st.caption("Diagnoses classified by anatomical region based on SNOMED concept names")

    body = query(f"""
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

    # Visit type breakdown
    st.subheader("Visit Type & Duration")
    col_l, col_r = st.columns(2)

    with col_l:
        vtypes = query(f"""
            SELECT COALESCE(c.concept_name, 'Unknown') AS visit_type, COUNT(*) AS visits
            FROM omop.visit_occurrence vo
            LEFT JOIN omop.concept c ON vo.visit_concept_id = c.concept_id
            WHERE EXTRACT(YEAR FROM vo.visit_start_date) BETWEEN {yr_lo} AND {yr_hi}
            GROUP BY 1 ORDER BY 2 DESC LIMIT 8
        """)
        fig = px.bar(
            vtypes.sort_values("visits"),
            y="visit_type", x="visits", orientation="h",
            title="Visit Types",
            color_discrete_sequence=["#42A5F5"],
        )
        fig.update_layout(yaxis_title="", xaxis_title="Visits")
        st.plotly_chart(fig, use_container_width=True)

    with col_r:
        duration = query(f"""
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
        fig = px.bar(
            duration, x="duration", y="visits",
            title="Visit Duration Distribution",
            color_discrete_sequence=["#26A69A"],
        )
        fig.update_layout(xaxis_title="Length of Stay", yaxis_title="Visits")
        st.plotly_chart(fig, use_container_width=True)


# ====================  TAB 3: CONDITION INTELLIGENCE  ======================
with tab3:
    col_l, col_r = st.columns(2)

    with col_l:
        top_cond = query(f"""
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
        icd_ver = query(f"""
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

    # ICD transition timeline
    st.subheader("ICD-9 to ICD-10 Transition Timeline")
    icd_timeline = query(f"""
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

    # Condition co-occurrence
    st.subheader("Condition Co-occurrence")
    st.caption("Most common diagnosis pairs seen in the same patient")
    cooccur = query(f"""
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

    # Patient complexity
    st.subheader("Patient Complexity Distribution")
    complexity = query("""
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


# ======================  TAB 4: COST & PAYMENTS  ==========================
with tab4:
    st.caption(
        "Cost data sourced from raw billing headers (professional, institutional, pharmacy)."
    )

    cost_summary = query(f"""
        WITH {ALL_BILLS_CTE}
        SELECT
            COUNT(*) AS bills,
            SUM(charge) AS total_charges,
            SUM(paid) AS total_paid,
            AVG(charge) AS avg_charge,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY charge) AS median_charge,
            CASE WHEN SUM(charge) > 0 THEN SUM(paid) / SUM(charge) * 100 ELSE 0 END AS payment_rate
        FROM all_bills
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
        st.divider()

    col_l, col_r = st.columns(2)

    with col_l:
        charge_dist = query(f"""
            WITH {ALL_BILLS_CTE}
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
            FROM all_bills
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
        cost_by_type = query(f"""
            WITH {ALL_BILLS_CTE}
            SELECT claim_type,
                   COUNT(*) AS bills,
                   SUM(charge) AS total_charges,
                   AVG(charge) AS avg_charge
            FROM all_bills
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
    cost_yr = query(f"""
        WITH {ALL_BILLS_CTE}
        SELECT EXTRACT(YEAR FROM bill_date)::INT AS year,
               SUM(charge) AS total_charges,
               SUM(paid) AS total_paid
        FROM all_bills
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

    # Charges by claim type over time
    cost_yr_type = query(f"""
        WITH {ALL_BILLS_CTE}
        SELECT EXTRACT(YEAR FROM bill_date)::INT AS year,
               claim_type,
               SUM(charge) AS total_charges
        FROM all_bills
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

    # Insurance carriers
    st.subheader("Insurance Carriers")
    carriers = query("""
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


# =======================  TAB 5: GEOGRAPHY  ===============================
with tab5:
    col_l, col_r = st.columns(2)

    with col_l:
        cities = query("""
            SELECT city, state, COUNT(*) AS patients
            FROM omop.location
            WHERE city IS NOT NULL AND state IS NOT NULL
            GROUP BY 1, 2 ORDER BY 3 DESC LIMIT 20
        """)
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
        states = query("""
            SELECT COALESCE(state, 'Unknown') AS state, COUNT(*) AS patients
            FROM omop.location WHERE state IS NOT NULL
            GROUP BY 1 ORDER BY 2 DESC LIMIT 10
        """)
        fig = px.pie(
            states, names="state", values="patients",
            title="Patient Distribution by State",
            color_discrete_sequence=px.colors.qualitative.Set2,
        )
        st.plotly_chart(fig, use_container_width=True)

    # Care facility distribution
    st.subheader("Healthcare Facility Concentration")
    fac_cities = query("""
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
    ratio = query("""
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
    "Built with Streamlit + DuckDB + dbt"
)
