from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import streamlit as st

ROOT = Path(__file__).resolve().parent
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from batch_ingestion_pipeline.database import connect


APP_TITLE = "Batch Ingestion Dashboard"
DB_PATH = ROOT / "data" / "warehouse" / "ingestion.db"

st.set_page_config(page_title=APP_TITLE, layout="wide", page_icon="DB")

st.markdown(
    """
    <style>
    .stApp {
        background:
            radial-gradient(circle at top left, rgba(59, 130, 246, 0.18), transparent 28%),
            radial-gradient(circle at top right, rgba(16, 185, 129, 0.12), transparent 24%),
            linear-gradient(180deg, #f8fbff 0%, #eef4ff 100%);
    }
    .block-container {
        padding-top: 1.5rem;
        padding-bottom: 2rem;
    }
    .hero {
        background: linear-gradient(135deg, rgba(15, 23, 42, 0.97), rgba(30, 41, 59, 0.92));
        color: white;
        padding: 1.4rem 1.6rem;
        border-radius: 22px;
        border: 1px solid rgba(148, 163, 184, 0.22);
        box-shadow: 0 20px 50px rgba(15, 23, 42, 0.18);
        margin-bottom: 1rem;
    }
    .hero h1 {
        font-size: 2.2rem;
        margin-bottom: 0.25rem;
    }
    .hero p {
        margin-top: 0.3rem;
        margin-bottom: 0;
        color: rgba(255, 255, 255, 0.82);
        line-height: 1.5;
    }
    .pill-row {
        display: flex;
        flex-wrap: wrap;
        gap: 0.5rem;
        margin-top: 0.9rem;
    }
    .pill {
        display: inline-block;
        padding: 0.35rem 0.75rem;
        border-radius: 999px;
        background: rgba(255, 255, 255, 0.12);
        border: 1px solid rgba(255, 255, 255, 0.12);
        font-size: 0.82rem;
        letter-spacing: 0.02em;
    }
    .metric-card {
        background: linear-gradient(135deg, #0f172a 0%, #1e293b 100%);
        color: white;
        padding: 1rem 1.2rem;
        border-radius: 18px;
        border: 1px solid rgba(255,255,255,0.08);
        box-shadow: 0 16px 35px rgba(15, 23, 42, 0.16);
    }
    .small-label {
        font-size: 0.85rem;
        opacity: 0.8;
        text-transform: uppercase;
        letter-spacing: 0.08em;
    }
    .big-value {
        font-size: 1.9rem;
        font-weight: 700;
        margin-top: 0.25rem;
    }
    </style>
    """,
    unsafe_allow_html=True,
)


def load_table(connection, query: str) -> pd.DataFrame:
    return pd.read_sql_query(query, connection)


@st.cache_data(show_spinner=False)
def load_data(db_file: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    path = Path(db_file)
    if not path.exists():
        return pd.DataFrame(), pd.DataFrame()
    with connect(path) as connection:
        orders = load_table(connection, "SELECT * FROM orders_fact ORDER BY order_date DESC")
        audits = load_table(connection, "SELECT * FROM batch_runs ORDER BY run_at DESC")
    return orders, audits


def metric_card(label: str, value: str) -> None:
    st.markdown(
        f"""
        <div class="metric-card">
            <div class="small-label">{label}</div>
            <div class="big-value">{value}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def format_currency(value: float) -> str:
    return f"{value:,.2f}"


st.title(APP_TITLE)
st.markdown(
    """
    <div class="hero">
        <h1>Batch Ingestion Dashboard</h1>
        <p>
            Track raw CSV intake, validate row quality, inspect the curated warehouse,
            and review batch audit history from one simple interface.
        </p>
        <div class="pill-row">
            <span class="pill">CSV ingestion</span>
            <span class="pill">Data quality</span>
            <span class="pill">Audit trail</span>
            <span class="pill">Revenue analytics</span>
            <span class="pill">SQLite warehouse</span>
        </div>
    </div>
    """,
    unsafe_allow_html=True,
)

orders_df, audits_df = load_data(str(DB_PATH))

if orders_df.empty:
    st.warning("No warehouse data found. Run the ingestion demo first with .\\batch_ingestion_pipeline\\run_demo.ps1 or batch-ingest from that folder.")
    st.stop()

for column in ["order_date", "order_year", "order_month"]:
    if column in orders_df.columns:
        orders_df[column] = orders_df[column].astype(str)

orders_df["order_date"] = pd.to_datetime(orders_df["order_date"], errors="coerce")

left, center, right, fourth = st.columns(4)
with left:
    metric_card("Total Orders", str(len(orders_df)))
with center:
    metric_card("Revenue", format_currency(float(orders_df["total_amount"].sum())))
with right:
    avg_order_value = float(orders_df["total_amount"].mean()) if not orders_df.empty else 0.0
    metric_card("Avg Order Value", format_currency(avg_order_value))
with fourth:
    loaded_rows = int(audits_df["rows_loaded"].sum()) if not audits_df.empty else 0
    metric_card("Rows Loaded", str(loaded_rows))

st.write("")

sidebar = st.sidebar
sidebar.header("Filters")

available_categories = sorted(orders_df["category"].dropna().unique().tolist())
available_countries = sorted(orders_df["country"].dropna().unique().tolist())
available_months = sorted(orders_df["order_month"].dropna().astype(str).unique().tolist())
available_batches = sorted(audits_df["batch_name"].dropna().unique().tolist()) if not audits_df.empty else []

selected_categories = sidebar.multiselect("Category", available_categories, default=available_categories)
selected_countries = sidebar.multiselect("Country", available_countries, default=available_countries)
selected_months = sidebar.multiselect("Order Month", available_months, default=available_months)
selected_batches = sidebar.multiselect("Batch Name", available_batches, default=available_batches) if available_batches else []

filtered_orders = orders_df.copy()
if selected_categories:
    filtered_orders = filtered_orders[filtered_orders["category"].isin(selected_categories)]
if selected_countries:
    filtered_orders = filtered_orders[filtered_orders["country"].isin(selected_countries)]
if selected_months:
    filtered_orders = filtered_orders[filtered_orders["order_month"].isin(selected_months)]

if not audits_df.empty and selected_batches:
    filtered_audits = audits_df[audits_df["batch_name"].isin(selected_batches)]
else:
    filtered_audits = audits_df.copy()

summary_left, summary_right = st.columns([2, 1])
with summary_left:
    st.subheader("Revenue by Category")
    category_revenue = (
        filtered_orders.groupby("category", as_index=False)["total_amount"].sum().sort_values("total_amount", ascending=False)
    )
    if category_revenue.empty:
        st.info("No rows match the selected filters.")
    else:
        st.bar_chart(category_revenue.set_index("category")["total_amount"])

    st.subheader("Revenue by Month")
    monthly_revenue = (
        filtered_orders.groupby("order_month", as_index=False)["total_amount"].sum().sort_values("order_month")
    )
    if not monthly_revenue.empty:
        st.line_chart(monthly_revenue.set_index("order_month")["total_amount"])

with summary_right:
    st.subheader("Pipeline Health")
    total_loaded = int(filtered_audits["rows_loaded"].sum()) if not filtered_audits.empty else 0
    total_rejected = int(filtered_audits["rows_rejected"].sum()) if not filtered_audits.empty else 0
    total_read = int(filtered_audits["rows_read"].sum()) if not filtered_audits.empty else len(filtered_orders)
    success_rate = (total_loaded / total_read * 100) if total_read else 0

    st.metric("Rows Read", total_read)
    st.metric("Rows Loaded", total_loaded)
    st.metric("Rows Rejected", total_rejected)
    st.metric("Load Success %", f"{success_rate:.1f}%")

    st.subheader("Country Mix")
    country_mix = filtered_orders.groupby("country", as_index=False)["total_amount"].sum().sort_values("total_amount", ascending=False)
    if not country_mix.empty:
        st.dataframe(country_mix, use_container_width=True, hide_index=True)

st.divider()

orders_tab, audit_tab, quality_tab = st.tabs(["Loaded Orders", "Batch Audit", "Data Quality"])

with orders_tab:
    st.subheader("Loaded Orders")
    if filtered_orders.empty:
        st.info("No rows match the selected filters.")
    else:
        st.dataframe(
            filtered_orders[
                [
                    "order_id",
                    "customer_id",
                    "order_date",
                    "product",
                    "category",
                    "quantity",
                    "unit_price",
                    "total_amount",
                    "country",
                    "source_file",
                ]
            ],
            use_container_width=True,
            hide_index=True,
        )

with audit_tab:
    st.subheader("Batch Runs")
    if filtered_audits.empty:
        st.info("No audit rows found for the selected filters.")
    else:
        st.dataframe(filtered_audits, use_container_width=True, hide_index=True)

with quality_tab:
    st.subheader("Data Quality Snapshot")
    quality_summary = pd.DataFrame(
        [
            {"check": "Null order IDs", "value": int(orders_df["order_id"].isna().sum())},
            {"check": "Null customer IDs", "value": int(orders_df["customer_id"].isna().sum())},
            {"check": "Unique orders", "value": int(orders_df["order_id"].nunique())},
            {"check": "Source files", "value": int(orders_df["source_file"].nunique())},
            {"check": "Rejected rows", "value": int(filtered_audits["rows_rejected"].sum()) if not filtered_audits.empty else 0},
        ]
    )
    st.dataframe(quality_summary, use_container_width=True, hide_index=True)
    st.caption("The ingestion pipeline filters invalid rows before loading into the warehouse.")
