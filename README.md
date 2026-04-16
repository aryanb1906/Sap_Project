# SAP Project - Batch Ingestion Dashboard

**Name:** Aryan Bhargava  
**Roll No:** 23051491

A polished data engineering project that ingests CSV files, validates and transforms records, loads curated data into SQLite, and presents the results in a Streamlit dashboard.

## What This Project Demonstrates

- Batch CSV ingestion from a landing folder
- Data validation and cleaning
- Curated warehouse loading with SQLite
- Batch audit logging
- Business summary reporting
- Interactive Streamlit frontend
- Windows-friendly demo launcher

## Repository Structure

```text
Sap_Project/
├── app.py
├── ARCHITECTURE.md
└── batch_ingestion_pipeline/
    ├── app.py
    ├── DEMO.md
    ├── README.md
    ├── run_demo.ps1
    ├── pyproject.toml
    ├── data/
    ├── src/
    └── tests/
```

## How It Works

1. Raw CSV files are placed in `batch_ingestion_pipeline/data/raw/`.
2. The pipeline validates required columns and cleans bad values.
3. Valid rows are loaded into `orders_fact` in SQLite.
4. Each file load is recorded in `batch_runs`.
5. The dashboard reads the warehouse and visualizes the results.

## Run the Project

### 1. Install Dependencies

```powershell
cd batch_ingestion_pipeline
python -m venv .venv
.venv\Scripts\activate
pip install -e .
```

### 2. Run the Batch Demo

```powershell
.\run_demo.ps1
```

### 3. Open the Dashboard

From the repository root:

```powershell
streamlit run app.py
```

## Dashboard Features

- KPI cards for total orders, revenue, average order value, and loaded rows
- Filters for category, country, month, and batch name
- Revenue visualizations by category and month
- Batch audit history
- Loaded orders table
- Data quality snapshot
- Country mix and load success indicators

## Expected Output

After running the demo, the dashboard shows data similar to:

- Files processed: 2
- Total rows loaded: 8
- Total revenue: 1394.00
- Top category: Electronics

## Data Model

- `orders_fact` stores the cleaned order-level facts
- `batch_runs` stores the audit trail for each ingestion run

## Architecture

See [ARCHITECTURE.md](ARCHITECTURE.md) for the full system flow.

## Why This Project Is Strong

This project combines ETL logic, data quality handling, persistence, and analytics into one portfolio-ready solution. It is simple enough to run locally, but complete enough to demonstrate real data engineering thinking.
