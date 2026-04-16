# Demo Walkthrough

This project is a ready-to-run batch ingestion demo for a data engineering portfolio.

## Demo Goal

Show how raw CSV files are validated, cleaned, loaded into SQLite, and summarized through a repeatable batch pipeline.

## Demo Assets

- Raw CSV inputs in `data/raw/`
- Pipeline code in `src/batch_ingestion_pipeline/`
- SQLite warehouse in `data/warehouse/`
- Audit logs in `data/logs/`
- Windows launcher in `run_demo.ps1`
- Streamlit dashboard in `app.py`

## Demo Run

```powershell
.\run_demo.ps1
```

Expected output:

```text
Batch Ingestion Summary
------------------------
Files processed: 2
Total rows loaded: 8
Total revenue: 1394.00
Top categories:
  - Electronics: 906.00
  - Furniture: 405.00
  - Bags: 70.00
  - Stationery: 13.00
```

## Dashboard Run

After loading the data, run:

```powershell
streamlit run app.py
```

The dashboard includes KPI cards, filters, revenue charts, batch audit history, loaded rows, and a data quality snapshot.

## What This Demonstrates

- CSV ingestion from a landing zone
- Column normalization and data validation
- Transformation into a curated analytical table
- SQLite persistence for a lightweight warehouse
- Batch audit logging
- Summary reporting for business review
