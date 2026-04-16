# Batch Ingestion Pipeline

This is a polished CSV-to-database batch ingestion demo for a data engineering portfolio.

## Project Goal

Take raw CSV files from a landing folder, validate and clean them, load the curated output into SQLite, and record every batch run in an audit table.

## Highlights

- Batch ingestion from multiple CSV files
- Column normalization and quality checks
- Lightweight transformation into a curated fact table
- SQLite warehouse for simple local execution
- Audit logging for every file processed
- Final summary for revenue and category analysis
- Windows-ready demo launcher
- Streamlit dashboard for warehouse exploration

## Quick Start

```powershell
python -m venv .venv
.venv\Scripts\activate
pip install -e .
```

If you want the test tooling as well:

```powershell
pip install -e .[dev]
```

To run the dashboard, install the project dependencies and launch:

```powershell
streamlit run app.py
```

If you are in the parent folder `sap project`, run:

```powershell
streamlit run app.py
```

## Run the Demo

On Windows, the easiest way to run the project is:

```powershell
.\run_demo.ps1
```

Or run the CLI directly:

```powershell
batch-ingest --input-dir data/raw --db-path data/warehouse/ingestion.db --log-dir data/logs --reset-db --batch-name demo_run
```

To explore the loaded warehouse visually:

```powershell
streamlit run app.py
```

From the parent folder, use the root-level `app.py` launcher.

## Expected Output

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

## Folder Layout

```text
batch_ingestion_pipeline/
├── data/
│   ├── raw/
│   ├── warehouse/
│   └── logs/
├── src/batch_ingestion_pipeline/
├── tests/
├── DEMO.md
├── run_demo.ps1
├── pyproject.toml
└── README.md
```

## Input Schema

The source CSV files should include these columns:

- order_id
- customer_id
- order_date
- product
- category
- quantity
- unit_price
- country

## Output Tables

- `orders_fact` stores cleaned and enriched order rows
- `batch_runs` stores the audit trail for each ingested file

## Dashboard Features

- KPI cards for total orders, revenue, average order value, and loaded rows
- Filters for category, country, month, and batch name
- Revenue charts by category and month
- Batch audit table
- Loaded orders table
- Data quality snapshot

## Testing

```powershell
pytest
```

## Why This Version Is Better

The project now reads like a finished demo package instead of only a code sample. It has a simple run path, sample data, an auditable warehouse, and a documented output that makes it easy to present in an interview or submission.
