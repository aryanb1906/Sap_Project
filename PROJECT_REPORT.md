# Project Documentation

**Project Name:** SAP Project - Batch Ingestion Dashboard  
**Name:** Aryan Bhargava  
**Roll No:** 23051491

## 1. Title

SAP Project - Batch Ingestion Dashboard

## 2. Problem Statement

Raw CSV files often contain inconsistent formatting, missing values, and no audit trail. This makes them hard to trust, analyze, and present. The goal of this project was to build a compact batch pipeline that cleans the data, stores it in a structured warehouse, and displays the results in a professional dashboard.

## 3. Solution / Features

- Reads multiple CSV files from a raw landing folder
- Normalizes column names and validates required fields
- Cleans text, numeric, and date fields before loading
- Calculates `total_amount`, `order_year`, and `order_month`
- Writes batch audit records for each file processed
- Provides KPI cards, filters, charts, and audit views in Streamlit
- Includes a root-level launcher for easy GitHub repository execution

## 4. Screenshots

- [Project Banner](assets/banner.svg)
- [Dashboard Preview](assets/dashboard-preview.svg)

## 5. Tech Stack

- Python 3.12
- Pandas
- SQLite
- Streamlit
- Plotly
- PowerShell

## 6. Unique Points

- Clean repo presentation with badges and a visual banner
- Batch audit trail for each CSV file
- Data quality snapshot in the dashboard
- Simple Windows demo runner
- Ready-to-present portfolio structure

## 7. Future Improvements

- Move from SQLite to PostgreSQL
- Add scheduled execution with Airflow or Task Scheduler
- Export reports as PDF from the dashboard
- Store rejected rows in a dedicated error table
- Deploy the dashboard online

## 8. Conclusion

This project demonstrates a complete batch data engineering workflow: ingestion, validation, transformation, storage, audit logging, and presentation. It is lightweight enough for a student project while still being polished enough for a portfolio submission.

## Export to PDF

Open `PROJECT_REPORT.html` in a browser and print it to PDF using the browser print dialog.
