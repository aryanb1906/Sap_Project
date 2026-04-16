from __future__ import annotations

import sqlite3
from pathlib import Path

import pandas as pd

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS orders_fact (
    order_id TEXT PRIMARY KEY,
    customer_id TEXT NOT NULL,
    order_date TEXT NOT NULL,
    product TEXT NOT NULL,
    category TEXT NOT NULL,
    quantity INTEGER NOT NULL,
    unit_price REAL NOT NULL,
    total_amount REAL NOT NULL,
    country TEXT NOT NULL,
    order_year INTEGER NOT NULL,
    order_month TEXT NOT NULL,
    source_file TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS batch_runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_name TEXT NOT NULL,
    source_file TEXT NOT NULL,
    rows_read INTEGER NOT NULL,
    rows_loaded INTEGER NOT NULL,
    rows_rejected INTEGER NOT NULL,
    status TEXT NOT NULL,
    run_at TEXT NOT NULL
);
"""


def connect(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(db_path)
    connection.execute("PRAGMA foreign_keys = ON")
    return connection


def initialize_database(connection: sqlite3.Connection) -> None:
    connection.executescript(SCHEMA_SQL)
    connection.commit()


def reset_database(connection: sqlite3.Connection) -> None:
    connection.executescript(
        """
        DROP TABLE IF EXISTS batch_runs;
        DROP TABLE IF EXISTS orders_fact;
        """
    )
    initialize_database(connection)


def load_dataframe(connection: sqlite3.Connection, frame: pd.DataFrame) -> int:
    if frame.empty:
        return 0
    frame.to_sql("orders_fact", connection, if_exists="append", index=False)
    connection.commit()
    return len(frame)


def write_audit_row(
    connection: sqlite3.Connection,
    batch_name: str,
    source_file: str,
    rows_read: int,
    rows_loaded: int,
    rows_rejected: int,
    status: str,
    run_at: str,
) -> None:
    connection.execute(
        """
        INSERT INTO batch_runs (
            batch_name, source_file, rows_read, rows_loaded, rows_rejected, status, run_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (batch_name, source_file, rows_read, rows_loaded, rows_rejected, status, run_at),
    )
    connection.commit()


def fetch_summary(connection: sqlite3.Connection) -> dict[str, object]:
    orders = pd.read_sql_query("SELECT * FROM orders_fact", connection)
    if orders.empty:
        return {
            "total_rows": 0,
            "total_revenue": 0.0,
            "top_categories": [],
        }

    top_categories = (
        orders.groupby("category", as_index=False)["total_amount"].sum().sort_values("total_amount", ascending=False)
    )
    return {
        "total_rows": int(len(orders)),
        "total_revenue": float(orders["total_amount"].sum()),
        "top_categories": top_categories.head(5).to_dict(orient="records"),
    }
