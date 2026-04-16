from __future__ import annotations

import pandas as pd

REQUIRED_COLUMNS = [
    "order_id",
    "customer_id",
    "order_date",
    "product",
    "category",
    "quantity",
    "unit_price",
    "country",
]


def normalize_columns(frame: pd.DataFrame) -> pd.DataFrame:
    renamed = frame.copy()
    renamed.columns = [
        str(column).strip().lower().replace(" ", "_").replace("-", "_")
        for column in renamed.columns
    ]
    return renamed


def _clean_text_series(series: pd.Series) -> pd.Series:
    cleaned = series.astype("string").fillna("")
    cleaned = cleaned.str.strip()
    cleaned = cleaned.replace({"nan": "", "none": "", "null": ""}, regex=False)
    return cleaned


def validate_required_columns(frame: pd.DataFrame) -> list[str]:
    missing = [column for column in REQUIRED_COLUMNS if column not in frame.columns]
    return missing


def transform_orders(frame: pd.DataFrame, source_file: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    normalized = normalize_columns(frame)
    missing = validate_required_columns(normalized)
    if missing:
        raise ValueError(f"Missing required columns: {', '.join(missing)}")

    cleaned = normalized.copy()
    cleaned["source_file"] = source_file
    cleaned["order_id"] = _clean_text_series(cleaned["order_id"])
    cleaned["customer_id"] = _clean_text_series(cleaned["customer_id"])
    cleaned["product"] = cleaned["product"].fillna("unknown").astype(str).str.strip().str.title()
    cleaned["category"] = cleaned["category"].fillna("unknown").astype(str).str.strip().str.title()
    cleaned["country"] = cleaned["country"].fillna("unknown").astype(str).str.strip().str.title()
    cleaned["order_date"] = pd.to_datetime(cleaned["order_date"], errors="coerce")
    cleaned["quantity"] = pd.to_numeric(cleaned["quantity"], errors="coerce")
    cleaned["unit_price"] = pd.to_numeric(cleaned["unit_price"], errors="coerce")

    invalid_mask = (
        cleaned["order_id"].eq("")
        | cleaned["customer_id"].eq("")
        | cleaned["order_date"].isna()
        | cleaned["quantity"].isna()
        | cleaned["unit_price"].isna()
        | (cleaned["quantity"] <= 0)
        | (cleaned["unit_price"] <= 0)
    )
    rejected = cleaned.loc[invalid_mask].copy()
    accepted = cleaned.loc[~invalid_mask].copy()

    accepted["quantity"] = accepted["quantity"].astype(int)
    accepted["unit_price"] = accepted["unit_price"].astype(float)
    accepted["total_amount"] = (accepted["quantity"] * accepted["unit_price"]).round(2)
    accepted["order_year"] = accepted["order_date"].dt.year.astype(int)
    accepted["order_month"] = accepted["order_date"].dt.strftime("%Y-%m")
    accepted["order_date"] = accepted["order_date"].dt.strftime("%Y-%m-%d")

    columns = [
        "order_id",
        "customer_id",
        "order_date",
        "product",
        "category",
        "quantity",
        "unit_price",
        "total_amount",
        "country",
        "order_year",
        "order_month",
        "source_file",
    ]
    return accepted[columns], rejected
