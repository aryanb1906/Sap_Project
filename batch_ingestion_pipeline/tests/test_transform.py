import pandas as pd

from batch_ingestion_pipeline.transform import transform_orders


def test_transform_orders_cleans_and_enriches_rows():
    frame = pd.DataFrame(
        [
            {
                "order_id": 1,
                "customer_id": "c1",
                "order_date": "2026-03-01",
                "product": "phone",
                "category": "electronics",
                "quantity": 2,
                "unit_price": 100,
                "country": "india",
            },
            {
                "order_id": 2,
                "customer_id": "c2",
                "order_date": "invalid-date",
                "product": "bag",
                "category": "bags",
                "quantity": 1,
                "unit_price": 50,
                "country": "india",
            },
        ]
    )

    accepted, rejected = transform_orders(frame, "sample.csv")

    assert len(accepted) == 1
    assert len(rejected) == 1
    assert accepted.iloc[0]["total_amount"] == 200.0
    assert accepted.iloc[0]["category"] == "Electronics"
    assert accepted.iloc[0]["order_month"] == "2026-03"
