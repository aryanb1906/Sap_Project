import sqlite3

from batch_ingestion_pipeline.config import PipelineConfig
from batch_ingestion_pipeline.pipeline import BatchIngestionPipeline


def test_pipeline_loads_sample_files(tmp_path):
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir()
    db_path = tmp_path / "warehouse" / "ingestion.db"
    log_dir = tmp_path / "logs"

    sample_one = raw_dir / "orders_a.csv"
    sample_one.write_text(
        "order_id,customer_id,order_date,product,category,quantity,unit_price,country\n"
        "1,C1,2026-01-01,Phone,Electronics,2,100,India\n"
        "2,C2,2026-01-02,Book,Stationery,3,10,India\n",
        encoding="utf-8",
    )

    sample_two = raw_dir / "orders_b.csv"
    sample_two.write_text(
        "order_id,customer_id,order_date,product,category,quantity,unit_price,country\n"
        "3,C3,2026-02-01,Desk,Furniture,1,150,India\n"
        "4,C4,invalid-date,Chair,Furniture,1,80,India\n",
        encoding="utf-8",
    )

    config = PipelineConfig(input_dir=raw_dir, db_path=db_path, log_dir=log_dir, batch_name="pytest")
    pipeline = BatchIngestionPipeline(config)
    summary = pipeline.run(reset_db=True)

    assert summary["files_processed"] == 2
    assert summary["total_rows"] == 8
    assert summary["total_revenue"] == 1394.0
    assert len(summary["top_categories"]) >= 1

    with sqlite3.connect(db_path) as connection:
        loaded = connection.execute("SELECT COUNT(*) FROM orders_fact").fetchone()[0]
        audits = connection.execute("SELECT COUNT(*) FROM batch_runs").fetchone()[0]

    assert loaded == 8
    assert audits == 2
