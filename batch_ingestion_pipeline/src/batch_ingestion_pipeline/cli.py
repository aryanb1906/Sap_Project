from __future__ import annotations

import argparse
from pathlib import Path

from .config import PipelineConfig
from .pipeline import BatchIngestionPipeline


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Batch CSV ingestion pipeline")
    parser.add_argument("--input-dir", type=Path, default=Path("data/raw"), help="Directory containing CSV files")
    parser.add_argument(
        "--db-path",
        type=Path,
        default=Path("data/warehouse/ingestion.db"),
        help="SQLite database path",
    )
    parser.add_argument("--log-dir", type=Path, default=Path("data/logs"), help="Directory for pipeline logs")
    parser.add_argument("--batch-name", type=str, default="default_batch", help="Name stored in audit logs")
    parser.add_argument("--reset-db", action="store_true", help="Recreate tables before loading")
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    config = PipelineConfig(
        input_dir=args.input_dir,
        db_path=args.db_path,
        log_dir=args.log_dir,
        batch_name=args.batch_name,
    )
    pipeline = BatchIngestionPipeline(config)
    summary = pipeline.run(reset_db=args.reset_db)

    print("\nBatch Ingestion Summary")
    print("-" * 24)
    print(f"Files processed: {summary['files_processed']}")
    print(f"Total rows loaded: {summary['total_rows']}")
    print(f"Total revenue: {summary['total_revenue']:.2f}")
    print("Top categories:")
    for item in summary["top_categories"]:
        print(f"  - {item['category']}: {item['total_amount']:.2f}")


if __name__ == "__main__":
    main()
