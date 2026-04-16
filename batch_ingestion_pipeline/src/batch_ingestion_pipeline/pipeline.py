from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from .config import PipelineConfig
from .database import connect, fetch_summary, initialize_database, load_dataframe, reset_database, write_audit_row
from .transform import transform_orders


@dataclass(frozen=True)
class FileResult:
    source_file: str
    rows_read: int
    rows_loaded: int
    rows_rejected: int
    status: str


class BatchIngestionPipeline:
    def __init__(self, config: PipelineConfig) -> None:
        self.config = config
        self.logger = logging.getLogger("batch_ingestion_pipeline")

    def setup_logging(self) -> None:
        self.config.log_dir.mkdir(parents=True, exist_ok=True)
        handlers = [logging.FileHandler(self.config.log_file, encoding="utf-8")]
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
            handlers=handlers,
            force=True,
        )
        self.logger.info("Pipeline logging initialized")

    def discover_files(self) -> list[Path]:
        return sorted(self.config.input_dir.glob("*.csv"))

    def run(self, reset_db: bool = False) -> dict[str, object]:
        self.setup_logging()
        files = self.discover_files()
        if not files:
            raise FileNotFoundError(f"No CSV files found in {self.config.input_dir}")

        with connect(self.config.db_path) as connection:
            if reset_db:
                reset_database(connection)
            else:
                initialize_database(connection)

            results: list[FileResult] = []
            for file_path in files:
                results.append(self.process_file(connection, file_path))

            summary = fetch_summary(connection)
            summary["files_processed"] = len(files)
            summary["file_results"] = [result.__dict__ for result in results]
            return summary

    def process_file(self, connection, file_path: Path) -> FileResult:
        self.logger.info("Processing file %s", file_path.name)
        frame = pd.read_csv(file_path)
        rows_read = len(frame)

        try:
            accepted, rejected = transform_orders(frame, file_path.name)
        except ValueError as exc:
            self.logger.error("Validation failed for %s: %s", file_path.name, exc)
            now = datetime.now(timezone.utc).isoformat()
            write_audit_row(
                connection,
                self.config.batch_name,
                file_path.name,
                rows_read,
                0,
                rows_read,
                "failed",
                now,
            )
            return FileResult(file_path.name, rows_read, 0, rows_read, "failed")

        rows_loaded = load_dataframe(connection, accepted)
        rows_rejected = len(rejected)
        now = datetime.now(timezone.utc).isoformat()
        write_audit_row(
            connection,
            self.config.batch_name,
            file_path.name,
            rows_read,
            rows_loaded,
            rows_rejected,
            "loaded",
            now,
        )
        self.logger.info(
            "Loaded %s with %s accepted rows and %s rejected rows",
            file_path.name,
            rows_loaded,
            rows_rejected,
        )
        return FileResult(file_path.name, rows_read, rows_loaded, rows_rejected, "loaded")
