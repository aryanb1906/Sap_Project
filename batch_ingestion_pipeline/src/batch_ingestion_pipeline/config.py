from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class PipelineConfig:
    input_dir: Path
    db_path: Path
    log_dir: Path
    batch_name: str = "default_batch"

    @property
    def log_file(self) -> Path:
        return self.log_dir / "pipeline.log"
