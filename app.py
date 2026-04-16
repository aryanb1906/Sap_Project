from __future__ import annotations

from pathlib import Path
import runpy
import sys

ROOT = Path(__file__).resolve().parent
PROJECT_DIR = ROOT / "batch_ingestion_pipeline"
PROJECT_APP = PROJECT_DIR / "app.py"
PROJECT_SRC = PROJECT_DIR / "src"

if str(PROJECT_SRC) not in sys.path:
    sys.path.insert(0, str(PROJECT_SRC))

if not PROJECT_APP.exists():
    raise FileNotFoundError(f"Dashboard file not found: {PROJECT_APP}")

runpy.run_path(str(PROJECT_APP), run_name="__main__")
