import hashlib
import os
import uuid
from pathlib import Path

from pipeline.settings import RUNS_DIR


def run_directory(kind: str, run_id: str) -> Path:
    key = hashlib.sha256(run_id.encode()).hexdigest()[:20]
    path = RUNS_DIR / kind / key
    path.mkdir(parents=True, exist_ok=True)
    return path


def write_parquet(frame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    frame.to_parquet(temporary, index=False)
    os.replace(temporary, path)
