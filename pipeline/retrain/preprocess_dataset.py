import sys
from pathlib import Path

import pandas as pd

from pipeline.artifacts import write_parquet
from pipeline.settings import FRAUD_SRC


def preprocess_dataset(split_info: dict) -> dict:
    """Читает train/test, пишет признаки; повтор заменяет файлы своего запуска."""
    if FRAUD_SRC not in sys.path:
        sys.path.insert(0, FRAUD_SRC)
    from preprocessing import run_preproc

    directory = Path(split_info["dataset_path"]).parent
    train_path, test_path = directory / "train_proc.parquet", directory / "test_proc.parquet"
    write_parquet(run_preproc(pd.read_parquet(split_info["train_raw_path"])), train_path)
    write_parquet(run_preproc(pd.read_parquet(split_info["test_raw_path"])), test_path)
    return {**split_info, "train_proc_path": str(train_path), "test_proc_path": str(test_path)}
