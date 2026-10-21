from pathlib import Path

import pandas as pd

from pipeline.artifacts import write_parquet
from pipeline.dq.check_dataset import require_quality
from pipeline.settings import CURRENT_DATASET, WINDOW_MONTHS


def slide_training_window(batch_info: dict) -> dict:
    """Читает окно и батч, пишет pending; повтор использует готовый файл."""
    path = Path(batch_info["dataset_path"])
    if path.exists():
        return batch_info
    current = pd.read_parquet(CURRENT_DATASET)
    batch = pd.read_parquet(batch_info["batch_path"])
    current["transaction_time"] = pd.to_datetime(current["transaction_time"])
    batch["transaction_time"] = pd.to_datetime(batch["transaction_time"])
    month = pd.Period(batch_info["batch_month"], freq="M")
    periods = pd.to_datetime(current["transaction_time"]).dt.to_period("M")
    history = current.loc[(periods >= month - WINDOW_MONTHS + 1) & (periods < month)]
    updated = pd.concat([history, batch], ignore_index=True).sort_values("transaction_time")
    require_quality(updated)
    write_parquet(updated, path)
    return batch_info
