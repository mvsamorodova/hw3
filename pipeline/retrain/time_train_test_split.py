from pathlib import Path

import pandas as pd

from pipeline.artifacts import write_parquet
from pipeline.dq.check_dataset import require_quality


def time_train_test_split(window_info: dict) -> dict:
    """Читает pending, пишет train/test по времени; повтор заменяет те же файлы."""
    frame = pd.read_parquet(window_info["dataset_path"]).sort_values("transaction_time")
    require_quality(frame)
    periods = pd.to_datetime(frame["transaction_time"]).dt.to_period("M")
    month = pd.Period(window_info["batch_month"], freq="M")
    if periods.max() != month:
        raise ValueError("Последний месяц окна не совпадает с месяцем запуска")
    train, test = frame.loc[periods < month], frame.loc[periods == month]
    directory = Path(window_info["dataset_path"]).parent
    train_path, test_path = directory / "train_raw.parquet", directory / "test_raw.parquet"
    write_parquet(train, train_path)
    write_parquet(test, test_path)
    return {
        **window_info, "train_raw_path": str(train_path), "test_raw_path": str(test_path),
        "train_months": sorted(periods.loc[periods < month].astype(str).unique().tolist()),
        "test_month": str(month),
    }
