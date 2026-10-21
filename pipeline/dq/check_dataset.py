import numpy as np
import pandas as pd

from pipeline.db import LABELED_COLUMNS
from pipeline.settings import CAT_FEATURES, WINDOW_MONTHS


def check_frame(frame: pd.DataFrame) -> list:
    checks = []

    def add(rule, violations):
        checks.append({"rule": rule, "passed": int(violations) == 0, "violations": int(violations)})

    missing = set(LABELED_COLUMNS) - set(frame.columns)
    add("Все обязательные столбцы присутствуют", len(missing))
    if missing:
        return checks
    add("Ключевые поля заполнены", frame[["transaction_time", "amount", "target", *CAT_FEATURES]].isna().any(axis=1).sum())
    times = pd.to_datetime(frame["transaction_time"], errors="coerce")
    months = times.dt.to_period("M")
    observed = sorted(months.dropna().unique())
    expected = list(pd.period_range(observed[0], periods=WINDOW_MONTHS, freq="M")) if observed else []
    add("Корректное время и полное окно последовательных месяцев", times.isna().sum() + int(not observed or observed != expected))
    amount = pd.to_numeric(frame["amount"], errors="coerce")
    add("Сумма конечна и неотрицательна", (~np.isfinite(amount) | (amount < 0)).sum())
    target = pd.to_numeric(frame["target"], errors="coerce")
    invalid = int(frame.empty) + int((~target.isin([0, 1])).sum())
    invalid += sum(set(target.loc[months == month].unique()) != {0, 1} for month in observed)
    add("Метки 0 и 1, оба класса в каждом месяце", invalid)
    return checks


def require_quality(frame: pd.DataFrame) -> None:
    failed = [item["rule"] for item in check_frame(frame) if not item["passed"]]
    if failed:
        raise ValueError("Data Quality: " + "; ".join(failed))


def check_dataset(dataset_info: dict) -> dict:
    """Читает parquet, возвращает результаты DQ; повтор проверяет тот же срез."""
    frame = pd.read_parquet(dataset_info["dataset_path"])
    checks = check_frame(frame)
    return {**dataset_info, "rows": len(frame), "checks": checks, "passed": all(item["passed"] for item in checks)}
