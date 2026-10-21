import calendar
import logging

import pandas as pd
from dateutil.relativedelta import relativedelta

from pipeline.artifacts import run_directory, write_parquet
from pipeline.db import LABELED_COLUMNS, get_conn, read_training_state
from pipeline.settings import HISTORY_MONTHS, SAMPLE_SIZE, WINDOW_MONTHS

logger = logging.getLogger(__name__)


def next_month_start(max_time):
    return (pd.Timestamp(max_time).to_period("M") + 1).to_timestamp()


def shift_into_month(frame, month_start):
    shifted = frame.copy()
    times = pd.to_datetime(shifted["transaction_time"])
    year, month = int(month_start.year), int(month_start.month)
    shifted["transaction_time"] = pd.to_datetime({
        "year": year, "month": month,
        "day": times.dt.day.clip(upper=calendar.monthrange(year, month)[1]),
        "hour": times.dt.hour, "minute": times.dt.minute,
    })
    return shifted


def sample_next_month(run_id: str) -> dict:
    """Читает витрину, пишет батч в parquet; повтор использует тот же файл и месяц."""
    directory = run_directory("supply", run_id)
    path = directory / "batch.parquet"
    if path.exists():
        batch = pd.read_parquet(path)
    else:
        conn = get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT max(transaction_time), count(*) FROM labeled_transactions")
                max_time, row_count = cur.fetchone()
            if not row_count:
                raise ValueError("Размеченная витрина пуста")
            month = next_month_start(max_time)
            needed_start = read_training_state(conn)["next_batch_month"] - relativedelta(months=WINDOW_MONTHS - 1)
            cutoff = month.date() - relativedelta(months=HISTORY_MONTHS - 1)
            if cutoff > needed_start:
                from airflow.exceptions import AirflowSkipException
                raise AirflowSkipException("Переобучение отстаёт")
            frame = pd.read_sql_query(
                f"SELECT {', '.join(LABELED_COLUMNS)} FROM labeled_transactions ORDER BY random() LIMIT %s",
                conn, params=(min(SAMPLE_SIZE, row_count),),
            )
        finally:
            conn.close()
        batch = shift_into_month(frame, month)
        write_parquet(batch, path)
    month = pd.Timestamp(batch["transaction_time"].iloc[0]).to_period("M").start_time.date()
    logger.info("Подготовлен месяц %s: %s строк", month, len(batch))
    return {"batch_path": str(path), "batch_month": month.isoformat(), "rows": len(batch), "run_id": run_id}
