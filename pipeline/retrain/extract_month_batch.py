import pandas as pd

from pipeline.artifacts import run_directory, write_parquet
from pipeline.db import LABELED_COLUMNS, get_conn, month_end_exclusive, read_training_state


def extract_month_batch(run_id: str) -> dict:
    """Читает следующий месяц из БД, пишет parquet; повтор использует сохранённый батч."""
    directory = run_directory("retrain", run_id)
    path = directory / "batch.parquet"
    if path.exists():
        frame = pd.read_parquet(path)
    else:
        conn = get_conn()
        try:
            month = read_training_state(conn)["next_batch_month"]
            frame = pd.read_sql_query(
                f"SELECT {', '.join(LABELED_COLUMNS)} FROM labeled_transactions WHERE transaction_time >= %s AND transaction_time < %s ORDER BY transaction_time",
                conn, params=(month, month_end_exclusive(month)),
            )
        finally:
            conn.close()
        if frame.empty:
            from airflow.exceptions import AirflowSkipException
            raise AirflowSkipException(f"Месяц {month} ещё не загружен")
        write_parquet(frame, path)
    month = pd.Timestamp(frame["transaction_time"].iloc[0]).to_period("M").start_time.date()
    return {
        "run_id": run_id, "batch_month": month.isoformat(), "batch_path": str(path),
        "dataset_path": str(directory / "pending_window.parquet"),
        "version": f"{month:%Y%m}_{directory.name[:12]}",
    }
