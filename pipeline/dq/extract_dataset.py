import pandas as pd
from dateutil.relativedelta import relativedelta

from pipeline.artifacts import run_directory, write_parquet
from pipeline.db import get_conn
from pipeline.settings import WINDOW_MONTHS


def extract_dataset(run_id: str) -> dict:
    """Читает витрину, пишет parquet для DQ; повтор использует тот же срез."""
    directory = run_directory("dq", run_id)
    path = directory / "dataset.parquet"
    info = {"run_id": run_id, "report_id": directory.name, "dataset_path": str(path)}
    if path.exists():
        return info
    conn = get_conn()
    try:
        conn.set_session(isolation_level="REPEATABLE READ", readonly=True)
        with conn.cursor() as cur:
            cur.execute("SELECT date_trunc('month', max(transaction_time))::date FROM labeled_transactions")
            latest = cur.fetchone()[0]
        if latest is None:
            frame = pd.read_sql_query("SELECT * FROM labeled_transactions LIMIT 0", conn)
        else:
            start = latest - relativedelta(months=WINDOW_MONTHS - 1)
            frame = pd.read_sql_query("SELECT * FROM labeled_transactions WHERE transaction_time >= %s ORDER BY transaction_time", conn, params=(start,))
    finally:
        conn.close()
    write_parquet(frame, path)
    return info
