
import pandas as pd

from pipeline.db import copy_labeled_frame, get_conn


def append_labeled_batch(batch_info: dict) -> dict:
    """Читает батч, дописывает витрину; отметка в БД исключает повторную вставку."""
    conn = get_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("SELECT pg_advisory_xact_lock(47110)")
                cur.execute("SELECT rows FROM supplied_batches WHERE batch_month = %s", (batch_info["batch_month"],))
                existing = cur.fetchone()
                if existing:
                    return {**batch_info, "rows": existing[0]}
                frame = pd.read_parquet(batch_info["batch_path"])
                copy_labeled_frame(conn, frame)
                cur.execute("INSERT INTO supplied_batches (batch_month, run_id, rows) VALUES (%s, %s, %s)",
                            (batch_info["batch_month"], batch_info["run_id"], len(frame)))
        return {**batch_info, "rows": len(frame)}
    finally:
        conn.close()
