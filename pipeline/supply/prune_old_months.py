from dateutil.relativedelta import relativedelta

from pipeline.db import get_conn
from pipeline.settings import HISTORY_MONTHS, WINDOW_MONTHS


def prune_old_months(batch_info: dict) -> dict:
    """Читает границы истории, удаляет старые строки; повторный DELETE безопасен."""
    conn = get_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("SELECT next_batch_month FROM training_state WHERE id = 1 FOR UPDATE")
                needed_start = cur.fetchone()[0] - relativedelta(months=WINDOW_MONTHS - 1)
                cur.execute("SELECT date_trunc('month', max(transaction_time))::date FROM labeled_transactions")
                newest = cur.fetchone()[0]
                cutoff = newest - relativedelta(months=HISTORY_MONTHS - 1)
                if cutoff > needed_start:
                    raise RuntimeError("Очистка затрагивает необработанное окно обучения")
                cur.execute("DELETE FROM labeled_transactions WHERE transaction_time < %s", (cutoff,))
                deleted = cur.rowcount
        return {**batch_info, "cutoff": cutoff.isoformat(), "deleted_rows": deleted}
    finally:
        conn.close()
