import logging
import os
import shutil
from datetime import date
from pathlib import Path

from pipeline.db import advance_training_state, get_conn
from pipeline.retrain.evaluate_models import current_version
from pipeline.settings import CURRENT_DATASET, MODELS_DIR

logger = logging.getLogger(__name__)


def publish_model(train_info: dict) -> str:
    """По решению БД публикует модель и фиксирует окно; завершённый запуск не сдвигает месяц повторно."""
    conn = get_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("SELECT next_batch_month FROM training_state WHERE id = 1 FOR UPDATE")
                next_month = cur.fetchone()[0]
                cur.execute("SELECT status, decision FROM training_runs WHERE run_id = %s FOR UPDATE", (train_info["run_id"],))
                existing = cur.fetchone()
                if existing is None:
                    raise RuntimeError("Сначала выполните evaluate_models")
                if existing[0] in ("published", "rejected"):
                    return existing[0]
                info = existing[1]
                month = date.fromisoformat(info["batch_month"])
                if next_month != month:
                    from airflow.exceptions import AirflowSkipException
                    raise AirflowSkipException(f"Месяц {month} уже обработан или нарушен порядок")
                candidate = Path(info["candidate_model"])
                if not candidate.exists():
                    raise FileNotFoundError(candidate)
                if info["accepted"]:
                    if current_version() not in (info["incumbent_version"], info["version"]):
                        raise RuntimeError("Рабочая модель изменилась: повторите evaluate_models")
                    MODELS_DIR.mkdir(parents=True, exist_ok=True)
                    final = MODELS_DIR / f"my_catboost_{info['version']}.cbm"
                    temporary = final.with_suffix(".tmp.cbm")
                    shutil.copyfile(candidate, temporary)
                    os.replace(temporary, final)
                    latest = MODELS_DIR / f".latest.{info['version']}.txt"
                    latest.write_text(info["version"] + "\n", encoding="utf-8")
                    os.replace(latest, MODELS_DIR / "latest.txt")
                temporary = CURRENT_DATASET.with_suffix(".tmp.parquet")
                shutil.copyfile(info["dataset_path"], temporary)
                os.replace(temporary, CURRENT_DATASET)
                advance_training_state(conn, month)
                status = "published" if info["accepted"] else "rejected"
                cur.execute("UPDATE training_runs SET status = %s WHERE run_id = %s", (status, info["run_id"]))
        logger.info("Месяц %s обработан: %s", month, status)
        return status
    finally:
        conn.close()
