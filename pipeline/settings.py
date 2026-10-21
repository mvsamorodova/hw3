import os
from pathlib import Path

TRAINING_DIR = Path(os.getenv("TRAINING_DATA_DIR", "/opt/airflow/training_data"))
MODELS_DIR = Path(os.getenv("MODELS_DIR", "/opt/airflow/models"))
FRAUD_SRC = os.getenv("FRAUD_SRC", "/opt/airflow/fraud_src")
CANDIDATES_DIR = Path(os.getenv("CANDIDATES_DIR", str(MODELS_DIR.parent / "candidates")))
DQ_REPORTS_DIR = Path(os.getenv("DQ_REPORTS_DIR", str(MODELS_DIR.parent / "dq_reports")))
RUNS_DIR = TRAINING_DIR / "runs"

CURRENT_DATASET = TRAINING_DIR / "current.parquet"

CAT_FEATURES = ["merch", "cat_id", "gender", "one_city", "us_state", "jobs"]
LABEL = "target"
SAMPLE_SIZE = int(os.getenv("SAMPLE_SIZE", "10000"))
HISTORY_MONTHS = int(os.getenv("HISTORY_MONTHS", "36"))
WINDOW_MONTHS = int(os.getenv("WINDOW_MONTHS", "3"))
if SAMPLE_SIZE < 1 or WINDOW_MONTHS < 2 or HISTORY_MONTHS < WINDOW_MONTHS:
    raise ValueError("Проверьте SAMPLE_SIZE, WINDOW_MONTHS и HISTORY_MONTHS")
