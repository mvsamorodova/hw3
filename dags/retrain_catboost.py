from datetime import datetime, timedelta

from airflow.decorators import dag, task

from pipeline.retrain.extract_month_batch import extract_month_batch
from pipeline.retrain.evaluate_models import evaluate_models
from pipeline.retrain.fit_catboost import fit_catboost
from pipeline.retrain.preprocess_dataset import preprocess_dataset
from pipeline.retrain.publish_model import publish_model
from pipeline.retrain.slide_training_window import slide_training_window
from pipeline.retrain.time_train_test_split import time_train_test_split


@dag(
    dag_id="retrain_catboost",
    description="Раз в 5 минут сдвигает 3-месячное окно и переобучает CatBoost",
    schedule="*/5 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=15),
    default_args={"retries": 1, "retry_delay": timedelta(seconds=30)},
    tags=["mlops", "catboost"],
)
def retrain_catboost():
    extracted = task(extract_month_batch, task_id="extract_month_batch")(run_id="{{ run_id }}")
    slid = task(slide_training_window, task_id="slide_training_window")(extracted)
    split = task(time_train_test_split, task_id="time_train_test_split")(slid)
    processed = task(preprocess_dataset, task_id="preprocess_dataset")(split)
    trained = task(fit_catboost, task_id="fit_catboost")(processed)
    evaluated = task(evaluate_models, task_id="evaluate_models")(trained)
    task(publish_model, task_id="publish_model")(evaluated)


retrain_catboost()
