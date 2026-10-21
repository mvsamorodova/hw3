from datetime import datetime, timedelta

from airflow.decorators import dag, task

from pipeline.supply.append_labeled_batch import append_labeled_batch
from pipeline.supply.prune_old_months import prune_old_months
from pipeline.supply.sample_next_month import sample_next_month


@dag(
    dag_id="supply_labeled_batch",
    description="Дописывает следующий размеченный месяц и очищает историю",
    schedule="*/5 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=15),
    default_args={"retries": 1, "retry_delay": timedelta(seconds=30)},
    tags=["mlops", "etl"],
)
def supply_labeled_batch():
    sampled = task(sample_next_month, task_id="sample_next_month")(run_id="{{ run_id }}")
    appended = task(append_labeled_batch, task_id="append_labeled_batch")(sampled)
    task(prune_old_months, task_id="prune_old_months")(appended)


supply_labeled_batch()
