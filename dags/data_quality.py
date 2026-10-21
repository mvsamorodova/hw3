from datetime import datetime, timedelta

from airflow.decorators import dag, task

from pipeline.dq.check_dataset import check_dataset
from pipeline.dq.extract_dataset import extract_dataset
from pipeline.dq.write_report import assert_quality, write_report


@dag(
    dag_id="data_quality", schedule="*/5 * * * *", start_date=datetime(2026, 1, 1),
    catchup=False, max_active_runs=1, dagrun_timeout=timedelta(minutes=15),
    default_args={"retries": 1, "retry_delay": timedelta(seconds=30)}, tags=["mlops", "dq"],
)
def data_quality():
    snapshot = task(extract_dataset, task_id="extract_dataset")(run_id="{{ run_id }}")
    checked = task(check_dataset, task_id="check_dataset")(snapshot)
    report = task(write_report, task_id="write_report")(checked)
    task(assert_quality, task_id="assert_quality")(report)


data_quality()
