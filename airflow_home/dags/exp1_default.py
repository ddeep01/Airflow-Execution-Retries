from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

def fail_task():
    print("Failing...")
    raise Exception("Failure")

default_args = {
    "owner": "deep",
    "retries": 3,
    "retry_delay": timedelta(seconds=10),
}

with DAG(
    dag_id="exp1_default_retry",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:

    task = PythonOperator(
        task_id="fail_task",
        python_callable=fail_task
    )
