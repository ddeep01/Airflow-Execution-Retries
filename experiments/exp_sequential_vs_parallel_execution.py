from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import time


def sample_task(task_name):

    start = time.time()

    print(f"STARTING: {task_name}")
    print(f"START TIME: {start}")

    time.sleep(5)

    end = time.time()

    print(f"FINISHED: {task_name}")
    print(f"END TIME: {end}")
    print(f"TOTAL TASK TIME: {end - start}")


with DAG(
    dag_id="exp_sequential_vs_parallel_execution",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:

    task1 = PythonOperator(
        task_id="task_1",
        python_callable=sample_task,
        op_args=["task_1"],
    )

    task2 = PythonOperator(
        task_id="task_2",
        python_callable=sample_task,
        op_args=["task_2"],
    )

    task3 = PythonOperator(
        task_id="task_3",
        python_callable=sample_task,
        op_args=["task_3"],
    )