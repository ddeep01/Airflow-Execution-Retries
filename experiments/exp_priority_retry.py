from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta


def fail_task(name, **context):
    ti = context["ti"]
    task = context["task"]

    priority = getattr(task, "priority_weight", 1)

    # Priority logic
    if priority >= 10:
        level = "HIGH"
        delay = 5
    elif priority >= 5:
        level = "MEDIUM"
        delay = 10
    else:
        level = "LOW"
        delay = 20

    # 🚀 TASK START
    ti.log.info("========== 🚀 TASK START ==========")
    ti.log.info(f"Task ID        : {ti.task_id}")
    ti.log.info(f"Priority       : {level}")
    ti.log.info(f"Execution Date : {context['ts']}")
    ti.log.info(f"Try Number     : {ti.try_number}")
    ti.log.info("==================================")

    # 🔁 RETRY INFO
    ti.log.info("---------- 🔁 RETRY INFO ----------")
    ti.log.info(f"Attempt        : {ti.try_number}")
    ti.log.info(f"Max Retries    : {task.retries + 1}")
    ti.log.info(f"Delay Applied  : {delay} seconds")
    ti.log.info("----------------------------------")

    # ⚙️ EXECUTION
    ti.log.info("---------- ⚙️ EXECUTION ----------")
    ti.log.info("Running business logic...")
    ti.log.info("----------------------------------")

    # ❌ ERROR
    ti.log.error("❌ ERROR OCCURRED")
    ti.log.error("Reason : Failure")

    # 🔄 RETRY DECISION
    ti.log.info("🔄 RETRY SCHEDULED")
    ti.log.info(f"Next Attempt In : {delay} seconds")
    ti.log.info(f"Priority Logic  : {level} priority → adjusted delay")

    # Force failure
    raise Exception("Failure")


default_args = {
    "owner": "deep",
    "retries": 3,
    "retry_delay": timedelta(seconds=10),
}

with DAG(
    dag_id="exp_priority_retry",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:

    high = PythonOperator(
        task_id="high_priority",
        python_callable=fail_task,
        op_kwargs={"name": "HIGH"},
        priority_weight=10,
    )

    medium = PythonOperator(
        task_id="medium_priority",
        python_callable=fail_task,
        op_kwargs={"name": "MEDIUM"},
        priority_weight=5,
    )

    low = PythonOperator(
        task_id="low_priority",
        python_callable=fail_task,
        op_kwargs={"name": "LOW"},
        priority_weight=1,
    )