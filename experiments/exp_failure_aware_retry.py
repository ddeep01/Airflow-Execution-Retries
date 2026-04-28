from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowException, AirflowFailException
from datetime import datetime, timedelta


def network_error():
    print("\n========== 🚀 TASK START ==========")
    print("Simulating NETWORK error...")

    try:
        raise Exception("Connection timeout error")

    except Exception as e:
        print("❌ ERROR OCCURRED:", str(e))

        if "connection" in str(e).lower():
            print("🔄 RETRY DECISION: Network error → RETRY")
            raise AirflowException("Retrying due to network issue")

        else:
            raise AirflowFailException("Unexpected error")


def logic_error():
    print("\n========== 🚀 TASK START ==========")
    print("Simulating LOGIC error...")

    try:
        raise Exception("Invalid calculation logic")

    except Exception as e:
        print("❌ ERROR OCCURRED:", str(e))

        if "connection" in str(e).lower():
            raise AirflowException("Retry")

        else:
            print("🚫 RETRY DECISION: Logic error → NO RETRY")
            raise AirflowFailException("Do not retry")


default_args = {
    "owner": "deep",
    "retries": 2,  # IMPORTANT: still keep retries
    "retry_delay": timedelta(seconds=5),
}


with DAG(
    dag_id="exp_failure_smart_retry",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:

    t1 = PythonOperator(
        task_id="network_task",
        python_callable=network_error,
    )

    t2 = PythonOperator(
        task_id="logic_task",
        python_callable=logic_error,
    )