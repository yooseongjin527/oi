"""
Sample DAG to verify Airflow setup.
Prints a hello message and the current time.
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


def say_hello(**context):
    print(f"Hello from Airflow! Run time: {context['ts']}")
    print(f"Logical date: {context['logical_date']}")
    return "OK"


default_args = {
    "owner": "jin",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="hello_world",
    description="Smoke test DAG for OI Airflow setup",
    default_args=default_args,
    start_date=datetime(2026, 4, 30),
    schedule=None,           # manual trigger only
    catchup=False,
    tags=["smoke-test"],
) as dag:

    bash_task = BashOperator(
        task_id="echo_hello",
        bash_command='echo "Hello from BashOperator at $(date)"',
    )

    python_task = PythonOperator(
        task_id="say_hello_python",
        python_callable=say_hello,
    )

    bash_task >> python_task
