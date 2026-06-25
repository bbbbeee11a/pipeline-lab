from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="hello_celery",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    hello = BashOperator(
        task_id="say_hello",
        bash_command="echo hello from git-sync v2",
    )
