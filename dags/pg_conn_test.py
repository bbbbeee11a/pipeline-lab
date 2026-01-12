from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime

def test_pg():
    hook = PostgresHook(postgres_conn_id="PG_AIRFLOWDB")
    with hook.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT 1;")
            print("SELECT 1 result:", cur.fetchone())

with DAG(
    dag_id="pg_conn_test",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    t1 = PythonOperator(
        task_id="select_1",
        python_callable=test_pg,
    )
