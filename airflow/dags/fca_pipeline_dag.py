"""
FCA Register Pipeline DAG
Runs daily: extract → load → dbt run → dbt snapshot → dbt test
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "daud",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

DBT_DIR = "/opt/airflow/dbt/fca_dbt"

with DAG(
    dag_id="fca_register_pipeline",
    default_args=default_args,
    description="Daily FCA Register ingestion and dbt transformation",
    schedule_interval="0 6 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["fca", "data-engineering", "fintech"],
) as dag:

    def run_extract():
        import sys
        sys.path.insert(0, "/opt/airflow")
        from extract.extract_fca import extract_all_firms
        extract_all_firms()

    def run_load():
        import sys
        sys.path.insert(0, "/opt/airflow")
        from load.load_to_postgres import load_firms
        load_firms()

    t1_extract = PythonOperator(
        task_id="extract_fca_data",
        python_callable=run_extract,
    )

    t2_load = PythonOperator(
        task_id="load_to_postgres",
        python_callable=run_load,
    )

    t3_dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"cd {DBT_DIR} && dbt run --profiles-dir .",
    )

    t4_dbt_snapshot = BashOperator(
        task_id="dbt_snapshot",
        bash_command=f"cd {DBT_DIR} && dbt snapshot --profiles-dir .",
    )

    t5_dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"cd {DBT_DIR} && dbt test --profiles-dir .",
    )

    t1_extract >> t2_load >> t3_dbt_run >> t4_dbt_snapshot >> t5_dbt_test
