from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import sys

sys.path.insert(0, '/opt/airflow/project')

default_args = {
    'owner': 'daud',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def run_extract(**context):
    from extract import extract
    raw_records = extract()
    context['ti'].xcom_push(key='raw_records', value=raw_records)

def run_transform(**context):
    from transform import transform
    raw_records = context['ti'].xcom_pull(key='raw_records', task_ids='extract')
    df = transform(raw_records)
    context['ti'].xcom_push(key='transformed_data', value=df.to_json())

def run_load(**context):
    import pandas as pd
    from load import load
    json_data = context['ti'].xcom_pull(key='transformed_data', task_ids='transform')
    df = pd.read_json(json_data)
    load(df)

with DAG(
    dag_id='uk_crime_pipeline',
    default_args=default_args,
    description='Daily UK crime data pipeline',
    schedule_interval='@daily',
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['uk-crime', 'etl', 'dbt'],
) as dag:

    extract_task = PythonOperator(
        task_id='extract',
        python_callable=run_extract,
    )

    transform_task = PythonOperator(
        task_id='transform',
        python_callable=run_transform,
    )

    load_task = PythonOperator(
        task_id='load',
        python_callable=run_load,
    )

    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command='cd /opt/airflow/project/uk_crime_dbt && dbt run',
    )

    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command='cd /opt/airflow/project/uk_crime_dbt && dbt test',
    )

    extract_task >> transform_task >> load_task >> dbt_run >> dbt_test 