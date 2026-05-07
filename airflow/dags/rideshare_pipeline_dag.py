from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'ameen',
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id='rideshare_pipeline',
    default_args=default_args,
    description='NYC Rideshare Analytics Pipeline — ingest, load, transform, test',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['rideshare', 'snowflake', 'dbt']
) as dag:

    ingest = BashOperator(
        task_id='ingest',
        bash_command='cd /opt/airflow/project && python pipeline/ingest.py'
    )

    load_snowflake = BashOperator(
        task_id='load_snowflake',
        bash_command='cd /opt/airflow/project && python pipeline/snowflake_load.py'
    )

    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command='cd /opt/airflow/project/rideshare && dbt run'
    )

    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command='cd /opt/airflow/project/rideshare && dbt test'
    )

    ingest >> load_snowflake >> dbt_run >> dbt_test