from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'daud',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

PROJECT_DIR = '/opt/airflow/project'
DBT_DIR = f'{PROJECT_DIR}/nhs_waiting_dbt'

with DAG(
    dag_id='nhs_waitinglist_pipeline',
    default_args=default_args,
    description='NHS A&E waiting list pipeline - extract, transform, load, dbt',
    schedule_interval='0 6 1 * *',  # 6am on the 1st of every month
    start_date=datetime(2025, 4, 1),
    catchup=False,
    tags=['nhs', 'health', 'pipeline'],
) as dag:

    extract = BashOperator(
        task_id='extract',
        bash_command=f'cd {PROJECT_DIR} && python3 etl/extract/extract.py',
    )

    transform = BashOperator(
        task_id='transform',
        bash_command=f'cd {PROJECT_DIR} && python3 etl/transform/transform.py',
    )

    load = BashOperator(
        task_id='load_to_snowflake',
        bash_command=f'cd {PROJECT_DIR} && python3 etl/load/load_to_snowflake.py',
    )

    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command=f'cd {DBT_DIR} && dbt run',
    )

    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command=f'cd {DBT_DIR} && dbt test',
    )

    extract >> transform >> load >> dbt_run >> dbt_test
