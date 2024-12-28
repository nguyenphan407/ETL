from datetime import datetime
from airflow import DAG
from docker.types import Mount

from airflow.providers.docker.operators.docker import DockerOperator
from airflow.utils.dates import days_ago
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator


CONN_ID = '8a56f091-7a5b-4776-8064-e2a7fe25cde3'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
}

dag = DAG(
    'elt_and_dbt',
    default_args=default_args,
    description='An ELT workflow with dbt',
    start_date=datetime(2024, 12, 28),
    catchup=False,
)


# Need to change this to be an Airbyte DAG instead
t1 = AirbyteTriggerSyncOperator(
    task_id='airbyte_money_json_example',
    airbyte_conn_id='airbyte',  # Airbyte Connection ID trong Airflow
    connection_id='8a56f091-7a5b-4776-8064-e2a7fe25cde3',  # Connection ID từ Airbyte
    asynchronous=False,
    timeout=3600,
    wait_seconds=3
)

t2 = DockerOperator(
    task_id='dbt_run',
    image='ghcr.io/dbt-labs/dbt-postgres:1.4.7',
    command=[
        "run",
        "--profiles-dir",
        "/root",
        "--project-dir",
        "/opt/dbt",
        "--full-refresh"
    ],
    auto_remove=True,
    docker_url="unix://var/run/docker.sock",
    network_mode="bridge",
    mounts=[
        Mount(source='/Users/phanhoangnguyen/LearnNew/ETL/custom_postgres',
              target='/opt/dbt', type='bind'),
        Mount(source='/Users/phanhoangnguyen/.dbt', target='/root', type='bind'),
    ],
    dag=dag
)

t1 >> t2