from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator

default_args = {
    "owner":"data_quality",
    "email":['meissagningue7@gmail.com'],
    "email_on_failure":True,
    "retries":0
}

with DAG(
    dag_id="data_quality",
    default_args=default_args,
    description="Validation automatiques avec Soda Core et test de qualité",
    start_date=datetime(2020, 10, 10),
    schedule="@daily",
    catchup=False
) as dags:

    validation_with_soda_code_task=BashOperator(
        task_id="validation_with_soda_code",
        bash_command="""
        soda scan -d postgres_db -c /Users/meissagningue/Projects/dev_ia/pipeline_etl_youtube/dags/soda/configuration.yml /Users/meissagningue/Projects/dev_ia/pipeline_etl_youtube/dags/soda/checks.yml
        """,
        retries=3,
        retry_delay=timedelta(minutes=5)
    )
    validation_with_soda_code_task
