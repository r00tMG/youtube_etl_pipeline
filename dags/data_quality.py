from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator


with DAG(
    dag_id="data_quality",
    description="Validation automatiques avec Soda Core et test de qualité",
    start_date=datetime(2020, 10, 10),
    schedule="@daily",
    catchup=False
) as dags:

    validation_with_soda_code_task=BashOperator(
        task_id="validation_with_soda_code",
        bash_command="""
        soda scan -d postgres_db -c soda/configuration.yml soda/checks.yml
        """,
        retries=3,
        retry_delay=timedelta(minutes=5)
    )
    validation_with_soda_code_task
