from airflow import DAG
from datetime import datetime
from airflow.providers.smtp.operators.smtp import EmailOperator

with DAG(
    dag_id="test_email",
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    send_email = EmailOperator(
        task_id="send_email",
        to="meissagningue7@gmail.com",
        subject="Airflow Test",
        html_content="<h3>Ceci est un test d'email Airflow ✅</h3>",
    )
    send_email
