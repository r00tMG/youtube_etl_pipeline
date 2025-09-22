from datetime import datetime
from airflow import DAG

with DAG(
    dag_id="update_db",
    description="chargement, transformation des données",
    start_date=datetime(2025, 1, 1),
    scheduler="@daily",
    catchup=False
) as dag:
    pass