from airflow import DAG
from datetime import datetime

def

with DAG(
    dag_id="produce_json",
    description="Sauvegarde des données de MrBeast dans un fichier json",
    start_date=datetime(2025, 10, 20),
    schedule="@daily",
    catchup=False
) as dag:
    pass
    