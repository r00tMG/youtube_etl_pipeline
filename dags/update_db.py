import json
from datetime import datetime
from airflow import DAG

def read_json():
    DIR_FILE="./files"
    filename="{DIR_FILE}/data_youtube_extract.json"
    with open(filename, "r", encoding="utf-8") as f:
        datas=json.load(f)
    return datas
        

with DAG(
    dag_id="update_db",
    description="chargement, transformation des données",
    start_date=datetime(2025, 1, 1),
    scheduler="@daily",
    catchup=False
) as dag:
    pass