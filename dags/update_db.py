import json
import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

def read_json():
    try:
        DIR_FILE="./files"
        filename=os.path.join(DIR_FILE,"data_youtube_extract.json")
        if filename and os.path.exists(filename):
            with open(filename, "r", encoding="utf-8") as f:
                datas=json.load(f)
            return datas
        else:
            raise FileNotFoundError(f"Le fichier {filename} est introuvable")
    except Exception as e:
        raise Exception("Erreur lors de la lecture du fichier json dans staging area")

def staging_area():
    
    
        

with DAG(
    dag_id="update_db",
    description="chargement, transformation des données",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False
) as dag:
    read_json_task = PythonOperator(
        task_id="read_json",
        python_callable=read_json,
        retries=3,
        retry_delay=timedelta(minutes=5)
    )
    read_json_task