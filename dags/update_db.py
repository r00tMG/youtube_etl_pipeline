import json
import os, sys
from datetime import datetime, timedelta

from fastapi import Depends
from sqlalchemy.orm import Session

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from app.database import SessionLocal
from app.models import StagingYoutubeData

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
    datas=read_json()
    if datas:
        db = SessionLocal()
        try:
            record = StagingYoutubeData(
                channel_handle=datas["channel_handle"],
                extraction_date=datas["extraction_date"],
                total_videos=datas["total_videos"],
                videos=datas["videos"]
            )
            db.add(record)
            db.commit()
            print("Chargement staging réussi")
        except Exception as e:
            db.rollback()
            raise Exception(f"Erreur lors du chargement staging: {e}")
        finally:
            db.close()
    else:
        raise Exception("La lecture du fichier json a soit échoué ou retourne un objet vide dans le staging area: ",datas)

def transformation_and_clean():
    db=SessionLocal()
    datas = db.query(StagingYoutubeData).all()
    channel_handle=datas[0].channel_handle
    extraction_date=datetime.fromisoformat(datas[0].extraction_date).date()
    for video in datas[0].videos:
        pass

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
    staging_area_task=PythonOperator(
        task_id="staging_area",
        python_callable=staging_area,
        retries=3,
        retry_delay=timedelta(minutes=5)
    )

    transformation_and_clean_task=PythonOperator(
        task_id="transformation_and_clean",
        python_callable=transformation_and_clean,
        retries=3,
        retry_delay=timedelta(minutes=5)
    )
    read_json_task >> staging_area_task >> transformation_and_clean_task