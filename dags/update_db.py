import json
import os, sys
from datetime import datetime, timedelta

import isodate
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from sqlalchemy import MetaData, Table, Column, String, Integer, DateTime, JSON, create_engine

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from app.database import SessionLocal, DATABASE_URL
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


def parse_duration(value):
    """Parse ISO8601 ou hh:mm:ss string"""
    try:
        # Cas 1: ISO8601 (PTxxHxxMxxS)
        return isodate.parse_duration(value)
    except Exception:
        # Cas 2: format hh:mm:ss ou mm:ss
        parts = value.split(":")
        if len(parts) == 2:  # mm:ss
            minutes, seconds = map(int, parts)
            return timedelta(minutes=minutes, seconds=seconds)
        elif len(parts) == 3:  # hh:mm:ss
            hours, minutes, seconds = map(int, parts)
            return timedelta(hours=hours, minutes=minutes, seconds=seconds)
        else:
            raise ValueError(f"Durée invalide: {value}")

def transformation_and_clean():
    db=SessionLocal()
    datas = db.query(StagingYoutubeData).all()
    channel_handle=datas[0].channel_handle
    extraction_date=datetime.fromisoformat(datas[0].extraction_date).date()
    seen_ids = set()
    clean_videos = []

    for video in datas[0].videos:  # vidéos déjà list/dict
        video_id = video["video_id"]

        if video_id in seen_ids:
            print(f"Vidéo {video_id} déjà présente, skipping...")
            continue

        seen_ids.add(video_id)

        clean_videos.append({
            "title": video["title"],
            "duration": parse_duration(video["duration"]),
            "video_id": video_id,
            "like_count": int(video["like_count"]),
            "view_count": int(video["view_count"]),
            "published_at":datetime.fromisoformat(video["published_at"]).date(),
            "comment_count":int(video["comment_count"]),
            "duration_readable":parse_duration(video["duration_readable"])
        })
    return {
        "channel_handle": channel_handle,
        "extraction_date": extraction_date,
        "total_videos":datas[0].total_videos,
        "videos": clean_videos
    }

def load_core():
    engine = create_engine(DATABASE_URL)
    metadata = MetaData()
    transform_datas = transformation_and_clean()
    youtube_videos = Table(
        "youtube_datas",
        metadata,
        Column("id", Integer, primary_key=True),
        Column("channel_handle", String),
        Column("extraction_date", DateTime),
        Column("total_videos", Integer),
        Column("videos", JSON)
    )
    metadata.create_all(engine)

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