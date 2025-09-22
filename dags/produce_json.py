import os
import json
from airflow import DAG
from fastapi import status
from datetime import datetime
from airflow.operators.python import PythonOperator
from googleapiclient.discovery import build
from dotenv import load_dotenv


load_dotenv()
API_KEY = "AIzaSyDix3bIAoLldA6cpjJYqGuErKny3PFQAn0"
youtube = build("youtube", "v3", developerKey=API_KEY)
channel_handle:str="MrBeast"
videos = []

def extract_data_youtube(channel_handle):
    # Récupérer l'ID de la chaîne depuis le handle
    search_channel = youtube.search().list(
        q=channel_handle,
        part="id,snippet",
        type="channel",
        maxResults=1
    ).execute()

    channel_id = search_channel["items"][0]["id"]["channelId"]

    # Récupérer les vidéos récentes de la chaîne
    search_videos = youtube.search().list(
        channelId=channel_id,
        part="id,snippet",
        order="date",
        type="video",  
        maxResults=5
    ).execute()
    return search_videos

def data_extract():
    search_videos=extract_data_youtube(channel_handle)
    for item in search_videos.get("items", []):
        id_info = item.get("id", {})
        snippet = item.get("snippet", {})

        video_id = id_info.get("videoId")
        if not video_id:
            continue  

        # Récupérer les stats et la durée
        video_details = youtube.videos().list(
            part="contentDetails,statistics",
            id=video_id
        ).execute()

        if not video_details["items"]:
            continue

        details = video_details["items"][0]
        content = details["contentDetails"]
        stats = details["statistics"]

        # Construire un format lisible
        duration_iso = content["duration"]
        duration_readable = convert_duration(duration_iso)
        print("snippet: ",snippet.get("comment"))
        videos.append({
            "title": snippet.get("title"),
            "duration": duration_iso,
            "video_id": video_id,
            "like_count": stats.get("likeCount", "0"),
            "view_count": stats.get("viewCount", "0"),
            "published_at": snippet.get("publishedAt"),
            "comment_count": stats.get("commentCount", "0"),
            "duration_readable": duration_readable
        })
    return videos
def format_out():
    data = {
        "channel_handle": channel_handle,
        "extraction_date": datetime.utcnow().isoformat(),
        "total_videos": len(videos),
        "videos": videos
    }
    return data

def save_json():
    DATA_DIR = "./files"  
    os.makedirs(DATA_DIR, exist_ok=True) 
    print("file created")
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    filename = f"{DATA_DIR}/data_{timestamp}.json"
    datas=format_out() 
    with open(filename, "w", encoding="utf-8") as f:
        print("le fichier est en écriture")
        json.dump(datas, f, ensure_ascii=False, indent=4)
        print("le fichier est sauvegarder")
        
    return {
        "status":status.HTTP_200_OK,
        "message":"L'extraction a réussie avec succes",
    }


def convert_duration(duration: str) -> str:
    """Convertit une durée ISO8601 (PT#M#S) en format mm:ss ou hh:mm:ss"""
    import isodate
    from datetime import timedelta

    try:
        td: timedelta = isodate.parse_duration(duration)
        total_seconds = int(td.total_seconds())
        minutes, seconds = divmod(total_seconds, 60)
        hours, minutes = divmod(minutes, 60)
        if hours > 0:
            return f"{hours}:{minutes:02d}:{seconds:02d}"
        else:
            return f"{minutes}:{seconds:02d}"
    except Exception:
        return duration

with DAG(
    dag_id="produce_json",
    description="Sauvegarde des données de MrBeast dans un fichier json",
    start_date=datetime(2025, 9, 20),
    schedule="@daily",
    catchup=False
) as dag:
    
    extraction_data_youtube_task = PythonOperator(
        task_id="extraction_data_youtube",
        python_callable=extract_data_youtube
    )
    data_extract_task = PythonOperator(
        task_id="data_extract",
        python_callable=data_extract
    )
    format_out_task=PythonOperator(
        task_id="format_out",
        python_callable=format_out
    )
    save_json_task=PythonOperator(
        task_id="save_task",
        python_callable=save_json
    )
    
    
    extraction_data_youtube_task >> data_extract_task >> format_out_task >> save_json_task