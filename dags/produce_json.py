import os
import json
import isodate
from airflow import DAG
from fastapi import status
from datetime import datetime
from airflow.operators.python import PythonOperator
from googleapiclient.discovery import build
from dotenv import load_dotenv
from datetime import timedelta
from googleapiclient.errors import HttpError




load_dotenv()
API_KEY = "AIzaSyDix3bIAoLldA6cpjJYqGuErKny3PFQAn0"
youtube = build("youtube", "v3", developerKey=API_KEY)
channel_handle:str="MrBeast"
DATA_DIR = "./files"
MAX_QUOTA_PER_DAY = 10000
USED_QUOTA = 0
videos = []

def convert_duration(duration: str) -> str:

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

def extract_data_youtube_paginated():
    try:
        global USED_QUOTA
        search_channel = youtube.search().list(
            q=channel_handle,
            part="id",
            type="channel",
            maxResults=1
        ).execute()
        USED_QUOTA += 100  # approx pour search.list

        channel_id = search_channel["items"][0]["id"]["channelId"]
        all_videos = []
        next_page_token = None

        while True:
            if USED_QUOTA >= MAX_QUOTA_PER_DAY:
                print("Quota quotidien atteint, arrêt de l'extraction")
                break

            search_videos = youtube.search().list(
                channelId=channel_id,
                part="id,snippet",
                order="date",
                type="video",
                maxResults=50,
                pageToken=next_page_token
            ).execute()
            USED_QUOTA += 100  
            items = search_videos.get("items", [])
            video_items = [item for item in items if "videoId" in item.get("id", {})]
            all_videos.extend(video_items)
            print("all videos: ", len(all_videos))
            next_page_token = search_videos.get("nextPageToken")

            if not next_page_token:
                break

        return all_videos
    except HttpError as e:
        print("Error extract data youtube: ", e)

def data_extract():
    try:
        global videos, USED_QUOTA
        search_videos = extract_data_youtube_paginated()
        if search_videos:
            print("search videos: ",len(search_videos))
            video_ids = [item['id']['videoId'] for item in search_videos if 'videoId' in item['id']]

            for i in range(0, len(video_ids), 50):
                batch_ids = video_ids[i:i+50]

                if USED_QUOTA >= MAX_QUOTA_PER_DAY:
                    print("Quota quotidien atteint, arrêt de la récupération des détails vidéos")
                    break

                video_details = youtube.videos().list(
                    part="contentDetails,statistics,snippet",
                    id=",".join(batch_ids)
                ).execute()
                USED_QUOTA += len(batch_ids) * 2  # approx 2 unités par vidéo

                for details in video_details.get("items", []):
                    content = details["contentDetails"]
                    stats = details["statistics"]
                    snippet = details["snippet"]

                    duration_iso = content["duration"]
                    duration_readable = convert_duration(duration_iso)

                    videos.append({
                        "title": snippet.get("title"),
                        "duration": duration_iso,
                        "video_id": details.get("id"),
                        "like_count": stats.get("likeCount", "0"),
                        "view_count": stats.get("viewCount", "0"),
                        "published_at": snippet.get("publishedAt"),
                        "comment_count": stats.get("commentCount", "0"),
                        "duration_readable": duration_readable
                    })
            return videos
    except HttpError as e:
        print("Error data extract: ", e)


def format_out():
    try:
        return {
            "channel_handle": channel_handle,
            "extraction_date": datetime.utcnow().isoformat(),
            "total_videos": len(videos),
            "videos": data_extract()
        }
    except HttpError as e:  
        print(f"Error format out : {e}")  

def save_json():
    try:
        os.makedirs(DATA_DIR, exist_ok=True)
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        filename = f"{DATA_DIR}/data_{timestamp}.json"
        data = format_out()
        if data:
            with open(filename, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=4)
            print(f"Fichier sauvegardé: {filename}")
    except HttpError as e:
        print("Error save json: ",e)


with DAG(
    dag_id="produce_json",
    description="Sauvegarde des données de MrBeast dans un fichier json",
    start_date=datetime(2025, 9, 20),
    schedule="@daily",
    catchup=False
) as dag:
    
    extraction_data_youtube_task = PythonOperator(
        task_id="extraction_data_youtube",
        python_callable=extract_data_youtube_paginated
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