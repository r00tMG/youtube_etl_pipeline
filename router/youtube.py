from _datetime import datetime
import os
import json
from fastapi import APIRouter
from dotenv import load_dotenv
from googleapiclient.discovery import build

router = APIRouter()
load_dotenv()

API_KEY = os.getenv("API_KEY_YOUTUBE")
youtube = build("youtube", "v3", developerKey=API_KEY)
@router.get("/youtube")
async def get(channel_handle:str="MrBeast"):
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
        type="video",  # <--- ici on force uniquement les vidéos
        maxResults=5
    ).execute()

    videos = []
    for item in search_videos.get("items", []):
        id_info = item.get("id", {})
        snippet = item.get("snippet", {})

        video_id = id_info.get("videoId")
        if not video_id:
            continue  # skip si ce n'est pas une vidéo

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
    DATA_DIR = "./files"  
    os.makedirs(DATA_DIR, exist_ok=True) 
    print("file created")
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    filename = f"{DATA_DIR}/data_{timestamp}.json"
    datas={
        "channel_handle": channel_handle,
        "extraction_date": datetime.utcnow().isoformat(),
        "total_videos": len(videos),
        "videos": videos
    }
    with open(filename, "w", encoding="utf-8") as f:
        print("le fichier est en écriture")
        json.dump(datas, f, ensure_ascii=False, indent=4)
        print("le fichier est sauvegarder")

    return {
        "channel_handle": channel_handle,
        "extraction_date": datetime.utcnow().isoformat(),
        "total_videos": len(videos),
        "videos": videos
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
