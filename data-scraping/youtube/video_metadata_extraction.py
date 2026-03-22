import csv
import time
from googleapiclient.discovery import build

API_KEY = ""

youtube = build("youtube", "v3", developerKey=API_KEY)

INPUT_FILE = "/content/youtube_creators.csv"
OUTPUT_FILE = "youtube_videos.csv"

MAX_VIDEOS=30
# --------------------------------
# Load channel IDs from CSV
# --------------------------------
def load_channel_ids(file_path):

    channel_ids = []

    with open(file_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)

        for row in reader:
            channel_ids.append(row["Channel ID"])

    return channel_ids


# --------------------------------
# Get uploads playlist for channel
# --------------------------------
def get_upload_playlist(channel_id):

    request = youtube.channels().list(
        part="contentDetails",
        id=channel_id
    )

    response = request.execute()

    items = response.get("items", [])

    if not items:
        return None

    return items[0]["contentDetails"]["relatedPlaylists"]["uploads"]


# --------------------------------
# Get video IDs from playlist
# --------------------------------
def get_video_ids(playlist_id):

    video_ids = []
    next_page = None

    while len(video_ids)<MAX_VIDEOS:

        request = youtube.playlistItems().list(
            part="contentDetails",
            playlistId=playlist_id,
            maxResults=50,
            pageToken=next_page
        )

        response = request.execute()

        for item in response.get("items", []):
            video_ids.append(item["contentDetails"]["videoId"])
            if len(video_ids)>=MAX_VIDEOS:
              break
        next_page = response.get("nextPageToken")

        if not next_page:
            break

    return video_ids[:MAX_VIDEOS]


# --------------------------------
# Fetch video metadata in batches
# --------------------------------

def get_video_metadata(video_ids):

    request = youtube.videos().list(
        part="snippet",
        id=",".join(video_ids)
    )

    response = request.execute()
    return response["items"]


with open(INPUT_FILE, "r", encoding="utf-8") as f:
    reader = csv.reader(f)
    next(reader)
    channels = [row[0] for row in reader]

print("Channels loaded:", len(channels))

with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:

    writer = csv.writer(f)

    writer.writerow([
        "video_id",
        "channel_id",
        "title",
        "description",
        "tags",
        "language"
    ])

    for channel_id in channels:

        try:
            playlist_id = get_upload_playlist(channel_id)

            if not playlist_id:
                continue

            video_ids = get_video_ids(playlist_id)

            videos = get_video_metadata(video_ids)

            for video in videos:

                snippet = video["snippet"]

                video_id = video["id"]
                title = snippet.get("title", "")
                description = snippet.get("description", "")
                tags = ",".join(snippet.get("tags", []))
                language = snippet.get("defaultLanguage", "")

                writer.writerow([
                    video_id,
                    channel_id,
                    title,
                    description,
                    tags,
                    language
                ])

        except Exception as e:
            print("Error for channel:", channel_id, e)



print("Video extraction complete.")
