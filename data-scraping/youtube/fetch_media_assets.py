import csv
import os
import subprocess
from googleapiclient.discovery import build

API_KEY = ""

INPUT_FILE = "/content/youtube_videos-2.csv"
OUTPUT_FILE = "video_assets.csv"
AUDIO_DIR = "audio"

os.makedirs(AUDIO_DIR, exist_ok=True)

youtube = build("youtube", "v3", developerKey=API_KEY)


def chunk_list(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i+n]


def read_video_ids():
    with open(INPUT_FILE, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        return [row["video_id"] for row in reader]


def fetch_thumbnails(video_ids):
    thumbnails = {}

    for batch in chunk_list(video_ids, 50):
        request = youtube.videos().list(
            part="snippet",
            id=",".join(batch)
        )

        response = request.execute()

        for item in response["items"]:
            vid = item["id"]

            thumbs = item["snippet"]["thumbnails"]

            if "maxres" in thumbs:
                url = thumbs["maxres"]["url"]
            elif "high" in thumbs:
                url = thumbs["high"]["url"]
            else:
                url = list(thumbs.values())[0]["url"]

            thumbnails[vid] = url

    return thumbnails


def extract_audio_and_caption(video_id):
    url = f"https://www.youtube.com/watch?v={video_id}"

    audio_path = os.path.join(AUDIO_DIR, f"{video_id}.mp3")
    caption_text = ""

    try:
        # download audio
        subprocess.run([
            "yt-dlp",
            "-x",
            "--audio-format", "mp3",
            "-o", f"{AUDIO_DIR}/{video_id}.%(ext)s",
            url
        ], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

        # download captions (auto if needed)
        subprocess.run([
            "yt-dlp",
            "--skip-download",
            "--write-auto-subs",
            "--sub-format", "vtt",
            "-o", f"{AUDIO_DIR}/{video_id}",
            url
        ], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

        # read caption file
        caption_file = f"{AUDIO_DIR}/{video_id}.en.vtt"

        if os.path.exists(caption_file):
            with open(caption_file, "r", encoding="utf-8") as f:
                caption_text = f.read().replace("\n", " ")

    except Exception:
        pass

    if not os.path.exists(audio_path):
        audio_path = ""

    return caption_text, audio_path


def main():

    video_ids = read_video_ids()

    thumbnails = fetch_thumbnails(video_ids)

    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["video_id", "thumbnail_url", "caption_text", "audio_path"])

        for vid in video_ids:

            thumb = thumbnails.get(vid, "")

            caption, audio = extract_audio_and_caption(vid)

            writer.writerow([vid, thumb, caption, audio])

            print("processed:", vid)


if __name__ == "__main__":
    main()
