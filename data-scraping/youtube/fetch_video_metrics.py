
import csv
import datetime
from googleapiclient.discovery import build

API_KEY = ""

INPUT_FILE = "/content/youtube_videos-2.csv"
OUTPUT_FILE = "youtube_video_metrics.csv"

# Build YouTube API client
youtube = build("youtube", "v3", developerKey=API_KEY)

def read_video_ids(file_path):
    video_ids = []
    with open(file_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            video_ids.append(row["video_id"])
    return video_ids


def chunks(lst, n):
    """Yield successive n-sized chunks"""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


def fetch_metrics(video_ids):
    metrics = []
    collected_at = datetime.datetime.utcnow().isoformat()

    for batch in chunks(video_ids, 50):  # API allows 50 IDs per request
        request = youtube.videos().list(
            part="statistics",
            id=",".join(batch)
        )

        response = request.execute()

        for item in response["items"]:
            stats = item["statistics"]

            metrics.append({
                "video_id": item["id"],
                "collected_at": collected_at,
                "views": stats.get("viewCount", 0),
                "likes": stats.get("likeCount", 0),
                "comment_count": stats.get("commentCount", 0)
            })

    return metrics


def write_metrics(metrics, file_path):
    fieldnames = [
        "video_id",
        "collected_at",
        "views",
        "likes",
        "comment_count"
    ]

    with open(file_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(metrics)


def main():
    video_ids = read_video_ids(INPUT_FILE)
    metrics = fetch_metrics(video_ids)
    write_metrics(metrics, OUTPUT_FILE)
    print(f"Collected metrics for {len(metrics)} videos.")


if __name__ == "__main__":
    main()
