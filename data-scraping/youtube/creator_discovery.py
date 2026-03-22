import csv
from googleapiclient.discovery import build

# Setup API
api_key = ""
youtube = build("youtube", "v3", developerKey=api_key)

# Load keywords
def load_keywords(file_path):
    with open(file_path, "r", encoding="utf-8") as f:
        return [line.strip() for line in f if line.strip()]

keywords = load_keywords("/content/fitness-keywords.txt")

output_file = "youtube_creators.csv"

headers = [
    "Channel ID",
    "Channel Title",
    "Published At",
    "Thumbnail URL",
    "Language",
    "Country",
    "Subscriber Count",
    "Video Count"
]

print(f"Loaded {len(keywords)} keywords")

# Step 1 — Collect channel IDs
channel_ids = set()

for keyword in keywords:
    print("Searching:", keyword)

    try:
        request = youtube.search().list(
            q=keyword,
            part="snippet",
            type="video",
            maxResults=50
        )

        response = request.execute()

        for item in response.get("items", []):
            channel_ids.add(item["snippet"]["channelId"])

    except Exception as e:
        print("Error:", e)

print(f"Collected {len(channel_ids)} unique channels")

# Step 2 — Fetch channel details in batches
def chunk_list(lst, size):
    for i in range(0, len(lst), size):
        yield lst[i:i + size]

channel_ids = list(channel_ids)

with open(output_file, "w", newline="", encoding="utf-8") as file:
    writer = csv.writer(file)
    writer.writerow(headers)

    for batch in chunk_list(channel_ids, 50):

        try:
            request = youtube.channels().list(
                part="snippet,statistics",
                id=",".join(batch)
            )

            response = request.execute()

            for item in response.get("items", []):

                snippet = item["snippet"]
                stats = item.get("statistics", {})

                writer.writerow([
                    item["id"],
                    snippet.get("title", ""),
                    snippet.get("publishedAt", ""),
                    snippet.get("thumbnails", {}).get("default", {}).get("url", ""),
                    snippet.get("defaultLanguage", ""),
                    snippet.get("country",""),
                    stats.get("subscriberCount", ""),
                    stats.get("videoCount", "")
                ])

        except Exception as e:
            print("Batch error:", e)

print("Extraction complete!")                maxResults=10
            )
            response = request.execute()

            for item in response.get('items', []):
                channel_id = item['snippet']['channelId']
                channel_title = item['snippet']['channelTitle']
                publishedAt=item['snippet']['publishedAt']
                # Clean description to prevent CSV formatting issues
                description = item['snippet']['description'].replace('\n', ' ').replace('\r', '')

                writer.writerow([keyword, channel_id, channel_title, description])

        except Exception as e:
            print(f"Error searching for '{keyword}': {e}")

print("Extraction complete!")
