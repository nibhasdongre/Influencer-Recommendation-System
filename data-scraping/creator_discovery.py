import csv
from googleapiclient.discovery import build

# 1. Setup API
api_key = ""
youtube = build('youtube', 'v3', developerKey=api_key)

# 2. Read Keywords from the .txt file
def load_keywords(file_path):
    with open(file_path, 'r', encoding='utf-8') as f:
        return [line.strip() for line in f if line.strip()]

keywords = load_keywords("/fitness-keywords.txt")

# 3. Define the CSV output
output_file = "youtube_creators.csv"
headers = ["Keyword", "Channel ID", "Channel Title","PublishedAt","Thumbnail URL",
    "Language",
    "Subscriber Count",
    "Video Count"]

print(f"Reading {len(keywords)} keywords. Saving results to {output_file}...")

# 4. Open CSV and execute search
with open(output_file, mode='w', newline='', encoding='utf-8') as file:
    writer = csv.writer(file)
    writer.writerow(headers)

    for keyword in keywords:
        print(f"Searching: {keyword}")

        try:
            request = youtube.search().list(
                q=keyword,
                part="snippet",
                type="video",
                maxResults=10
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
