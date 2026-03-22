# Influencer-Recommendation-System

📂 Repository Structure
--data/
  -youtube/          # Raw YouTube data
  -instagram/        # Raw Instagram data
--data-scraping/
  -youtube/          # Scripts for YouTube Data API extraction
  -instagram/        # Script used to extract Instagram using Apify Client
--README.md

⚙️ Data Description

YouTube Data
Includes:
Creators (channel_id,metadata such as subscribers,language country, etc.)
Videos (title, description, tags, language)
Video metrics (views, likes, comments)
Video Assets (media URLS)

Instagram Data
Includes:
Profile information (bio, followers, verification status)
Posts (post_id, type, meta-data)
Metrics (likes,comments,engagement)
Media Assets (media URLs, thumbnails)

🛠️ Data Collection
YouTube
Data is collected using the YouTube Data API, including:
Channel details
Video metadata
Engagement metrics
Instagram
Data is collected using scraping tools (Apify), including:
Profile information
Engagement data
Post content
