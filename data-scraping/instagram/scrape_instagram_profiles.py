import os
import re
import json
import pandas as pd
from apify_client import ApifyClient

# =========================
# CONFIG
# =========================
APIFY_TOKEN = ""
INPUT_CSV = "instagram_creators.csv.csv"   

ACTOR_ID = ""   # Instagram profile API actor

os.makedirs(OUTPUT_DIR, exist_ok=True)

# Initialize Apify client
client = ApifyClient(APIFY_TOKEN)


# =========================
# HELPERS
# =========================
def get_first(obj, keys, default=None):
    """
    Return the first existing non-None value for any key in keys.
    Works with dict-like objects.
    """
    if not isinstance(obj, dict):
        return default
    for k in keys:
        if k in obj and obj[k] not in [None, ""]:
            return obj[k]
    return default


def safe_str(x, default=""):
    if x is None:
        return default
    return str(x)


def parse_hashtags(text_or_list):
    """
    Extract hashtags from either caption text or a list field.
    Returns a comma-separated string.
    """
    if text_or_list is None:
        return ""

    if isinstance(text_or_list, list):
        tags = []
        for item in text_or_list:
            if isinstance(item, str):
                tags.append(item.lstrip("#"))
        return ",".join(sorted(set(tags)))

    if isinstance(text_or_list, str):
        tags = re.findall(r"#(\w+)", text_or_list)
        return ",".join(sorted(set(tags)))

    return ""


def normalize_username(u):
    if u is None:
        return None
    u = str(u).strip()
    if not u:
        return None
    # Remove @ if present
    if u.startswith("@"):
        u = u[1:]
    return u


def load_usernames_from_csv(path):
    """
    Handles both:
    1) a CSV with a header called username
    2) a single-column CSV without a proper header
    """
    df = pd.read_csv(path)

    if "username" in df.columns:
        usernames = df["username"].tolist()
    else:
        # Use first column
        usernames = df.iloc[:, 0].tolist()

    usernames = [normalize_username(u) for u in usernames]
    usernames = [u for u in usernames if u]
    # deduplicate while preserving order
    seen = set()
    unique_usernames = []
    for u in usernames:
        if u not in seen:
            unique_usernames.append(u)
            seen.add(u)
    return unique_usernames


def extract_profile_fields(item, fallback_username=None):
    """
    Extract creator-level fields from Apify output.
    Field names may vary a little, so we use fallbacks.
    """
    account_id = get_first(item, ["account_id", "accountId", "id", "profile_id", "userId", "pk"])
    username = get_first(item, ["username", "userName", "handle"], fallback_username)
    bio = get_first(item, ["bio", "biography", "profile_bio"], "")
    followers = get_first(item, ["followers", "followersCount", "edge_followed_by", "followerCount"], 0)
    posts = get_first(item, ["posts", "postsCount", "mediaCount", "postCount"], 0)
    verified = get_first(item, ["verified", "isVerified"], False)

    return {
        "account_id": account_id,
        "username": username,
        "bio": bio,
        "followers": followers,
        "posts": posts,
        "verified": verified,
    }


def find_post_lists(item):
    """
    Try to collect post-like lists from common keys returned by Instagram profile scrapers.
    """
    possible_keys = [
        "latestPosts",
        "latestImagePosts",
        "latestVideoPosts",
        "latestCarouselPosts",
        "latestReels",
        "latestIGTVVideos",
        "posts",
    ]

    all_posts = []
    for key in possible_keys:
        val = item.get(key)
        if isinstance(val, list):
            all_posts.extend(val)

    # Also support nested sections sometimes used by scrapers
    sections = item.get("latestPostsSections") or item.get("postSections") or []
    if isinstance(sections, list):
        for sec in sections:
            if isinstance(sec, dict):
                for k in ["posts", "items", "latestPosts"]:
                    val = sec.get(k)
                    if isinstance(val, list):
                        all_posts.extend(val)

    # Deduplicate by post id if possible
    seen = set()
    unique_posts = []
    for p in all_posts:
        if not isinstance(p, dict):
            continue
        pid = get_first(p, ["post_id", "postId", "id", "pk", "shortCode", "shortcode"])
        key = safe_str(pid)
        if key and key not in seen:
            seen.add(key)
            unique_posts.append(p)

    return unique_posts


def extract_post_id(post):
    return get_first(post, ["post_id", "postId", "id", "pk", "shortCode", "shortcode"])


def extract_post_type(post):
    """
    Map scraper-specific types into image / video / carousel / reel / igtv / unknown
    """
    t = get_first(post, ["type", "mediaType", "productType"], "")
    t = safe_str(t).lower()

    if "image" in t or "photo" in t:
        return "images"
    if "video" in t or "reel" in t or "igtv" in t:
        return "videos"
    if "carousel" in t:
        return "carousel"
    return t if t else "unknown"


def extract_metric_value(post, keys, default=0):
    val = get_first(post, keys, default)
    try:
        if val in [None, ""]:
            return default
        return int(float(val))
    except Exception:
        return default


def extract_post_timestamp(post):
    return get_first(post, ["timestamp", "takenAt", "takenAtTimestamp", "createdAt", "date"], "")


def extract_media_url(post):
    return get_first(post, ["mediaUrl", "mediaURL", "url", "displayUrl", "displayURL", "videoUrl", "imageUrl"], "")


def extract_thumbnail_url(post):
    return get_first(post, ["thumbnailUrl", "thumbnailURL", "thumbUrl", "previewUrl", "coverUrl"], "")


def extract_caption(post):
    caption = get_first(post, ["caption", "text", "description"], "")
    if isinstance(caption, dict):
        caption = caption.get("text", "")
    return safe_str(caption, "")


def extract_audio_path(post):
    # Apify may not provide this directly; leave empty unless present
    return get_first(post, ["audio_path", "audioPath", "audioUrl", "audioURL"], "")


def extract_shares(post):
    return extract_metric_value(post, ["shares", "shareCount", "reshareCount"], 0)


def extract_views(post):
    return extract_metric_value(post, ["views", "viewCount", "videoViewCount", "playCount"], 0)


def extract_likes(post):
    return extract_metric_value(post, ["likes", "likeCount", "edge_liked_by", "likesCount"], 0)


def extract_comments(post):
    return extract_metric_value(post, ["comments", "commentCount", "edge_media_to_comment", "commentsCount"], 0)


# =========================
# LOAD USERNAMES
# =========================
usernames = load_usernames_from_csv(INPUT_CSV)
print(f"Loaded {len(usernames)} usernames")


# =========================
# RUN APIFY AND BUILD TABLES
# =========================
creators_rows = []
posts_rows = []
metrics_rows = []
assets_rows = []

for username in usernames:
    print(f"Processing: {username}")

    run_input = {
        "usernames": [username],
        "includeAboutSection": True
    }

    try:
        run = client.actor(ACTOR_ID).call(run_input=run_input)

        for item in client.dataset(run["defaultDatasetId"]).iterate_items():
            # ---- CREATOR TABLE ----
            creator = extract_profile_fields(item, fallback_username=username)
            creators_rows.append(creator)

            account_id = creator["account_id"]
            post_items = find_post_lists(item)

            # ---- POSTS / METRICS / ASSETS ----
            for post in post_items:
                post_id = extract_post_id(post)
                if not post_id:
                    continue

                post_type = extract_post_type(post)
                caption = extract_caption(post)
                hashtags = parse_hashtags(caption)

                # Posts table
                posts_rows.append({
                    "post_id": post_id,
                    "account_id": account_id,
                    "type": post_type,
                    "hashtags": hashtags
                })

                # Metrics table
                metrics_rows.append({
                    "post_id": post_id,
                    "likes": extract_likes(post),
                    "views": extract_views(post),
                    "comments": extract_comments(post),
                    "timestamp": extract_post_timestamp(post),
                    "shares": extract_shares(post)
                })

                # Assets table
                assets_rows.append({
                    "post_id": post_id,
                    "type": post_type,
                    "media_URL": extract_media_url(post),
                    "thumbnail_URL": extract_thumbnail_url(post),
                    "caption": caption,
                    "audio_path": extract_audio_path(post)
                })

    except Exception as e:
        print(f"Failed for {username}: {e}")


# =========================
# CONVERT TO DATAFRAMES
# =========================
creators_df = pd.DataFrame(creators_rows).drop_duplicates(subset=["account_id", "username"], keep="first")
posts_df = pd.DataFrame(posts_rows).drop_duplicates(subset=["post_id"], keep="first")
metrics_df = pd.DataFrame(metrics_rows).drop_duplicates(subset=["post_id"], keep="first")
assets_df = pd.DataFrame(assets_rows).drop_duplicates(subset=["post_id"], keep="first")


# Ensure required columns exist even if empty
creators_cols = ["account_id", "username", "bio", "followers", "posts", "verified"]
posts_cols = ["post_id", "account_id", "type", "hashtags"]
metrics_cols = ["post_id", "likes", "views", "comments", "timestamp", "shares"]
assets_cols = ["post_id", "type", "media_URL", "thumbnail_URL", "caption", "audio_path"]

for col in creators_cols:
    if col not in creators_df.columns:
        creators_df[col] = ""
for col in posts_cols:
    if col not in posts_df.columns:
        posts_df[col] = ""
for col in metrics_cols:
    if col not in metrics_df.columns:
        metrics_df[col] = ""
for col in assets_cols:
    if col not in assets_df.columns:
        assets_df[col] = ""


creators_df = creators_df[creators_cols]
posts_df = posts_df[posts_cols]
metrics_df = metrics_df[metrics_cols]
assets_df = assets_df[assets_cols]


# =========================
# SAVE CSV FILES
# =========================
creators_path = os.path.join(OUTPUT_DIR, "creators.csv")
posts_path = os.path.join(OUTPUT_DIR, "posts.csv")
metrics_path = os.path.join(OUTPUT_DIR, "metrics.csv")
assets_path = os.path.join(OUTPUT_DIR, "assets.csv")

creators_df.to_csv(creators_path, index=False)
posts_df.to_csv(posts_path, index=False)
metrics_df.to_csv(metrics_path, index=False)
assets_df.to_csv(assets_path, index=False)

print("\nDone!")
print(f"Saved: {creators_path}")
print(f"Saved: {posts_path}")
print(f"Saved: {metrics_path}")
print(f"Saved: {assets_path}")
