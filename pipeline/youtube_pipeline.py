"""
youtube_pipeline.py
===================
Full preprocessing pipeline for YouTube (Steps 1–8).
Returns one DataFrame with one row per creator.
"""

import re
import numpy as np
import pandas as pd
from scipy import stats as scipy_stats

from data_loader import load_youtube
from dedup import dedup_creators, dedup_content
from utils import (
    log_step, norm_cols,
    clean_yt_title, clean_yt_description, parse_yt_tags,
    minmax_norm,
)

# ── engagement feature column names ────────────────────────────────────────
YT_ENG_COLS = [
    "mean_like_rate", "mean_comment_rate", "view_consistency",
    "upload_frequency", "like_view_trend", "video_count_ratio",
]


def run_youtube_pipeline() -> pd.DataFrame:
    print("\n" + "─"*50)
    print("  YOUTUBE PIPELINE")
    print("─"*50)

    # ── Load ────────────────────────────────────────────────────
    tables = load_youtube()
    creators = tables["creators"]
    videos   = tables["videos"]
    metrics  = tables["metrics"]
    assets   = tables["assets"]

    # ── Step 1: Deduplicate creators ───────────────────────────
    creators = dedup_creators(creators, "channel_id", "YouTube")

    # ── Step 2: Deduplicate content tables ─────────────────────
    content  = dedup_content(
        {"videos": videos, "metrics": metrics, "assets": assets},
        "video_id", "YouTube"
    )
    videos, metrics, assets = content["videos"], content["metrics"], content["assets"]

    # ── Step 3: JOIN ────────────────────────────────────────────
    # videos ⟕ metrics ⟕ assets on video_id, then ⟕ creators on channel_id
    df = (
        videos
        .merge(metrics, on="video_id", how="left")
        .merge(assets,  on="video_id", how="left")
        .merge(creators, on="channel_id", how="left")
    )
    log_step("STEP 3", f"YouTube joined table: {len(df):,} rows × {df.shape[1]} cols")

    # ── Step 4: Filter ──────────────────────────────────────────
    before = len(df)
    # Coerce to numeric
    df["video_count"]      = pd.to_numeric(df.get("video_count"),      errors="coerce")
    df["subscriber_count"] = pd.to_numeric(df.get("subscriber_count"), errors="coerce")

    df = df[
        (df["video_count"].fillna(0) >= 5) &
        (df["subscriber_count"].notna()) &
        (df["subscriber_count"] > 0)
    ].reset_index(drop=True)
    log_step("STEP 4", f"YouTube filter: {before:,} → {len(df):,} rows  ({before-len(df)} removed)")

    # ── Step 5: Text Preprocessing ─────────────────────────────
    df["views"] = pd.to_numeric(df.get("views"), errors="coerce").fillna(0)

    # Field-level cleaning
    df["_clean_title"]  = df["title"].apply(clean_yt_title)
    df["_clean_desc"]   = df["description"].apply(clean_yt_description)
    df["_clean_tags"]   = df["tags"].apply(parse_yt_tags)
    df["_per_video_text"] = (
        df["_clean_title"] + " " +
        df["_clean_desc"]  + " " +
        df["_clean_tags"]
    ).str.strip()

    # Creator-level aggregation: top-10 videos by views per channel
    def agg_text(group):
        top10 = group.nlargest(10, "views")
        channel_title = str(group["channel_title"].iloc[0]) if "channel_title" in group.columns else ""
        blob = channel_title + " " + " ".join(top10["_per_video_text"].tolist())
        return blob.strip()

    text_blobs = (
        df.groupby("channel_id", sort=False)
          .apply(agg_text)
          .rename("text_blob")
          .reset_index()
    )
    log_step("STEP 5", f"YouTube text blobs built for {len(text_blobs):,} creators")

    # ── Step 6: Engagement Feature Engineering ─────────────────
    df["likes"]         = pd.to_numeric(df.get("likes"),         errors="coerce")
    df["comment_count"] = pd.to_numeric(df.get("comment_count"), errors="coerce")

    # Parse collected_at / published_at
    for col in ("collected_at", "published_at"):
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce", utc=True)

    def eng_features(g):
        g = g.copy()
        g_views = g["views"].replace(0, np.nan)

        # Mean like rate
        like_rate = (g["likes"] / g_views).replace([np.inf, -np.inf], np.nan)
        mean_like_rate = like_rate.mean()

        # Mean comment rate
        comment_rate = (g["comment_count"] / g_views).replace([np.inf, -np.inf], np.nan)
        mean_comment_rate = comment_rate.mean()

        # View consistency (CoV)
        v = g["views"].dropna()
        view_consistency = (v.std() / v.mean()) if v.mean() > 0 else np.nan

        # Upload frequency (videos / months since channel published)
        upload_frequency = np.nan
        if "published_at" in g.columns:
            pub = g["published_at"].dropna()
            if len(pub):
                earliest = pub.min()
                months = (pd.Timestamp.now(tz="UTC") - earliest).days / 30.44
                if months > 0:
                    upload_frequency = len(g) / months

        # Like-to-view trend (slope of like_rate over collected_at)
        like_view_trend = np.nan
        if "collected_at" in g.columns:
            tmp = g[["collected_at", "likes", "views"]].dropna()
            tmp = tmp[tmp["views"] > 0].copy()
            tmp["lr"] = tmp["likes"] / tmp["views"]
            tmp = tmp.sort_values("collected_at")
            if len(tmp) >= 3:
                x = np.arange(len(tmp), dtype=float)
                slope, *_ = scipy_stats.linregress(x, tmp["lr"].values)
                like_view_trend = slope

        # Video count ratio
        video_count_ratio = np.nan
        reported = g["video_count"].iloc[0] if "video_count" in g.columns else np.nan
        if pd.notna(reported) and reported > 0:
            video_count_ratio = len(g) / reported

        return pd.Series({
            "mean_like_rate"     : mean_like_rate,
            "mean_comment_rate"  : mean_comment_rate,
            "view_consistency"   : view_consistency,
            "upload_frequency"   : upload_frequency,
            "like_view_trend"    : like_view_trend,
            "video_count_ratio"  : video_count_ratio,
        })

    eng = (
        df.groupby("channel_id", sort=False)
          .apply(eng_features)
          .reset_index()
    )
    eng = minmax_norm(eng, YT_ENG_COLS)
    log_step("STEP 6", f"YouTube engagement features computed + normalised for {len(eng):,} creators")

    # ── Step 7: Thumbnail URLs ──────────────────────────────────
    # Use thumbnail_url from assets; fall back to reconstructed CDN URL
    if "thumbnail_url" not in df.columns and "thumbnail_url_x" in df.columns:
        df["thumbnail_url"] = df["thumbnail_url_x"]

    def fallback_thumbnail(row):
        url = row.get("thumbnail_url")
        if isinstance(url, str) and url.strip():
            return url.strip()
        vid = row.get("video_id", "")
        if vid:
            return f"https://i.ytimg.com/vi/{vid}/hqdefault.jpg"
        return None

    df["_thumb"] = df.apply(fallback_thumbnail, axis=1)

    def collect_thumbs(g):
        top10 = g.nlargest(10, "views")
        urls  = top10["_thumb"].dropna().tolist()
        return urls

    thumb_map = (
        df.groupby("channel_id", sort=False)
          .apply(collect_thumbs)
          .rename("thumbnail_urls")
          .reset_index()
    )
    log_step("STEP 7", f"YouTube thumbnail URL lists built for {len(thumb_map):,} creators")

    # ── Step 8: Assemble master creator record ──────────────────
    # One row per creator from creators table
    creator_base = creators.copy()

    # Resolve column name for channel title
    if "channel_title" not in creator_base.columns:
        # Try alternate names
        for alt in ("channeltitle", "title", "name"):
            if alt in creator_base.columns:
                creator_base = creator_base.rename(columns={alt: "channel_title"})
                break

    # domain: take the merged domain per channel (most common or first)
    domain_map = (
        df.groupby("channel_id")["domain"]
          .apply(lambda s: s.dropna().iloc[0] if len(s.dropna()) else np.nan)
          .rename("domain")
          .reset_index()
    )

    master = (
        creator_base
        .merge(domain_map,   on="channel_id", how="left")
        .merge(text_blobs,   on="channel_id", how="left")
        .merge(eng,          on="channel_id", how="left")
        .merge(thumb_map,    on="channel_id", how="left")
    )

    # Standardise output columns
    master["creator_id"]                    = "yt_" + master["channel_id"].astype(str)
    master["platform"]                      = "youtube"
    master["is_verified"]                   = False  # YT table has no verified flag
    master["subscriber_or_follower_count"]  = pd.to_numeric(
        master.get("subscriber_count"), errors="coerce"
    )

    keep_cols = [
        "creator_id", "platform", "channel_id", "domain",
        "text_blob", "thumbnail_urls", "is_verified",
        "subscriber_or_follower_count",
        *YT_ENG_COLS,
        # extra useful metadata
        "channel_title", "language", "country",
        "subscriber_count", "video_count",
    ]
    keep_cols = [c for c in keep_cols if c in master.columns]
    master = master[keep_cols].copy()

    log_step("STEP 8", f"YouTube master: {len(master):,} creators, {master.shape[1]} fields")
    return master
