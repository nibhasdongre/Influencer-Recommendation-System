"""
instagram_pipeline.py
=====================
Full preprocessing pipeline for Instagram (Steps 1–8).
Returns one DataFrame with one row per creator.
"""

import re
import numpy as np
import pandas as pd
from collections import Counter

from data_loader import load_instagram
from dedup import dedup_creators, dedup_content
from utils import (
    log_step,
    clean_ig_bio, clean_ig_caption, parse_ig_hashtags,
    minmax_norm,
)

IG_ENG_COLS = [
    "mean_like_rate", "mean_comment_rate", "share_rate",
    "posting_frequency", "engagement_variance", "follower_post_ratio",
]


def run_instagram_pipeline() -> pd.DataFrame:
    print("\n" + "─"*50)
    print("  INSTAGRAM PIPELINE")
    print("─"*50)

    # ── Load ────────────────────────────────────────────────────
    tables = load_instagram()
    creators = tables["creators"]
    posts    = tables["posts"]
    metrics  = tables["metrics"]
    assets   = tables["assets"]

    # ── Step 1: Deduplicate creators ───────────────────────────
    creators = dedup_creators(creators, "account_id", "Instagram")

    # ── Step 2: Deduplicate content tables ─────────────────────
    content = dedup_content(
        {"posts": posts, "metrics": metrics, "assets": assets},
        "post_id", "Instagram"
    )
    posts, metrics, assets = content["posts"], content["metrics"], content["assets"]

    # ── Step 3: JOIN ────────────────────────────────────────────
    df = (
        posts
        .merge(metrics,  on="post_id",   how="left")
        .merge(assets,   on="post_id",   how="left")
        .merge(creators, on="account_id", how="left")
    )
    log_step("STEP 3", f"Instagram joined table: {len(df):,} rows × {df.shape[1]} cols")

    # ── Step 4: Filter ──────────────────────────────────────────
    before = len(df)
    df["followers"] = pd.to_numeric(df.get("followers"), errors="coerce")

    # posts count per account_id (from posts table, not creators.posts field)
    posts_per_account = df.groupby("account_id")["post_id"].transform("nunique")
    df = df[
        (df["followers"].fillna(0) >= 1000) &
        (posts_per_account >= 3)
    ].reset_index(drop=True)
    log_step("STEP 4", f"Instagram filter: {before:,} → {len(df):,} rows  ({before-len(df)} removed)")

    # ── Step 5: Text Preprocessing ─────────────────────────────
    df["likes"] = pd.to_numeric(df.get("likes"), errors="coerce").fillna(0)

    # Field-level cleaning
    df["_clean_bio"]      = df["bio"].apply(clean_ig_bio)
    df["_clean_caption"]  = df["caption"].apply(clean_ig_caption)
    df["_hashtag_tokens"] = df["hashtags"].apply(parse_ig_hashtags)
    df["_hashtag_str"]    = df["_hashtag_tokens"].apply(lambda t: " ".join(t))

    # Creator-level aggregation
    def agg_text(g):
        bio = str(g["_clean_bio"].iloc[0]) if "_clean_bio" in g.columns else ""

        # Top-15 posts by likes
        top15  = g.nlargest(15, "likes")
        pieces = [bio]
        for _, row in top15.iterrows():
            pieces.append(row["_clean_caption"])
            pieces.append(row["_hashtag_str"])

        # Hashtag frequency signature across ALL posts
        all_tags: list = []
        for tlist in g["_hashtag_tokens"]:
            all_tags.extend(tlist)
        top10_freq = [t for t, _ in Counter(all_tags).most_common(10)]
        pieces.append(" ".join(top10_freq))

        return " ".join(p for p in pieces if p).strip()

    text_blobs = (
        df.groupby("account_id", sort=False)
          .apply(agg_text)
          .rename("text_blob")
          .reset_index()
    )
    log_step("STEP 5", f"Instagram text blobs built for {len(text_blobs):,} creators")

    # ── Step 6: Engagement Feature Engineering ─────────────────
    df["views"]    = pd.to_numeric(df.get("views"),    errors="coerce")
    df["comments"] = pd.to_numeric(df.get("comments"), errors="coerce")
    df["shares"]   = pd.to_numeric(df.get("shares"),   errors="coerce").fillna(0)

    if "timestamp" in df.columns:
        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce", utc=True)

    def eng_features(g):
        g = g.copy()

        # If views == 0, fall back to followers as denominator
        denom = g["views"].copy()
        follower_val = g["followers"].iloc[0] if "followers" in g.columns else np.nan
        denom = denom.where(denom > 0, other=follower_val)
        denom = denom.replace(0, np.nan)

        like_rate    = (g["likes"]    / denom).replace([np.inf, -np.inf], np.nan)
        comment_rate = (g["comments"] / denom).replace([np.inf, -np.inf], np.nan)
        share_rate   = (g["shares"]   / denom).replace([np.inf, -np.inf], np.nan)

        mean_like_rate    = like_rate.mean()
        mean_comment_rate = comment_rate.mean()
        share_rate_mean   = share_rate.mean()

        # Posting frequency (posts / months active)
        posting_frequency = np.nan
        if "timestamp" in g.columns:
            ts = g["timestamp"].dropna()
            if len(ts) >= 2:
                months = (ts.max() - ts.min()).days / 30.44
                if months > 0:
                    posting_frequency = len(g) / months

        # Engagement variance
        lk = g["likes"].dropna()
        engagement_variance = (lk.std() / lk.mean()) if lk.mean() > 0 else np.nan

        # Follower-to-post ratio
        n_posts = len(g)
        follower_post_ratio = (follower_val / n_posts) if (n_posts > 0 and pd.notna(follower_val)) else np.nan

        return pd.Series({
            "mean_like_rate"      : mean_like_rate,
            "mean_comment_rate"   : mean_comment_rate,
            "share_rate"          : share_rate_mean,
            "posting_frequency"   : posting_frequency,
            "engagement_variance" : engagement_variance,
            "follower_post_ratio" : follower_post_ratio,
        })

    eng = (
        df.groupby("account_id", sort=False)
          .apply(eng_features)
          .reset_index()
    )
    eng = minmax_norm(eng, IG_ENG_COLS)
    log_step("STEP 6", f"Instagram engagement features computed + normalised for {len(eng):,} creators")

    # ── Step 7: Thumbnail URLs ──────────────────────────────────
    if "media_url" not in df.columns and "media_url_x" in df.columns:
        df["media_url"] = df["media_url_x"]

    def collect_thumbs(g):
        top15 = g.nlargest(15, "likes")
        col   = "media_url" if "media_url" in top15.columns else (
                "thumbnail_url" if "thumbnail_url" in top15.columns else None)
        if col:
            return top15[col].dropna().tolist()
        return []

    thumb_map = (
        df.groupby("account_id", sort=False)
          .apply(collect_thumbs)
          .rename("thumbnail_urls")
          .reset_index()
    )
    log_step("STEP 7", f"Instagram thumbnail URL lists built for {len(thumb_map):,} creators")

    # ── Step 8: Assemble master creator record ──────────────────
    creator_base = creators.copy()

    # domain per account
    domain_map = (
        df.groupby("account_id")["domain"]
          .apply(lambda s: s.dropna().iloc[0] if len(s.dropna()) else np.nan)
          .rename("domain")
          .reset_index()
    )

    master = (
        creator_base
        .merge(domain_map,  on="account_id", how="left")
        .merge(text_blobs,  on="account_id", how="left")
        .merge(eng,         on="account_id", how="left")
        .merge(thumb_map,   on="account_id", how="left")
    )

    master["creator_id"]                   = "ig_" + master["account_id"].astype(str)
    master["platform"]                     = "instagram"
    master["is_verified"]                  = master.get("verified", pd.Series(False, index=master.index)).fillna(False).astype(bool)
    master["subscriber_or_follower_count"] = pd.to_numeric(
        master.get("followers"), errors="coerce"
    )

    keep_cols = [
        "creator_id", "platform", "account_id", "domain",
        "text_blob", "thumbnail_urls", "is_verified",
        "subscriber_or_follower_count",
        *IG_ENG_COLS,
        # extra metadata
        "username", "followers", "posts", "verified",
    ]
    keep_cols = [c for c in keep_cols if c in master.columns]
    master = master[keep_cols].copy()

    log_step("STEP 8", f"Instagram master: {len(master):,} creators, {master.shape[1]} fields")
    return master
