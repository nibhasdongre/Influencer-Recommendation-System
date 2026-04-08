"""
data_loader.py
==============
Loads the 8 raw tables (4 YT + 4 IG) from the data/ directory.
Place your CSV files in:
    data/youtube/creators.csv
    data/youtube/videos.csv
    data/youtube/metrics.csv
    data/youtube/assets.csv
    data/instagram/creators.csv
    data/instagram/posts.csv
    data/instagram/metrics.csv
    data/instagram/assets.csv

File names are matched case-insensitively.
"""

import os, glob
import pandas as pd
from utils import norm_cols, log_step

DATA_DIR = os.path.join(os.path.dirname(__file__), "data")

_YT_MAP = {
    "creators": ["creators", "creator", "channels", "channel"],
    "videos"  : ["videos", "video"],
    "metrics" : ["metrics", "metric"],
    "assets"  : ["assets", "asset"],
}

_IG_MAP = {
    "creators": ["creators", "creator", "accounts", "account"],
    "posts"   : ["posts", "post"],
    "metrics" : ["metrics", "metric"],
    "assets"  : ["assets", "asset"],
}


def _find_csvs(folder: str, candidates: list) -> str:
    """Return the first CSV whose stem matches any candidate (case-insensitive)."""
    all_csvs = glob.glob(os.path.join(folder, "*.csv"))
    stem_map  = {os.path.splitext(os.path.basename(p))[0].lower(): p for p in all_csvs}
    for c in candidates:
        if c in stem_map:
            return stem_map[c]
    raise FileNotFoundError(
        f"No CSV matching {candidates} found in {folder}.\n"
        f"Available: {list(stem_map.keys())}"
    )


def load_platform(platform: str, table_map: dict) -> dict:
    folder = os.path.join(DATA_DIR, platform)
    tables = {}
    for key, candidates in table_map.items():
        path = _find_csvs(folder, candidates)
        df   = pd.read_csv(path,low_memory=False)
        df   = norm_cols(df)
        tables[key] = df
        log_step("LOAD", f"{platform}/{key}: {len(df):,} rows × {df.shape[1]} cols  ← {os.path.basename(path)}")
    return tables


def load_youtube() -> dict:
    return load_platform("youtube", _YT_MAP)


def load_instagram() -> dict:
    return load_platform("instagram", _IG_MAP)
