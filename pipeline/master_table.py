"""
master_table.py
===============
Combines the YouTube and Instagram master DataFrames into one
flat master_creators table and writes it to Parquet + CSV.
"""

import os
import pandas as pd
from utils import log_step

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "outputs")


def build_master_table(yt_df: pd.DataFrame, ig_df: pd.DataFrame) -> pd.DataFrame:
    print("\n" + "─"*50)
    print("  STEP 8 — MASTER TABLE")
    print("─"*50)

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    master = pd.concat([yt_df, ig_df], ignore_index=True, sort=False)

    # Ensure the 8 required fields are always present
    required = [
        "creator_id", "platform", "domain", "text_blob",
        "thumbnail_urls", "is_verified", "subscriber_or_follower_count",
    ]
    for col in required:
        if col not in master.columns:
            master[col] = None

    # Reorder: required cols first, then everything else
    other_cols = [c for c in master.columns if c not in required]
    master = master[required + other_cols]

    parquet_path = os.path.join(OUTPUT_DIR, "master_creators.parquet")
    csv_path     = os.path.join(OUTPUT_DIR, "master_creators.csv")

    master.to_parquet(parquet_path, index=False, engine="pyarrow")
    master.to_csv(csv_path, index=False)

    log_step(
        "STEP 8",
        f"Master table: {len(master):,} total creators  "
        f"(YT={len(yt_df):,} | IG={len(ig_df):,})  "
        f"→ {parquet_path}"
    )
    return master
