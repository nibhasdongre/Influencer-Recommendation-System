"""
Influencer Analytics Preprocessing Pipeline
============================================
Orchestrates all 8 steps for YouTube + Instagram data.
Run: python main.py
"""

import os, time
from utils import log_step
from youtube_pipeline import run_youtube_pipeline
from instagram_pipeline import run_instagram_pipeline
from master_table import build_master_table
from validation import run_validation

def main():
    print("\n" + "="*60)
    print("  INFLUENCER ANALYTICS PREPROCESSING PIPELINE")
    print("="*60)

    t0 = time.time()

    # ── Run platform pipelines ──────────────────────────────────
    yt_df  = run_youtube_pipeline()
    ig_df  = run_instagram_pipeline()

    # ── Step 8 : Build master table ─────────────────────────────
    master = build_master_table(yt_df, ig_df)

    # ── Validation ──────────────────────────────────────────────
    run_validation(master)

    elapsed = time.time() - t0
    print(f"\n✅  Pipeline complete in {elapsed:.1f}s")
    print(f"    Master table → outputs/master_creators.parquet  ({len(master)} rows)")
    print("="*60 + "\n")

if __name__ == "__main__":
    main()
