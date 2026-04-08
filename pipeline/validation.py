"""
validation.py
=============
Sanity-check script for the preprocessed master table.
Can be imported (run_validation) or run standalone:
    python validation.py
"""

import os, random
import numpy as np
import pandas as pd
from urllib.request import urlopen, Request
from urllib.error import URLError, HTTPError

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "outputs")

ENG_COLS_YT = [
    "mean_like_rate", "mean_comment_rate", "view_consistency",
    "upload_frequency", "like_view_trend", "video_count_ratio",
]
ENG_COLS_IG = [
    "mean_like_rate", "mean_comment_rate", "share_rate",
    "posting_frequency", "engagement_variance", "follower_post_ratio",
]


# ── URL checker ─────────────────────────────────────────────────────────────

def _check_url(url: str, timeout: int = 5) -> bool:
    try:
        req = Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urlopen(req, timeout=timeout) as r:
            return r.status == 200
    except Exception:
        return False


def _sample_urls(master: pd.DataFrame, n: int = 50) -> float:
    """Sample n random thumbnail URLs across both platforms; return live fraction."""
    all_urls = []
    for lst in master["thumbnail_urls"].dropna():
        if isinstance(lst, list):
            all_urls.extend(lst)
        elif isinstance(lst, str) and lst.strip():
            all_urls.append(lst.strip())

    if not all_urls:
        return None

    sample = random.sample(all_urls, min(n, len(all_urls)))
    live   = sum(_check_url(u) for u in sample)
    return live / len(sample)


# ── Main validation ──────────────────────────────────────────────────────────

def run_validation(master: pd.DataFrame, check_urls: bool = False):
    """
    Runs all sanity checks on the master creator DataFrame.
    Set check_urls=True to test a sample of thumbnail URLs (slow, requires network).
    """
    print("\n" + "="*50)
    print("  VALIDATION REPORT")
    print("="*50)

    sep = "  " + "-"*44

    # ── 1. Text blob length distribution ────────────────────────
    print("\n[1] Text blob length distribution")
    blobs = master["text_blob"].dropna().astype(str)
    lengths = blobs.str.len()
    print(f"     count  : {len(lengths):,}")
    print(f"     median : {lengths.median():.0f} chars")
    print(f"     mean   : {lengths.mean():.0f} chars")
    print(f"     < 50   : {(lengths < 50).sum():,}  ← consider dropping")
    print(f"     50-800 : {((lengths >= 50) & (lengths <= 800)).sum():,}  ← ideal range")
    print(f"     > 800  : {(lengths > 800).sum():,}")
    sparse_ids = master.loc[blobs[lengths < 50].index, "creator_id"].tolist()
    if sparse_ids:
        print(f"     ⚠  Sparse creators (<50 chars): {sparse_ids[:10]}")

    # ── 2. Engagement feature NaN rate ──────────────────────────
    print(f"\n[2] Engagement feature NaN rate  (threshold: 20%)")
    yt = master[master["platform"] == "youtube"]
    ig = master[master["platform"] == "instagram"]

    for platform_name, subset, eng_cols in [
        ("YouTube",   yt, ENG_COLS_YT),
        ("Instagram", ig, ENG_COLS_IG),
    ]:
        present = [c for c in eng_cols if c in subset.columns]
        if not present:
            continue
        print(f"     {platform_name}:")
        for c in present:
            nan_pct = subset[c].isna().mean() * 100
            flag = " ⚠  INVESTIGATE" if nan_pct > 20 else ""
            print(f"       {c:<28}: {nan_pct:5.1f}%{flag}")

    # ── 3. URL validity (optional — network call) ────────────────
    print(f"\n[3] Thumbnail URL validity")
    if check_urls:
        live_frac = _sample_urls(master, n=50)
        if live_frac is not None:
            flag = "✅" if live_frac >= 0.8 else "⚠ "
            print(f"     {flag} {live_frac*100:.1f}% of 50 sampled URLs are live  (target ≥80%)")
        else:
            print("     No URLs found to test.")
    else:
        # Just count non-null, non-empty
        def _has_urls(v):
            if isinstance(v, list):
                return len(v) > 0
            return isinstance(v, str) and len(v.strip()) > 0

        n_with_urls = master["thumbnail_urls"].apply(_has_urls).sum()
        print(f"     Creators with ≥1 URL: {n_with_urls:,} / {len(master):,}")
        print(f"     (Set check_urls=True in run_validation() to test live rate)")

    # ── 4. Creator count breakdown ───────────────────────────────
    print(f"\n[4] Creator count breakdown  (platform × domain)")
    breakdown = (
        master.groupby(["platform", "domain"], dropna=False)
              .size()
              .reset_index(name="count")
    )
    for _, row in breakdown.iterrows():
        print(f"     {str(row['platform']):<12} | {str(row['domain']):<12}: {row['count']:,}")
    print(f"     {'TOTAL':<12}   {'':12}  {len(master):,}")

    # ── 5. Verified creator count ────────────────────────────────
    print(f"\n[5] Verified creator count")
    n_verified = master["is_verified"].fillna(False).astype(bool).sum()
    flag = "✅" if n_verified >= 20 else "⚠  < 20 verified — weak trust anchor for GNN"
    print(f"     {flag}  {n_verified} verified creators total")
    per_platform = master.groupby("platform")["is_verified"].apply(
        lambda s: s.fillna(False).astype(bool).sum()
    )
    for plat, cnt in per_platform.items():
        print(f"       {plat}: {cnt}")

    print("\n" + "="*50 + "\n")


# ── Standalone entry point ───────────────────────────────────────────────────

if __name__ == "__main__":
    parquet = os.path.join(OUTPUT_DIR, "master_creators.parquet")
    csv     = os.path.join(OUTPUT_DIR, "master_creators.csv")

    if os.path.exists(parquet):
        master = pd.read_parquet(parquet)
        print(f"Loaded {parquet}  ({len(master):,} rows)")
    elif os.path.exists(csv):
        master = pd.read_csv(csv)
        print(f"Loaded {csv}  ({len(master):,} rows)")
    else:
        raise FileNotFoundError(
            f"No master table found in {OUTPUT_DIR}. "
            "Run the full pipeline first (python main.py)."
        )

    # Set check_urls=True if you want live URL testing (needs network + time)
    run_validation(master, check_urls=False)
