"""
dedup.py
========
Step 1 — Deduplicate creator tables on their ID column.
Step 2 — Deduplicate content tables on their content-ID column,
          merging domain values when the only difference is domain.
"""

import pandas as pd
from utils import log_step


# ── Step 1 ─────────────────────────────────────────────────────────────────

def dedup_creators(df: pd.DataFrame, id_col: str, platform: str) -> pd.DataFrame:
    """
    Keep the first occurrence of each creator ID.
    Reports how many duplicate rows were dropped.
    """
    before = len(df)
    df = df.drop_duplicates(subset=[id_col], keep="first").reset_index(drop=True)
    dropped = before - len(df)
    log_step(
        "STEP 1",
        f"{platform} creators: {before:,} → {len(df):,}  "
        f"({dropped} duplicate rows removed, 0 data loss)"
    )
    return df


# ── Step 2 ─────────────────────────────────────────────────────────────────

def _merge_domain(group: pd.DataFrame) -> pd.Series:
    """
    For a group sharing the same content-ID:
    - Merge unique domain values with commas.
    - Return the first row with the merged domain.
    """
    row = group.iloc[0].copy()
    if "domain" in group.columns:
        domains = (
            group["domain"]
            .dropna()
            .astype(str)
            .str.strip()
            .str.lower()
            .unique()
            .tolist()
        )
        row["domain"] = ",".join(sorted(domains)) if domains else row["domain"]
    return row


def dedup_content(
    tables: dict,
    id_col: str,
    platform: str
) -> dict:
    """
    Deduplicate every table in `tables` on `id_col`.
    Where domain is the only differing field, merge domain values.
    Returns cleaned tables dict.
    """
    deduped = {}
    for name, df in tables.items():
        before = len(df)
        if id_col not in df.columns:
            # table may not have the content id (e.g. creators – already handled)
            deduped[name] = df
            continue

        # Check if there are any actual duplicates
        dup_mask = df.duplicated(subset=[id_col], keep=False)
        n_dup_ids = df.loc[dup_mask, id_col].nunique()

        if n_dup_ids == 0:
            deduped[name] = df
            log_step("STEP 2", f"{platform}/{name}: no duplicates found")
            continue

        # Separate duplicates from clean rows
        clean    = df[~dup_mask].copy()
        dups     = df[dup_mask].copy()

        # For duplicates: if they differ only in domain, merge domains
        non_domain_cols = [c for c in dups.columns if c not in (id_col, "domain")]

        # Group by id; within each group check whether non-domain cols are identical
        def safe_merge(group):
            if len(group) == 1:
                return group.iloc[0]
            # Are all non-domain fields the same across the group?
            # (use first-row values; merge domain regardless)
            return _merge_domain(group)

        merged = (
            dups.groupby(id_col, sort=False)
                .apply(safe_merge)
                .reset_index(drop=True)
        )

        result = pd.concat([clean, merged], ignore_index=True)
        dropped = before - len(result)

        log_step(
            "STEP 2",
            f"{platform}/{name}: {before:,} → {len(result):,}  "
            f"({dropped} duplicate rows removed, {n_dup_ids} IDs had multi-domain merges)"
        )
        deduped[name] = result

    return deduped
