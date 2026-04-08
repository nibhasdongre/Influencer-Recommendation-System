"""utils.py — shared helpers used across the pipeline."""

import re, html, unicodedata
import pandas as pd
import numpy as np

# ── Logging ────────────────────────────────────────────────────────────────

def log_step(step: str, msg: str):
    print(f"  [{step}] {msg}")


# ── Column normalisation ────────────────────────────────────────────────────

def norm_cols(df: pd.DataFrame) -> pd.DataFrame:
    """Strip whitespace, lowercase, replace spaces/hyphens with underscores."""
    df.columns = (
        df.columns
          .str.strip()
          .str.lower()
          .str.replace(r"[\s\-]+", "_", regex=True)
    )
    return df


# ── Text helpers ────────────────────────────────────────────────────────────

_URL_RE       = re.compile(r"http[s]?://\S+|www\.\S+")
_EMAIL_RE     = re.compile(r"\S+@\S+\.\S+")
_TIMESTAMP_RE = re.compile(r"\b\d{1,2}:\d{2}(?::\d{2})?\b")
_EMOJI_RE     = re.compile(
    "["
    u"\U0001F600-\U0001F64F"
    u"\U0001F300-\U0001F5FF"
    u"\U0001F680-\U0001F9FF"
    u"\U0001FA00-\U0001FA6F"
    u"\U00002600-\U000027BF"
    u"\U0000FE00-\U0000FE0F"
    u"\U00002500-\U00002BEF"
    "]+", flags=re.UNICODE
)
_MENTION_RE   = re.compile(r"@\w+")
_HASHTAG_INLINE_RE = re.compile(r"#\w+")
_PHONE_RE     = re.compile(r"\+?\d[\d\s\-\(\)]{7,}\d")
_HTML_ENTITY_RE = re.compile(r"&[a-zA-Z]+;|&#\d+;")

YT_BOILERPLATE = re.compile(
    r"\b(subscribe|click the bell|affiliate|sponsored by|use code"
    r"|promo code|discount code|follow me on|follow us on"
    r"|check out my|link in bio|link below|links below"
    r"|patreon|merch|shop now|buy now|get yours)\b",
    re.IGNORECASE
)

YT_STOP_TAGS  = {"video","youtube","watch","new","2024","2023","official","shorts","clip","best"}
IG_STOP_TAGS  = {"fyp","viral","trending","reels","instagram","explore",
                 "like4like","follow","love","instagood","photooftheday",
                 "picoftheday","instadaily","followme","likeforlikes"}


def clean_text(s):
    if not isinstance(s, str):
        return ""
    s = _HTML_ENTITY_RE.sub(" ", s)
    s = html.unescape(s)
    s = _EMOJI_RE.sub(" ", s)
    return s.strip()


def clean_yt_title(s):
    s = clean_text(s)
    return s.lower().strip()


def clean_yt_description(s):
    if not isinstance(s, str):
        return ""
    s = clean_text(s)
    s = _URL_RE.sub(" ", s)
    s = _EMAIL_RE.sub(" ", s)
    s = _TIMESTAMP_RE.sub(" ", s)
    s = YT_BOILERPLATE.sub(" ", s)
    s = re.sub(r"\s{2,}", " ", s).strip()
    return s[:200]


def parse_yt_tags(s, max_tags=15):
    if not isinstance(s, str) or not s.strip():
        return ""
    # handle list-like strings
    s = s.strip("[]'\"")
    tokens = re.split(r"[,\|;]+|'|\"", s)
    tokens = [t.strip().lower() for t in tokens if t.strip()]
    tokens = [t for t in tokens if t not in YT_STOP_TAGS]
    tokens = list(dict.fromkeys(tokens))   # dedup, preserve order
    return " ".join(tokens[:max_tags])


def clean_ig_bio(s):
    if not isinstance(s, str):
        return ""
    s = _EMOJI_RE.sub(" ", s)
    s = _URL_RE.sub(" ", s)
    s = _PHONE_RE.sub(" ", s)
    s = re.sub(r"\s{2,}", " ", s).strip()
    return s


def clean_ig_caption(s):
    if not isinstance(s, str):
        return ""
    s = _EMOJI_RE.sub(" ", s)
    s = _URL_RE.sub(" ", s)
    s = _HASHTAG_INLINE_RE.sub(" ", s)
    s = _MENTION_RE.sub(" ", s)
    s = re.sub(r"\s{2,}", " ", s).strip()
    return s[:150]


def parse_ig_hashtags(s, max_tags=10):
    if not isinstance(s, str) or not s.strip():
        return []
    tokens = re.split(r"[#,\s]+", s)
    tokens = [t.strip().lower() for t in tokens if t.strip()]
    tokens = [t for t in tokens if t not in IG_STOP_TAGS and len(t) > 1]
    tokens = list(dict.fromkeys(tokens))
    return tokens[:max_tags]


# ── Normalisation ───────────────────────────────────────────────────────────

def minmax_norm(df: pd.DataFrame, cols: list) -> pd.DataFrame:
    for c in cols:
        mn, mx = df[c].min(), df[c].max()
        if mx - mn > 0:
            df[c] = (df[c] - mn) / (mx - mn)
        else:
            df[c] = 0.0
    return df
