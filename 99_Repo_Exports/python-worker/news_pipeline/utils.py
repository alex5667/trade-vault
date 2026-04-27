from __future__ import annotations
from utils.time_utils import get_ny_time_millis

import hashlib
import time
from typing import Any, Optional


def now_ms() -> int:
    return get_ny_time_millis()

def ts_bucket(ts_ms: int, bucket_sec: int) -> int:
    if bucket_sec <= 0:
        bucket_sec = 60
    return int(ts_ms // (bucket_sec * 1000))

def sha1_hex(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8", errors="ignore")).hexdigest()

def make_news_uid(source: str, url: str, title: str, ts_ms: int, bucket_sec: int) -> str:
    b = ts_bucket(ts_ms, bucket_sec)
    base = f"{source}|{url}|{title}|{b}"
    return sha1_hex(base)

def safe_int(x: Any, default: int = 0) -> int:
    try:
        return int(x)
    except Exception:
        return default

def safe_float(x: Any, default: float = 0.0) -> float:
    try:
        return float(x)
    except Exception:
        return default
