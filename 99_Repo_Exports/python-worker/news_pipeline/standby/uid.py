# python-worker/news_pipeline/standby/uid.py
from __future__ import annotations
import hashlib
import time
from dataclasses import dataclass

SEP = b"\x1f"

def stable_uid(*parts: str) -> str:
    """
    Совместимо с Go StableUID:
      sha256(parts joined with 0x1f) -> hex -> [:24]
    """
    h = hashlib.sha256()
    for p in parts:
        h.update((p or "").encode("utf-8", errors="ignore"))
        h.update(SEP)
    return h.hexdigest()[:24]

def bucket_start_ms(ts_ms: int, bucket_ms: int) -> int:
    if ts_ms <= 0 or bucket_ms <= 0:
        return 0
    return (ts_ms // bucket_ms) * bucket_ms

@dataclass(frozen=True, slots=True)
class UIDPolicy:
    bucket_ms: int  # например 6h = 21_600_000

    def uid_for_news(self, *, source: str, url: str, title: str, provider_id: str, published_ts_ms: int) -> str:
        b = bucket_start_ms(published_ts_ms, self.bucket_ms)
        return stable_uid(source, url, title, provider_id, str(b))
