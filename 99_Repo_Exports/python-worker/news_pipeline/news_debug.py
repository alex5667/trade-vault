from __future__ import annotations

"""python-worker/news_pipeline/news_debug.py

News logging helpers.

Goals:
- Never log the whole ctx.
- Provide a small, cheap "mini" log line that is safe to enable.
- Provide optional "full" debug behind NEWS_DEBUG_FULL=true with sampling.

Sampling must be deterministic per (symbol, ts) so you can compare runs.
No external dependencies.
"""


import json
import os
import zlib
from typing import Any


def stable_sample_pct(symbol: str, ts_ms: int, pct: int) -> bool:
    """Deterministic sampling based on (symbol, ts_ms).

    pct is 0..100. Uses CRC32 for speed and stability.
    """

    if pct <= 0:
        return False
    if pct >= 100:
        return True

    key = f"{symbol}|{int(ts_ms)}".encode("utf-8", "ignore")
    h = zlib.crc32(key) & 0xFFFFFFFF
    return (h % 100) < pct


def resolve_news_ref(ref: str) -> str:
    """Normalize ref to a Redis key for the heavy JSON.

    Conventions observed in the pipeline:
    - Some components store just UID ("abc123").
    - Some store a full key ("news:analysis:abc123").

    We normalize to "news:analysis:<uid>".
    """

    r = (ref or "").strip()
    if not r:
        return ""
    if r.startswith("news:analysis:"):
        return r
    # If someone stored "news:analysis:<uid>" in ref already with different separator
    if r.startswith("news:analysis/"):
        return "news:analysis:" + r.split("/", 1)[1]
    # UID only
    return f"news:analysis:{r}"


def _safe_getattr(obj: Any, name: str, default: Any = None) -> Any:
    try:
        return getattr(obj, name, default)
    except Exception:
        return default


def extract_news_mini(ctx: Any) -> tuple[str, int, float, int, int, int] | None:
    """Return a compact tuple used by the mini logger.

    Returns:
        (symbol, ts_ms, news_risk, event_tminus_sec, news_grade_id, tags_mask)

    If ctx has no news => returns None.
    """

    news = _safe_getattr(ctx, "news", None)
    if not news:
        return None

    symbol = str(_safe_getattr(ctx, "symbol", "") or "")
    ts_ms = int(_safe_getattr(ctx, "ts", 0) or 0)

    news_risk = float(_safe_getattr(news, "news_risk", 0.0) or 0.0)
    event_tminus = int(_safe_getattr(news, "event_tminus_sec", -1) or -1)
    news_grade_id = int(_safe_getattr(news, "news_grade_id", 0) or 0)
    tags_mask = int(_safe_getattr(news, "tags_mask", 0) or 0)

    return (symbol, ts_ms, news_risk, event_tminus, news_grade_id, tags_mask)


def log_news_mini(logger: Any, ctx: Any, *, sample_pct: int = 1) -> None:
    """Mini log line with sampling.

    Intended for DEBUG level. Never logs full ctx/payload.
    """

    mini = extract_news_mini(ctx)
    if not mini:
        return

    symbol, ts_ms, news_risk, event_tminus, news_grade_id, tags_mask = mini
    if not stable_sample_pct(symbol, ts_ms, sample_pct):
        return

    # Hex mask is helpful when debugging bitmaps.
    logger.debug(
        "news_mini symbol=%s ts_ms=%d risk=%.4f tminus=%d grade=%d tags_mask=%d(0x%x)",
        symbol,
        ts_ms,
        news_risk,
        event_tminus,
        news_grade_id,
        tags_mask,
        tags_mask,
    )


def maybe_log_news_full(redis_client: Any, logger: Any, ctx: Any) -> None:
    """Optional full debug log (sampled) pulling heavy JSON by ctx.news.ref.

    Controlled by env:
    - NEWS_DEBUG_FULL=true
    - NEWS_DEBUG_FULL_SAMPLE_PCT (default 1)
    - NEWS_DEBUG_FULL_MAX_CHARS (default 2000)

    Safe guards:
    - If Redis is slow/unavailable => fail-open (silently).
    - Truncates payload.
    """

    if os.getenv("NEWS_DEBUG_FULL", "false").lower() not in ("1", "true", "yes", "on"):
        return

    mini = extract_news_mini(ctx)
    if not mini:
        return

    symbol, ts_ms, *_ = mini
    pct = int(os.getenv("NEWS_DEBUG_FULL_SAMPLE_PCT", "1") or "1")
    if not stable_sample_pct(symbol, ts_ms, pct):
        return

    news = _safe_getattr(ctx, "news", None)
    ref = str(_safe_getattr(news, "ref", "") or "")
    key = resolve_news_ref(ref)
    if not key:
        return

    max_chars = int(os.getenv("NEWS_DEBUG_FULL_MAX_CHARS", "2000") or "2000")

    try:
        raw = redis_client.get(key)
        if not raw:
            return
        s = str(raw)
        if len(s) > max_chars:
            s = s[:max_chars] + "…"

        # If it's JSON, re-dump compactly to avoid whitespace spam.
        try:
            obj = json.loads(s)
            s = json.dumps(obj, ensure_ascii=False, separators=(",", ":"))
            if len(s) > max_chars:
                s = s[:max_chars] + "…"
        except Exception:
            pass

        logger.debug("news_full symbol=%s ts_ms=%d ref=%s payload=%s", symbol, ts_ms, key, s)
    except Exception:
        # fail-open
        return
