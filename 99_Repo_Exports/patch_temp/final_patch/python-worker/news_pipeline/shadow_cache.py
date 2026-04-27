"""news_pipeline.shadow_cache

This module implements a *shadow cache* for news/calendar features.

Why
---
Your current tick-loop enrichment does a Redis pipeline (1 RTT). Even with a
fast Redis client, any network stall can block the tick-loop.

The next level up is to make tick-loop *zero I/O*:
- tick-loop never calls Redis
- a background thread refreshes compact aggregates from Redis
- tick-loop uses only an in-memory dict lookup

Key properties
--------------
- Fail-open: any error in refresher or Redis yields empty cache, tick-loop stays
  alive.
- Bounded overhead in tick-loop: single dict lookup + a few conversions.
- Staleness control: optionally drop features if cache is too old.

Assumptions
-----------
- Redis keys:
  - news:agg:<symbol> HASH with fields:
    ref, risk_ema, surprise_ema, news_grade_id, tags_mask, primary_tag_id,
    horizon_sec, confidence, asof_ts_ms
  - calendar:agg:<asset_class> HASH with fields:
    event_tminus_sec, event_grade_id (plus next_ts_ms/event_ref/etc if you keep)

- contexts.NewsFeatures exists and is (slots=True, frozen=True).

Integration
-----------
Use NewsShadowEnricher (see enricher_shadow.py) in your tick-loop instead of
NewsEnricherSync.

The only wiring needed is to instantiate NewsShadowEnricher with:
- redis: a *fast* Redis client/pool (socket_timeout <= 50ms, connect_timeout <= 200ms)
- start() once during handler init
- call attach(ctx, asset_class=...) in tick-loop

"""

from __future__ import annotations

import os
import time
import threading
import logging
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Tuple

try:
    # Your project helper (if present)
    from common.dq_flags import append_dq_flag  # type: ignore
except Exception:  # pragma: no cover
    append_dq_flag = None  # type: ignore

from contexts import NewsFeatures, OrderflowSignalContext

log = logging.getLogger("news_shadow_cache")

# Fields we read from news:agg:<symbol>. HMGET is cheaper than HGETALL.
NEWS_HASH_FIELDS: Tuple[str, ...] = (
    "ref",
    "risk_ema",
    "surprise_ema",
    "news_grade_id",
    "tags_mask",
    "primary_tag_id",
    "confidence",
    "horizon_sec",
    "asof_ts_ms",
)


def _now_ms() -> int:
    return int(time.time() * 1000)


def _f(v: Any, default: float = 0.0) -> float:
    try:
        return float(v)
    except Exception:
        return default


def _i(v: Any, default: int = 0) -> int:
    try:
        return int(float(v))
    except Exception:
        return default


def _ensure_ref(ref: str) -> str:
    """Normalize ref.

    We want ref to be a *pointer* to the heavy JSON:
        news:analysis:<uid>

    Some older code stores only uid in `ref`. Normalize here.
    """
    ref = (ref or "").strip()
    if not ref:
        return ""
    if ref.startswith("news:analysis:"):
        return ref
    # If it's already a key-like string with ':' but not the expected prefix,
    # keep it as-is (fail-open, avoid surprising rewrites).
    if ":" in ref:
        return ref
    return f"news:analysis:{ref}"


def _dq(ctx: Any, flag: str) -> None:
    """Fail-open data-quality flag append."""
    if not flag:
        return
    try:
        if append_dq_flag is not None:
            append_dq_flag(ctx, flag)  # type: ignore
            return
    except Exception:
        pass

    # Fallback (no dependency): store list[str] if present.
    try:
        lst = getattr(ctx, "data_quality_flags", None)
        if lst is None:
            return
        if flag not in lst:
            lst.append(flag)
    except Exception:
        pass


@dataclass(slots=True)
class ShadowCacheConfig:
    """Runtime knobs.

    Defaults are conservative for a 50ms tick-loop budget.
    """

    enabled: bool = True

    # Refresher intervals.
    refresh_ms: int = 250
    calendar_refresh_ms: int = 1000

    # How long we keep a symbol in the "hot" set after it was seen in tick-loop.
    symbol_interest_ttl_ms: int = 30_000

    # Hard cap to bound Redis work per refresh.
    max_symbols_per_refresh: int = 256

    # If cached features are older than this, drop them (fail-open).
    # 0 disables staleness check.
    max_feature_age_ms: int = 5 * 60_000

    # Background thread should not spam logs.
    log_every_n_errors: int = 50


class ShadowCache:
    """Thread-safe-ish cache holder.

    Tick-loop only does dict.get (no iteration). Background thread updates keys.
    Python dict operations are atomic under the GIL; we avoid iterating over a
    dict while another thread might resize it.

    If you later need lock-free iteration, switch to copy-on-write swapping.
    """

    def __init__(self) -> None:
        self.news_by_symbol: Dict[str, NewsFeatures] = {}
        self.cal_by_asset: Dict[str, Tuple[int, int, int]] = {}
        # cal tuple: (event_tminus_sec, event_grade_id, updated_ts_ms)

        # Interest sets (updated by tick-loop).
        self._seen_symbol_ms: Dict[str, int] = {}
        self._seen_asset_ms: Dict[str, int] = {}

        # Diagnostics
        self.last_refresh_ms: int = 0
        self.last_calendar_refresh_ms: int = 0

    # --- tick-loop API (no Redis, constant time) ---

    def mark_seen(self, symbol: str, asset_class: str = "") -> None:
        now = _now_ms()
        sym = (symbol or "GLOBAL").upper()
        self._seen_symbol_ms[sym] = now
        ac = (asset_class or "").strip().lower()
        if ac:
            self._seen_asset_ms[ac] = now

    def get(self, symbol: str, asset_class: str = "", *, max_age_ms: int = 0) -> Optional[NewsFeatures]:
        """Get merged NewsFeatures (news + calendar fields)."""
        sym = (symbol or "GLOBAL").upper()
        nf = self.news_by_symbol.get(sym)
        if nf is None:
            return None

        if max_age_ms and nf.asof_ts_ms > 0:
            age = _now_ms() - int(nf.asof_ts_ms)
            if age > max_age_ms:
                return None

        ac = (asset_class or "").strip().lower()
        if not ac:
            return nf

        cal = self.cal_by_asset.get(ac)
        if not cal:
            return nf

        event_tminus_sec, event_grade_id, _updated = cal

        # Because NewsFeatures is frozen, create a new instance only if calendar exists.
        # This allocation happens only when calendar is enabled for the asset.
        return NewsFeatures(
            ref=nf.ref,
            news_risk=nf.news_risk,
            surprise_score=nf.surprise_score,
            news_grade_id=nf.news_grade_id,
            tags_mask=nf.tags_mask,
            primary_tag_id=nf.primary_tag_id,
            confidence=nf.confidence,
            horizon_sec=nf.horizon_sec,
            asof_ts_ms=nf.asof_ts_ms,
            event_tminus_sec=int(event_tminus_sec),
            event_grade_id=int(event_grade_id),
        )

    # --- background thread support (called from refresher) ---

    def active_symbols(self, *, ttl_ms: int, limit: int) -> List[str]:
        now = _now_ms()
        out: List[Tuple[int, str]] = []
        for sym, ts in list(self._seen_symbol_ms.items()):
            if now - ts <= ttl_ms:
                out.append((ts, sym))
        out.sort(reverse=True)
        return [sym for _ts, sym in out[: max(0, int(limit))]]

    def active_assets(self, *, ttl_ms: int) -> List[str]:
        now = _now_ms()
        out: List[Tuple[int, str]] = []
        for ac, ts in list(self._seen_asset_ms.items()):
            if now - ts <= ttl_ms:
                out.append((ts, ac))
        out.sort(reverse=True)
        return [ac for _ts, ac in out]


class ShadowRefresher:
    """Background refresher that pulls compact aggregates from Redis.

    It is intentionally *not* async: separate thread avoids interfering with your
    existing event loops.

    Redis client requirements:
    - decode_responses=True (strings)
    - short timeouts (socket_timeout <= 50ms) recommended
    """

    def __init__(
        self,
        *,
        redis,
        cache: ShadowCache,
        cfg: ShadowCacheConfig,
        news_key_prefix: str = "news:agg:",
        cal_key_prefix: str = "calendar:agg:",
    ) -> None:
        self.r = redis
        self.cache = cache
        self.cfg = cfg
        self.news_key_prefix = news_key_prefix
        self.cal_key_prefix = cal_key_prefix

        self._stop = threading.Event()
        self._t: Optional[threading.Thread] = None
        self._err_count = 0

    def start(self) -> None:
        if not self.cfg.enabled:
            return
        if self._t and self._t.is_alive():
            return
        self._stop.clear()
        self._t = threading.Thread(target=self._run, name="news-shadow-refresh", daemon=True)
        self._t.start()

    def stop(self, *, join_timeout: float = 1.0) -> None:
        self._stop.set()
        try:
            if self._t:
                self._t.join(timeout=join_timeout)
        except Exception:
            pass

    # --- internal loop ---

    def _run(self) -> None:
        next_cal = 0
        while not self._stop.is_set():
            try:
                self.refresh_news_once()
                now = _now_ms()
                if now >= next_cal:
                    self.refresh_calendar_once()
                    next_cal = now + max(50, int(self.cfg.calendar_refresh_ms))
                # Sleep a little. If refresh_ms is tiny, clamp to 25ms.
                time.sleep(max(0.025, float(self.cfg.refresh_ms) / 1000.0))
            except Exception as e:  # pragma: no cover
                self._err_count += 1
                if self._err_count % max(1, self.cfg.log_every_n_errors) == 0:
                    log.warning("shadow refresher error (count=%d): %s", self._err_count, e)
                # Backoff a bit to avoid hot error loop
                time.sleep(0.25)

    # --- single-iteration refresh (unit-test friendly) ---

    def refresh_news_once(self) -> None:
        syms = self.cache.active_symbols(
            ttl_ms=self.cfg.symbol_interest_ttl_ms,
            limit=self.cfg.max_symbols_per_refresh,
        )
        if not syms:
            self.cache.last_refresh_ms = _now_ms()
            return

        pipe = self.r.pipeline(transaction=False)
        for sym in syms:
            key = f"{self.news_key_prefix}{sym}"
            # HMGET returns list in the same order as fields
            pipe.hmget(key, *NEWS_HASH_FIELDS)
        res = pipe.execute()

        now_ms = _now_ms()
        for sym, row in zip(syms, res):
            try:
                if not row:
                    # key missing or no fields
                    self.cache.news_by_symbol.pop(sym, None)
                    continue
                # redis-py returns list[str|None]
                d = dict(zip(NEWS_HASH_FIELDS, row))

                ref = _ensure_ref(str(d.get("ref") or ""))

                nf = NewsFeatures(
                    ref=ref,
                    news_risk=_f(d.get("risk_ema"), 0.0),
                    surprise_score=_f(d.get("surprise_ema"), 0.0),
                    news_grade_id=_i(d.get("news_grade_id"), 0),
                    tags_mask=_i(d.get("tags_mask"), 0),
                    primary_tag_id=_i(d.get("primary_tag_id"), 0),
                    confidence=_f(d.get("confidence"), 0.0),
                    horizon_sec=_i(d.get("horizon_sec"), 0),
                    asof_ts_ms=_i(d.get("asof_ts_ms"), 0),
                    # calendar fields filled by ShadowCache.get() merge
                    event_tminus_sec=-1,
                    event_grade_id=0,
                )

                # If Redis returned no asof_ts_ms, still keep; staleness check will ignore.
                self.cache.news_by_symbol[sym] = nf
            except Exception:
                # Fail-open: drop this symbol
                self.cache.news_by_symbol.pop(sym, None)

        self.cache.last_refresh_ms = now_ms

    def refresh_calendar_once(self) -> None:
        # Assets are few (crypto/forex/metals). Use same ttl.
        assets = self.cache.active_assets(ttl_ms=self.cfg.symbol_interest_ttl_ms)
        if not assets:
            self.cache.last_calendar_refresh_ms = _now_ms()
            return

        pipe = self.r.pipeline(transaction=False)
        for ac in assets:
            key = f"{self.cal_key_prefix}{ac}"
            pipe.hgetall(key)
        res = pipe.execute()

        now_ms = _now_ms()
        for ac, h in zip(assets, res):
            try:
                if not h:
                    self.cache.cal_by_asset.pop(ac, None)
                    continue
                tminus = _i(h.get("event_tminus_sec"), -1)
                grade = _i(h.get("event_grade_id"), 0)
                self.cache.cal_by_asset[ac] = (tminus, grade, now_ms)
            except Exception:
                self.cache.cal_by_asset.pop(ac, None)

        self.cache.last_calendar_refresh_ms = now_ms
