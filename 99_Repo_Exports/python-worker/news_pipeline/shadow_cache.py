"""news_pipeline.shadow_cache

Shadow cache for news/calendar enrichment.

Key property: tick-loop performs **zero Redis I/O**.
- Tick-loop: register interest (dict write) + read cached features (dict read)
- Background thread: refreshes compact hashes from Redis

Fail-open: any refresher / Redis error results in missing cache entries.

Redis keys (expected)
---------------------
- news:agg:<SYMBOL> (HASH) fields:
    ref, risk_ema, surprise_ema, news_grade_id, tags_mask, primary_tag_id,
    confidence, horizon_sec, asof_ts_ms
- calendar:agg:<asset_class> (HASH) fields (minimal):
    event_tminus_sec, event_grade_id, updated_ts_ms
"""

from __future__ import annotations
from utils.time_utils import get_ny_time_millis

import time
import threading
import logging
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Tuple

try:
    # Prefer project helper if present
    from common.dq_flags import append_dq_flag  # type: ignore
except Exception:  # pragma: no cover
    append_dq_flag = None  # type: ignore

from contexts import NewsFeatures

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

# Calendar: keep it fixed-width as well.
CAL_HASH_FIELDS: Tuple[str, ...] = (
    "event_tminus_sec",
    "event_grade_id",
    "updated_ts_ms",
    # optional fields (ignored by cache.get merge but useful for debugging)
    "next_ts_ms",
    "event_ref",
)


def _now_ms() -> int:
    return get_ny_time_millis()


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


def normalize_asset_class(ac: str) -> str:
    ac = (ac or "").strip().lower()
    if not ac:
        return ""
    if ac in ("fx", "forex"):
        return "forex"
    if ac in ("metal", "metals"):
        return "metals"
    if ac in ("crypto", "cryptos"):
        return "crypto"
    if ac in ("macro",):
        return "macro"
    return ac


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

    # Fallback: store list[str] if present.
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

    # How long we keep symbol/asset in the "hot" set after seen in tick-loop.
    symbol_interest_ttl_ms: int = 30_000
    asset_interest_ttl_ms: int = 60_000

    # Hard caps to bound Redis work per refresh.
    max_symbols_per_refresh: int = 256
    max_assets_per_refresh: int = 8

    # If cached features are older than this, drop them (fail-open).
    # 0 disables staleness drop.
    max_feature_age_ms: int = 5 * 60_000

    # Background thread should not spam logs.
    log_every_n_errors: int = 50


class ShadowCache:
    """Thread-safe-ish cache holder.

    Tick-loop only does dict.get / dict.set (no iteration).
    Refresher thread updates keys.

    Also includes an optional micro-cache to avoid per-tick allocations when
    merging calendar fields into NewsFeatures.
    """

    def __init__(self, *, per_symbol_cache_ms: int = 1500, max_age_ms: int = 300_000) -> None:
        self.news_by_symbol: Dict[str, NewsFeatures] = {}
        self.cal_by_asset: Dict[str, Tuple[int, int, int]] = {}
        # cal tuple: (event_tminus_sec, event_grade_id, updated_ts_ms)

        # Interest sets (updated by tick-loop).
        self._seen_symbol_ms: Dict[str, int] = {}
        self._seen_asset_ms: Dict[str, int] = {}

        # Diagnostics
        self.last_refresh_ms: int = 0
        self.last_calendar_refresh_ms: int = 0

        # Micro-cache for merged (symbol, asset_class) -> NewsFeatures
        self._per_symbol_cache_ms = max(0, int(per_symbol_cache_ms))
        self._max_age_ms = max(0, int(max_age_ms))
        self._merged_cache: Dict[Tuple[str, str], Tuple[int, Tuple[Any, ...], Tuple[int, int, int], NewsFeatures]] = {}

    # --- tick-loop API (no Redis, constant time) ---

    def mark_seen(self, symbol: str, asset_class: str = "") -> None:
        now = _now_ms()
        sym = (symbol or "GLOBAL").upper()
        self._seen_symbol_ms[sym] = now
        ac = normalize_asset_class(asset_class)
        if ac:
            self._seen_asset_ms[ac] = now

    def is_stale(self, nf: NewsFeatures, *, max_age_ms: Optional[int] = None) -> bool:
        age_limit = self._max_age_ms if max_age_ms is None else max(0, int(max_age_ms))
        if age_limit <= 0:
            return False
        if int(getattr(nf, "asof_ts_ms", 0) or 0) <= 0:
            return False
        age = _now_ms() - int(nf.asof_ts_ms)
        return age > age_limit

    def get(self, symbol: str, asset_class: str = "", *, max_age_ms: int = 0) -> Optional[NewsFeatures]:
        """Get merged NewsFeatures (news + calendar fields).

        max_age_ms:
            Optional staleness drop override. If 0, uses self._max_age_ms.
        """
        sym = (symbol or "GLOBAL").upper()
        nf = self.news_by_symbol.get(sym)
        if nf is None:
            return None

        # Staleness drop
        age_limit = max(0, int(max_age_ms)) if max_age_ms else self._max_age_ms
        if age_limit > 0 and int(nf.asof_ts_ms) > 0:
            age = _now_ms() - int(nf.asof_ts_ms)
            if age > age_limit:
                return None

        ac = normalize_asset_class(asset_class)
        if not ac:
            return nf

        cal = self.cal_by_asset.get(ac)
        if not cal:
            return nf

        event_tminus_sec, event_grade_id, updated_ts_ms = cal

        # Micro-cache: avoid per-tick allocation of merged NewsFeatures.
        if self._per_symbol_cache_ms > 0:
            key = (sym, ac)
            now = _now_ms()
            entry = self._merged_cache.get(key)
            sig_news = (
                nf.ref,
                int(nf.asof_ts_ms),
                float(nf.news_risk),
                float(nf.surprise_score),
                int(nf.news_grade_id),
                int(nf.tags_mask),
                int(nf.primary_tag_id),
                float(nf.confidence),
                int(nf.horizon_sec),
            )
            if entry is not None:
                exp_ms, old_sig_news, old_cal, old_nf = entry
                if now <= exp_ms and old_sig_news == sig_news and old_cal == cal:
                    return old_nf

            merged = NewsFeatures(
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
            self._merged_cache[key] = (now + self._per_symbol_cache_ms, sig_news, cal, merged)
            return merged

        # No micro-cache
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
        # Copy items to avoid RuntimeError if dict resizes
        for sym, ts in list(self._seen_symbol_ms.items()):
            if now - ts <= ttl_ms:
                out.append((ts, sym))
        out.sort(reverse=True)
        lim = max(0, int(limit))
        return [sym for _ts, sym in out[:lim]]

    def active_assets(self, *, ttl_ms: int, limit: int) -> List[str]:
        now = _now_ms()
        out: List[Tuple[int, str]] = []
        for ac, ts in list(self._seen_asset_ms.items()):
            if now - ts <= ttl_ms:
                out.append((ts, ac))
        out.sort(reverse=True)
        lim = max(0, int(limit))
        return [ac for _ts, ac in out[:lim]]


class ShadowRefresher:
    """Background refresher that pulls compact aggregates from Redis.

    It is intentionally not async: separate thread avoids interfering with
    tick-loop and keeps bounded latency.

    Redis client requirements:
    - decode_responses=True (strings)
    - short timeouts (socket_timeout <= 50ms recommended)
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

    # --- public API ---

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
                self._t.join(timeout=float(join_timeout))
        except Exception:
            pass

    def is_alive(self) -> bool:
        try:
            return bool(self._t and self._t.is_alive())
        except Exception:
            return False

    # Backward-compatible name used by some enrichers
    def register_interest(self, *, symbol: str, asset_class: str = "") -> None:
        self.cache.mark_seen(symbol=symbol, asset_class=asset_class)

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
                time.sleep(0.25)

    # --- single-iteration refresh (unit-test friendly) ---

    def refresh_news_once(self) -> None:
        syms = self.cache.active_symbols(
            ttl_ms=int(self.cfg.symbol_interest_ttl_ms),
            limit=int(self.cfg.max_symbols_per_refresh),
        )
        if not syms:
            self.cache.last_refresh_ms = _now_ms()
            return

        pipe = self.r.pipeline(transaction=False)
        for sym in syms:
            key = f"{self.news_key_prefix}{sym}"
            pipe.hmget(key, *NEWS_HASH_FIELDS)
        res = pipe.execute()

        now_ms = _now_ms()
        for sym, row in zip(syms, res):
            try:
                if not row:
                    self.cache.news_by_symbol.pop(sym, None)
                    continue
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

                self.cache.news_by_symbol[sym] = nf
            except Exception:
                self.cache.news_by_symbol.pop(sym, None)

        self.cache.last_refresh_ms = now_ms

    def refresh_calendar_once(self) -> None:
        assets = self.cache.active_assets(
            ttl_ms=int(self.cfg.asset_interest_ttl_ms),
            limit=int(self.cfg.max_assets_per_refresh),
        )
        if not assets:
            self.cache.last_calendar_refresh_ms = _now_ms()
            return

        pipe = self.r.pipeline(transaction=False)
        for ac in assets:
            key = f"{self.cal_key_prefix}{ac}"
            pipe.hmget(key, *CAL_HASH_FIELDS)
        res = pipe.execute()

        now_ms = _now_ms()
        for ac, row in zip(assets, res):
            try:
                if not row:
                    self.cache.cal_by_asset.pop(ac, None)
                    continue
                d = dict(zip(CAL_HASH_FIELDS, row))
                tminus = _i(d.get("event_tminus_sec"), -1)
                grade = _i(d.get("event_grade_id"), 0)
                upd = _i(d.get("updated_ts_ms"), 0) or now_ms
                self.cache.cal_by_asset[normalize_asset_class(ac)] = (tminus, grade, upd)
            except Exception:
                self.cache.cal_by_asset.pop(ac, None)

        self.cache.last_calendar_refresh_ms = now_ms
