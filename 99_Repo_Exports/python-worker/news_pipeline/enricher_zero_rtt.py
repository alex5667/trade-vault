from __future__ import annotations
from utils.time_utils import get_ny_time_millis

import os
import time
import threading
from dataclasses import dataclass
from typing import Any, Dict, Optional, Set, Tuple

# ВАЖНО:
# - tick-loop НЕ делает Redis calls вообще
# - Redis calls идут только в background thread
# - Redis client ДОЛЖЕН быть "fast" (socket_timeout ~ 20-100ms)

try:
    # если есть dq_flags — ставим маркеры деградации, иначе молча пропускаем
    from common.dq_flags import append_dq_flag  # type: ignore
except Exception:  # pragma: no cover
    append_dq_flag = None  # type: ignore

from contexts import NewsFeatures, OrderflowSignalContext  # ваш пакетный импорт


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


@dataclass(frozen=True, slots=True)
class _CacheStats:
    last_ok_ts_ms: int = 0
    last_cycle_ms: int = 0
    ok_cycles: int = 0
    err_cycles: int = 0
    last_err: str = ""


class NewsAggCache:
    """
    Zero-RTT cache for tick-loop.

    Background thread periodically:
      - reads news:agg:<symbol> (HASH)
      - reads calendar:agg:<asset_class> (HASH) if enabled
      - builds compact NewsFeatures snapshots in memory
    Tick-loop:
      - only reads snapshot (O(1)), no network calls.

    Safety:
      - fail-open (any error => keep previous snapshot)
      - bounded symbol set to avoid unbounded pipelines
      - micro-timeouts on Redis client
    """

    NEWS_FIELDS = (
        "ref"
        "risk_ema"
        "surprise_ema"
        "news_grade_id"
        "tags_mask"
        "primary_tag_id"
        "confidence"
        "horizon_sec"
        "asof_ts_ms"
    )

    CAL_FIELDS = (
        "event_tminus_sec"
        "event_grade_id"
    )

    def __init__(
        self
        *
        redis_fast
        poll_ms: int = 250
        max_symbols: int = 512
        enable_calendar: bool = True
        stale_warn_ms: int = 2_000,   # если snapshot слишком старый — dq flag
        stale_drop_ms: int = 30_000,  # если слишком старый — возвращаем None (fail-open)
    ) -> None:
        self.r = redis_fast
        self.poll_ms = int(poll_ms)
        self.max_symbols = int(max_symbols)
        self.enable_calendar = bool(enable_calendar)
        self.stale_warn_ms = int(stale_warn_ms)
        self.stale_drop_ms = int(stale_drop_ms)

        self._symbols: Set[str] = set(["GLOBAL"])
        self._asset_classes: Set[str] = set()

        # snapshot maps
        self._news_snap: Dict[str, NewsFeatures] = {}
        self._cal_snap: Dict[str, Tuple[int, int]] = {}  # asset_class -> (tminus, grade)

        self._stats = _CacheStats()
        self._stop = threading.Event()
        self._th: Optional[threading.Thread] = None

        # "circuit breaker" чтобы не молотить Redis при серии ошибок
        self._backoff_ms = 0

    def start(self) -> None:
        if self._th and self._th.is_alive():
            return
        self._stop.clear()
        self._th = threading.Thread(target=self._run, name="news-agg-cache", daemon=True)
        self._th.start()

    def stop(self, timeout: float = 1.0) -> None:
        self._stop.set()
        if self._th:
            self._th.join(timeout=timeout)

    def note_symbol(self, symbol: str, asset_class: str = "") -> None:
        """
        Called from tick-loop (cheap):
        - records active symbols/asset_class for next refresh cycle
        """
        s = (symbol or "GLOBAL").upper()
        if s:
            self._symbols.add(s)
            # keep bounded
            if len(self._symbols) > self.max_symbols:
                # deterministic pruning: drop arbitrary extras (fine for cache)
                # (в реале можно LRU, но это уже избыточно)
                while len(self._symbols) > self.max_symbols:
                    self._symbols.discard(next(iter(self._symbols - {"GLOBAL"}), "GLOBAL"))

        ac = (asset_class or "").strip().lower()
        if ac:
            self._asset_classes.add(ac)

    def stats(self) -> _CacheStats:
        return self._stats

    def get_features(self, symbol: str, asset_class: str = "") -> Optional[NewsFeatures]:
        """
        Tick-loop API: get compact NewsFeatures from snapshot.
        - never touches Redis
        - fail-open: may return None if stale/absent
        """
        now_ms = get_ny_time_millis()
        sym = (symbol or "GLOBAL").upper()

        nf = self._news_snap.get(sym) or self._news_snap.get("GLOBAL")
        if not nf:
            return None

        age_ms = now_ms - int(nf.asof_ts_ms or 0)
        if age_ms > self.stale_drop_ms:
            return None

        # calendar overlay (optional)
        if self.enable_calendar:
            ac = (asset_class or "").strip().lower()
            if ac:
                cal = self._cal_snap.get(ac)
                if cal:
                    tminus, grade = cal
                    # создаём новый NewsFeatures (frozen=True)
                    nf = NewsFeatures(
                        ref=nf.ref
                        news_risk=nf.news_risk
                        surprise_score=nf.surprise_score
                        news_grade_id=nf.news_grade_id
                        tags_mask=nf.tags_mask
                        primary_tag_id=nf.primary_tag_id
                        confidence=nf.confidence
                        horizon_sec=nf.horizon_sec
                        asof_ts_ms=nf.asof_ts_ms
                        event_tminus_sec=tminus
                        event_grade_id=grade
                    )
        return nf

    def refresh_once(self) -> None:
        """
        One refresh cycle. Exposed for tests.
        """
        now_ms = get_ny_time_millis()

        # backoff if needed
        if self._backoff_ms > 0:
            time.sleep(self._backoff_ms / 1000.0)

        symbols = list(self._symbols)[: self.max_symbols]
        asset_classes = list(self._asset_classes)[:64]  # календарь обычно маленький

        t0 = time.perf_counter()

        try:
            pipe = self.r.pipeline(transaction=False)

            # hmget существенно дешевле hgetall
            for s in symbols:
                pipe.hmget(f"news:agg:{s}", self.NEWS_FIELDS)

            if self.enable_calendar:
                for ac in asset_classes:
                    pipe.hmget(f"calendar:agg:{ac}", self.CAL_FIELDS)

            res = pipe.execute()

            # parse
            new_news: Dict[str, NewsFeatures] = {}
            idx = 0
            for s in symbols:
                vals = res[idx] or []
                idx += 1
                if not vals or len(vals) < len(self.NEWS_FIELDS):
                    continue

                m = dict(zip(self.NEWS_FIELDS, vals))
                nf = NewsFeatures(
                    ref=str(m.get("ref") or "")
                    news_risk=_f(m.get("risk_ema"), 0.0)
                    surprise_score=_f(m.get("surprise_ema"), 0.0)
                    news_grade_id=_i(m.get("news_grade_id"), 0)
                    tags_mask=_i(m.get("tags_mask"), 0)
                    primary_tag_id=_i(m.get("primary_tag_id"), 0)
                    confidence=_f(m.get("confidence"), 0.0)
                    horizon_sec=_i(m.get("horizon_sec"), 0)
                    asof_ts_ms=_i(m.get("asof_ts_ms"), 0)
                )
                new_news[s] = nf

            new_cal: Dict[str, Tuple[int, int]] = {}
            if self.enable_calendar:
                for ac in asset_classes:
                    vals = res[idx] if idx < len(res) else []
                    idx += 1
                    if not vals or len(vals) < len(self.CAL_FIELDS):
                        continue
                    m = dict(zip(self.CAL_FIELDS, vals))
                    new_cal[ac] = (_i(m.get("event_tminus_sec"), -1), _i(m.get("event_grade_id"), 0))

            # atomic-ish swap (pointer swap under GIL)
            self._news_snap = new_news or self._news_snap
            self._cal_snap = new_cal or self._cal_snap

            cycle_ms = int((time.perf_counter() - t0) * 1000)
            self._stats = _CacheStats(
                last_ok_ts_ms=now_ms
                last_cycle_ms=cycle_ms
                ok_cycles=self._stats.ok_cycles + 1
                err_cycles=self._stats.err_cycles
                last_err=""
            )
            self._backoff_ms = 0

        except Exception as e:
            cycle_ms = int((time.perf_counter() - t0) * 1000)
            # exponential backoff up to ~2s
            self._backoff_ms = min(2000, max(50, self._backoff_ms * 2 or 50))
            self._stats = _CacheStats(
                last_ok_ts_ms=self._stats.last_ok_ts_ms
                last_cycle_ms=cycle_ms
                ok_cycles=self._stats.ok_cycles
                err_cycles=self._stats.err_cycles + 1
                last_err=str(e)[:256]
            )

    def _run(self) -> None:
        while not self._stop.is_set():
            self.refresh_once()
            time.sleep(max(0.01, self.poll_ms / 1000.0))


class NewsEnricherZeroRTT:
    """
    Drop-in replacement for NewsEnricherSync:
    - attach() is constant-time, no Redis calls
    - uses NewsAggCache background refresh
    """

    def __init__(self, *, cache: NewsAggCache) -> None:
        self.cache = cache

    def attach(self, ctx: OrderflowSignalContext, *, asset_class: str = "") -> None:
        try:
            sym = (ctx.symbol or "GLOBAL").upper()
            ac = (asset_class or getattr(ctx, "asset_class", "") or "").strip().lower()

            # record symbol for next refresh cycle (cheap)
            self.cache.note_symbol(sym, ac)

            nf = self.cache.get_features(sym, ac)
            if nf is None:
                # dq markers (optional)
                if append_dq_flag:
                    try:
                        append_dq_flag(ctx, "news_cache_miss")
                    except Exception:
                        pass
                ctx.news = None
                return

            # staleness warning
            now_ms = get_ny_time_millis()
            age_ms = now_ms - int(nf.asof_ts_ms or 0)
            if age_ms > self.cache.stale_warn_ms and append_dq_flag:
                try:
                    append_dq_flag(ctx, "news_cache_stale")
                except Exception:
                    pass

            ctx.news = nf

        except Exception:
            # fail-open
            try:
                ctx.news = None
            except Exception:
                pass
