from __future__ import annotations

import logging
import threading
import time
from typing import Any

from common.dq_flags import append_dq_flag
from contexts import NewsFeatures, OrderflowSignalContext
from utils.time_utils import get_ny_time_millis

log = logging.getLogger("news_enricher_shadow")

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

class NewsEnricherShadow:
    """
    Tick-loop safe enricher:
      - attach(): zero IO (только память)
      - refresh выполняется в фоне отдельным потоком (fast redis client)
      - fail-open: любая ошибка -> ctx.news=None + dq-flag
    """

    def __init__(
        self,
        *,
        redis,
        refresh_interval_ms: int = 250,      # фон обновляет батчами
        cache_ttl_ms: int = 1500,            # сколько держим в памяти (per symbol)
        max_batch: int = 256,
    ) -> None:
        self.r = redis
        self.refresh_interval_ms = int(refresh_interval_ms)
        self.cache_ttl_ms = int(cache_ttl_ms)
        self.max_batch = int(max_batch)

        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._thr: threading.Thread | None = None

        # ключ запроса: (symbol, asset_class)
        self._wanted: set[tuple[str, str]] = set()

        # кэш: (symbol, asset_class) -> (ts_ms, NewsFeatures)
        self._cache: dict[tuple[str, str], tuple[int, NewsFeatures]] = {}

    def start(self) -> None:
        if self._thr and self._thr.is_alive():
            return
        self._stop.clear()
        self._thr = threading.Thread(target=self._loop, name="news_enricher_shadow", daemon=True)
        self._thr.start()

    def stop(self) -> None:
        self._stop.set()
        th = self._thr
        if th and th.is_alive():
            th.join(timeout=1.0)

    def attach(self, ctx: OrderflowSignalContext, *, asset_class: str = "crypto") -> None:
        """
        ZERO-IO attach: только память.
        Если кэш устарел/нет — возвращаем пустой NewsFeatures или None.
        """
        try:
            sym = (getattr(ctx, "symbol", "") or "GLOBAL").upper()
            ac = (asset_class or getattr(ctx, "asset_class", "") or "crypto").strip().lower()
            if not ac:
                ac = "crypto"

            now_ms = get_ny_time_millis()
            key = (sym, ac)

            # зарегистрировать потребность на обновление (фон подтянет)
            with self._lock:
                self._wanted.add(key)
                cached = self._cache.get(key)

            if cached and (now_ms - cached[0] <= self.cache_ttl_ms):
                ctx.news = cached[1]
                return

            # fail-open: если кэша нет — ставим пустые фичи
            ctx.news = NewsFeatures()
        except Exception:
            try:
                ctx.news = None
                append_dq_flag(ctx, "news_enricher_attach_failed")
            except Exception:
                pass

    def _loop(self) -> None:
        while not self._stop.is_set():
            try:
                time.sleep(self.refresh_interval_ms / 1000.0)
                self._refresh_once()
            except Exception:
                # никогда не падаем, просто следующий цикл
                continue

    def _refresh_once(self) -> None:
        now_ms = get_ny_time_millis()

        with self._lock:
            # We refresh everything we've been asked for recently.
            # In a real heavy system we might want to prune self._wanted.
            items = list(self._wanted)[: self.max_batch]

        if not items:
            return

        # 1 RTT: pipeline hgetall(news) + hgetall(calendar)
        pipe = self.r.pipeline(transaction=False)
        keys: list[tuple[str, str, str, str]] = []
        for (sym, ac) in items:
            news_key = f"news:agg:{sym}"
            cal_key = f"calendar:agg:{ac}"
            pipe.hgetall(news_key)
            pipe.hgetall(cal_key)
            keys.append((sym, ac, news_key, cal_key))

        res = pipe.execute()

        # распаковываем попарно
        idx = 0
        updates: dict[tuple[str, str], tuple[int, NewsFeatures]] = {}
        for sym, ac, _, _ in keys:
            news = res[idx] or {}
            cal = res[idx + 1] or {}
            idx += 2

            # ref: normalize UID to full key
            ref = (news.get("ref", "") or "")
            if ref and not ref.startswith("news:analysis:"):
                ref = f"news:analysis:{ref}"

            nf = NewsFeatures(
                ref=ref,
                news_risk=_f(news.get("risk_ema", 0.0)),
                surprise_score=_f(news.get("surprise_ema", 0.0)),
                news_grade_id=_i(news.get("news_grade_id", 0)),
                tags_mask=_i(news.get("tags_mask", 0)) & ((1 << 64) - 1),
                primary_tag_id=_i(news.get("primary_tag_id", 0)),
                confidence=_f(news.get("confidence", 0.0)),
                horizon_sec=_i(news.get("horizon_sec", 0)),
                asof_ts_ms=_i(news.get("asof_ts_ms", 0)),
                event_tminus_sec=_i(cal.get("event_tminus_sec", -1), -1),
                event_grade_id=_i(cal.get("event_grade_id", 0)),
            )
            updates[(sym, ac)] = (now_ms, nf)

        with self._lock:
            self._cache.update(updates)
            # Prune old wanted items? Let's keep it simple for now as per instructions.
            # In production we might want: self._wanted = {k for k in self._wanted if ...}
