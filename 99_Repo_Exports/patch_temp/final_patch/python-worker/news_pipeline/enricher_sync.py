from __future__ import annotations

import time
from typing import Any, Dict, Optional

from contexts import NewsFeatures, OrderflowSignalContext


def _safe_float(v: Any, default: float = 0.0) -> float:
    try:
        return float(v)
    except Exception:
        return default


def _safe_int(v: Any, default: int = 0) -> int:
    try:
        # allow "1.0"
        return int(float(v))
    except Exception:
        return default


def _append_dq_flag(ctx: Any, flag: str) -> None:
    """Fail-open telemetry helper.

    Many parts of the pipeline already keep `data_quality_flags: list[str]` on ctx.
    This helper is defensive: it never raises and never duplicates flags.
    """
    try:
        flags = getattr(ctx, "data_quality_flags", None)
        if flags is None:
            return
        if flag not in flags:
            flags.append(flag)
    except Exception:
        return


class NewsEnricherSync:
    """Attach compact news/calendar features to a signal context.

    Design constraints:
    - Tick-loop safe: only Redis reads, no network other than Redis
    - 1 RTT: Redis pipeline(transaction=False)
    - Fail-open: any failure just results in ctx.news = None + dq flag

    Redis keys:
      news:agg:<SYMBOL>       (HASH) - online aggregates from NewsFeatureStoreWorker
      calendar:agg:<scope>    (HASH) - next-event features from CalendarStoreWorker

    Notes on performance:
    - You SHOULD pass a dedicated fast Redis client (short socket_timeout) to this
      class, so degradation of Redis does not stall tick processing.
    """

    def __init__(self, *, redis, per_symbol_cache_ms: int = 1500) -> None:
        self.r = redis
        self.cache_ms = int(per_symbol_cache_ms)
        # sym -> (cached_at_ms, NewsFeatures)
        self._cache: Dict[str, tuple[int, NewsFeatures]] = {}

    def attach(self, ctx: OrderflowSignalContext, *, asset_class: str = "") -> None:
        """Populate ctx.news.

        asset_class controls which calendar scope we read:
          calendar:agg:<asset_class_lower>
        """
        try:
            sym = (getattr(ctx, "symbol", "") or "GLOBAL").upper()
            now_ms = int(time.time() * 1000)

            # Cheap hot-loop cache: avoids Redis hits on every tick burst.
            cached = self._cache.get(sym)
            if cached and (now_ms - cached[0] <= self.cache_ms):
                ctx.news = cached[1]
                return

            news_key = f"news:agg:{sym}"

            cal_scope = (asset_class or "").strip().lower()
            cal_key = f"calendar:agg:{cal_scope}" if cal_scope else ""

            pipe = self.r.pipeline(transaction=False)
            pipe.hgetall(news_key)
            if cal_key:
                pipe.hgetall(cal_key)

            res = pipe.execute()  # 1 RTT
            news = res[0] or {}
            cal = res[1] if cal_key and len(res) > 1 else {}

            # `ref` is expected to be a pointer to heavy JSON, not plain uid.
            # We accept both formats to stay backward compatible.
            ref = str(news.get("ref", "") or "")
            if ref and not ref.startswith("news:analysis:"):
                ref = f"news:analysis:{ref}"

            nf = NewsFeatures(
                ref=ref,
                news_risk=_safe_float(news.get("risk_ema", 0.0)),
                surprise_score=_safe_float(news.get("surprise_ema", 0.0)),
                news_grade_id=_safe_int(news.get("news_grade_id", 0)),
                tags_mask=_safe_int(news.get("tags_mask", 0)),
                primary_tag_id=_safe_int(news.get("primary_tag_id", 0)),
                confidence=_safe_float(news.get("confidence", 0.0)),
                horizon_sec=_safe_int(news.get("horizon_sec", 0)),
                asof_ts_ms=_safe_int(news.get("asof_ts_ms", 0)),
                event_tminus_sec=_safe_int(cal.get("event_tminus_sec", -1)) if cal else -1,
                event_grade_id=_safe_int(cal.get("event_grade_id", 0)) if cal else 0,
            )

            ctx.news = nf
            self._cache[sym] = (now_ms, nf)

        except Exception:
            _append_dq_flag(ctx, "news_enrich_fail_open")
            try:
                ctx.news = None
            except Exception:
                pass
