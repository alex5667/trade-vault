"""services.news_reco_reader.metrics

Minimal Prometheus metrics for trade-side news recommendations reader.

Design goals
------------
- Zero impact on hot-path: gate reads in-memory cache without IO.
- Metrics are best-effort: if prometheus_client is not installed, we expose
  no-op stubs (unit tests and minimal environments must not break).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional


class _NoopMetric:
    def inc(self, amount: float = 1.0) -> None:
        return

    def set(self, value: float) -> None:
        return

    def observe(self, value: float) -> None:
        return

    def labels(self, **kwargs: Any) -> "_NoopMetric":  # noqa: ANN401
        return self


def _mk_noop() -> _NoopMetric:
    return _NoopMetric()


@dataclass(frozen=True)
class NewsRecoMetrics:
    hits_total: Any
    miss_total: Any
    stale_total: Any
    update_total: Any
    parse_errors_total: Any
    redis_errors_total: Any

    symbols: Any
    last_ok_ts_ms: Any
    stale_seconds: Any
    lag_ms: Any


def build_metrics(prefix: str = "trade_news_reco_reader") -> NewsRecoMetrics:
    """Build Prometheus metrics (or no-op stubs)."""
    try:
        from prometheus_client import Counter, Gauge  # type: ignore
    except Exception:  # pragma: no cover
        return NewsRecoMetrics(
            hits_total=_mk_noop()
            miss_total=_mk_noop()
            stale_total=_mk_noop()
            update_total=_mk_noop()
            parse_errors_total=_mk_noop()
            redis_errors_total=_mk_noop()
            symbols=_mk_noop()
            last_ok_ts_ms=_mk_noop()
            stale_seconds=_mk_noop()
            lag_ms=_mk_noop()
        )

    hits_total = Counter(f"{prefix}_hits_total", "Cache hits (hot-path)")
    miss_total = Counter(f"{prefix}_miss_total", "Cache miss (hot-path)")
    stale_total = Counter(f"{prefix}_stale_total", "Reco became stale/expired")
    update_total = Counter(f"{prefix}_update_total", "Successful cache updates from Redis map")
    parse_errors_total = Counter(f"{prefix}_parse_errors_total", "JSON parse/validate errors")
    redis_errors_total = Counter(f"{prefix}_redis_errors_total", "Redis IO errors")

    symbols = Gauge(f"{prefix}_symbols", "Number of symbols in cache")
    last_ok_ts_ms = Gauge(f"{prefix}_last_ok_ts_ms", "Last successful refresh (epoch ms)")
    stale_seconds = Gauge(f"{prefix}_stale_seconds", "Seconds since last successful refresh")
    lag_ms = Gauge(f"{prefix}_lag_ms", "Lag between now and map.ts_ms (ms)")

    return NewsRecoMetrics(
        hits_total=hits_total
        miss_total=miss_total
        stale_total=stale_total
        update_total=update_total
        parse_errors_total=parse_errors_total
        redis_errors_total=redis_errors_total
        symbols=symbols
        last_ok_ts_ms=last_ok_ts_ms
        stale_seconds=stale_seconds
        lag_ms=lag_ms
    )
