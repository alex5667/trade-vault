from __future__ import annotations
from utils.time_utils import get_ny_time_millis

import json
import logging
import os
import time
from typing import Dict, Iterable, List

try:
    import redis
except Exception:  # pragma: no cover
    redis = None  # type: ignore
from prometheus_client import Gauge, Counter, start_http_server

from services.orderflow.derivatives_context import from_json
from services.observability.latency_semconv import default_symbol_allowlist

logger = logging.getLogger("derivatives_context_exporter_v1")


g_up = Gauge("deriv_ctx_exporter_up", "Derivatives context exporter up")
g_last_ts_ms = Gauge("deriv_ctx_exporter_last_snapshot_ts_ms", "Last derivatives context snapshot ts_ms", ["symbol"])
g_age_ms = Gauge("deriv_ctx_exporter_snapshot_age_ms", "Age of derivatives context snapshot in ms", ["symbol"])
g_funding_z = Gauge("deriv_ctx_exporter_funding_rate_z", "Funding rate robust z-score", ["symbol"])
g_basis_bps = Gauge("deriv_ctx_exporter_basis_bps", "Basis bps", ["symbol"])
g_oi_usd = Gauge("deriv_ctx_exporter_oi_notional_usd", "OI notional USD", ["symbol"])
g_flag = Gauge("deriv_ctx_exporter_flag", "Derivatives context flags", ["symbol", "flag"])
c_errors = Counter("deriv_ctx_exporter_errors_total", "Exporter errors", ["where"])


class Exporter:
    def __init__(self) -> None:
        self.redis_url = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
        if redis is None:
            raise RuntimeError("redis package is required for derivatives_context_exporter_v1")
        self.r = redis.Redis.from_url(self.redis_url, decode_responses=True, socket_timeout=5.0, socket_connect_timeout=5.0)
        self.interval_s = float(os.getenv("DERIV_CTX_EXPORTER_INTERVAL_S", "15") or 15.0)
        self.port = int(os.getenv("DERIV_CTX_EXPORTER_PORT", "9831") or 9831)
        self.prefix = os.getenv("DERIV_CTX_PREFIX", "ctx:deriv:")
        self.allow = {s.strip().upper() for s in str(os.getenv("DERIV_CTX_METRICS_SYMBOLS", "")).split(",") if s.strip()}

    def _symbols(self) -> List[str]:
        # Use explicit allowlist if provided in env, else use the default system-wide allowlist.
        # This replaces r.scan_iter which was blocking the Redis Event Loop.
        if self.allow:
            return sorted(list(self.allow))
        
        return sorted(list(default_symbol_allowlist()))

    def scrape_once(self) -> None:
        now_ms = get_ny_time_millis()
        for sym in self._symbols():
            try:
                raw = self.r.get(f"{self.prefix}{sym}")
                snap = from_json(raw)
                if not snap:
                    continue
                label = sym if (not self.allow or sym in self.allow) else "__all__"
                g_last_ts_ms.labels(symbol=label).set(float(snap.ts_ms))
                g_age_ms.labels(symbol=label).set(max(0.0, float(now_ms - int(snap.ts_ms))))
                g_funding_z.labels(symbol=label).set(float(snap.funding_rate_z))
                g_basis_bps.labels(symbol=label).set(float(snap.basis_bps))
                g_oi_usd.labels(symbol=label).set(float(snap.oi_notional_usd))
                g_flag.labels(symbol=label, flag="funding_extreme").set(float(snap.funding_extreme))
                g_flag.labels(symbol=label, flag="basis_extreme").set(float(snap.basis_extreme))
                g_flag.labels(symbol=label, flag="oi_accel").set(float(snap.oi_accel))
            except Exception as exc:
                logger.exception("export fail for %s: %s", sym, exc)
                c_errors.labels(where="scrape_symbol").inc()

    def run(self) -> None:
        start_http_server(self.port)
        logger.info("derivatives context exporter listening on :%d", self.port)
        while True:
            try:
                g_up.set(1)
                self.scrape_once()
            except Exception as exc:
                logger.exception("export loop fail: %s", exc)
                c_errors.labels(where="loop").inc()
            time.sleep(self.interval_s)


def main() -> None:
    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"), format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
    Exporter().run()


if __name__ == "__main__":
    main()
