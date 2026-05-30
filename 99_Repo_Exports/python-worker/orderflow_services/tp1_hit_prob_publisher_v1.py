from __future__ import annotations

"""tp1_hit_prob_publisher_v1.py — periodic publisher for empirical P_hit(TP1_R)
curves consumed by core/adaptive_tp1_policy.py (Plan 3 Phase 2, 2026-05-29).

Pipeline (every TP1_PHIT_INTERVAL sec):
  1) XREVRANGE last `WINDOW_HOURS` of `trades:closed`.
  2) parse_trade_for_phit — extract (symbol, kind, regime, direction, mfe_r).
  3) build_phit_buckets — six-level fallback hierarchy.
  4) build_phit_recommendations — compute_phit_curve + sanity flags.
  5) publish `autocal:tp1_phit:state`:
       {
         "ts_ms": ...,
         "window_hours": 168,
         "n_trades": 1500,
         "grid": [0.65, 0.80, 1.00, 1.15, 1.30, 1.50],
         "min_samples": 200,
         "buckets": {
           "BTCUSDT|of|range|LONG": {
             "n_total": 380, "curve": {"0.65": 0.61, ...},
             "calibration_ok": 1, "passes": 1
           },
           "*|*|*|*": {...}
         },
         "sig": "<hmac-sha256-hex>"   # optional
       }

ENV:
  TP1_PHIT_PUBLISH_ENABLE      0        — service main loop gate
  TP1_PHIT_INTERVAL            900      — sec (15 min)
  TP1_PHIT_WINDOW_H            168.0    — analysis window (7d)
  TP1_PHIT_MIN_SAMPLES         200      — min trades per bucket for passes=1
  TP1_PHIT_GRID                "0.65,0.80,1.00,1.15,1.30,1.50"
  TP1_PHIT_INCLUDE_VIRTUAL     1        — include is_virtual=1 trades
  TP1_PHIT_HMAC_SECRET         ""       — optional; falls back to RECS/LAYERS secrets
  TP1_PHIT_PROM_PORT           9865
  TP1_PHIT_STREAM              trades:closed
  TP1_PHIT_REDIS_URL           redis://redis-worker-1:6379/0
  TP1_PHIT_STATE_KEY           autocal:tp1_phit:state

Reader: `services/tp1_hit_prob_reader.py`.
"""

import hashlib
import hmac
import json
import logging
import os
import time
from dataclasses import dataclass
from typing import Any

import redis  # type: ignore
from prometheus_client import Counter, Gauge, start_http_server  # type: ignore

from core.tp1_hit_prob_cdf import (
    ALL,
    BucketKey,
    build_phit_buckets,
    build_phit_recommendations,
    parse_trade_for_phit,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [tp1-phit-publisher] %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)


def _env(k: str, d: str = "") -> str:
    return os.environ.get(k, d)


def _env_int(k: str, d: int) -> int:
    try:
        return int(_env(k, str(d)))
    except Exception:
        return d


def _env_float(k: str, d: float) -> float:
    try:
        return float(_env(k, str(d)))
    except Exception:
        return d


def _env_bool(k: str, d: bool) -> bool:
    raw = _env(k, "")
    if not raw:
        return d
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _parse_grid_csv(raw: str) -> list[float]:
    out: list[float] = []
    for x in (raw or "").split(","):
        try:
            v = float(x.strip())
            if v > 0:
                out.append(v)
        except Exception:
            continue
    return sorted(set(out))


@dataclass
class Cfg:
    enable: bool
    interval_sec: int
    window_h: float
    min_samples: int
    grid: list[float]
    include_virtual: bool
    hmac_secret: str
    prom_port: int
    stream: str
    redis_url: str
    state_key: str


def load_cfg() -> Cfg:
    return Cfg(
        enable          = _env_bool("TP1_PHIT_PUBLISH_ENABLE", False),
        interval_sec    = _env_int("TP1_PHIT_INTERVAL", 900),
        window_h        = _env_float("TP1_PHIT_WINDOW_H", 168.0),
        min_samples     = _env_int("TP1_PHIT_MIN_SAMPLES", 200),
        grid            = _parse_grid_csv(_env("TP1_PHIT_GRID", "0.65,0.80,1.00,1.15,1.30,1.50")),
        include_virtual = _env_bool("TP1_PHIT_INCLUDE_VIRTUAL", True),
        hmac_secret     = (_env("TP1_PHIT_HMAC_SECRET", "")
                           or _env("RECS_HMAC_SECRET", "")
                           or _env("LAYERS_CAL_HMAC_SECRET", "")),
        prom_port       = _env_int("TP1_PHIT_PROM_PORT", 9865),
        stream          = _env("TP1_PHIT_STREAM", "trades:closed"),
        redis_url       = _env("TP1_PHIT_REDIS_URL", "redis://redis-worker-1:6379/0"),
        state_key       = _env("TP1_PHIT_STATE_KEY", "autocal:tp1_phit:state"),
    )


# Prometheus
g_up         = Gauge("tp1_phit_publisher_up", "service loop up")
g_last_run   = Gauge("tp1_phit_publisher_last_run_ts", "last run unix ts")
g_n_trades   = Gauge("tp1_phit_publisher_n_trades", "trades evaluated last cycle")
g_n_buckets  = Gauge("tp1_phit_publisher_n_buckets", "buckets in last cycle")
g_n_passing  = Gauge("tp1_phit_publisher_n_passing", "buckets passing calibration")
g_global_n   = Gauge("tp1_phit_publisher_global_n_total", "global bucket n_total")
c_publishes  = Counter("tp1_phit_publisher_publishes_total", "publishes", ["outcome"])


def _hmac_sign(payload: dict, secret: str) -> str:
    canon = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    return hmac.new(secret.encode(), canon, hashlib.sha256).hexdigest()


def _read_trades_window(r: Any, stream: str, window_h: float) -> list[dict[str, Any]]:
    now_ms = int(time.time() * 1000)
    min_ms = now_ms - int(window_h * 3_600_000)
    try:
        entries = r.xrevrange(stream, max="+", min=str(min_ms), count=20_000)
    except Exception as e:
        log.warning("xrevrange %s failed: %s", stream, e)
        return []
    out: list[dict[str, Any]] = []
    for _eid, fields in entries or []:
        norm = {
            (k.decode() if isinstance(k, (bytes, bytearray)) else str(k)):
            (v.decode() if isinstance(v, (bytes, bytearray)) else v)
            for k, v in fields.items()
        }
        parsed = parse_trade_for_phit(norm)
        if parsed is not None:
            out.append(parsed)
    return out


def evaluate_window(parsed_trades: list[dict[str, Any]], cfg: Cfg) -> dict[str, dict[str, Any]]:
    """Pure: bucket + curve + flags. No Redis I/O."""
    buckets = build_phit_buckets(parsed_trades, include_virtual=cfg.include_virtual)
    return build_phit_recommendations(buckets, grid=cfg.grid, min_samples=cfg.min_samples)


def publish_state(r: Any, recs: dict[str, dict[str, Any]], cfg: Cfg, n_trades: int) -> bool:
    payload: dict[str, Any] = {
        "ts_ms": int(time.time() * 1000),
        "window_hours": cfg.window_h,
        "n_trades": n_trades,
        "grid": list(cfg.grid),
        "min_samples": cfg.min_samples,
        "buckets": recs,
    }
    if cfg.hmac_secret:
        payload["sig"] = _hmac_sign(payload, cfg.hmac_secret)
    try:
        r.set(cfg.state_key, json.dumps(payload), ex=cfg.interval_sec * 4)
        c_publishes.labels(outcome="ok").inc()
        return True
    except Exception as e:
        log.error("publish state failed: %s", e)
        c_publishes.labels(outcome="error").inc()
        return False


def run_once(r: Any, cfg: Cfg) -> dict[str, dict[str, Any]]:
    trades = _read_trades_window(r, cfg.stream, cfg.window_h)
    recs = evaluate_window(trades, cfg)
    publish_state(r, recs, cfg, n_trades=len(trades))
    now_s = int(time.time())
    g_last_run.set(now_s)
    g_n_trades.set(len(trades))
    g_n_buckets.set(len(recs))
    g_n_passing.set(sum(1 for b in recs.values() if int(b.get("passes", 0)) == 1))
    glob = recs.get(BucketKey(ALL, ALL, ALL, ALL).encode()) or {}
    g_global_n.set(int(glob.get("n_total") or 0))
    log.info(
        "tp1-phit cycle: n_trades=%d buckets=%d passing=%d global_n=%d",
        len(trades), len(recs),
        sum(1 for b in recs.values() if int(b.get("passes", 0)) == 1),
        int(glob.get("n_total") or 0),
    )
    return recs


def main() -> int:
    cfg = load_cfg()
    if not cfg.enable:
        log.info("TP1_PHIT_PUBLISH_ENABLE=0 — exiting")
        return 0
    if not cfg.grid:
        log.error("TP1_PHIT_GRID is empty — exiting")
        return 2
    try:
        start_http_server(cfg.prom_port)
    except Exception as e:
        log.warning("prom server start failed: %s", e)
    r = redis.from_url(cfg.redis_url, decode_responses=True)
    log.info(
        "tp1-phit publisher start: interval=%ds window=%.1fh min_samples=%d grid=%s",
        cfg.interval_sec, cfg.window_h, cfg.min_samples,
        ",".join(f"{x:.2f}" for x in cfg.grid),
    )
    while True:
        g_up.set(1)
        try:
            run_once(r, cfg)
        except Exception as e:
            log.exception("tp1-phit cycle error: %s", e)
        time.sleep(cfg.interval_sec)


if __name__ == "__main__":
    raise SystemExit(main())
