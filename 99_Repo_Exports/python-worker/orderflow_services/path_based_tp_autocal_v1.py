from __future__ import annotations

"""path_based_tp_autocal_v1.py — distribution-aware TP1 autocal (Plan 3.3).

Pipeline (every PATH_TP_AUTOCAL_INTERVAL sec):
  1) XREVRANGE last `WINDOW_HOURS` of `trades:closed`.
  2) parse_trade_for_cdf — extract (symbol, regime, direction, mfe_r, winner).
  3) build_cdf_buckets — hierarchy of buckets with MFE_R CDFs.
  4) build_recommendations — for each bucket, TP1_R = p50(MFE_R | winner).
  5) publish `autocal:path_tp:state`:
       {
         "ts_ms": ...,
         "window_hours": 72,
         "n_trades": 1500,
         "quantile": 0.5,
         "min_winners": 30,
         "buckets": {
           "BTCUSDT|range|LONG":    {"tp1_r": 0.42, "p50": 0.42, "n_winners": 110, "n_total": 230, "passes": 1, "enforce": 0},
           "*|*|LONG":              {...},
           "*|*|*":                 {"tp1_r": 0.50, "passes": 1, "enforce": 0},
           ...
         },
         "sig": "<hmac-sha256-hex>"   # optional
       }

Auto-promote (enforce flag per bucket):
  Each bucket needs `dwell_h` consecutive passing windows + `PATH_TP_AUTOCAL_ENFORCE=1`
  before its `enforce` flips to 1. Reader treats `enforce=0` as shadow / no override.

ENV:
  PATH_TP_AUTOCAL_ENABLE        0           — service main loop gate
  PATH_TP_AUTOCAL_ENFORCE       0           — allow per-bucket auto-promote enforce=1
  PATH_TP_AUTOCAL_INTERVAL      900         — sec (15 min default)
  PATH_TP_AUTOCAL_WINDOW_H      168.0       — analysis window (7d default; tail-sensitive)
  PATH_TP_AUTOCAL_MIN_WINNERS   30          — min winners per bucket
  PATH_TP_AUTOCAL_QUANTILE      0.5         — MFE_R percentile for TP1
  PATH_TP_AUTOCAL_TP1_MIN_R     0.20        — clip floor
  PATH_TP_AUTOCAL_TP1_MAX_R     1.50        — clip ceiling
  PATH_TP_AUTOCAL_DWELL_H       24.0        — consecutive passing hours for enforce
  PATH_TP_AUTOCAL_INCLUDE_VIRTUAL 1         — include is_virtual=1 trades
  PATH_TP_AUTOCAL_HMAC_SECRET   ""          — optional; falls back to RECS/LAYERS secrets
  PATH_TP_AUTOCAL_PROM_PORT     9862
  PATH_TP_AUTOCAL_STREAM        trades:closed
  PATH_TP_AUTOCAL_REDIS_URL     redis://redis-worker-1:6379/0

Reader: `services/path_based_tp_runtime_overrides.py`.
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

from core.path_based_tp_cdf import (
    ALL,
    BucketKey,
    build_cdf_buckets,
    build_recommendations,
    parse_trade_for_cdf,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [path-tp-autocal] %(levelname)s %(message)s",
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
    return raw.strip().lower() in ("1", "true", "yes", "on")


@dataclass
class Cfg:
    enable: bool
    enforce: bool
    interval_sec: int
    window_h: float
    min_winners: int
    quantile: float
    tp1_r_min: float
    tp1_r_max: float
    dwell_h: float
    include_virtual: bool
    hmac_secret: str
    prom_port: int
    stream: str
    redis_url: str


def load_cfg() -> Cfg:
    return Cfg(
        enable          = _env_bool("PATH_TP_AUTOCAL_ENABLE", False),
        enforce         = _env_bool("PATH_TP_AUTOCAL_ENFORCE", False),
        interval_sec    = _env_int("PATH_TP_AUTOCAL_INTERVAL", 900),
        window_h        = _env_float("PATH_TP_AUTOCAL_WINDOW_H", 168.0),
        min_winners     = _env_int("PATH_TP_AUTOCAL_MIN_WINNERS", 30),
        quantile        = _env_float("PATH_TP_AUTOCAL_QUANTILE", 0.5),
        tp1_r_min       = _env_float("PATH_TP_AUTOCAL_TP1_MIN_R", 0.20),
        tp1_r_max       = _env_float("PATH_TP_AUTOCAL_TP1_MAX_R", 1.50),
        dwell_h         = _env_float("PATH_TP_AUTOCAL_DWELL_H", 24.0),
        include_virtual = _env_bool("PATH_TP_AUTOCAL_INCLUDE_VIRTUAL", True),
        hmac_secret     = (_env("PATH_TP_AUTOCAL_HMAC_SECRET", "")
                           or _env("RECS_HMAC_SECRET", "")
                           or _env("LAYERS_CAL_HMAC_SECRET", "")),
        prom_port       = _env_int("PATH_TP_AUTOCAL_PROM_PORT", 9862),
        stream          = _env("PATH_TP_AUTOCAL_STREAM", "trades:closed"),
        redis_url       = _env("PATH_TP_AUTOCAL_REDIS_URL",
                               "redis://redis-worker-1:6379/0"),
    )


STATE_KEY = "autocal:path_tp:state"


# Prometheus
g_up         = Gauge("path_tp_autocal_up", "service loop up")
g_last_run   = Gauge("path_tp_autocal_last_run_ts", "last run unix ts")
g_n_trades   = Gauge("path_tp_autocal_n_trades", "trades evaluated last cycle")
g_n_buckets  = Gauge("path_tp_autocal_n_buckets", "buckets in last cycle")
g_n_passing  = Gauge("path_tp_autocal_n_passing", "buckets that passed sanity")
g_n_enforced = Gauge("path_tp_autocal_n_enforced", "buckets with enforce=1")
g_global_tp1 = Gauge("path_tp_autocal_global_tp1_r", "global bucket tp1_r")
c_publishes  = Counter("path_tp_autocal_publishes_total", "publishes", ["outcome"])


def _hmac_sign(payload: dict, secret: str) -> str:
    canon = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    return hmac.new(secret.encode(), canon, hashlib.sha256).hexdigest()


def _load_prev_state(r: Any, state_key: str = STATE_KEY) -> dict[str, dict[str, Any]]:
    """Read previous bucket map to carry dwell tracking across runs."""
    try:
        raw = r.get(state_key)
        if not raw:
            return {}
        if isinstance(raw, (bytes, bytearray)):
            raw = raw.decode()
        data = json.loads(raw)
        return data.get("buckets") or {}
    except Exception:
        return {}


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
        # fields may have bytes keys depending on client config; normalize.
        norm = {
            (k.decode() if isinstance(k, (bytes, bytearray)) else str(k)):
            (v.decode() if isinstance(v, (bytes, bytearray)) else v)
            for k, v in fields.items()
        }
        parsed = parse_trade_for_cdf(norm)
        if parsed is not None:
            out.append(parsed)
    return out


def evaluate_window(
    parsed_trades: list[dict[str, Any]],
    cfg: Cfg,
    prev_buckets: dict[str, dict[str, Any]],
    now_ms: int,
) -> dict[str, dict[str, Any]]:
    """Compute per-bucket recommendation + dwell + enforce. Pure (no Redis)."""
    buckets = build_cdf_buckets(parsed_trades, include_virtual=cfg.include_virtual)
    recs = build_recommendations(
        buckets,
        quantile=cfg.quantile,
        min_winners=cfg.min_winners,
        tp1_r_min=cfg.tp1_r_min,
        tp1_r_max=cfg.tp1_r_max,
    )

    out: dict[str, dict[str, Any]] = {}
    for enc, rec in recs.items():
        d = rec.to_dict()
        prev = prev_buckets.get(enc) or {}
        prev_dwell_h = float(prev.get("dwell_h") or 0.0)
        prev_last_pass = int(prev.get("last_pass_ms") or 0)
        passes = int(d["passes"]) == 1
        if passes:
            delta_h = (now_ms - prev_last_pass) / 3_600_000.0 if prev_last_pass else 0.0
            # Cap growth at 2× interval/hour to absorb missed runs without
            # over-accumulating from a single tick.
            cap_h = (cfg.interval_sec / 3600.0) * 2.0
            new_dwell = prev_dwell_h + max(0.0, min(delta_h, cap_h))
            last_pass_ms = now_ms
        else:
            new_dwell = 0.0
            last_pass_ms = 0
        enforce = (
            cfg.enforce
            and passes
            and new_dwell >= cfg.dwell_h
        )
        d["dwell_h"] = round(new_dwell, 3)
        d["last_pass_ms"] = last_pass_ms
        d["enforce"] = int(enforce)
        out[enc] = d
    return out


def publish_state(
    r: Any,
    buckets: dict[str, dict[str, Any]],
    cfg: Cfg,
    n_trades: int,
    state_key: str = STATE_KEY,
) -> bool:
    payload: dict[str, Any] = {
        "ts_ms": int(time.time() * 1000),
        "window_hours": cfg.window_h,
        "n_trades": n_trades,
        "quantile": cfg.quantile,
        "min_winners": cfg.min_winners,
        "tp1_r_min": cfg.tp1_r_min,
        "tp1_r_max": cfg.tp1_r_max,
        "buckets": buckets,
    }
    if cfg.hmac_secret:
        payload["sig"] = _hmac_sign(payload, cfg.hmac_secret)
    try:
        r.set(state_key, json.dumps(payload), ex=cfg.interval_sec * 4)
        c_publishes.labels(outcome="ok").inc()
        return True
    except Exception as e:
        log.error("publish state failed: %s", e)
        c_publishes.labels(outcome="error").inc()
        return False


def run_once(r: Any, cfg: Cfg) -> dict[str, dict[str, Any]]:
    trades = _read_trades_window(r, cfg.stream, cfg.window_h)
    prev_buckets = _load_prev_state(r)
    now_ms = int(time.time() * 1000)
    buckets = evaluate_window(trades, cfg, prev_buckets, now_ms)
    publish_state(r, buckets, cfg, n_trades=len(trades))
    g_last_run.set(now_ms / 1000)
    g_n_trades.set(len(trades))
    g_n_buckets.set(len(buckets))
    g_n_passing.set(sum(1 for b in buckets.values() if int(b.get("passes", 0)) == 1))
    g_n_enforced.set(sum(1 for b in buckets.values() if int(b.get("enforce", 0)) == 1))
    glob = buckets.get(BucketKey(ALL, ALL, ALL).encode()) or {}
    g_global_tp1.set(float(glob.get("tp1_r") or 0.0))
    log.info(
        "path-tp autocal cycle: n_trades=%d buckets=%d passing=%d enforced=%d global_tp1_r=%.3f",
        len(trades), len(buckets),
        sum(1 for b in buckets.values() if int(b.get("passes", 0)) == 1),
        sum(1 for b in buckets.values() if int(b.get("enforce", 0)) == 1),
        float(glob.get("tp1_r") or 0.0),
    )
    return buckets


def main() -> int:
    cfg = load_cfg()
    if not cfg.enable:
        log.info("PATH_TP_AUTOCAL_ENABLE=0 — exiting")
        return 0
    try:
        start_http_server(cfg.prom_port)
    except Exception as e:
        log.warning("prom server start failed: %s", e)
    r = redis.from_url(cfg.redis_url, decode_responses=True)
    log.info(
        "path-tp autocal start: enforce=%d interval=%ds window=%.1fh min_winners=%d q=%.2f",
        int(cfg.enforce), cfg.interval_sec, cfg.window_h, cfg.min_winners, cfg.quantile,
    )
    while True:
        g_up.set(1)
        try:
            run_once(r, cfg)
        except Exception as e:
            log.exception("path-tp autocal cycle error: %s", e)
        time.sleep(cfg.interval_sec)


if __name__ == "__main__":
    raise SystemExit(main())
