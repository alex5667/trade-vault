from __future__ import annotations

"""counter_trend_regime_calibrator_v1.py — data-driven counter-trend block list.

Решает проблему из audit_counter_trend_gate_root_cause_2026_05_30:
P0-fix 2026-05-26 настроил `OF_SOFT_PASS_SHORT_BLOCK_REGIMES` по гипотезе
("SHORT хуже в range/squeeze"), но реальные данные показали что главный leak —
SHORT × trending_bull (-13.41R/24h). Этот сервис автоматически пересчитывает
список заблокированных regime per (direction) на скользящем 7d окне.

Pipeline (every CT_CAL_INTERVAL_SEC):
  1) XREVRANGE last WINDOW_HOURS of `trades:closed`.
  2) Group by (direction, regime) — extract r_multiple.
  3) Build per-bucket stats: n_total, n_winners, avg_r, sum_r, win_rate.
  4) Recommend block если avg_r ≤ BLOCK_AVG_R_MAX AND n_total ≥ MIN_SAMPLES.
  5) Publish `autocal:counter_trend:state`:
      {
        "ts_ms": ...,
        "window_hours": 168,
        "n_trades": 1500,
        "min_samples": 30,
        "block_avg_r_max": -0.5,
        "buckets": {
          "SHORT|trending_bull": {"n_total":24, "avg_r":-0.56, "block":1, "passes":1, "dwell_h":3.5, "enforce":0},
          "LONG|trending_bear":  {...},
          ...
        },
        "short_block_regimes": ["trending_bull","expansion"],
        "long_block_regimes":  ["trending_bear"],
        "sig": "<hmac-sha256-hex>"
      }

Auto-promote per bucket: passes consecutive `dwell_h` hours + CT_CAL_ENFORCE=1.
Reader: `services/counter_trend_runtime_overrides.py`.

ENV:
  CT_CAL_ENABLE          0     — service main loop gate
  CT_CAL_ENFORCE         0     — allow per-bucket auto-promote enforce=1
  CT_CAL_INTERVAL_SEC    900   — sec (15 min default)
  CT_CAL_WINDOW_H        168.0 — analysis window (7d default)
  CT_CAL_MIN_SAMPLES     30    — min trades per (direction × regime)
  CT_CAL_BLOCK_AVG_R_MAX -0.5  — block if avg_r ≤ this value
  CT_CAL_DWELL_H         24.0  — consecutive passing hours for enforce
  CT_CAL_INCLUDE_VIRTUAL 1     — include is_virtual=1 trades
  CT_CAL_HMAC_SECRET     ""    — optional; falls back to RECS/LAYERS secrets
  CT_CAL_PROM_PORT       9870
  CT_CAL_STREAM          trades:closed
  CT_CAL_REDIS_URL       redis://redis-worker-1:6379/0
"""

import hashlib
import hmac
import json
import logging
import math
import os
import time
from dataclasses import dataclass
from typing import Any

import redis  # type: ignore
from prometheus_client import Counter, Gauge, start_http_server  # type: ignore

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [counter-trend-cal] %(levelname)s %(message)s",
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


STATE_KEY = "autocal:counter_trend:state"

# Defaults — fallback if neither ENV nor calibrator are warm
_REGIME_ALIASES = {
    "uptrend": "trending_bull",
    "trending_up": "trending_bull",
    "trending": "trending_bull",
    "downtrend": "trending_bear",
    "trending_down": "trending_bear",
    "mixed": "range",
}


@dataclass
class Cfg:
    enable: bool
    enforce: bool
    interval_sec: int
    window_h: float
    min_samples: int
    block_avg_r_max: float
    dwell_h: float
    include_virtual: bool
    hmac_secret: str
    prom_port: int
    stream: str
    redis_url: str


def load_cfg() -> Cfg:
    return Cfg(
        enable=_env_bool("CT_CAL_ENABLE", False),
        enforce=_env_bool("CT_CAL_ENFORCE", False),
        interval_sec=_env_int("CT_CAL_INTERVAL_SEC", 900),
        window_h=_env_float("CT_CAL_WINDOW_H", 168.0),
        min_samples=_env_int("CT_CAL_MIN_SAMPLES", 30),
        block_avg_r_max=_env_float("CT_CAL_BLOCK_AVG_R_MAX", -0.5),
        dwell_h=_env_float("CT_CAL_DWELL_H", 24.0),
        include_virtual=_env_bool("CT_CAL_INCLUDE_VIRTUAL", True),
        hmac_secret=(
            _env("CT_CAL_HMAC_SECRET", "")
            or _env("RECS_HMAC_SECRET", "")
            or _env("LAYERS_CAL_HMAC_SECRET", "")
        ),
        prom_port=_env_int("CT_CAL_PROM_PORT", 9870),
        stream=_env("CT_CAL_STREAM", "trades:closed"),
        redis_url=_env("CT_CAL_REDIS_URL", "redis://redis-worker-1:6379/0"),
    )


# Prometheus
g_up = Gauge("ct_cal_up", "service loop up")
g_last_run = Gauge("ct_cal_last_run_ts", "last run unix ts")
g_n_trades = Gauge("ct_cal_n_trades", "trades evaluated last cycle")
g_n_buckets = Gauge("ct_cal_n_buckets", "total (direction × regime) buckets last cycle")
g_n_blocking = Gauge("ct_cal_n_blocking", "buckets recommended for blocking")
g_n_enforced = Gauge("ct_cal_n_enforced", "buckets with enforce=1")
g_bucket_avg_r = Gauge("ct_cal_bucket_avg_r", "avg_r per bucket", ["direction", "regime"])
g_bucket_n = Gauge("ct_cal_bucket_n_total", "n_total per bucket", ["direction", "regime"])
g_short_block_size = Gauge("ct_cal_short_block_regimes_size", "size of SHORT block list")
g_long_block_size = Gauge("ct_cal_long_block_regimes_size", "size of LONG block list")
c_publishes = Counter("ct_cal_publishes_total", "publishes", ["outcome"])


def _hmac_sign(payload: dict, secret: str) -> str:
    canon = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    return hmac.new(secret.encode(), canon, hashlib.sha256).hexdigest()


def _normalize_direction(raw: Any) -> str:
    if not isinstance(raw, str):
        return ""
    s = raw.strip().upper()
    if s in {"LONG", "BUY"}:
        return "LONG"
    if s in {"SHORT", "SELL"}:
        return "SHORT"
    return ""


def _normalize_regime(raw: Any) -> str:
    if not isinstance(raw, str):
        return ""
    s = raw.strip().lower()
    if s in {"", "na", "unknown", "none"}:
        return ""
    return _REGIME_ALIASES.get(s, s)


def _parse_trade_for_ct(fields: dict[str, Any]) -> dict[str, Any] | None:
    """Extract (direction, regime, r_multiple) from a `trades:closed` entry.

    Returns None when essential fields are missing or invalid.
    """
    direction = _normalize_direction(fields.get("direction") or fields.get("side"))
    if not direction:
        return None
    regime = _normalize_regime(
        fields.get("entry_regime") or fields.get("regime"),
    )
    if not regime:
        return None
    try:
        r_raw = fields.get("r_multiple")
        if r_raw is None or r_raw == "":
            return None
        r = float(r_raw)
        if not math.isfinite(r):
            return None
    except Exception:
        return None
    is_virtual_raw = fields.get("is_virtual")
    is_virtual = False
    if isinstance(is_virtual_raw, str):
        is_virtual = is_virtual_raw.strip().lower() in {"1", "true", "yes"}
    elif isinstance(is_virtual_raw, (int, float, bool)):
        is_virtual = bool(is_virtual_raw)
    return {
        "direction": direction,
        "regime": regime,
        "r_multiple": r,
        "is_virtual": is_virtual,
    }


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
            for k, v in (fields or {}).items()
        }
        parsed = _parse_trade_for_ct(norm)
        if parsed is not None:
            out.append(parsed)
    return out


def aggregate_buckets(
    parsed_trades: list[dict[str, Any]],
    include_virtual: bool,
) -> dict[str, dict[str, Any]]:
    """Aggregate per (direction × regime). Pure function (testable)."""
    buckets: dict[str, dict[str, Any]] = {}
    for t in parsed_trades:
        if not include_virtual and t["is_virtual"]:
            continue
        key = f"{t['direction']}|{t['regime']}"
        b = buckets.setdefault(key, {"n_total": 0, "n_winners": 0, "sum_r": 0.0})
        b["n_total"] += 1
        b["sum_r"] += t["r_multiple"]
        if t["r_multiple"] > 0:
            b["n_winners"] += 1
    for k, b in buckets.items():
        n = max(1, int(b["n_total"]))
        b["avg_r"] = round(b["sum_r"] / n, 4)
        b["win_rate"] = round(b["n_winners"] / n, 4)
        b["sum_r"] = round(b["sum_r"], 4)
    return buckets


def evaluate_buckets(
    buckets: dict[str, dict[str, Any]],
    cfg: Cfg,
    prev_buckets: dict[str, dict[str, Any]],
    now_ms: int,
) -> dict[str, dict[str, Any]]:
    """Apply block decision + dwell tracking. Returns enriched buckets."""
    out: dict[str, dict[str, Any]] = {}
    for key, b in buckets.items():
        passes = (
            int(b["n_total"]) >= cfg.min_samples
            and float(b["avg_r"]) <= cfg.block_avg_r_max
        )
        prev = prev_buckets.get(key) or {}
        prev_dwell_h = float(prev.get("dwell_h") or 0.0)
        prev_last_pass = int(prev.get("last_pass_ms") or 0)
        if passes:
            delta_h = (
                (now_ms - prev_last_pass) / 3_600_000.0 if prev_last_pass else 0.0
            )
            cap_h = (cfg.interval_sec / 3600.0) * 2.0
            new_dwell = prev_dwell_h + max(0.0, min(delta_h, cap_h))
            last_pass_ms = now_ms
        else:
            new_dwell = 0.0
            last_pass_ms = 0
        enforce = cfg.enforce and passes and new_dwell >= cfg.dwell_h
        out[key] = {
            **b,
            "passes": int(passes),
            "block": int(passes),
            "dwell_h": round(new_dwell, 3),
            "last_pass_ms": last_pass_ms,
            "enforce": int(enforce),
        }
    return out


def build_block_lists(
    eval_buckets: dict[str, dict[str, Any]],
    require_enforce: bool = True,
) -> tuple[list[str], list[str]]:
    """Project enforced buckets into per-direction regime block lists."""
    short_set: set[str] = set()
    long_set: set[str] = set()
    for key, b in eval_buckets.items():
        if require_enforce and int(b.get("enforce", 0)) != 1:
            continue
        if not require_enforce and int(b.get("block", 0)) != 1:
            continue
        try:
            direction, regime = key.split("|", 1)
        except ValueError:
            continue
        if direction == "SHORT":
            short_set.add(regime)
        elif direction == "LONG":
            long_set.add(regime)
    return sorted(short_set), sorted(long_set)


def _load_prev_state(r: Any, state_key: str = STATE_KEY) -> dict[str, dict[str, Any]]:
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


def publish_state(
    r: Any,
    eval_buckets: dict[str, dict[str, Any]],
    cfg: Cfg,
    n_trades: int,
    state_key: str = STATE_KEY,
) -> bool:
    short_block, long_block = build_block_lists(eval_buckets, require_enforce=True)
    payload: dict[str, Any] = {
        "ts_ms": int(time.time() * 1000),
        "window_hours": cfg.window_h,
        "n_trades": n_trades,
        "min_samples": cfg.min_samples,
        "block_avg_r_max": cfg.block_avg_r_max,
        "dwell_h_required": cfg.dwell_h,
        "buckets": eval_buckets,
        "short_block_regimes": short_block,
        "long_block_regimes": long_block,
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
    raw_buckets = aggregate_buckets(trades, include_virtual=cfg.include_virtual)
    prev = _load_prev_state(r)
    now_ms = int(time.time() * 1000)
    eval_b = evaluate_buckets(raw_buckets, cfg, prev, now_ms)
    publish_state(r, eval_b, cfg, n_trades=len(trades))

    g_last_run.set(now_ms / 1000)
    g_n_trades.set(len(trades))
    g_n_buckets.set(len(eval_b))
    g_n_blocking.set(sum(1 for b in eval_b.values() if int(b.get("block", 0)) == 1))
    g_n_enforced.set(sum(1 for b in eval_b.values() if int(b.get("enforce", 0)) == 1))

    short_block, long_block = build_block_lists(eval_b, require_enforce=True)
    g_short_block_size.set(len(short_block))
    g_long_block_size.set(len(long_block))
    for key, b in eval_b.items():
        try:
            direction, regime = key.split("|", 1)
            g_bucket_avg_r.labels(direction=direction, regime=regime).set(float(b.get("avg_r", 0.0)))
            g_bucket_n.labels(direction=direction, regime=regime).set(int(b.get("n_total", 0)))
        except Exception:
            pass

    log.info(
        "counter-trend cal cycle: n_trades=%d buckets=%d blocking=%d enforced=%d "
        "short_block=%s long_block=%s",
        len(trades), len(eval_b),
        sum(1 for b in eval_b.values() if int(b.get("block", 0)) == 1),
        sum(1 for b in eval_b.values() if int(b.get("enforce", 0)) == 1),
        short_block, long_block,
    )
    return eval_b


def main() -> int:
    cfg = load_cfg()
    if not cfg.enable:
        log.info("CT_CAL_ENABLE=0 — exiting")
        return 0
    try:
        start_http_server(cfg.prom_port)
        log.info("prometheus on :%d", cfg.prom_port)
    except Exception as e:
        log.warning("prometheus server failed: %s", e)
    r = redis.from_url(cfg.redis_url, decode_responses=False)
    g_up.set(1)
    log.info(
        "counter-trend calibrator started: window=%.1fh interval=%ds min_n=%d block_avg_r_max=%.3f enforce=%s",
        cfg.window_h, cfg.interval_sec, cfg.min_samples, cfg.block_avg_r_max, cfg.enforce,
    )
    while True:
        try:
            run_once(r, cfg)
        except Exception as e:
            log.exception("cycle failed: %s", e)
        time.sleep(cfg.interval_sec)


if __name__ == "__main__":
    raise SystemExit(main())
