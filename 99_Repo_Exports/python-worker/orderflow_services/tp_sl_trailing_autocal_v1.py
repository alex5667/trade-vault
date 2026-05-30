from __future__ import annotations

"""tp_sl_trailing_autocal_v1.py — autocalibrator with auto-promote for TP/SL/trailing knobs.

Knobs (Plan 1.1, 2026-05-19):
  - tp1_target_r          (current default 0.0=disabled → recommend 0.5)
  - tp1_target_r_enforce  ("0"|"1") — enforce flag for TP1@0.5R
  - be_after_tp1_mode     ("OFF"|"SHADOW"|"ENFORCE")
  - partial_close_tp1_mode("OFF"|"SHADOW"|"ENFORCE")
  - arm_threshold_r       (current default 0.25 → confirms 0.25 vs 0.5)
  - atr_mult_rocket_v1    (current default 1.2 → recommend 1.0)
  - atr_mult_expansion_v1 (current default 1.5 → recommend 1.0)
  - atr_mult_rocket_v1_bear (current default 1.0)

Pipeline:
  1) Read last `WINDOW_HOURS` of trades:closed (XREVRANGE).
  2) Compute per-knob counterfactual lift_R per trade.
  3) Aggregate: mean lift, n_eligible, regression check (worst regime).
  4) If passes criteria (n_eligible >= MIN_TRADES, lift_R >= TARGET_LIFT_R,
     worst-regime delta >= -TOLERANCE_R), write recommendation to Redis.
  5) Auto-promote: if ENFORCE=1 AND has been in `dwell_hours` consecutive
     passing windows AND HMAC_SECRET set, mark `enforce=1` in published bundle.
     Else publish `enforce=0` (shadow).

Output:
  Redis key `autocal:tp_sl_trailing:state` — JSON:
    {
      "ts_ms": 1700000000000,
      "window_hours": 72,
      "n_trades": 850,
      "knobs": {
         "tp1_target_r":          {"value": 0.5,  "lift_r": 0.07, "n": 320, "enforce": 0},
         "atr_mult_rocket_v1":    {"value": 1.0,  "lift_r": 0.03, "n": 95,  "enforce": 0},
         ...
      },
      "sig": "<hmac-sha256-hex>"   # optional if HMAC_SECRET set
    }

ENV:
  TP_SL_TRAIL_AUTOCAL_ENABLE      0           — service main loop gate
  TP_SL_TRAIL_AUTOCAL_ENFORCE     0           — allow auto-promote enforce=1
  TP_SL_TRAIL_AUTOCAL_INTERVAL    600         — sec
  TP_SL_TRAIL_AUTOCAL_WINDOW_H    72.0        — analysis window
  TP_SL_TRAIL_AUTOCAL_MIN_TRADES  200         — min eligible trades per knob
  TP_SL_TRAIL_AUTOCAL_LIFT_R      0.05        — min mean lift_R to recommend
  TP_SL_TRAIL_AUTOCAL_TOL_R       0.10        — max worst-regime negative drift
  TP_SL_TRAIL_AUTOCAL_DWELL_H     24.0        — consecutive passing hours required for enforce
  TP_SL_TRAIL_AUTOCAL_HMAC_SECRET ""          — optional HMAC for bundle
  TP_SL_TRAIL_AUTOCAL_PROM_PORT   9861
  TP_SL_TRAIL_AUTOCAL_STREAM      trades:closed
  TP_SL_TRAIL_AUTOCAL_REDIS_URL   redis://redis-worker-1:6379/0

Reader: `services/tp_sl_trailing_runtime_overrides.py`.
"""

import hashlib
import hmac
import json
import logging
import math
import os
import time
from dataclasses import dataclass, field
from typing import Any

import redis
from prometheus_client import Counter, Gauge, start_http_server  # type: ignore

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [tp-sl-trail-autocal] %(levelname)s %(message)s",
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


# Knob metadata: name → (current_default, candidate_value, computer_fn_name)
KNOB_SPECS: dict[str, dict[str, Any]] = {
    "tp1_target_r":           {"candidate": 0.5,  "current": 0.0, "kind": "tp1_partial"},
    "atr_mult_rocket_v1":     {"candidate": 1.0,  "current": 1.2, "kind": "trail_mult"},
    "atr_mult_expansion_v1":  {"candidate": 1.0,  "current": 1.5, "kind": "trail_mult"},
    "atr_mult_rocket_v1_bear":{"candidate": 0.8,  "current": 1.0, "kind": "trail_mult"},
    "arm_threshold_r":        {"candidate": 0.25, "current": 0.5, "kind": "arm"},
    "be_after_tp1_mode":      {"candidate": "ENFORCE", "current": "ENFORCE", "kind": "be_partial"},
    "partial_close_tp1_mode": {"candidate": "ENFORCE", "current": "ENFORCE", "kind": "be_partial"},
}


@dataclass
class Cfg:
    enable: bool
    enforce: bool
    interval_sec: int
    window_h: float
    min_trades: int
    lift_r: float
    tol_r: float
    dwell_h: float
    hmac_secret: str
    prom_port: int
    stream: str
    redis_url: str


def load_cfg() -> Cfg:
    return Cfg(
        enable      = _env_bool("TP_SL_TRAIL_AUTOCAL_ENABLE", False),
        enforce     = _env_bool("TP_SL_TRAIL_AUTOCAL_ENFORCE", False),
        interval_sec= _env_int("TP_SL_TRAIL_AUTOCAL_INTERVAL", 600),
        window_h    = _env_float("TP_SL_TRAIL_AUTOCAL_WINDOW_H", 72.0),
        min_trades  = _env_int("TP_SL_TRAIL_AUTOCAL_MIN_TRADES", 200),
        lift_r      = _env_float("TP_SL_TRAIL_AUTOCAL_LIFT_R", 0.05),
        tol_r       = _env_float("TP_SL_TRAIL_AUTOCAL_TOL_R", 0.10),
        dwell_h     = _env_float("TP_SL_TRAIL_AUTOCAL_DWELL_H", 24.0),
        hmac_secret = _env("TP_SL_TRAIL_AUTOCAL_HMAC_SECRET", "")
                       or _env("RECS_HMAC_SECRET", "")
                       or _env("LAYERS_CAL_HMAC_SECRET", ""),
        prom_port   = _env_int("TP_SL_TRAIL_AUTOCAL_PROM_PORT", 9861),
        stream      = _env("TP_SL_TRAIL_AUTOCAL_STREAM", "trades:closed"),
        redis_url   = _env("TP_SL_TRAIL_AUTOCAL_REDIS_URL",
                            "redis://redis-worker-1:6379/0"),
    )


# Prometheus
g_up        = Gauge("tp_sl_trail_autocal_up", "service loop up")
g_last_run  = Gauge("tp_sl_trail_autocal_last_run_ts", "last run unix ts")
g_n_trades  = Gauge("tp_sl_trail_autocal_n_trades", "trades evaluated last cycle")
g_lift_r    = Gauge("tp_sl_trail_autocal_lift_r", "mean lift_R per knob", ["knob"])
g_n_elig    = Gauge("tp_sl_trail_autocal_n_eligible", "eligible trades per knob", ["knob"])
g_enforce   = Gauge("tp_sl_trail_autocal_enforce", "enforce flag per knob (0/1)", ["knob"])
g_dwell     = Gauge("tp_sl_trail_autocal_dwell_h", "consecutive passing hours", ["knob"])
c_publishes = Counter("tp_sl_trail_autocal_publishes_total", "state publishes", ["outcome"])


def _hmac_sign(payload: dict, secret: str) -> str:
    canon = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    return hmac.new(secret.encode(), canon, hashlib.sha256).hexdigest()


def _parse_trade(fields: dict[str, Any]) -> dict[str, Any] | None:
    """Extract minimal fields from trades:closed event."""
    try:
        pnl_r = float(fields.get("pnl_r") or fields.get("r_multiple") or 0.0)
        sl_dist = float(fields.get("sl_dist") or fields.get("one_r_money") or 0.0)
        tp_hits = int(float(fields.get("tp_hits") or 0))
        # mfe_r: prefer direct field; fall back to mfe_pnl / one_r_money (both always present)
        mfe_r_raw = fields.get("mfe_r") or fields.get("max_favorable_r")
        if mfe_r_raw:
            mfe_r = float(mfe_r_raw)
        else:
            one_r = float(fields.get("one_r_money") or 0.0)
            mfe_pnl = float(fields.get("mfe_pnl") or 0.0)
            mfe_r = mfe_pnl / one_r if one_r > 0 else 0.0
        return {
            "pnl_r": pnl_r,
            "mfe_r": mfe_r,
            "sl_dist": sl_dist,
            "tp_hits": float(tp_hits),
            "regime": str(fields.get("regime") or fields.get("last_regime") or "na").lower(),
        }
    except Exception:
        return None


def _knob_lift(knob: str, trade: dict[str, Any], be_fee_bps: float = 6.0) -> float | None:
    """Per-trade counterfactual lift_R for given knob. None if not eligible.

    Eligibility is knob-specific: a trade is included in the mean only when
    the candidate would actually change the outcome (a "would-have-fired"
    event). Trades that the candidate cannot affect return None so that the
    denominator is restricted to eligible-only — preventing zeros from
    diluting the mean toward 0 (Plan 1.1 follow-up 2026-05-29).
    """
    mfe_r = trade["mfe_r"]
    pnl_r = trade["pnl_r"]
    kind  = KNOB_SPECS[knob]["kind"]
    if not math.isfinite(mfe_r) or not math.isfinite(pnl_r):
        return None

    if kind == "tp1_partial":
        # TP1@0.5R + close 50%; remaining 50% rides to actual exit.
        if mfe_r < 0.5:
            return None  # candidate never fires → not eligible
        partial_r = 0.5 * 0.5  # 50% of qty at 0.5R = 0.25R
        rem_r = 0.5 * pnl_r
        cf = partial_r + rem_r
        return cf - pnl_r

    if kind == "be_partial":
        # TP1@0.5R + 50% close + BE on remaining (conservative -fee_bps loss).
        if mfe_r < 0.5:
            return None
        partial_r = 0.5 * 0.5
        be_loss_r = -(be_fee_bps / 10_000.0) * 0.5  # fee on remaining 50%
        cf = partial_r + max(be_loss_r, 0.5 * pnl_r)  # whichever better
        return cf - pnl_r

    if kind == "arm":
        # Lower arm threshold (0.5→0.25); benefit only if mfe_r in [0.25, 0.5)
        # AND trade ended losing (pnl_r < 0) — would lock partial at 0.25R.
        if 0.25 <= mfe_r < 0.5 and pnl_r < 0:
            keep_r = 0.5 * mfe_r  # OF_LAYER_D_KEEP_FRACTION default
            cf = max(pnl_r, keep_r)
            return cf - pnl_r
        return None

    if kind == "trail_mult":
        # Tighter trailing (1.5→1.0 or 1.2→1.0): less giveback when TP1 hit.
        if trade["tp_hits"] >= 1 and mfe_r > pnl_r:
            giveback_r = mfe_r - pnl_r
            # Proportional reduction in giveback (rough: 0.33 for 1.5→1.0, 0.17 for 1.2→1.0).
            current = KNOB_SPECS[knob]["current"]
            candidate = KNOB_SPECS[knob]["candidate"]
            ratio = (current - candidate) / max(current, 1e-6)
            est_recovered = giveback_r * ratio * 0.5  # conservative 50% recovery factor
            return est_recovered
        return None

    return None


@dataclass
class KnobAgg:
    knob: str
    lifts: list[float] = field(default_factory=list)
    by_regime: dict[str, list[float]] = field(default_factory=dict)
    dwell_h: float = 0.0
    last_pass_ms: int = 0

    def add(self, lift: float, regime: str) -> None:
        self.lifts.append(lift)
        self.by_regime.setdefault(regime, []).append(lift)

    @property
    def mean(self) -> float:
        return sum(self.lifts) / len(self.lifts) if self.lifts else 0.0

    @property
    def n(self) -> int:
        return len(self.lifts)

    def worst_regime_mean(self) -> float:
        vals = [
            sum(v) / len(v) for v in self.by_regime.values() if len(v) >= 20
        ]
        return min(vals) if vals else self.mean


def _load_prev_dwell(r: redis.Redis, state_key: str) -> dict[str, dict[str, Any]]:
    """Load previous state to track dwell time across runs."""
    try:
        raw = r.get(state_key)
        if not raw:
            return {}
        data = json.loads(raw)
        return data.get("knobs") or {}
    except Exception:
        return {}


def evaluate_window(
    trades: list[dict[str, Any]],
    cfg: Cfg,
    prev_knobs: dict[str, dict[str, Any]],
    now_ms: int,
) -> dict[str, dict[str, Any]]:
    """Compute per-knob recommendation. Returns dict ready for publish."""
    aggs: dict[str, KnobAgg] = {k: KnobAgg(knob=k) for k in KNOB_SPECS}
    for t in trades:
        for knob in KNOB_SPECS:
            lift = _knob_lift(knob, t)
            if lift is None:
                continue
            aggs[knob].add(lift, t["regime"])

    out: dict[str, dict[str, Any]] = {}
    for knob, agg in aggs.items():
        passes = (
            agg.n >= cfg.min_trades
            and agg.mean >= cfg.lift_r
            and agg.worst_regime_mean() >= -cfg.tol_r
        )
        prev = prev_knobs.get(knob) or {}
        prev_dwell_h = float(prev.get("dwell_h") or 0.0)
        prev_last_pass = int(prev.get("last_pass_ms") or 0)

        if passes:
            delta_h = (now_ms - prev_last_pass) / 3_600_000.0 if prev_last_pass else 0.0
            new_dwell = prev_dwell_h + min(delta_h, cfg.interval_sec / 3600.0 * 2.0)
            last_pass_ms = now_ms
        else:
            new_dwell = 0.0
            last_pass_ms = 0

        enforce = (
            cfg.enforce
            and passes
            and new_dwell >= cfg.dwell_h
        )
        out[knob] = {
            "value": KNOB_SPECS[knob]["candidate"] if passes else KNOB_SPECS[knob]["current"],
            "lift_r": round(agg.mean, 4),
            "n": agg.n,
            "worst_regime_r": round(agg.worst_regime_mean(), 4),
            "passes": int(passes),
            "enforce": int(enforce),
            "dwell_h": round(new_dwell, 3),
            "last_pass_ms": last_pass_ms,
        }

        # Prometheus per-knob
        try:
            g_lift_r.labels(knob=knob).set(agg.mean)
            g_n_elig.labels(knob=knob).set(agg.n)
            g_enforce.labels(knob=knob).set(1 if enforce else 0)
            g_dwell.labels(knob=knob).set(new_dwell)
        except Exception:
            pass
    return out


def _read_trades_window(r: redis.Redis, stream: str, window_h: float) -> list[dict[str, Any]]:
    """XREVRANGE last `window_h` hours from trades:closed."""
    now_ms = int(time.time() * 1000)
    min_ms = now_ms - int(window_h * 3_600_000)
    try:
        # type: ignore[arg-type]
        entries = r.xrevrange(stream, max="+", min=str(min_ms), count=20_000)
    except Exception as e:
        log.warning("xrevrange failed for %s: %s", stream, e)
        return []
    out: list[dict[str, float]] = []
    for _eid, fields in entries:
        parsed = _parse_trade(fields)
        if parsed is not None:
            out.append(parsed)
    return out


def publish_state(
    r: redis.Redis,
    knobs: dict[str, dict[str, Any]],
    cfg: Cfg,
    n_trades: int,
    state_key: str = "autocal:tp_sl_trailing:state",
) -> bool:
    payload = {
        "ts_ms": int(time.time() * 1000),
        "window_hours": cfg.window_h,
        "n_trades": n_trades,
        "knobs": knobs,
    }
    if cfg.hmac_secret:
        payload["sig"] = _hmac_sign(payload, cfg.hmac_secret)
    try:
        r.set(state_key, json.dumps(payload), ex=int(cfg.interval_sec * 4))
        c_publishes.labels(outcome="ok").inc()
        return True
    except Exception as e:
        log.error("publish state failed: %s", e)
        c_publishes.labels(outcome="error").inc()
        return False


def run_once(r: redis.Redis, cfg: Cfg) -> dict[str, dict[str, Any]]:
    state_key = "autocal:tp_sl_trailing:state"
    trades = _read_trades_window(r, cfg.stream, cfg.window_h)
    prev_knobs = _load_prev_dwell(r, state_key)
    now_ms = int(time.time() * 1000)
    knobs = evaluate_window(trades, cfg, prev_knobs, now_ms)
    publish_state(r, knobs, cfg, n_trades=len(trades), state_key=state_key)
    g_last_run.set(now_ms / 1000)
    g_n_trades.set(len(trades))
    log.info("autocal cycle: n_trades=%d enforce=%d knobs=%s",
             len(trades), int(cfg.enforce),
             {k: v["enforce"] for k, v in knobs.items()})
    return knobs


def main() -> int:
    cfg = load_cfg()
    if not cfg.enable:
        log.info("TP_SL_TRAIL_AUTOCAL_ENABLE=0 — exiting")
        return 0
    try:
        start_http_server(cfg.prom_port)
    except Exception as e:
        log.warning("prom server start failed: %s", e)
    r = redis.from_url(cfg.redis_url, decode_responses=True)
    log.info("autocal start: enforce=%d interval=%ds window=%.1fh min_trades=%d lift_r=%.3f",
             int(cfg.enforce), cfg.interval_sec, cfg.window_h, cfg.min_trades, cfg.lift_r)
    while True:
        g_up.set(1)
        try:
            run_once(r, cfg)
        except Exception as e:
            log.exception("autocal cycle error: %s", e)
        time.sleep(cfg.interval_sec)


if __name__ == "__main__":
    raise SystemExit(main())
