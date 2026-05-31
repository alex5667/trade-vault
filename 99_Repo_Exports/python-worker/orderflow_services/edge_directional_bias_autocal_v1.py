from __future__ import annotations

"""edge_directional_bias_autocal_v1.py — auto-step EDGE_DIRECTIONAL_BIAS_* values.

Implements the rollout plan documented in Plan 2 (counter-trend leak fix):

  Step 1: bias=0.00  (OBSERVE)      — prove wiring, collect baseline
  Step 2: bias=0.03  (CANARY_LOW)   — first canary
  Step 3: bias=0.05  (CANARY_MID)   — escalation
  Step 4: bias=0.06  (CANARY_HIGH)  — terminal (target)

Each (direction × regime) bucket has its own phase. The calibrator advances
phases on dwell + R-no-harm + sample-count gates. LLM advisory (Gemini /
DeepSeek via news_pipeline.llm_client style HTTP) is consulted at promotion
time and routed through `llm_recommendation_guard_v1.guard_recommendations`
— it can VETO a promotion but never force one. Rollback (→ OBSERVE) fires
immediately when applied-window R degrades materially vs baseline.

Reader: `services/edge_directional_bias_overrides.py` (TTL+HMAC, fail-open).
Wire-in: `handlers/crypto_orderflow/utils/edge_cost_gate.py:_apply_directional_bias`.

ENV (all optional, defaults = OBSERVE-only, fully safe):
  EDB_AC_ENABLE            0      — service loop gate
  EDB_AC_ENFORCE           0      — allow auto-promotion past OBSERVE
                                    (rollback to OBSERVE always allowed)
  EDB_AC_INTERVAL_SEC      900    — cycle interval (15 min)
  EDB_AC_WINDOW_H          168.0  — rolling stats window
  EDB_AC_MIN_APPLIED       50     — n_trades in current phase needed for promotion
  EDB_AC_MIN_BASELINE      50     — n_trades in baseline (phase=OBSERVE) needed
  EDB_AC_STEP_DWELL_H      48.0   — min hours bucket must hold current phase
  EDB_AC_R_LEAK_MAX        -0.3   — baseline avg_R ≤ this → bucket is leaking
                                    (only leaking buckets are eligible for canary)
  EDB_AC_R_NO_HARM_TOL     0.10   — applied avg_R must be ≥ baseline - tol
  EDB_AC_R_ROLLBACK_MARGIN 0.30   — applied avg_R < baseline - margin → rollback
  EDB_AC_PASS_RATE_DROP    0.10   — max acceptable global pass-rate drop
  EDB_AC_LLM_ENABLED       0      — consult LLM advisor at promotion
  EDB_AC_LLM_TIMEOUT_SEC   8.0
  EDB_AC_INCLUDE_VIRTUAL   1
  EDB_AC_HMAC_SECRET       ""     — falls back to RECS/LAYERS secrets
  EDB_AC_PROM_PORT         9904
  EDB_AC_STREAM            trades:closed
  EDB_AC_REDIS_URL         redis://redis-worker-1:6379/0
  EDB_AC_NOTIFY_TELEGRAM   1      — XADD to notify:telegram on startup,
                                    phase transitions, and after LLM analysis
  EDB_AC_NOTIFY_STREAM     notify:telegram  — override target stream

Bucket key: f"{direction}|{regime}" — SHORT|trending_bull, LONG|trending_bear, etc.
Only counter-trend pairs are tracked (LONG×trending_bear, SHORT×trending_bull,
SHORT×expansion) — others skipped to keep the state machine focused.

State published to Redis key `autocal:edge_directional_bias:state`:
  {
    "schema_version": 1,
    "ts_ms": ...,
    "window_hours": 168.0,
    "n_trades": 1500,
    "buckets": {
      "SHORT|trending_bull": {
        "phase": "CANARY_LOW",
        "bias_value": 0.03,
        "n_applied": 84,
        "applied_avg_r": -0.18,
        "n_baseline": 312,
        "baseline_avg_r": -0.31,
        "dwell_h": 54.2,
        "rollback_count": 0,
        "last_promotion_ms": 17324...,
        "llm_advisory": {"action":"propose_threshold_canary","risk":"low"}
      },
      ...
    },
    "sig": "<hmac-sha256-hex>"
  }
"""

import contextlib
import hashlib
import hmac
import json
import logging
import math
import os
import time
from dataclasses import dataclass, field
from typing import Any

import redis  # type: ignore
from prometheus_client import Counter, Gauge, start_http_server  # type: ignore

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [edge-dir-bias-ac] %(levelname)s %(message)s",
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


STATE_KEY = "autocal:edge_directional_bias:state"
SCHEMA_VERSION = 1

# Phase ladder. Index defines progression order.
PHASE_LADDER = ("OBSERVE", "CANARY_LOW", "CANARY_MID", "CANARY_HIGH")
PHASE_BIAS = {
    "OBSERVE": 0.00,
    "CANARY_LOW": 0.03,
    "CANARY_MID": 0.05,
    "CANARY_HIGH": 0.06,
    "ROLLED_BACK": 0.00,
}
TAU_CEIL = 0.80

# Counter-trend (direction × regime) cells the calibrator manages.
# Other cells stay at ENV defaults (no automatic adjustment).
TRACKED_BUCKETS = frozenset({
    "SHORT|trending_bull",
    "SHORT|expansion",
    "LONG|trending_bear",
})

# Regime aliases mirror counter_trend_regime_calibrator_v1 for consistency.
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
    min_applied: int
    min_baseline: int
    step_dwell_h: float
    r_leak_max: float
    r_no_harm_tol: float
    r_rollback_margin: float
    pass_rate_drop: float
    llm_enabled: bool
    llm_timeout_sec: float
    include_virtual: bool
    hmac_secret: str
    prom_port: int
    stream: str
    redis_url: str
    notify_telegram: bool
    notify_stream: str


def load_cfg() -> Cfg:
    return Cfg(
        enable=_env_bool("EDB_AC_ENABLE", False),
        enforce=_env_bool("EDB_AC_ENFORCE", False),
        interval_sec=_env_int("EDB_AC_INTERVAL_SEC", 900),
        window_h=_env_float("EDB_AC_WINDOW_H", 168.0),
        min_applied=_env_int("EDB_AC_MIN_APPLIED", 50),
        min_baseline=_env_int("EDB_AC_MIN_BASELINE", 50),
        step_dwell_h=_env_float("EDB_AC_STEP_DWELL_H", 48.0),
        r_leak_max=_env_float("EDB_AC_R_LEAK_MAX", -0.3),
        r_no_harm_tol=_env_float("EDB_AC_R_NO_HARM_TOL", 0.10),
        r_rollback_margin=_env_float("EDB_AC_R_ROLLBACK_MARGIN", 0.30),
        pass_rate_drop=_env_float("EDB_AC_PASS_RATE_DROP", 0.10),
        llm_enabled=_env_bool("EDB_AC_LLM_ENABLED", False),
        llm_timeout_sec=_env_float("EDB_AC_LLM_TIMEOUT_SEC", 8.0),
        include_virtual=_env_bool("EDB_AC_INCLUDE_VIRTUAL", True),
        hmac_secret=(
            _env("EDB_AC_HMAC_SECRET", "")
            or _env("RECS_HMAC_SECRET", "")
            or _env("LAYERS_CAL_HMAC_SECRET", "")
        ),
        prom_port=_env_int("EDB_AC_PROM_PORT", 9904),
        stream=_env("EDB_AC_STREAM", "trades:closed"),
        redis_url=_env("EDB_AC_REDIS_URL", "redis://redis-worker-1:6379/0"),
        notify_telegram=_env_bool("EDB_AC_NOTIFY_TELEGRAM", True),
        notify_stream=_env("EDB_AC_NOTIFY_STREAM", "notify:telegram"),
    )


g_up = Gauge("edb_ac_up", "service loop up")
g_last_run = Gauge("edb_ac_last_run_ts", "last cycle unix ts")
g_n_trades = Gauge("edb_ac_n_trades", "trades processed last cycle")
g_n_buckets = Gauge("edb_ac_n_buckets_tracked", "tracked buckets")
g_bucket_phase_idx = Gauge(
    "edb_ac_bucket_phase_idx",
    "phase index (-1=ROLLED_BACK, 0=OBSERVE, 1=LOW, 2=MID, 3=HIGH)",
    ["direction", "regime"],
)
g_bucket_bias = Gauge(
    "edb_ac_bucket_bias_value", "live bias value per bucket", ["direction", "regime"]
)
g_bucket_applied_avg_r = Gauge(
    "edb_ac_bucket_applied_avg_r",
    "avg_R inside current phase window",
    ["direction", "regime"],
)
g_bucket_baseline_avg_r = Gauge(
    "edb_ac_bucket_baseline_avg_r",
    "avg_R from OBSERVE / baseline phase",
    ["direction", "regime"],
)
g_bucket_dwell_h = Gauge(
    "edb_ac_bucket_dwell_h", "hours bucket held current phase", ["direction", "regime"]
)
c_phase_transitions = Counter(
    "edb_ac_phase_transitions_total",
    "phase transitions",
    ["direction", "regime", "from_phase", "to_phase", "reason"],
)
c_llm_calls = Counter(
    "edb_ac_llm_calls_total", "LLM advisor calls", ["verdict"]
)
c_publishes = Counter("edb_ac_publishes_total", "state publishes", ["outcome"])
c_telegram = Counter(
    "edb_ac_telegram_notifications_total",
    "telegram notifications emitted",
    ["event", "outcome"],
)


def _send_telegram(
    r: Any,
    *,
    cfg: Cfg,
    event: str,
    text: str,
    subtype: str = "edb_autocal",
) -> None:
    """XADD an envelope onto the project's notify:telegram stream.

    Mirrors the pattern used by manip_gate_calibrator_v1, cross_venue_
    calibrator_v1, etc. — telegram dispatcher consumes the stream and
    formats the message. Fail-open: any error is logged + counted.
    """
    if not cfg.notify_telegram:
        return
    try:
        r.xadd(
            cfg.notify_stream,
            {
                "type": "report",
                "subtype": subtype,
                "event": event,
                "ts": str(int(time.time() * 1000)),
                "text": text,
                "parse_mode": "HTML",
            },
            maxlen=5_000,
        )
        c_telegram.labels(event=event, outcome="ok").inc()
    except Exception as e:
        log.warning("telegram notify (%s) failed: %s", event, e)
        c_telegram.labels(event=event, outcome="error").inc()


def _format_startup_msg(cfg: Cfg) -> str:
    """Concise startup HTML message — used by tests too."""
    return (
        "<b>🟢 EDB autocal started</b>\n"
        f"enforce=<b>{int(cfg.enforce)}</b> "
        f"llm=<b>{int(cfg.llm_enabled)}</b> "
        f"window=<b>{cfg.window_h:.0f}h</b> "
        f"dwell=<b>{cfg.step_dwell_h:.0f}h</b>\n"
        f"buckets: {', '.join(sorted(TRACKED_BUCKETS))}"
    )


def _format_phase_transition_msg(
    bucket_key: str, from_phase: str, to_phase: str, decision: "BucketDecision"
) -> str:
    emoji = "🔻" if to_phase == "ROLLED_BACK" else (
        "🔺" if PHASE_BIAS.get(to_phase, 0) > PHASE_BIAS.get(from_phase, 0) else "↔"
    )
    return (
        f"<b>{emoji} EDB phase change</b> <code>{bucket_key}</code>\n"
        f"{from_phase} → <b>{to_phase}</b> "
        f"(bias <b>{decision.bias_value:.2f}</b>)\n"
        f"applied n=<b>{decision.n_applied}</b> avg_r=<b>{decision.applied_avg_r:+.3f}</b> | "
        f"baseline n=<b>{decision.n_baseline}</b> avg_r=<b>{decision.baseline_avg_r:+.3f}</b>\n"
        f"reason: <i>{decision.transition_reason[:160]}</i>"
    )


def _format_llm_msg(decision: "BucketDecision") -> str:
    advisory = decision.llm_advisory or {}
    blocked = advisory.get("blocked_recommendations") or []
    guarded = advisory.get("guarded_recommendations") or []
    skipped = advisory.get("skipped")
    if skipped:
        verdict = f"⏭ skipped:{skipped}"
    elif blocked:
        verdict = f"🛑 vetoed ({blocked[0].get('reason','?')})"
    elif guarded:
        actions = ",".join(r.get("action", "?") for r in guarded[:3])
        verdict = f"✅ allowed: {actions}"
    else:
        verdict = "🤷 neutral"
    return (
        f"<b>🧠 EDB LLM advisory</b> <code>{decision.key}</code>\n"
        f"{decision.phase} → proposed <b>{decision.proposed_transition or '-'}</b>\n"
        f"verdict: <b>{verdict}</b>\n"
        f"applied n=<b>{decision.n_applied}</b> avg_r=<b>{decision.applied_avg_r:+.3f}</b> | "
        f"baseline n=<b>{decision.n_baseline}</b> avg_r=<b>{decision.baseline_avg_r:+.3f}</b>"
    )


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


def _parse_trade(fields: dict[str, Any]) -> dict[str, Any] | None:
    """Extract (direction, regime, r, bias_applied, ts_ms) from a trades:closed row.

    `bias_applied` reads `edge_directional_bias_value` if present (set by
    EdgeCostGate when bias > 0). Missing/zero → 0.0 (counts as baseline).
    """
    direction = _normalize_direction(fields.get("direction") or fields.get("side"))
    if not direction:
        return None
    # redis_repo.save_closed writes `market_regime` (preferred by calibrators,
    # see lines around stream_data.setdefault("market_regime", ...)) along
    # with `entry_regime` / `regime` aliases. Support all three so an
    # upstream rename does not silently strand baseline samples.
    regime = _normalize_regime(
        fields.get("entry_regime")
        or fields.get("market_regime")
        or fields.get("regime")
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
    try:
        bias_raw = fields.get("edge_directional_bias_value")
        bias = float(bias_raw) if bias_raw not in (None, "") else 0.0
        if not math.isfinite(bias):
            bias = 0.0
    except Exception:
        bias = 0.0
    try:
        ts_ms_raw = fields.get("close_ts_ms") or fields.get("ts_ms")
        ts_ms = int(ts_ms_raw) if ts_ms_raw not in (None, "") else 0
    except Exception:
        ts_ms = 0
    is_virtual_raw = fields.get("is_virtual")
    is_virtual = False
    if isinstance(is_virtual_raw, str):
        is_virtual = is_virtual_raw.strip().lower() in {"1", "true", "yes"}
    elif isinstance(is_virtual_raw, (int, float, bool)):
        is_virtual = bool(is_virtual_raw)
    return {
        "direction": direction,
        "regime": regime,
        "r": r,
        "bias_applied": bias,
        "ts_ms": ts_ms,
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
        p = _parse_trade(norm)
        if p is not None:
            out.append(p)
    return out


def _bucket_key(direction: str, regime: str) -> str:
    return f"{direction}|{regime}"


def aggregate_per_bucket(
    trades: list[dict[str, Any]],
    include_virtual: bool,
) -> dict[str, dict[str, Any]]:
    """Group by (direction × regime) and split each into baseline vs applied.

    Baseline = bias_applied == 0.0. Applied = bias_applied > 0.0. Each side
    gets count + sum_r so the caller can compute avg_r. Pure function.
    """
    buckets: dict[str, dict[str, Any]] = {}
    for t in trades:
        if not include_virtual and t["is_virtual"]:
            continue
        key = _bucket_key(t["direction"], t["regime"])
        if key not in TRACKED_BUCKETS:
            continue
        b = buckets.setdefault(
            key,
            {
                "n_baseline": 0,
                "sum_r_baseline": 0.0,
                "n_applied": 0,
                "sum_r_applied": 0.0,
                "applied_bias_observed": 0.0,
            },
        )
        if t["bias_applied"] > 0.0:
            b["n_applied"] += 1
            b["sum_r_applied"] += t["r"]
            if t["bias_applied"] > b["applied_bias_observed"]:
                b["applied_bias_observed"] = t["bias_applied"]
        else:
            b["n_baseline"] += 1
            b["sum_r_baseline"] += t["r"]
    for b in buckets.values():
        nb = max(1, int(b["n_baseline"]))
        na = max(1, int(b["n_applied"]))
        b["baseline_avg_r"] = round(b["sum_r_baseline"] / nb, 4) if b["n_baseline"] else 0.0
        b["applied_avg_r"] = round(b["sum_r_applied"] / na, 4) if b["n_applied"] else 0.0
        b["sum_r_baseline"] = round(b["sum_r_baseline"], 4)
        b["sum_r_applied"] = round(b["sum_r_applied"], 4)
    return buckets


def _phase_idx(phase: str) -> int:
    if phase == "ROLLED_BACK":
        return -1
    try:
        return PHASE_LADDER.index(phase)
    except ValueError:
        return 0


def _next_phase(phase: str) -> str | None:
    """Return next phase in ladder or None if at top / ROLLED_BACK."""
    if phase == "ROLLED_BACK":
        return None
    try:
        idx = PHASE_LADDER.index(phase)
    except ValueError:
        return None
    if idx + 1 >= len(PHASE_LADDER):
        return None
    return PHASE_LADDER[idx + 1]


@dataclass
class BucketDecision:
    """Outcome of one evaluation cycle for one bucket. Pure data."""

    key: str
    phase: str
    bias_value: float
    n_baseline: int
    baseline_avg_r: float
    n_applied: int
    applied_avg_r: float
    dwell_h: float
    last_phase_change_ms: int
    rollback_count: int
    proposed_transition: str | None = None  # next phase to promote to
    transition_reason: str = ""
    llm_advisory: dict[str, Any] = field(default_factory=dict)


def evaluate_bucket(
    key: str,
    raw: dict[str, Any],
    prev: dict[str, Any],
    cfg: Cfg,
    now_ms: int,
) -> BucketDecision:
    """Decide phase/bias/transition for one bucket. Pure function.

    Always returns a decision; promotion is only PROPOSED here, an LLM
    advisory may veto it before publish_state commits the transition.
    Rollback is decided here numerically and applied immediately.
    """
    prev_phase = str(prev.get("phase") or "OBSERVE")
    prev_bias = float(prev.get("bias_value") or PHASE_BIAS.get(prev_phase, 0.0))
    # last_phase_change_ms missing entirely → first observation; treat
    # current cycle as start of dwell. Present-but-zero (explicit reset)
    # means dwell started at epoch 0 → unbounded dwell counts.
    if "last_phase_change_ms" in prev:
        try:
            prev_last_change = int(prev["last_phase_change_ms"])
        except (TypeError, ValueError):
            prev_last_change = now_ms
    else:
        prev_last_change = now_ms
    prev_rollbacks = int(prev.get("rollback_count") or 0)

    dwell_h = max(0.0, (now_ms - prev_last_change) / 3_600_000.0)

    n_baseline = int(raw.get("n_baseline", 0))
    baseline_avg_r = float(raw.get("baseline_avg_r", 0.0))
    n_applied = int(raw.get("n_applied", 0))
    applied_avg_r = float(raw.get("applied_avg_r", 0.0))

    decision = BucketDecision(
        key=key,
        phase=prev_phase,
        bias_value=prev_bias,
        n_baseline=n_baseline,
        baseline_avg_r=baseline_avg_r,
        n_applied=n_applied,
        applied_avg_r=applied_avg_r,
        dwell_h=round(dwell_h, 3),
        last_phase_change_ms=prev_last_change,
        rollback_count=prev_rollbacks,
    )

    # 1) Rollback (highest priority). Fires immediately, no LLM consultation
    #    — protection against active harm must not depend on external services.
    if prev_phase not in ("OBSERVE", "ROLLED_BACK") and n_applied >= cfg.min_applied:
        if applied_avg_r < (baseline_avg_r - cfg.r_rollback_margin):
            decision.phase = "ROLLED_BACK"
            decision.bias_value = PHASE_BIAS["ROLLED_BACK"]
            decision.dwell_h = 0.0
            decision.last_phase_change_ms = now_ms
            decision.rollback_count = prev_rollbacks + 1
            decision.transition_reason = (
                f"rollback:applied_r={applied_avg_r:.3f}<baseline_r={baseline_avg_r:.3f}"
                f"-margin={cfg.r_rollback_margin:.3f}"
            )
            return decision

    # 2) ROLLED_BACK is sticky — manual unfreeze required (rollback_count
    #    cleared externally). Stay put.
    if prev_phase == "ROLLED_BACK":
        decision.transition_reason = "rolled_back_sticky"
        return decision

    # 3) Promotion gate. Requires enforce=1 to actually flip phase.
    nxt = _next_phase(prev_phase)
    if nxt is None:
        decision.transition_reason = "terminal_phase"
        return decision

    if dwell_h < cfg.step_dwell_h:
        decision.transition_reason = f"dwell_h={dwell_h:.1f}<{cfg.step_dwell_h}"
        return decision

    if prev_phase == "OBSERVE":
        # Step 1 → 2: bucket must show enough baseline samples + actual leak.
        if n_baseline < cfg.min_baseline:
            decision.transition_reason = (
                f"baseline_n={n_baseline}<{cfg.min_baseline}"
            )
            return decision
        if baseline_avg_r > cfg.r_leak_max:
            # No leak detected — no need to escalate.
            decision.transition_reason = (
                f"no_leak:baseline_r={baseline_avg_r:.3f}>r_leak_max={cfg.r_leak_max:.3f}"
            )
            return decision
    else:
        # CANARY_LOW/MID → next: need applied samples + no-harm vs baseline.
        if n_applied < cfg.min_applied:
            decision.transition_reason = (
                f"applied_n={n_applied}<{cfg.min_applied}"
            )
            return decision
        if applied_avg_r < (baseline_avg_r - cfg.r_no_harm_tol):
            decision.transition_reason = (
                f"harm:applied_r={applied_avg_r:.3f}<baseline_r={baseline_avg_r:.3f}"
                f"-tol={cfg.r_no_harm_tol:.3f}"
            )
            return decision

    # Numerical criteria met → propose transition. Actual commit happens in
    # publish_state after LLM advisory + enforce check.
    decision.proposed_transition = nxt
    decision.transition_reason = f"eligible:{prev_phase}->{nxt}"
    return decision


def commit_transition(
    decision: BucketDecision,
    advisory_blocks: bool,
    enforce: bool,
    now_ms: int,
) -> BucketDecision:
    """Apply proposed transition or veto it. Mutates decision in place."""
    if decision.proposed_transition is None:
        return decision
    if advisory_blocks:
        decision.transition_reason += "|llm_veto"
        decision.proposed_transition = None
        return decision
    if not enforce:
        decision.transition_reason += "|shadow_no_enforce"
        decision.proposed_transition = None
        return decision
    new_phase = decision.proposed_transition
    decision.phase = new_phase
    decision.bias_value = PHASE_BIAS.get(new_phase, 0.0)
    decision.dwell_h = 0.0
    decision.last_phase_change_ms = now_ms
    decision.proposed_transition = None
    return decision


def _load_prev_buckets(r: Any, state_key: str = STATE_KEY) -> dict[str, dict[str, Any]]:
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
    decisions: dict[str, BucketDecision],
    cfg: Cfg,
    n_trades: int,
    state_key: str = STATE_KEY,
) -> bool:
    payload: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "ts_ms": int(time.time() * 1000),
        "window_hours": cfg.window_h,
        "n_trades": n_trades,
        "buckets": {
            key: {
                "phase": d.phase,
                "bias_value": d.bias_value,
                "n_baseline": d.n_baseline,
                "baseline_avg_r": d.baseline_avg_r,
                "n_applied": d.n_applied,
                "applied_avg_r": d.applied_avg_r,
                "dwell_h": d.dwell_h,
                "last_phase_change_ms": d.last_phase_change_ms,
                "rollback_count": d.rollback_count,
                "transition_reason": d.transition_reason,
                "llm_advisory": d.llm_advisory,
            }
            for key, d in decisions.items()
        },
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


def _ask_llm_advisory(decision: BucketDecision, cfg: Cfg) -> dict[str, Any]:
    """Consult LLM advisor; routed through recommendation guard.

    Returns the GUARDED payload — caller inspects `blocked` to know whether
    the LLM (or guard) vetoed promotion. Fail-open: returns advisory that
    DOES NOT block (so absence of LLM service can't strand promotion forever
    — numerical gates still apply, and humans must turn EDB_AC_ENFORCE=1).
    """
    if not cfg.llm_enabled:
        return {"valid": True, "guarded_recommendations": [], "blocked_recommendations": [], "skipped": "llm_disabled"}
    try:
        from orderflow_services.edge_directional_bias_llm_advisor import (
            advise_bucket_transition,
        )
    except Exception as e:
        log.debug("llm advisor import failed: %s", e)
        return {"valid": True, "guarded_recommendations": [], "blocked_recommendations": [], "skipped": "llm_unavailable"}
    try:
        result = advise_bucket_transition(
            bucket_key=decision.key,
            current_phase=decision.phase,
            proposed_phase=decision.proposed_transition or decision.phase,
            n_baseline=decision.n_baseline,
            baseline_avg_r=decision.baseline_avg_r,
            n_applied=decision.n_applied,
            applied_avg_r=decision.applied_avg_r,
            dwell_h=decision.dwell_h,
            timeout_sec=cfg.llm_timeout_sec,
        )
        verdict = "block" if result.get("blocked_recommendations") else "allow"
        c_llm_calls.labels(verdict=verdict).inc()
        return result
    except Exception as e:
        log.warning("llm advisory failed: %s", e)
        c_llm_calls.labels(verdict="error").inc()
        # Fail-open: don't strand promotion forever on LLM outages
        return {"valid": True, "guarded_recommendations": [], "blocked_recommendations": [], "skipped": "llm_error"}


def _advisory_blocks_promotion(advisory: dict[str, Any]) -> bool:
    """Inspect guarded advisory output — block if any blocked_action present
    OR a guarded recommendation explicitly says `freeze_candidate`."""
    if advisory.get("blocked_recommendations"):
        # Guard blocked the recommended action — typically because LLM
        # proposed something dangerous (enable_enforce, raise_risk_limit).
        # We treat that as "don't promote" since the LLM's intent was risky.
        return True
    for rec in advisory.get("guarded_recommendations", []) or []:
        if rec.get("action") == "freeze_candidate":
            return True
    return False


def run_once(r: Any, cfg: Cfg) -> dict[str, BucketDecision]:
    trades = _read_trades_window(r, cfg.stream, cfg.window_h)
    raw_buckets = aggregate_per_bucket(trades, include_virtual=cfg.include_virtual)
    # Ensure tracked buckets always present (even when no trades) so the
    # state always lists them with phase=OBSERVE — readers depend on this.
    for key in TRACKED_BUCKETS:
        raw_buckets.setdefault(
            key,
            {
                "n_baseline": 0,
                "sum_r_baseline": 0.0,
                "n_applied": 0,
                "sum_r_applied": 0.0,
                "applied_bias_observed": 0.0,
                "baseline_avg_r": 0.0,
                "applied_avg_r": 0.0,
            },
        )
    prev = _load_prev_buckets(r)
    now_ms = int(time.time() * 1000)

    decisions: dict[str, BucketDecision] = {}
    for key, raw in raw_buckets.items():
        prev_bucket = prev.get(key) or {}
        decision = evaluate_bucket(key, raw, prev_bucket, cfg, now_ms)
        if decision.proposed_transition is not None:
            advisory = _ask_llm_advisory(decision, cfg)
            decision.llm_advisory = advisory
            # Telegram: notify on every LLM consultation — operator wants
            # to see the verdict whether it allows, vetoes, or skips.
            _send_telegram(
                r, cfg=cfg, event="llm_advisory",
                text=_format_llm_msg(decision),
            )
            commit_transition(
                decision,
                advisory_blocks=_advisory_blocks_promotion(advisory),
                enforce=cfg.enforce,
                now_ms=now_ms,
            )
        # Record transition counter (for new phase OR sticky non-transition)
        prev_phase = str(prev_bucket.get("phase") or "OBSERVE")
        if prev_phase != decision.phase:
            try:
                direction, regime = key.split("|", 1)
            except ValueError:
                direction, regime = key, ""
            with contextlib.suppress(Exception):
                c_phase_transitions.labels(
                    direction=direction,
                    regime=regime,
                    from_phase=prev_phase,
                    to_phase=decision.phase,
                    reason=(decision.transition_reason or "")[:48],
                ).inc()
            # Telegram: phase change is the most important event — fires
            # for both promotion and rollback.
            _send_telegram(
                r, cfg=cfg, event="phase_transition",
                text=_format_phase_transition_msg(
                    key, prev_phase, decision.phase, decision,
                ),
            )
        decisions[key] = decision

    publish_state(r, decisions, cfg, n_trades=len(trades))

    g_last_run.set(now_ms / 1000)
    g_n_trades.set(len(trades))
    g_n_buckets.set(len(decisions))
    for key, d in decisions.items():
        try:
            direction, regime = key.split("|", 1)
        except ValueError:
            continue
        with contextlib.suppress(Exception):
            g_bucket_phase_idx.labels(direction=direction, regime=regime).set(_phase_idx(d.phase))
            g_bucket_bias.labels(direction=direction, regime=regime).set(d.bias_value)
            g_bucket_applied_avg_r.labels(direction=direction, regime=regime).set(d.applied_avg_r)
            g_bucket_baseline_avg_r.labels(direction=direction, regime=regime).set(d.baseline_avg_r)
            g_bucket_dwell_h.labels(direction=direction, regime=regime).set(d.dwell_h)

    log.info(
        "cycle n_trades=%d buckets=%d enforce=%s | %s",
        len(trades), len(decisions), cfg.enforce,
        ", ".join(
            f"{k}:{d.phase}@{d.bias_value:.2f}(dwell={d.dwell_h:.1f}h,"
            f"applied_r={d.applied_avg_r:.2f}/{d.n_applied})"
            for k, d in decisions.items()
        ),
    )
    return decisions


def main() -> int:
    cfg = load_cfg()
    if not cfg.enable:
        log.info("EDB_AC_ENABLE=0 — exiting")
        return 0
    try:
        start_http_server(cfg.prom_port)
        log.info("prometheus on :%d", cfg.prom_port)
    except Exception as e:
        log.warning("prometheus server failed: %s", e)
    r = redis.from_url(cfg.redis_url, decode_responses=False)
    g_up.set(1)
    log.info(
        "edge-directional-bias autocal started: window=%.1fh interval=%ds enforce=%s llm=%s",
        cfg.window_h, cfg.interval_sec, cfg.enforce, cfg.llm_enabled,
    )
    # Startup telegram — confirms the service came up with the expected
    # config (enforce / llm flips visible at a glance in chat history).
    _send_telegram(r, cfg=cfg, event="startup", text=_format_startup_msg(cfg))
    while True:
        try:
            run_once(r, cfg)
        except Exception as e:
            log.exception("cycle failed: %s", e)
        time.sleep(cfg.interval_sec)


if __name__ == "__main__":
    raise SystemExit(main())
