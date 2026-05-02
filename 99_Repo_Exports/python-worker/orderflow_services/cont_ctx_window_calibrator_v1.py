#!/usr/bin/env python3
from __future__ import annotations
"""cont_ctx_window_calibrator_v1.py

Production-safe post-analysis calibrator for continuation context window
(`cont_ctx_valid_ms`).

What it does
------------
1) Consumes narrow continuation-capture rows from Redis Stream
   `stream:ofc:cont_ctx_capture`.
2) Selects *single-leg* continuation near-miss candidates where widening only
   `cont_ctx_valid_ms` can plausibly rescue the signal.
3) Emits rescued candidates into a paper/shadow stream
   `stream:ofc:cont_ctx_shadow_signals`.
4) Periodically scans `trades:closed` for shadow outcomes with
   `calib_kind=cont_ctx_window` (or `entry_reason=rescued_cont_ctx_window`) and
   computes a bounded recommendation for `config:orderflow:{symbol}`.
5) Optionally auto-applies the recommendation with step/cooldown guards.

Design goals
------------
- fail-open: worker failure never blocks trading
- bounded cardinality: labels are symbol and window only
- deterministic: uses event ts_ms/stream ids, not wall clock, when possible
- no hidden policy coupling: calibrates only one knob (`cont_ctx_valid_ms`)
""",
from utils.time_utils import get_ny_time_millis

import hashlib
import json
import math
import os
import socket
import time
import uuid
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Tuple

import redis  # type: ignore
from prometheus_client import Counter, Gauge, Histogram, start_http_server  # type: ignore

NOTIFY_STREAM = os.getenv("NOTIFY_STREAM", "notify:telegram")
PENDING_TTL_SEC = 48 * 3600  # 48h


def _env(name: str, default: str = "") -> str:
    return (os.getenv(name) or default).strip()


def _now_ms() -> int:
    return get_ny_time_millis()


def _i(v: Any, d: int = 0) -> int:
    try:
        return int(float(v))
    except Exception:
        return d


def _f(v: Any, d: float = 0.0) -> float:
    try:
        x = float(v)
        return x if math.isfinite(x) else d
    except Exception:
        return d


def _loads_maybe_json(v: Any) -> Any:
    if v is None:
        return None
    if isinstance(v, (dict, list)):
        return v
    if isinstance(v, (bytes, bytearray)):
        try:
            v = v.decode("utf-8", "replace")
        except Exception:
            return None
    s = str(v).strip()
    if not s:
        return ""
    if (s.startswith("{") and s.endswith("}")) or (s.startswith("[") and s.endswith("]")):
        try:
            return json.loads(s)
        except Exception:
            return s
    return s


def _parse_entry(fields: Dict[Any, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for k, v in (fields or {}).items():
        ks = k.decode("utf-8", "replace") if isinstance(k, (bytes, bytearray)) else str(k)
        out[ks] = _loads_maybe_json(v)
    if isinstance(out.get("payload"), dict):
        payload_obj = out.get("payload")
        merged = dict(out)
        merged.update(payload_obj)  # type: ignore[arg-type]
        return merged
    if isinstance(out.get("json"), dict):
        payload_obj = out.get("json")
        merged = dict(out)
        merged.update(payload_obj)  # type: ignore[arg-type]
        return merged
    return out


def _quantile(xs: List[float], q: float) -> float:
    if not xs:
        return 0.0
    arr = sorted(float(x) for x in xs)
    if len(arr) == 1:
        return arr[0]
    pos = max(0.0, min(1.0, q)) * float(len(arr) - 1)
    lo = int(math.floor(pos))
    hi = int(math.ceil(pos))
    if lo == hi:
        return arr[lo]
    a = arr[lo]
    b = arr[hi]
    t = pos - float(lo)
    return a + (b - a) * t


def _mean(xs: Iterable[float]) -> float:
    vals = [float(x) for x in xs]
    return sum(vals) / float(len(vals)) if vals else 0.0


def _clamp_int(v: int, lo: int, hi: int) -> int:
    return max(int(lo), min(int(hi), int(v)))


def _bucket_false_breakout(fields: Dict[str, Any]) -> int:
    if _i(fields.get("false_breakout"), 0) == 1:
        return 1
    rsn = str(fields.get("close_reason") or fields.get("exit_reason") or fields.get("reason") or "").lower()
    if any(x in rsn for x in ("false_breakout", "failed_breakout", "reversal_after_entry", "stop_loss", "sl_hit")):
        return 1
    return 0


def _bucket_outcome_r(fields: Dict[str, Any]) -> float:
    for k in ("r_net", "net_r", "r_mult_net", "r_mult", "pnl_r"):
        if k in fields:
            return _f(fields.get(k), 0.0)
    return 0.0


@dataclass
class Cfg:
    redis_url: str
    capture_stream: str
    shadow_stream: str
    closed_stream: str
    group: str
    consumer: str
    read_count: int
    block_ms: int
    port: int
    loop_sleep_s: float
    baseline_ms: int
    windows_ms: List[int]
    min_ms: int
    max_ms: int
    max_step_ms: int
    lookback_hours: int
    min_sample: int
    min_rescued: int
    confidence_min: float
    expectancy_min_r: float
    false_breakout_max: float
    exec_p95_norm_max: float
    exec_p95_delta_max: float
    stale_penalty_mid_s: int
    stale_penalty_hi_s: int
    stale_penalty_max_s: int
    mode: str
    shadow_enable: int
    require_ok_soft: int
    require_single_leg: int
    cooldown_sec: int
    apply_lock_key: str
    apply_lock_ttl_sec: int
    metrics_summary_prefix: str
    suggestions_prefix: str
    last_apply_prefix: str


def load_cfg() -> Cfg:
    raw_windows = [x.strip() for x in _env("CONT_CTX_CALIB_WINDOWS_MS", "120000,150000,180000,210000,240000").split(",") if x.strip()]
    windows = sorted({max(1, _i(x, 0)) for x in raw_windows if _i(x, 0) > 0})
    if not windows:
        windows = [120000, 150000, 180000, 210000, 240000]
    return Cfg(
        redis_url=_env("REDIS_URL", "redis://redis-worker-1:6379/0"),
        capture_stream=_env("CONT_CTX_CALIB_CAPTURE_STREAM", "stream:ofc:cont_ctx_capture"),
        shadow_stream=_env("CONT_CTX_CALIB_SHADOW_STREAM", "stream:ofc:cont_ctx_shadow_signals"),
        closed_stream=_env("TRADES_CLOSED_STREAM", "trades:closed"),
        group=_env("CONT_CTX_CALIB_CG", "cont_ctx_window_calibrator_v1"),
        consumer=_env("CONT_CTX_CALIB_CONSUMER", socket.gethostname()),
        read_count=_i(_env("CONT_CTX_CALIB_READ_COUNT", "200"), 200),
        block_ms=_i(_env("CONT_CTX_CALIB_BLOCK_MS", "5000"), 5000),
        port=_i(_env("CONT_CTX_CALIB_METRICS_PORT", "9137"), 9137),
        loop_sleep_s=float(_env("CONT_CTX_CALIB_LOOP_SLEEP_S", "0.2") or 0.2),
        baseline_ms=_i(_env("CONT_CTX_CALIB_BASELINE_MS", "120000"), 120000),
        windows_ms=windows,
        min_ms=_i(_env("CONT_CTX_CALIB_MIN_MS", "90000"), 90000),
        max_ms=_i(_env("CONT_CTX_CALIB_MAX_MS", "240000"), 240000),
        max_step_ms=_i(_env("CONT_CTX_CALIB_MAX_STEP_MS", "30000"), 30000),
        lookback_hours=_i(_env("CONT_CTX_CALIB_LOOKBACK_HOURS", "72"), 72),
        min_sample=_i(_env("CONT_CTX_CALIB_MIN_SAMPLE", "40"), 40),
        min_rescued=_i(_env("CONT_CTX_CALIB_MIN_RESCUED", "20"), 20),
        confidence_min=_f(_env("CONT_CTX_CALIB_CONFIDENCE_MIN", "0.80"), 0.80),
        expectancy_min_r=_f(_env("CONT_CTX_CALIB_EXPECTANCY_MIN_R", "0.02"), 0.02),
        false_breakout_max=_f(_env("CONT_CTX_CALIB_FALSE_BREAKOUT_MAX", "0.22"), 0.22),
        exec_p95_norm_max=_f(_env("CONT_CTX_CALIB_EXEC_P95_NORM_MAX", "0.75"), 0.75),
        exec_p95_delta_max=_f(_env("CONT_CTX_CALIB_EXEC_P95_DELTA_MAX", "0.05"), 0.05),
        stale_penalty_mid_s=_i(_env("CONT_CTX_CALIB_STALE_PENALTY_MID_S", "180"), 180),
        stale_penalty_hi_s=_i(_env("CONT_CTX_CALIB_STALE_PENALTY_HI_S", "210"), 210),
        stale_penalty_max_s=_i(_env("CONT_CTX_CALIB_STALE_PENALTY_MAX_S", "240"), 240),
        mode=_env("CONT_CTX_CALIB_MODE", "RECOMMEND").upper(),
        shadow_enable=_i(_env("CONT_CTX_CALIB_SHADOW_ENABLE", "1"), 1),
        require_ok_soft=_i(_env("CONT_CTX_CALIB_REQUIRE_OK_SOFT", "1"), 1),
        require_single_leg=_i(_env("CONT_CTX_CALIB_REQUIRE_SINGLE_LEG", "1"), 1),
        cooldown_sec=_i(_env("CONT_CTX_CALIB_COOLDOWN_SEC", "21600"), 21600),
        apply_lock_key=_env("CONT_CTX_CALIB_APPLY_LOCK_KEY", "lock:cont_ctx_window_calibrator:apply"),
        apply_lock_ttl_sec=_i(_env("CONT_CTX_CALIB_APPLY_LOCK_TTL_SEC", "30"), 30),
        metrics_summary_prefix=_env("CONT_CTX_CALIB_SUMMARY_PREFIX", "metrics:cont_ctx_window_calib:last"),
        suggestions_prefix=_env("CONT_CTX_CALIB_SUGGESTIONS_PREFIX", "cfg:suggestions:cont_ctx_valid_ms"),
        last_apply_prefix=_env("CONT_CTX_CALIB_LAST_APPLY_PREFIX", "metrics:cont_ctx_window_calib:applied"),
    )


@dataclass
class CaptureRow:
    signal_id: str
    symbol: str
    ts_ms: int
    direction: str
    scenario: str
    ok: int
    ok_soft: int
    have: int
    need: int
    score: float
    reason: str
    strong_gate_missing: str
    trend_dir_source: str
    cont_ctx_ts_ms: int
    cont_ctx_age_ms: int
    hidden_ctx_recent: int
    obi_stable: int
    cont_ctx_recent: int
    exec_risk_norm: float
    exec_risk_bps: float
    dq_veto: int
    hidden_ctx_warmup_bypass: int = 0
    cont_ctx_warmup_bypass: int = 0
    obi_stable_warmup_bypass: int = 0


RUNS = Counter("cont_ctx_calib_runs_total", "Calibration runs", ["symbol", "status"])
CANDIDATES = Counter("cont_ctx_calib_candidates_total", "Eligible continuation candidates", ["symbol"])
RESCUED = Counter("cont_ctx_calib_rescued_total", "Rescued continuation candidates", ["symbol", "window_ms"])
SHADOW_EMIT = Counter("cont_ctx_calib_shadow_entries_total", "Shadow entries emitted", ["symbol", "window_ms"])
APPLY = Counter("cont_ctx_calib_apply_total", "Auto-apply attempts", ["symbol", "status"])
APPLY_BLOCK = Counter("cont_ctx_calib_apply_block_total", "Auto-apply blocked", ["symbol", "reason"])
RECOMMENDED = Gauge("cont_ctx_calib_recommended_window_ms", "Recommended continuation context window (ms)", ["symbol"])
CONFIDENCE = Gauge("cont_ctx_calib_confidence", "Recommendation confidence", ["symbol"])
EXPECTANCY = Gauge("cont_ctx_calib_expectancy_r", "Expectancy in R for recommended window", ["symbol"])
FALSE_BREAKOUT = Gauge("cont_ctx_calib_false_breakout_rate", "False breakout rate", ["symbol"])
EXEC_P95 = Gauge("cont_ctx_calib_exec_p95_norm", "exec_risk_norm p95", ["symbol"])
SAMPLE_N = Gauge("cont_ctx_calib_sample_n", "Closed-trade sample size", ["symbol", "window_ms"])
RESCUED_SHARE = Gauge("cont_ctx_calib_rescued_share", "Rescued share vs sample", ["symbol", "window_ms"])
LAST_RUN_TS = Gauge("cont_ctx_calib_last_run_ts_seconds", "Last successful run timestamp", ["symbol"])
LOOP_LAT = Histogram("cont_ctx_calib_loop_seconds", "Main loop latency")


def _state_key(prefix: str, symbol: str) -> str:
    return f"{prefix}:{symbol}"


def _parse_capture(fields: Dict[Any, Any]) -> Optional[CaptureRow]:
    d = _parse_entry(fields)
    try:
        return CaptureRow(
            signal_id=str(d.get("signal_id") or ""),
            symbol=str(d.get("symbol") or "unknown"),
            ts_ms=_i(d.get("ts_ms"), 0),
            direction=str(d.get("direction") or ""),
            scenario=str(d.get("scenario") or ""),
            ok=_i(d.get("ok"), 0),
            ok_soft=_i(d.get("ok_soft"), 0),
            have=_i(d.get("have"), 0),
            need=_i(d.get("need"), 0),
            score=_f(d.get("score"), 0.0),
            reason=str(d.get("reason") or ""),
            strong_gate_missing=str(d.get("strong_gate_missing") or ""),
            trend_dir_source=str(d.get("trend_dir_source") or ""),
            cont_ctx_ts_ms=_i(d.get("cont_ctx_ts_ms"), 0),
            cont_ctx_age_ms=_i(d.get("cont_ctx_age_ms"), 0),
            hidden_ctx_recent=_i(d.get("hidden_ctx_recent"), 0),
            obi_stable=_i(d.get("obi_stable"), 0),
            cont_ctx_recent=_i(d.get("cont_ctx_recent"), 0),
            exec_risk_norm=_f(d.get("exec_risk_norm"), 999.0),
            exec_risk_bps=_f(d.get("exec_risk_bps"), 0.0),
            dq_veto=_i(d.get("dq_veto"), 0),
            hidden_ctx_warmup_bypass=_i(d.get("hidden_ctx_warmup_bypass"), 0),
            cont_ctx_warmup_bypass=_i(d.get("cont_ctx_warmup_bypass"), 0),
            obi_stable_warmup_bypass=_i(d.get("obi_stable_warmup_bypass"), 0),
        )
    except Exception:
        return None


def _missing_set(s: str) -> set:
    return {x.strip() for x in str(s or "").split(",") if x and x.strip()}


def is_candidate(c: CaptureRow, cfg: Cfg) -> bool:
    """Check if a capture row is eligible for calibrator analysis.

    A candidate must be a continuation near-miss where the single missing
    leg is cont_ctx_recent and no warmup bypasses are active.
    """,
    if c.scenario != "continuation":
        return False
    if c.ok != 0:
        return False
    if c.dq_veto == 1:
        return False
    if c.cont_ctx_ts_ms <= 0 or c.cont_ctx_age_ms <= 0:
        return False
    if c.exec_risk_norm > cfg.exec_p95_norm_max:
        return False
    if cfg.require_ok_soft == 1 and c.ok_soft != 1:
        return False
    if c.hidden_ctx_warmup_bypass or c.cont_ctx_warmup_bypass or c.obi_stable_warmup_bypass:
        return False
    miss = _missing_set(c.strong_gate_missing)
    if cfg.require_single_leg == 1:
        return miss == {"cont_ctx_recent"} and c.have == max(0, c.need - 1)
    return "cont_ctx_recent" in miss and "hidden_ctx_recent" not in miss and "obi_stable" not in miss


def rescued_by_window(c: CaptureRow, baseline_ms: int, window_ms: int) -> bool:
    """Check if widening from baseline to window would rescue this candidate.""",
    age = int(c.cont_ctx_age_ms)
    return age > int(baseline_ms) and age <= int(window_ms)


def _stale_penalty(age_ms: int, cfg: Cfg) -> float:
    """Piecewise stale penalty for context age.""",
    s = float(age_ms) / 1000.0
    if s <= float(cfg.stale_penalty_mid_s):
        return 0.0
    if s <= float(cfg.stale_penalty_hi_s):
        return 0.20
    if s <= float(cfg.stale_penalty_max_s):
        return 0.50
    return 1.0


def _ensure_group(r: Any, cfg: Cfg) -> None:
    try:
        r.xgroup_create(name=cfg.capture_stream, groupname=cfg.group, id="0-0", mkstream=True)
    except Exception:
        pass


def _emit_shadow(r: Any, cfg: Cfg, c: CaptureRow, window_ms: int, run_id: str) -> None:
    """Emit a shadow entry signal for the paper/shadow executor.""",
    payload = {
        "schema": "1",
        "event": "cont_ctx_shadow_entry",
        "signal_id": c.signal_id,
        "parent_signal_id": c.signal_id,
        "symbol": c.symbol,
        "ts_ms": str(c.ts_ms),
        "direction": c.direction,
        "scenario": c.scenario,
        "candidate_window_ms": str(window_ms),
        "baseline_window_ms": str(cfg.baseline_ms),
        "cont_ctx_age_ms": str(c.cont_ctx_age_ms),
        "entry_reason": "rescued_cont_ctx_window",
        "entry_policy": "paper_only",
        "calib_run_id": run_id,
        "calib": "1",
        "calib_kind": "cont_ctx_window",
    }
    r.xadd(cfg.shadow_stream, payload, maxlen=200_000, approximate=True)
    SHADOW_EMIT.labels(symbol=c.symbol, window_ms=str(window_ms)).inc()


def _acquire_apply_lock(r: Any, cfg: Cfg) -> str:
    token = f"{socket.gethostname()}:{_now_ms()}"
    try:
        ok = r.set(cfg.apply_lock_key, token, nx=True, ex=int(cfg.apply_lock_ttl_sec))
        if ok is True or ok == "OK":
            return token
    except Exception:
        return ""
    return ""


def _release_apply_lock(r: Any, cfg: Cfg, token: str) -> None:
    if not token:
        return
    try:
        lua = "if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) else return 0 end"
        r.eval(lua, 1, cfg.apply_lock_key, token)
    except Exception:
        return


def _read_recent_closed_trades(r: Any, cfg: Cfg) -> List[Dict[str, Any]]:
    """Read recent closed calibration trades from trades:closed stream.""",
    since_ms = _now_ms() - int(cfg.lookback_hours) * 3600 * 1000
    out: List[Dict[str, Any]] = []
    last_id = "+"
    scanned = 0
    max_scan = 250_000
    while scanned < max_scan:
        batch = r.xrevrange(cfg.closed_stream, max=last_id, min="-", count=2000)
        if not batch or (len(batch) == 1 and batch[0][0] == last_id):
            break
        for msg_id, raw_fields in batch:
            scanned += 1
            if msg_id == last_id:
                continue
            last_id = msg_id
            d = _parse_entry(raw_fields)
            ts_ms = _i(d.get("ts_ms") or d.get("exit_ts_ms") or d.get("timestamp") or msg_id.split("-", 1)[0], 0)
            if ts_ms and ts_ms < since_ms:
                scanned = max_scan
                break
            if _i(d.get("calib"), 0) != 1:
                continue
            kind = str(d.get("calib_kind") or "")
            reason = str(d.get("entry_reason") or "")
            if kind != "cont_ctx_window" and reason != "rescued_cont_ctx_window":
                continue
            d["_ts_ms"] = ts_ms
            out.append(d)
    return out


def _aggregate_outcomes(rows: List[Dict[str, Any]], cfg: Cfg) -> Dict[Tuple[str, int], Dict[str, Any]]:
    """Aggregate shadow trade outcomes by (symbol, window_ms).""",
    out: Dict[Tuple[str, int], Dict[str, Any]] = {}
    for d in rows:
        symbol = str(d.get("symbol") or "unknown")
        window_ms = _i(d.get("candidate_window_ms") or d.get("cont_ctx_candidate_window_ms"), 0)
        if window_ms <= 0:
            continue
        k = (symbol, window_ms)
        st = out.setdefault(k, {
            "n": 0,
            "rescued_n": 0,
            "r_vals": [],
            "exec_vals": [],
            "fb": 0,
            "age_vals": [],
        })
        st["n"] += 1
        st["rescued_n"] += 1
        st["r_vals"].append(_bucket_outcome_r(d))
        st["exec_vals"].append(_f(d.get("exec_risk_norm"), 0.0))
        st["fb"] += _bucket_false_breakout(d)
        st["age_vals"].append(_i(d.get("cont_ctx_age_ms"), 0))
    return out


def _score_candidate_window(st: Dict[str, Any], cfg: Cfg) -> Tuple[float, float, float, float, float]:
    """Compute utility, expectancy, fb_rate, exec_p95, confidence for a window cohort.

    J(W) = E[net_r|W] - λ_fb·FB(W) - λ_exec·TailExec(W) - λ_stale·StalePenalty(W)
    """,
    n = int(st.get("n", 0) or 0)
    if n <= 0:
        return (-999.0, 0.0, 0.0, 0.0, 0.0)
    expectancy = _mean(st.get("r_vals", []))
    fb_rate = float(st.get("fb", 0) or 0) / float(n)
    exec_p95 = _quantile(list(st.get("exec_vals", [])), 0.95)
    avg_stale_pen = _mean(_stale_penalty(_i(x, 0), cfg) for x in st.get("age_vals", []))
    utility = expectancy - 0.75 * fb_rate - 0.50 * max(0.0, exec_p95 - cfg.exec_p95_norm_max) - 0.20 * avg_stale_pen
    confidence = min(1.0, float(n) / float(max(cfg.min_sample, 1)))
    return utility, expectancy, fb_rate, exec_p95, confidence


def _write_summary(r: Any, cfg: Cfg, symbol: str, payload: Dict[str, Any]) -> None:
    key = _state_key(cfg.metrics_summary_prefix, symbol)
    r.hset(key, mapping={k: str(v) for k, v in payload.items()})


def _write_suggestion(r: Any, cfg: Cfg, symbol: str, payload: Dict[str, Any]) -> None:
    key = _state_key(cfg.suggestions_prefix, symbol)
    r.hset(key, mapping={k: str(v) for k, v in payload.items()})


# ---------------------------------------------------------------------------
# Telegram notification helpers
# ---------------------------------------------------------------------------

def _generate_run_id() -> str:
    raw = f"cont_ctx-{time.time()}-{uuid.uuid4().hex[:8]}"
    return hashlib.sha256(raw.encode()).hexdigest()[:12]


def _send_telegram(
    r: Any,
    text: str,
    buttons_json: Optional[str] = None,
) -> None:
    """Push message to notify:telegram stream.""",
    fields: Dict[str, str] = {
        "type": "report",
        "text": text,
        "ts": str(_now_ms()),
    }
    if buttons_json:
        fields["buttons"] = buttons_json
    try:
        r.xadd(
            NOTIFY_STREAM, fields,
            maxlen=200000, approximate=True,
        )
    except Exception:
        pass


def _build_cont_ctx_buttons(run_id: str, symbols: List[str]) -> Optional[str]:
    """Build inline keyboard for cont_ctx window approval.""",
    if not symbols:
        return None
    buttons = [[
        {"text": f"✅ Apply ({len(symbols)} sym)", "callback_data": f"cont_ctx_approve:{run_id}"},
        {"text": "❌ Reject", "callback_data": f"cont_ctx_reject:{run_id}"},
    ]]
    return json.dumps(buttons, ensure_ascii=False, separators=(",", ":"))


def _store_cont_ctx_pending(
    r: Any,
    run_id: str,
    recommendations: Dict[str, Dict[str, Any]],
) -> None:
    """Store pending approval data for Telegram callback handler.""",
    now_ms = _now_ms()
    pending = {
        "run_id": run_id,
        "status": "PENDING",
        "action": "apply_cont_ctx_window",
        "symbols": list(recommendations.keys()),
        "recommendations": recommendations,
        "created_at_ms": now_ms,
        "last_reminder_ms": now_ms,
        "reminder_count": 0,
    }
    try:
        r.set(
            f"cont_ctx_calib:pending:{run_id}",
            json.dumps(pending, default=str),
            ex=PENDING_TTL_SEC,
        )
    except Exception:
        pass


def _build_recommendation_verdict(rec: Dict[str, Any]) -> List[str]:
    """Build pros/cons analysis lines for a single symbol recommendation.""",
    pros: List[str] = []
    cons: List[str] = []
    warnings: List[str] = []

    expectancy = float(rec.get("expectancy_r", 0))
    fb = float(rec.get("false_breakout_rate", 0))
    n = int(rec.get("sample_n", 0))
    rescued = int(rec.get("rescued_n", 0))
    utility = float(rec.get("utility_score", 0))
    confidence = float(rec.get("confidence", 0))
    exec_p95 = float(rec.get("exec_p95_norm", 0))
    baseline = int(rec.get("baseline_ms", 0))
    recommended = int(rec.get("recommended_ms", 0))
    delta_ms = recommended - baseline

    # --- Pros ---
    if expectancy >= 0.10:
        pros.append(f"✅ Высокая ожидаемость E[R]={expectancy:+.4f} (порог >0.02)")
    elif expectancy >= 0.02:
        pros.append(f"✅ E[R]={expectancy:+.4f} положительная")

    if rescued >= 30:
        pros.append(f"✅ Много спасённых сигналов: {rescued} (надёжная выборка)")
    elif rescued >= 20:
        pros.append(f"✅ {rescued} спасённых сигналов")

    if confidence >= 0.95:
        pros.append(f"✅ Высокая уверенность: {confidence:.0%}")
    elif confidence >= 0.80:
        pros.append(f"✅ Уверенность ОК: {confidence:.0%}")

    if fb <= 0.10:
        pros.append(f"✅ Низкий false breakout: {fb:.1%}")

    if utility >= 0.10:
        pros.append(f"✅ Сильный utility score: {utility:.4f}")

    if delta_ms <= 30000:
        pros.append(f"✅ Умеренное расширение: +{delta_ms // 1000}с")

    # --- Cons ---
    if fb > 0.18:
        cons.append(f"⚠️ False breakout высокий: {fb:.1%} (макс 22%)")
    elif fb > 0.15:
        warnings.append(f"⚡ False breakout близок к лимиту: {fb:.1%}")

    if exec_p95 > 0.65:
        cons.append(f"⚠️ Exec tail risk повышен: p95={exec_p95:.2f}")

    if n < 50:
        cons.append(f"⚠️ Ограниченная выборка: n={n} (чем больше, тем надёжнее)")

    if delta_ms > 60000:
        cons.append(f"⚠️ Большое расширение: +{delta_ms // 1000}с — контекст может быть устаревшим")
    elif delta_ms > 30000:
        warnings.append(f"⚡ Расширение +{delta_ms // 1000}с — проверьте stale penalty")

    if expectancy < 0.05:
        warnings.append(f"⚡ E[R] невысокая: {expectancy:+.4f} — граничный случай")

    if confidence < 0.85:
        warnings.append(f"⚡ Уверенность средняя: {confidence:.0%} — возможно стоит подождать больше данных")

    # --- Build output ---
    out: List[str] = []
    if pros:
        out.append("  <b>За применение:</b>")
        for p in pros:
            out.append(f"    {p}")
    if cons:
        out.append("  <b>Против применения:</b>")
        for c in cons:
            out.append(f"    {c}")
    if warnings:
        out.append("  <b>Обратить внимание:</b>")
        for w in warnings:
            out.append(f"    {w}")

    # --- Overall verdict ---
    if cons:
        out.append("  🔴 <b>Вердикт:</b> есть риски — рекомендуется подождать или отклонить")
    elif warnings:
        out.append("  🟡 <b>Вердикт:</b> допустимо, но есть нюансы")
    else:
        out.append("  🟢 <b>Вердикт:</b> метрики хорошие — рекомендуется применить")

    return out


def _format_cont_ctx_telegram_report(
    recommendations: Dict[str, Dict[str, Any]],
    run_id: str,
    mode: str,
) -> str:
    """Build Telegram HTML report for cont_ctx window calibration.""",
    lines = [
        "🔧 <b>Cont Ctx Window Calibrator</b>",
        "",
        f"📊 <b>Mode:</b> <code>{mode}</code>",
        f"📋 <b>Recommendations:</b> {len(recommendations)}",
        "",
    ]
    for sym, rec in sorted(recommendations.items()):
        baseline = rec.get("baseline_ms", 0)
        recommended = rec.get("recommended_ms", 0)
        expectancy = rec.get("expectancy_r", 0)
        fb = rec.get("false_breakout_rate", 0)
        n = rec.get("sample_n", 0)
        rescued = rec.get("rescued_n", 0)
        utility = rec.get("utility_score", 0)
        confidence = rec.get("confidence", 0)
        lines.append(
            f"  📈 <code>{sym:12s}</code> "
            f"<code>{baseline}ms</code> → <code>{recommended}ms</code>\n"
            f"     E[R]=<code>{expectancy:+.4f}</code> "
            f"FB=<code>{fb:.1%}</code> "
            f"n=<code>{n}</code> rescued=<code>{rescued}</code>\n"
            f"     utility=<code>{utility:.4f}</code> "
            f"conf=<code>{confidence:.1%}</code>"
        )
        # Add analytical commentary per symbol
        verdict_lines = _build_recommendation_verdict(rec)
        lines.extend(verdict_lines)
        lines.append("")

    lines.append("── 💡 <b>Что означает Apply/Reject</b> ──")
    lines.append("  <b>Apply</b> — расширить окно свежести cont_ctx.")
    lines.append("  Больше continuation-сигналов пройдут фильтр,")
    lines.append("  но контекст будет старше → выше риск stale entry.")
    lines.append("  <b>Reject</b> — оставить текущее окно.")
    lines.append("  Консервативнее, но можете пропускать рабочие сигналы.")
    lines.append("")
    if mode == "RECOMMEND":
        lines.append("<i>Mode=RECOMMEND — авто-применение отключено.</i>")
        lines.append("<b>Нажмите Apply для ручного применения:</b>")
    elif mode == "AUTO_APPLY":
        lines.append("<i>Mode=AUTO_APPLY — окно будет применено автоматически.</i>")
    lines.append("")
    lines.append(f"Run ID: <code>{run_id}</code>")
    return "\n".join(lines)


def _maybe_apply(r: Any, cfg: Cfg, symbol: str, recommended_ms: int, summary: Dict[str, Any]) -> str:
    """Attempt to auto-apply the recommended window (bounded, locked, with cooldown).""",
    if cfg.mode != "AUTO_APPLY":
        return "recommend_only"
    token = _acquire_apply_lock(r, cfg)
    if not token:
        APPLY_BLOCK.labels(symbol=symbol, reason="lock_busy").inc()
        return "blocked_lock"
    try:
        apply_key = _state_key(cfg.last_apply_prefix, symbol)
        last = r.hgetall(apply_key) or {}
        last_ts_ms = _i(last.get("applied_ts_ms") or last.get(b"applied_ts_ms"), 0)
        if last_ts_ms > 0 and (_now_ms() - last_ts_ms) < cfg.cooldown_sec * 1000:
            APPLY_BLOCK.labels(symbol=symbol, reason="cooldown").inc()
            return "blocked_cooldown"
        dyn_key = f"config:orderflow:{symbol}"
        cur = _i(r.hget(dyn_key, "cont_ctx_valid_ms") or cfg.baseline_ms, cfg.baseline_ms)
        bounded = _clamp_int(recommended_ms, cfg.min_ms, cfg.max_ms)
        if abs(int(bounded) - int(cur)) > int(cfg.max_step_ms):
            bounded = cur + cfg.max_step_ms if bounded > cur else cur - cfg.max_step_ms
        bounded = _clamp_int(bounded, cfg.min_ms, cfg.max_ms)
        r.hset(dyn_key, mapping={"cont_ctx_valid_ms": str(bounded)})
        r.hset(apply_key, mapping={
            "applied_ts_ms": str(_now_ms()),
            "applied_ms": str(bounded),
            "prev_ms": str(cur),
            "summary_run_id": str(summary.get("run_id") or ""),
        })
        APPLY.labels(symbol=symbol, status="applied").inc()
        return f"applied:{cur}->{bounded}"
    finally:
        _release_apply_lock(r, cfg, token)


def _refresh_recommendations(r: Any, cfg: Cfg) -> None:
    """Periodic recommendation engine: read outcomes, score windows, emit suggestions.""",
    rows = _read_recent_closed_trades(r, cfg)
    agg = _aggregate_outcomes(rows, cfg)
    by_symbol: Dict[str, Dict[int, Dict[str, Any]]] = {}
    for (symbol, window_ms), st in agg.items():
        by_symbol.setdefault(symbol, {})[window_ms] = st

    all_recommendations: Dict[str, Dict[str, Any]] = {}
    for symbol, win_map in by_symbol.items():
        best_window = 0
        best_utility = -999.0
        best_payload: Dict[str, Any] = {}
        baseline_exec = 0.0
        if cfg.baseline_ms in win_map:
            baseline_exec = _quantile(list(win_map[cfg.baseline_ms].get("exec_vals", [])), 0.95)
        for window_ms, st in sorted(win_map.items()):
            n = int(st.get("n", 0) or 0)
            SAMPLE_N.labels(symbol=symbol, window_ms=str(window_ms)).set(float(n))
            RESCUED_SHARE.labels(symbol=symbol, window_ms=str(window_ms)).set(float(st.get("rescued_n", 0) or 0) / float(max(1, n)))
            utility, expectancy, fb_rate, exec_p95, confidence = _score_candidate_window(st, cfg)
            if n < cfg.min_sample:
                continue
            if int(st.get("rescued_n", 0) or 0) < cfg.min_rescued:
                continue
            if expectancy < cfg.expectancy_min_r:
                continue
            if fb_rate > cfg.false_breakout_max:
                continue
            if exec_p95 > cfg.exec_p95_norm_max:
                continue
            if baseline_exec > 0.0 and (exec_p95 - baseline_exec) > cfg.exec_p95_delta_max:
                continue
            if confidence < cfg.confidence_min:
                continue
            if utility > best_utility:
                best_utility = utility
                best_window = int(window_ms)
                best_payload = {
                    "run_id": time.strftime("%Y%m%dT%H%M%SZ", time.gmtime()),
                    "baseline_ms": cfg.baseline_ms,
                    "recommended_ms": int(window_ms),
                    "status": "recommend",
                    "sample_n": n,
                    "rescued_n": int(st.get("rescued_n", 0) or 0),
                    "expectancy_r": round(expectancy, 6),
                    "false_breakout_rate": round(fb_rate, 6),
                    "exec_p95_norm": round(exec_p95, 6),
                    "utility_score": round(utility, 6),
                    "confidence": round(confidence, 6),
                    "why": "single_leg_cont_ctx rescued cohort positive and bounded",
                }

        if best_window <= 0:
            RUNS.labels(symbol=symbol, status="no_recommendation").inc()
            LAST_RUN_TS.labels(symbol=symbol).set(time.time())
            continue

        status = _maybe_apply(r, cfg, symbol, best_window, best_payload)
        best_payload["status"] = status if status != "recommend_only" else "recommend"
        _write_summary(r, cfg, symbol, best_payload)
        _write_suggestion(r, cfg, symbol, best_payload)
        all_recommendations[symbol] = dict(best_payload)
        RECOMMENDED.labels(symbol=symbol).set(float(best_window))
        CONFIDENCE.labels(symbol=symbol).set(float(best_payload.get("confidence", 0.0)))
        EXPECTANCY.labels(symbol=symbol).set(float(best_payload.get("expectancy_r", 0.0)))
        FALSE_BREAKOUT.labels(symbol=symbol).set(float(best_payload.get("false_breakout_rate", 0.0)))
        EXEC_P95.labels(symbol=symbol).set(float(best_payload.get("exec_p95_norm", 0.0)))
        LAST_RUN_TS.labels(symbol=symbol).set(time.time())
        RUNS.labels(symbol=symbol, status=str(best_payload.get("status") or "recommend")).inc()
        if str(best_payload.get("status") or "").startswith("applied"):
            APPLY.labels(symbol=symbol, status="applied").inc()

    # --- Send Telegram notification if recommendations were generated ---
    send_tg = _env("CONT_CTX_CALIB_TELEGRAM", "1").lower() in ("1", "true", "yes")
    if send_tg and all_recommendations:
        try:
            run_id = _generate_run_id()
            text = _format_cont_ctx_telegram_report(all_recommendations, run_id, cfg.mode)
            buttons_json = _build_cont_ctx_buttons(run_id, list(all_recommendations.keys()))
            if buttons_json:
                _store_cont_ctx_pending(r, run_id, all_recommendations)
            _send_telegram(r, text, buttons_json)
        except Exception:
            pass


def main() -> int:
    cfg = load_cfg()
    r = redis.Redis.from_url(cfg.redis_url, decode_responses=False)
    start_http_server(cfg.port)
    _ensure_group(r, cfg)

    last_refresh_ms = 0
    while True:
        t0 = time.perf_counter()
        try:
            rows = r.xreadgroup(cfg.group, cfg.consumer, {cfg.capture_stream: ">"}, count=cfg.read_count, block=cfg.block_ms)
            run_id = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
            for _stream, messages in rows or []:
                for msg_id, fields in messages:
                    c = _parse_capture(fields)
                    if c is None:
                        try:
                            r.xack(cfg.capture_stream, cfg.group, msg_id)
                        except Exception:
                            pass
                        continue
                    if is_candidate(c, cfg):
                        CANDIDATES.labels(symbol=c.symbol).inc()
                        for w in cfg.windows_ms:
                            if int(w) <= int(cfg.baseline_ms):
                                continue
                            if rescued_by_window(c, cfg.baseline_ms, int(w)):
                                RESCUED.labels(symbol=c.symbol, window_ms=str(w)).inc()
                                if cfg.shadow_enable == 1:
                                    _emit_shadow(r, cfg, c, int(w), run_id)
                    try:
                        r.xack(cfg.capture_stream, cfg.group, msg_id)
                    except Exception:
                        pass

            now_ms = _now_ms()
            if now_ms - last_refresh_ms >= 300_000:
                _refresh_recommendations(r, cfg)
                last_refresh_ms = now_ms
        except Exception:
            time.sleep(max(0.2, cfg.loop_sleep_s))
        finally:
            LOOP_LAT.observe(max(0.0, time.perf_counter() - t0))
        time.sleep(cfg.loop_sleep_s)


if __name__ == "__main__":
    raise SystemExit(main())
