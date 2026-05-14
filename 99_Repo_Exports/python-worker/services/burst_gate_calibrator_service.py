from __future__ import annotations

"""
Burst Gate Calibrator Service

Monitors burst_gate:would_veto_share5m (Prometheus recording rule) and
auto-promotes burst_gate_mode across three stages:

  penalty  →  shadow  →  enforce (manual approval via Telegram)

Promotion criteria (per stage):
  penalty → shadow : proof_streak_days ≥ BURST_CALIB_MIN_STREAK_DAYS (default 7)
                     AND avg would_veto_share ∈ [MIN_SHARE, MAX_SHARE]
  shadow  → enforce: another BURST_CALIB_ENFORCE_STREAK_DAYS (default 7) streak
                     + Telegram approve button (no auto-enforce without human ACK)

Rollback (auto):
  share > MAX_SHARE for BURST_CALIB_ROLLBACK_STREAK consecutive checks → revert to penalty

State in Redis:
  cfg:burst_gate_calib:state   → JSON {mode, proof_streak, rollback_streak, ...}

Dynamic cfg written per symbol (takes effect without redeploy):
  config:orderflow:{symbol}    → HSET burst_gate_mode {penalty|shadow|enforce}

Telegram stream: notify:telegram

ENV:
  BURST_CALIB_ENABLE=1          enable the service
  BURST_CALIB_MIN_SHARE=0.05    minimum would_veto share to count as "useful"
  BURST_CALIB_MAX_SHARE=0.20    above this → calibration failure / rollback trigger
  BURST_CALIB_MIN_STREAK_DAYS=7 consecutive good days → promote penalty→shadow
  BURST_CALIB_ENFORCE_STREAK_DAYS=7  shadow streak before Telegram enforce prompt
  BURST_CALIB_ROLLBACK_STREAK=3 bad days → rollback
  BURST_CALIB_INTERVAL_SEC=3600 throttle between Telegram reports
  BURST_CALIB_PROM_URL=http://prometheus:9090
  REDIS_URL=redis://redis-worker-1:6379/0
  NOTIFY_STREAM=notify:telegram

Usage:
  python -m services.burst_gate_calibrator_service
"""

import contextlib
import json
import logging
import os
import time
import urllib.parse
import urllib.request
from dataclasses import asdict, dataclass, field
from typing import Any

try:
    from common.log import setup_logger
    logger = setup_logger("BurstGateCalibrator")
except ImportError:
    logger = logging.getLogger("BurstGateCalibrator")

try:
    import redis as redis_lib
except ImportError:
    redis_lib = None  # type: ignore[assignment]

try:
    from prometheus_client import Counter, Gauge
    _bgc_mode_gauge = Gauge("burst_gate_calib_mode", "0=penalty 1=shadow 2=enforce")
    _bgc_streak_gauge = Gauge("burst_gate_calib_proof_streak", "Consecutive good daily checks")
    _bgc_rollback_streak_gauge = Gauge("burst_gate_calib_rollback_streak", "Consecutive bad checks")
    _bgc_promote_total = Counter("burst_gate_calib_promote_total", "Promotions", ["from_mode", "to_mode"])
    _bgc_rollback_total = Counter("burst_gate_calib_rollback_total", "Rollbacks")
    _bgc_run_total = Counter("burst_gate_calib_run_total", "Calibration runs", ["result"])
    _HAS_PROM = True
except Exception:
    _HAS_PROM = False

from core.redis_keys import RedisStreams as RS

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

STATE_KEY = "cfg:burst_gate_calib:state"
STATE_TTL_SEC = 30 * 24 * 3600
PENDING_TTL_SEC = 48 * 3600
THROTTLE_KEY = "burst_gate_calib:last_tg_ts"
NOTIFY_STREAM = RS.NOTIFY_TELEGRAM

_MODE_INT = {"penalty": 0, "shadow": 1, "enforce": 2}


# ---------------------------------------------------------------------------
# Data
# ---------------------------------------------------------------------------

@dataclass
class BurstCalibState:
    mode: str = "penalty"
    proof_streak: int = 0
    rollback_streak: int = 0
    last_share: float = 0.0
    last_recommend: str = "hold"
    last_reason: str = ""
    n_symbols: int = 0
    updated_ms: int = 0


@dataclass
class BurstCalibResult:
    prev_mode: str
    effective_mode: str
    recommend: str          # hold | promote | rollback
    reason: str
    proof_streak: int
    rollback_streak: int
    avg_share: float
    n_symbols: int
    thresholds: dict[str, float] = field(default_factory=dict)

    @property
    def is_promote(self) -> bool:
        return self.effective_mode != self.prev_mode and self.recommend == "promote"

    @property
    def is_rollback(self) -> bool:
        return self.recommend == "rollback"


# ---------------------------------------------------------------------------
# Prometheus query
# ---------------------------------------------------------------------------

def _prom_query(prom_url: str, promql: str, timeout: int = 8) -> list[dict[str, Any]]:
    """Instant query against Prometheus HTTP API."""
    url = f"{prom_url.rstrip('/')}/api/v1/query?query={urllib.parse.quote(promql)}"
    try:
        req = urllib.request.Request(url, headers={"Accept": "application/json"})
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            data = json.loads(resp.read().decode())
        if data.get("status") != "success":
            logger.warning("Prometheus query failed: %s", data.get("error", "unknown"))
            return []
        return data["data"]["result"]
    except Exception as e:
        logger.warning("Prometheus query error (%s): %s", promql[:60], e)
        return []


def fetch_would_veto_shares(prom_url: str) -> dict[str, float]:
    """
    Returns {symbol: would_veto_share_5m} for all symbols.
    Falls back to global (no symbol label) if per-symbol data unavailable.
    """
    results = _prom_query(prom_url, "burst_gate:would_veto_share5m")
    shares: dict[str, float] = {}
    for item in results:
        sym = item.get("metric", {}).get("symbol", "GLOBAL")
        try:
            v = float(item["value"][1])
            if 0.0 <= v <= 1.0:
                shares[sym] = v
        except Exception:
            continue
    return shares


# ---------------------------------------------------------------------------
# Calibration logic (pure)
# ---------------------------------------------------------------------------

def evaluate_burst_gate_calibration(
    shares: dict[str, float],
    prev: BurstCalibState,
    *,
    min_share: float,
    max_share: float,
    min_streak: int,
    enforce_streak: int,
    rollback_streak_required: int,
) -> BurstCalibResult:
    """
    Pure calibration evaluation — no IO.

    Returns BurstCalibResult with promote/hold/rollback recommendation.
    """
    n_sym = len(shares)
    avg_share = sum(shares.values()) / max(n_sym, 1) if shares else 0.0
    prev_mode = prev.mode

    proof_streak = prev.proof_streak
    rollback_streak = prev.rollback_streak
    effective_mode = prev_mode
    recommend = "hold"
    reason = "ok"

    share_ok = min_share <= avg_share <= max_share

    if share_ok:
        proof_streak += 1
        rollback_streak = 0
    else:
        # Any bad day breaks the consecutive-streak requirement — reset, not just freeze.
        proof_streak = 0
        rollback_streak += 1
        if avg_share < min_share:
            reason = f"share_too_low:{avg_share:.1%}<{min_share:.0%}"
        else:
            reason = f"share_too_high:{avg_share:.1%}>{max_share:.0%}"

    # ── Rollback check (before promote — rollback wins) ──────────────────────
    if rollback_streak >= rollback_streak_required and prev_mode in ("shadow", "enforce"):
        effective_mode = "penalty"
        proof_streak = 0
        recommend = "rollback"
        reason = f"rollback_streak:{rollback_streak}>={rollback_streak_required}|{reason}"

    # ── Promote: penalty → shadow ─────────────────────────────────────────────
    elif prev_mode == "penalty" and proof_streak >= min_streak:
        effective_mode = "shadow"
        recommend = "promote"
        reason = f"streak:{proof_streak}>={min_streak}|share:{avg_share:.1%}"

    # ── Promote: shadow → enforce request (Telegram only, no auto) ───────────
    elif prev_mode == "shadow" and proof_streak >= min_streak + enforce_streak:
        # Do NOT auto-promote to enforce; just send Telegram request.
        # Calibrator sets recommend=promote but keeps mode=shadow until ACK.
        recommend = "promote"
        reason = f"enforce_ready:streak:{proof_streak}|share:{avg_share:.1%}"

    elif not share_ok and prev_mode == "penalty":
        reason = reason  # already set above, stay in penalty, no promotion

    return BurstCalibResult(
        prev_mode=prev_mode,
        effective_mode=effective_mode,
        recommend=recommend,
        reason=reason,
        proof_streak=proof_streak,
        rollback_streak=rollback_streak,
        avg_share=avg_share,
        n_symbols=n_sym,
        thresholds={"min_share": min_share, "max_share": max_share},
    )


# ---------------------------------------------------------------------------
# State persistence
# ---------------------------------------------------------------------------

def _load_state(r: Any) -> BurstCalibState:
    try:
        raw = r.get(STATE_KEY)
        if raw:
            d = json.loads(raw)
            return BurstCalibState(**{k: v for k, v in d.items() if k in BurstCalibState.__dataclass_fields__})
    except Exception as e:
        logger.warning("load_state failed: %s", e)
    return BurstCalibState()


def _save_state(r: Any, result: BurstCalibResult) -> None:
    state = BurstCalibState(
        mode=result.effective_mode,
        proof_streak=result.proof_streak,
        rollback_streak=result.rollback_streak,
        last_share=round(result.avg_share, 4),
        last_recommend=result.recommend,
        last_reason=result.reason,
        n_symbols=result.n_symbols,
        updated_ms=int(time.time() * 1000),
    )
    try:
        r.set(STATE_KEY, json.dumps(asdict(state), default=str), ex=STATE_TTL_SEC)
    except Exception as e:
        logger.error("save_state failed: %s", e)


# ---------------------------------------------------------------------------
# Dynamic cfg apply (no redeploy needed — override wins in merged_cfg)
# ---------------------------------------------------------------------------

def _apply_dynamic_cfg(r: Any, mode: str) -> int:
    """
    Write burst_gate_mode to all config:orderflow:{symbol} Redis hashes.
    Returns count of keys updated.
    """
    updated = 0
    cursor = 0
    try:
        while True:
            cursor, keys = r.scan(cursor=cursor, match="config:orderflow:*", count=500)
            pipe = r.pipeline(transaction=False)
            for key in keys:
                # Skip per-field sub-keys like config:orderflow:BTCUSDT:obi_stable
                parts = key.split(":")
                if len(parts) != 3:
                    continue
                pipe.hset(key, "burst_gate_mode", mode)
                updated += 1
            pipe.execute()
            if cursor == 0:
                break
    except Exception as e:
        logger.error("apply_dynamic_cfg failed: %s", e)
    logger.info("Applied burst_gate_mode=%s to %d config:orderflow:* keys", mode, updated)
    return updated


# ---------------------------------------------------------------------------
# Telegram
# ---------------------------------------------------------------------------

def _send_telegram(r: Any, text: str, buttons_json: str | None = None) -> None:
    stream = os.getenv("NOTIFY_STREAM", NOTIFY_STREAM)
    fields: dict[str, str] = {
        "type": "report",
        "text": text,
        "ts": str(int(time.time() * 1000)),
    }
    if buttons_json:
        fields["buttons"] = buttons_json
    with contextlib.suppress(Exception):
        r.xadd(stream, fields, maxlen=200_000, approximate=True)
    logger.info("Telegram report sent (buttons=%s)", bool(buttons_json))


def _throttle_ok(r: Any, interval_sec: int) -> bool:
    try:
        last = r.get(THROTTLE_KEY)
        if last and (time.time() - float(last)) < interval_sec:
            return False
    except Exception:
        pass
    return True


def _record_throttle(r: Any, interval_sec: int) -> None:
    with contextlib.suppress(Exception):
        r.set(THROTTLE_KEY, str(time.time()), ex=interval_sec * 3)


def _store_pending_approve(r: Any, result: BurstCalibResult) -> str:
    """Store pending enforcement approval so notify_worker can handle callback."""
    run_id = f"bgc-{int(time.time())}"
    pending = {
        "run_id": run_id,
        "status": "PENDING",
        "action": "burst_gate_enforce",
        "avg_share": round(result.avg_share, 4),
        "proof_streak": result.proof_streak,
        "n_symbols": result.n_symbols,
        "created_at_ms": int(time.time() * 1000),
        "last_reminder_ms": int(time.time() * 1000),
        "reminder_count": 0,
    }
    with contextlib.suppress(Exception):
        r.set(f"burst_gate_calib:pending:{run_id}", json.dumps(pending, default=str), ex=PENDING_TTL_SEC)
    return run_id


def _format_report(result: BurstCalibResult, run_id: str | None = None) -> str:
    mode_emoji = {"penalty": "🟡", "shadow": "🟠", "enforce": "🔴"}
    rec_emoji  = {"hold": "⏸️", "promote": "⬆️", "rollback": "⬇️"}

    m_e = mode_emoji.get(result.effective_mode, "❓")
    r_e = rec_emoji.get(result.recommend, "❓")

    lines = [
        "💥 <b>Burst Gate Calibrator</b>",
        "",
        f"📊 <b>Mode:</b> {m_e} <code>{result.effective_mode}</code>",
        f"🎯 <b>Recommend:</b> {r_e} <code>{result.recommend}</code>",
        f"📝 <b>Reason:</b> <code>{result.reason}</code>",
        "",
        f"── Метрики ──",
        f"📈 Would-veto share: <code>{result.avg_share:.1%}</code>  "
        f"(ok: {result.thresholds.get('min_share', 0):.0%}–{result.thresholds.get('max_share', 0):.0%})",
        f"🔢 Symbols tracked: <code>{result.n_symbols}</code>",
        "",
        f"── Proof Streak ──",
        f"📊 Streak: <code>{result.proof_streak}</code>",
    ]
    if result.rollback_streak > 0:
        lines.append(f"⚠️ Rollback streak: <code>{result.rollback_streak}</code>")
    if run_id:
        lines.append(f"\nRun ID: <code>{run_id}</code>")
    return "\n".join(lines)


def _build_enforce_buttons(run_id: str) -> str:
    buttons = [[
        {"text": "✅ Включить enforce", "callback_data": f"bgc_approve:{run_id}"},
        {"text": "⬇️ Оставить shadow", "callback_data": f"bgc_reject:{run_id}"},
    ]]
    return json.dumps(buttons, ensure_ascii=False, separators=(",", ":"))


# ---------------------------------------------------------------------------
# Prometheus metrics update
# ---------------------------------------------------------------------------

def _update_prometheus(result: BurstCalibResult) -> None:
    if not _HAS_PROM:
        return
    with contextlib.suppress(Exception):
        _bgc_mode_gauge.set(_MODE_INT.get(result.effective_mode, 0))
        _bgc_streak_gauge.set(result.proof_streak)
        _bgc_rollback_streak_gauge.set(result.rollback_streak)
        _bgc_run_total.labels(result=result.recommend).inc()
        if result.is_promote:
            _bgc_promote_total.labels(from_mode=result.prev_mode, to_mode=result.effective_mode).inc()
        if result.is_rollback:
            _bgc_rollback_total.inc()


# ---------------------------------------------------------------------------
# Main orchestrator
# ---------------------------------------------------------------------------

def run_burst_gate_calibration(
    redis_url: str,
    prom_url: str,
    *,
    min_share: float = 0.05,
    max_share: float = 0.20,
    min_streak_days: int = 7,
    enforce_streak_days: int = 7,
    rollback_streak: int = 3,
    send_telegram: bool = True,
    telegram_interval_sec: int = 3600,
) -> BurstCalibResult | None:
    """
    Run one calibration cycle.

    1. Fetch burst_gate:would_veto_share5m from Prometheus
    2. Load state from Redis
    3. Evaluate promotion logic
    4. Apply mode changes to dynamic_cfg (no redeploy needed)
    5. Send Telegram report
    6. Update Prometheus gauges
    """
    if redis_lib is None:
        logger.error("redis not available")
        return None

    r = redis_lib.Redis.from_url(redis_url, decode_responses=True)

    # 1. Fetch Prometheus data
    shares = fetch_would_veto_shares(prom_url)
    if not shares:
        logger.warning("No burst_gate:would_veto_share5m data from Prometheus — skipping")
        return None

    # 2. Load state
    prev = _load_state(r)

    # 3. Evaluate
    result = evaluate_burst_gate_calibration(
        shares,
        prev,
        min_share=min_share,
        max_share=max_share,
        min_streak=min_streak_days,
        enforce_streak=enforce_streak_days,
        rollback_streak_required=rollback_streak,
    )

    logger.info(
        "BurstGateCalib: mode=%s→%s recommend=%s streak=%d avg_share=%.1%%",
        result.prev_mode, result.effective_mode, result.recommend,
        result.proof_streak, result.avg_share * 100,
    )

    # 4. Apply dynamic cfg (only on state change)
    if result.effective_mode != result.prev_mode:
        # penalty/shadow written immediately; enforce requires manual approve
        if result.effective_mode in ("penalty", "shadow"):
            _apply_dynamic_cfg(r, result.effective_mode)
        # On rollback, also update Prometheus alert comment in env notes

    # 5. Telegram
    run_id = None
    if send_telegram and _throttle_ok(r, telegram_interval_sec):
        # Always report on promote/rollback; throttle on hold
        should_report = result.recommend in ("promote", "rollback") or result.effective_mode != result.prev_mode

        # For enforce request: store pending + show buttons
        enforce_request = (
            result.recommend == "promote"
            and result.effective_mode == "shadow"
            and result.proof_streak >= min_streak_days + enforce_streak_days
        )

        if should_report or enforce_request:
            run_id = _store_pending_approve(r, result) if enforce_request else None
            text = _format_report(result, run_id)
            assert not enforce_request or run_id is not None
            buttons = _build_enforce_buttons(run_id) if enforce_request else None  # type: ignore[arg-type]
            _send_telegram(r, text, buttons)
            _record_throttle(r, telegram_interval_sec)

    # 6. Save state
    _save_state(r, result)

    # 7. Prometheus
    _update_prometheus(result)

    return result


# ---------------------------------------------------------------------------
# Telegram callback handler (called by notify_worker on button press)
# ---------------------------------------------------------------------------

def handle_enforce_approve(r: Any, run_id: str) -> str:
    """
    Called when user taps ✅ Включить enforce in Telegram.
    Writes burst_gate_mode=enforce to all config:orderflow:* and updates state.
    """
    pending_key = f"burst_gate_calib:pending:{run_id}"
    try:
        raw = r.get(pending_key)
        if not raw:
            return f"❌ Pending approval {run_id} not found or expired"
        pending = json.loads(raw)
        if pending.get("status") != "PENDING":
            return f"⚠️ Already handled: status={pending.get('status')}"
    except Exception as e:
        return f"❌ Error loading pending: {e}"

    n = _apply_dynamic_cfg(r, "enforce")

    # Update state
    try:
        state = _load_state(r)
        state.mode = "enforce"
        state.updated_ms = int(time.time() * 1000)
        r.set(STATE_KEY, json.dumps(asdict(state), default=str), ex=STATE_TTL_SEC)
    except Exception as e:
        logger.error("Failed to update state on enforce approve: %s", e)

    # Mark pending as handled
    with contextlib.suppress(Exception):
        pending["status"] = "APPROVED"
        r.set(pending_key, json.dumps(pending, default=str), ex=3600)

    if _HAS_PROM:
        with contextlib.suppress(Exception):
            _bgc_mode_gauge.set(2)
            _bgc_promote_total.labels(from_mode="shadow", to_mode="enforce").inc()

    msg = f"✅ burst_gate_mode=enforce applied to {n} symbols (run_id={run_id})"
    logger.info(msg)
    return msg


def handle_enforce_reject(r: Any, run_id: str) -> str:
    """Called when user taps ⬇️ Оставить shadow."""
    pending_key = f"burst_gate_calib:pending:{run_id}"
    with contextlib.suppress(Exception):
        raw = r.get(pending_key)
        if raw:
            pending = json.loads(raw)
            pending["status"] = "REJECTED"
            r.set(pending_key, json.dumps(pending, default=str), ex=3600)
    msg = f"⬇️ Enforce rejected — staying in shadow (run_id={run_id})"
    logger.info(msg)
    return msg


# ---------------------------------------------------------------------------
# CLI entrypoint
# ---------------------------------------------------------------------------

def run_once() -> None:
    if os.getenv("BURST_CALIB_ENABLE", "0").strip().lower() not in ("1", "true", "yes"):
        logger.info("BurstGateCalibrator disabled (BURST_CALIB_ENABLE != 1)")
        return

    redis_url = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
    prom_url = os.getenv("BURST_CALIB_PROM_URL", os.getenv("PROMETHEUS_URL", "http://prometheus:9090"))
    min_share = float(os.getenv("BURST_CALIB_MIN_SHARE", "0.05"))
    max_share = float(os.getenv("BURST_CALIB_MAX_SHARE", "0.20"))
    min_streak = int(os.getenv("BURST_CALIB_MIN_STREAK_DAYS", "7"))
    enforce_streak = int(os.getenv("BURST_CALIB_ENFORCE_STREAK_DAYS", "7"))
    rollback_streak = int(os.getenv("BURST_CALIB_ROLLBACK_STREAK", "3"))
    send_tg = os.getenv("BURST_CALIB_TELEGRAM", "1").strip().lower() in ("1", "true", "yes")
    tg_interval = int(os.getenv("BURST_CALIB_INTERVAL_SEC", "3600"))

    result = run_burst_gate_calibration(
        redis_url, prom_url,
        min_share=min_share,
        max_share=max_share,
        min_streak_days=min_streak,
        enforce_streak_days=enforce_streak,
        rollback_streak=rollback_streak,
        send_telegram=send_tg,
        telegram_interval_sec=tg_interval,
    )
    if result:
        logger.info(
            "Done: mode=%s recommend=%s streak=%d avg_share=%.1%%",
            result.effective_mode, result.recommend, result.proof_streak, result.avg_share * 100,
        )


if __name__ == "__main__":
    run_once()
