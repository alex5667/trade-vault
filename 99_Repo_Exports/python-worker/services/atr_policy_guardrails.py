from __future__ import annotations

import json
import os
import time
from dataclasses import asdict, dataclass
from typing import Any, Dict

import redis


def _redis() -> redis.Redis:
    return redis.Redis.from_url(
        os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0"),
        decode_responses=True,
    )


def _safe_int(v: Any, default: int = 0) -> int:
    try:
        return int(v)
    except Exception:
        return default


def _safe_float(v: Any, default: float = 0.0) -> float:
    try:
        return float(v)
    except Exception:
        return default


def _cooldown_sec() -> int:
    return _safe_int(os.getenv("ATR_POLICY_ACTION_COOLDOWN_SEC", "1800"), 1800)


def _soft_min_n() -> int:
    return _safe_int(os.getenv("ATR_POLICY_SOFT_MIN_N", "50"), 50)


def _hard_min_n() -> int:
    return _safe_int(os.getenv("ATR_POLICY_HARD_MIN_N", "20"), 20)


def _marginal_pnl_bps() -> float:
    return _safe_float(os.getenv("ATR_POLICY_MARGIN_PNL_BPS", "0.5"), 0.5)


def _max_flips_per_day() -> int:
    return _safe_int(os.getenv("ATR_POLICY_MAX_FLIPS_PER_DAY", "3"), 3)


def _cohort_id(obj: Dict[str, Any]) -> str:
    return (
        f"{obj.get('source', '')}|{obj.get('symbol', '')}|{obj.get('scenario', '')}|"
        f"{obj.get('regime', '')}|{obj.get('risk_horizon_bucket', '')}"
    )


def cooldown_key(obj: Dict[str, Any]) -> str:
    return f"cfg:atr_policy:cooldown:{_cohort_id(obj)}"


def flip_count_key(obj: Dict[str, Any]) -> str:
    return f"cfg:atr_policy:flip_count:{_cohort_id(obj)}"


@dataclass(frozen=True)
class GuardrailResult:
    risk_class: str           # SAFE | WARN | BLOCK
    action: str
    require_confirm: bool
    reason_code: str
    reason_details: Dict[str, Any]


def _read_stop_evidence(obj: Dict[str, Any]) -> Dict[str, Any]:
    ev = obj.get("evidence", {})
    if not isinstance(ev, dict):
        return {}
    return ev.get("stop_ttl", {}) if isinstance(ev.get("stop_ttl"), dict) else {}


def _read_trailing_evidence(obj: Dict[str, Any]) -> Dict[str, Any]:
    ev = obj.get("evidence", {})
    if not isinstance(ev, dict):
        return {}
    return ev.get("trailing", {}) if isinstance(ev.get("trailing"), dict) else {}


def evaluate_guardrails(
    *, obj: Dict[str, Any], action: str, is_active: bool
) -> Dict[str, Any]:
    """
    Server-side operator guardrail evaluation.

    Returns a dict (from GuardrailResult) with:
      risk_class     SAFE | WARN | BLOCK
      action         echoed back
      require_confirm  whether a second-tap confirm token is needed
      reason_code    machine-readable code
      reason_details dict with supporting numbers
    """
    r = _redis()
    action = str(action or "").upper()
    now = int(time.time())

    # ── Emit prometheus metrics (best-effort) ─────────────────────────────
    def _inc_metric(risk_class: str, reason_code: str) -> None:
        try:
            from prometheus_client import Counter
            c = Counter(
                "atr_policy_guardrail_total",
                "ATR policy guardrail evaluations",
                ["action", "risk_class", "reason_code"],
            )
            c.labels(action=action, risk_class=risk_class, reason_code=reason_code).inc()
        except Exception:
            pass

    # ── 1. Cooldown check (always first) ─────────────────────────────────
    cd_raw = r.get(cooldown_key(obj))
    if cd_raw:
        try:
            cd = json.loads(cd_raw)
            until_ts = _safe_int(cd.get("until_ts"), 0)
            if until_ts > now:
                remaining = until_ts - now
                _inc_metric("BLOCK", "ATR_POLICY_COOLDOWN_ACTIVE")
                return asdict(GuardrailResult(
                    risk_class="BLOCK",
                    action=action,
                    require_confirm=False,
                    reason_code="ATR_POLICY_COOLDOWN_ACTIVE",
                    reason_details={"until_ts": until_ts, "cooldown_sec": remaining},
                ))
        except Exception:
            pass

    stop = _read_stop_evidence(obj)
    trail = _read_trailing_evidence(obj)

    stop_n = min(
        _safe_int(stop.get("n_canary"), 0),
        _safe_int(stop.get("n_control"), 0),
    ) if stop else 0
    trail_n = min(
        _safe_int(trail.get("n_canary"), 0),
        _safe_int(trail.get("n_control"), 0),
    ) if trail else 0
    pnl_delta_stop = (
        _safe_float(stop.get("pnl_canary"), 0.0) - _safe_float(stop.get("pnl_control"), 0.0)
    )
    pnl_delta_trail = (
        _safe_float(trail.get("pnl_canary"), 0.0) - _safe_float(trail.get("pnl_control"), 0.0)
    )

    # ── 2. APPROVE path ───────────────────────────────────────────────────
    if action == "APPROVE":
        # Hard block: sample far too small
        if stop_n > 0 and stop_n < _hard_min_n():
            _inc_metric("BLOCK", "ATR_POLICY_SAMPLE_TOO_LOW_BLOCK")
            return asdict(GuardrailResult(
                risk_class="BLOCK",
                action=action,
                require_confirm=False,
                reason_code="ATR_POLICY_SAMPLE_TOO_LOW_BLOCK",
                reason_details={"stop_n": stop_n, "hard_min_n": _hard_min_n()},
            ))
        if trail_n > 0 and trail_n < _hard_min_n():
            _inc_metric("BLOCK", "ATR_POLICY_TRAIL_SAMPLE_TOO_LOW_BLOCK")
            return asdict(GuardrailResult(
                risk_class="BLOCK",
                action=action,
                require_confirm=False,
                reason_code="ATR_POLICY_TRAIL_SAMPLE_TOO_LOW_BLOCK",
                reason_details={"trail_n": trail_n, "hard_min_n": _hard_min_n()},
            ))

        # Soft warn: sample below comfortable threshold
        if (0 < stop_n < _soft_min_n()) or (0 < trail_n < _soft_min_n()):
            _inc_metric("WARN", "ATR_POLICY_LOW_SAMPLE_WARN")
            return asdict(GuardrailResult(
                risk_class="WARN",
                action=action,
                require_confirm=True,
                reason_code="ATR_POLICY_LOW_SAMPLE_WARN",
                reason_details={
                    "stop_n": stop_n,
                    "trail_n": trail_n,
                    "soft_min_n": _soft_min_n(),
                }
            ))

        # Soft warn: both deltas are marginal
        if pnl_delta_stop < _marginal_pnl_bps() and pnl_delta_trail < _marginal_pnl_bps():
            _inc_metric("WARN", "ATR_POLICY_MARGINAL_EDGE_WARN")
            return asdict(GuardrailResult(
                risk_class="WARN",
                action=action,
                require_confirm=True,
                reason_code="ATR_POLICY_MARGINAL_EDGE_WARN",
                reason_details={
                    "pnl_delta_stop": pnl_delta_stop,
                    "pnl_delta_trail": pnl_delta_trail,
                    "threshold_bps": _marginal_pnl_bps(),
                }
            ))

        _inc_metric("SAFE", "ATR_POLICY_APPROVE_SAFE")
        return asdict(GuardrailResult(
            risk_class="SAFE",
            action=action,
            require_confirm=False,
            reason_code="ATR_POLICY_APPROVE_SAFE",
            reason_details={},
        ))

    # ── 3. REVOKE path ────────────────────────────────────────────────────
    if action == "REVOKE":
        if is_active:
            flips = _safe_int(r.get(flip_count_key(obj)), 0)
            if flips >= _max_flips_per_day():
                _inc_metric("BLOCK", "ATR_POLICY_FLIP_LIMIT_BLOCK")
                return asdict(GuardrailResult(
                    risk_class="BLOCK",
                    action=action,
                    require_confirm=False,
                    reason_code="ATR_POLICY_FLIP_LIMIT_BLOCK",
                    reason_details={"flip_count": flips, "max_flips": _max_flips_per_day()},
                ))
            _inc_metric("WARN", "ATR_POLICY_REVOKE_CONFIRM_REQUIRED")
            return asdict(GuardrailResult(
                risk_class="WARN",
                action=action,
                require_confirm=True,
                reason_code="ATR_POLICY_REVOKE_CONFIRM_REQUIRED",
                reason_details={"is_active": True, "flip_count": flips},
            ))
        _inc_metric("SAFE", "ATR_POLICY_REVOKE_SAFE")
        return asdict(GuardrailResult(
            risk_class="SAFE",
            action=action,
            require_confirm=False,
            reason_code="ATR_POLICY_REVOKE_SAFE",
            reason_details={"is_active": False},
        ))

    # ── 4. REJECT path ────────────────────────────────────────────────────
    if action == "REJECT":
        _inc_metric("SAFE", "ATR_POLICY_REJECT_SAFE")
        return asdict(GuardrailResult(
            risk_class="SAFE",
            action=action,
            require_confirm=False,
            reason_code="ATR_POLICY_REJECT_SAFE",
            reason_details={},
        ))

    # ── 5. Unknown action ─────────────────────────────────────────────────
    _inc_metric("BLOCK", "ATR_POLICY_UNKNOWN_ACTION")
    return asdict(GuardrailResult(
        risk_class="BLOCK",
        action=action,
        require_confirm=False,
        reason_code="ATR_POLICY_UNKNOWN_ACTION",
        reason_details={"action": action},
    ))


def arm_cooldown(obj: Dict[str, Any], *, actor: str, action: str) -> None:
    """Arm a cooldown key and increment daily flip counter for the cohort."""
    r = _redis()
    now = int(time.time())
    cd_sec = _cooldown_sec()
    payload = {
        "actor": actor,
        "action": action,
        "ts": now,
        "until_ts": now + cd_sec,
    }
    r.set(
        cooldown_key(obj),
        json.dumps(payload, ensure_ascii=False, sort_keys=True),
        ex=cd_sec,
    )
    r.incr(flip_count_key(obj))
    r.expire(flip_count_key(obj), 86400)

    # Emit metrics best-effort
    try:
        from prometheus_client import Counter
        Counter(
            "atr_policy_guardrail_cooldown_total",
            "ATR policy cooldowns armed",
        ).inc()
        Counter(
            "atr_policy_guardrail_flip_total",
            "ATR policy cohort flip counter increments",
        ).inc()
    except Exception:
        pass
