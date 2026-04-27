from __future__ import annotations
from utils.time_utils import get_ny_time_millis

"""Conservative rollout controller for OFC contextual gate.

Purpose
-------
- Manage stage progression: shadow -> tighten_only -> replace_score_veto.
- Keep rollout limited to a canary symbol allowlist.
- Trigger rollback-to-shadow/off when health breaches occur.
- Write a tiny env overlay file that deployment wrappers may include.

Inputs
------
Reads compact Redis hashes produced by Patch C jobs/exporters:
- OFC_CTX_EXPORTER_SUMMARY_KEY
- OFC_CTX_WRITER_SUMMARY_KEY
- OFC_CTX_NIGHTLY_SUMMARY_KEY
- OFC_CTX_ROLLOUT_STATE_KEY

Outputs
-------
- Optional Redis hash update for rollout state.
- Optional env overlay file.
- Optional rollback flag file touch/remove.
- Compact rollout summary hash for exporter/dashboarding.
"""

import argparse
import json
import os
import time
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional

try:
    import redis  # type: ignore
except Exception:  # pragma: no cover
    redis = None  # type: ignore


MODES = ("off", "shadow", "tighten_only", "replace_score_veto")


def _env(name: str, default: str = "") -> str:
    return (os.getenv(name) or default).strip()


def _now_ms() -> int:
    return get_ny_time_millis()


def _to_float(v: Any, default: float = 0.0) -> float:
    try:
        return float(v)
    except Exception:
        return float(default)


def _to_int(v: Any, default: int = 0) -> int:
    try:
        return int(float(v))
    except Exception:
        return int(default)


def _parse_symbols(raw: str) -> List[str]:
    out: List[str] = []
    for part in str(raw or "").replace(";", ",").split(","):
        s = part.strip().upper()
        if s and s not in out:
            out.append(s)
    return out


def _redis_client(redis_url: str):
    if redis is None:
        return None
    try:
        return redis.Redis.from_url(redis_url, decode_responses=True)
    except Exception:
        return None


def _read_hash(client: Any, key: str) -> Dict[str, str]:
    if client is None or not key:
        return {}
    try:
        data = client.hgetall(key)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _write_hash(client: Any, key: str, mapping: Dict[str, Any]) -> None:
    if client is None or not key:
        return
    payload: Dict[str, str] = {}
    for k, v in mapping.items():
        if isinstance(v, (dict, list)):
            payload[str(k)] = json.dumps(v, ensure_ascii=False, separators=(",", ":"))
        else:
            payload[str(k)] = str(v)
    if payload:
        client.hset(key, mapping=payload)


@dataclass(frozen=True)
class RolloutInputs:
    observations: int
    shadow_disagree_rate: float
    fail_open_rate: float
    fallback_rate: float
    bundle_age_seconds: float
    writer_lag_seconds: float
    nightly_success: int
    rollback_requested: bool


@dataclass(frozen=True)
class RolloutDecision:
    current_mode: str
    desired_mode: str
    blocked: bool
    blocked_reason: str
    should_write_overlay: bool
    should_set_rollback_flag: bool
    should_clear_rollback_flag: bool


@dataclass(frozen=True)
class Thresholds:
    min_observations: int
    max_shadow_disagree_rate: float
    max_fail_open_rate: float
    max_fallback_rate: float
    max_bundle_age_seconds: float
    max_writer_lag_seconds: float
    min_hold_sec: int


def _sanitize_mode(raw: str, default: str = "shadow") -> str:
    s = str(raw or "").strip().lower()
    return s if s in MODES else default


def _healthy(inp: RolloutInputs, th: Thresholds) -> tuple[bool, str]:
    if inp.rollback_requested:
        return False, "rollback_requested"
    if inp.nightly_success <= 0:
        return False, "nightly_failed"
    if inp.observations < th.min_observations:
        return False, "insufficient_observations"
    if inp.shadow_disagree_rate > th.max_shadow_disagree_rate:
        return False, "shadow_disagree_rate"
    if inp.fail_open_rate > th.max_fail_open_rate:
        return False, "fail_open_rate"
    if inp.fallback_rate > th.max_fallback_rate:
        return False, "fallback_rate"
    if inp.bundle_age_seconds > th.max_bundle_age_seconds:
        return False, "bundle_age"
    if inp.writer_lag_seconds > th.max_writer_lag_seconds:
        return False, "writer_lag"
    return True, "ok"


def compute_rollout_decision(
    *,
    current_mode: str,
    last_change_ms: int,
    inputs: RolloutInputs,
    thresholds: Thresholds,
    force_mode: str = "",
    now_ms: Optional[int] = None,
) -> RolloutDecision:
    now_ms = int(now_ms if now_ms is not None else _now_ms())
    current = _sanitize_mode(current_mode, default="shadow")
    force = _sanitize_mode(force_mode, default="") if force_mode else ""
    healthy, reason = _healthy(inputs, thresholds)

    if force:
        desired = force
        blocked = False
        blocked_reason = "forced"
    elif not healthy:
        desired = "off" if current == "off" else "shadow"
        blocked = True
        blocked_reason = reason
    else:
        desired = current
        if current == "off":
            desired = "shadow"
        elif current == "shadow":
            desired = "tighten_only"
        elif current == "tighten_only":
            desired = "replace_score_veto"
        blocked = False
        blocked_reason = "ok"

    if desired != current and not force:
        age_ms = max(0, now_ms - int(last_change_ms or 0))
        if age_ms < int(thresholds.min_hold_sec) * 1000:
            desired = current
            blocked = True
            blocked_reason = "min_hold"

    set_rb = bool(desired in ("off", "shadow") and blocked_reason not in ("ok", "forced"))
    clr_rb = bool(not set_rb and desired in ("shadow", "tighten_only", "replace_score_veto"))
    return RolloutDecision(
        current_mode=current,
        desired_mode=desired,
        blocked=blocked,
        blocked_reason=blocked_reason,
        should_write_overlay=(desired != current) or set_rb or clr_rb,
        should_set_rollback_flag=set_rb,
        should_clear_rollback_flag=clr_rb,
    )


def _write_overlay(path: str, mode: str, canary_symbols: Iterable[str], rollback_flag_path: str) -> None:
    if not path:
        return
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = path + ".tmp"
    symbols = ",".join(_parse_symbols(",".join(canary_symbols)))
    lines = [
        f"OFC_CTX_ENABLE={'0' if mode == 'off' else '1'}",
        f"OFC_CTX_MODE={mode}",
        f"OFC_CTX_CANARY_SYMBOLS={symbols}",
        f"OFC_CTX_ROLLBACK_FLAG_PATH={rollback_flag_path}",
        "OFC_CTX_RUNTIME_SOURCE=ofc_contextual_rollout_controller_v1",
    ]
    with open(tmp, "w", encoding="utf-8") as fh:
        fh.write("\n".join(lines) + "\n")
    os.replace(tmp, path)


def _touch(path: str) -> None:
    if not path:
        return
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "a", encoding="utf-8"):
        os.utime(path, None)


def _rm(path: str) -> None:
    if path and os.path.exists(path):
        os.remove(path)


def _load_inputs(client: Any, args: argparse.Namespace) -> RolloutInputs:
    exporter = _read_hash(client, args.exporter_summary_key)
    writer = _read_hash(client, args.writer_summary_key)
    nightly = _read_hash(client, args.nightly_summary_key)

    observations = max(
        _to_int(exporter.get("observations", 0), 0),
        _to_int(writer.get("rows_written", 0), 0),
    )
    rollback_requested = bool(_to_int(exporter.get("rollback_requested", "0"), 0) > 0)
    if args.rollback_flag_path and os.path.exists(args.rollback_flag_path):
        rollback_requested = True

    return RolloutInputs(
        observations=observations,
        shadow_disagree_rate=_to_float(exporter.get("shadow_disagree_rate", 0.0), 0.0),
        fail_open_rate=_to_float(exporter.get("fail_open_rate", 0.0), 0.0),
        fallback_rate=_to_float(exporter.get("fallback_rate", 0.0), 0.0),
        bundle_age_seconds=_to_float(exporter.get("bundle_age_seconds", 0.0), 0.0),
        writer_lag_seconds=_to_float(writer.get("lag_seconds", 0.0), 0.0),
        nightly_success=_to_int(nightly.get("success", 0), 0),
        rollback_requested=rollback_requested,
    )


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--redis-url", default=_env("REDIS_URL", "redis://redis-worker-1:6379/0"))
    ap.add_argument("--apply", type=int, default=int(_env("OFC_CTX_ROLLOUT_APPLY", "0") or 0))
    ap.add_argument("--current-mode", default=_env("OFC_CTX_CURRENT_MODE", "shadow"))
    ap.add_argument("--force-mode", default=_env("OFC_CTX_FORCE_MODE", ""))
    ap.add_argument("--exporter-summary-key", default=_env("OFC_CTX_EXPORTER_SUMMARY_KEY", "metrics:ofc_contextual_exporter:last"))
    ap.add_argument("--writer-summary-key", default=_env("OFC_CTX_WRITER_SUMMARY_KEY", "metrics:ofc_contextual_writer:last"))
    ap.add_argument("--runtime-summary-key", default=_env("OFC_CTX_RUNTIME_SUMMARY_KEY", "metrics:ofc_contextual_runtime:last"))
    ap.add_argument("--nightly-summary-key", default=_env("OFC_CTX_NIGHTLY_SUMMARY_KEY", "metrics:ofc_contextual_nightly:last"))
    ap.add_argument("--rollout-state-key", default=_env("OFC_CTX_ROLLOUT_STATE_KEY", "cfg:ofc_contextual:rollout:v1"))
    ap.add_argument("--rollout-summary-key", default=_env("OFC_CTX_ROLLOUT_SUMMARY_KEY", "metrics:ofc_contextual_rollout:last"))
    ap.add_argument("--overlay-env-path", default=_env("OFC_CTX_OVERLAY_ENV_PATH", "/var/lib/trade/ofc_contextual_runtime_overlay.env"))
    ap.add_argument("--rollback-flag-path", default=_env("OFC_CTX_ROLLBACK_FLAG_PATH", "/var/lib/trade/ofc_contextual.rollback.flag"))
    ap.add_argument("--canary-symbols", default=_env("OFC_CTX_CANARY_SYMBOLS", "BTCUSDT,ETHUSDT,SOLUSDT"))
    ap.add_argument("--min-observations", type=int, default=int(_env("OFC_CTX_ROLLOUT_MIN_OBSERVATIONS", "300") or 300))
    ap.add_argument("--max-shadow-disagree-rate", type=float, default=float(_env("OFC_CTX_MAX_SHADOW_DISAGREE_RATE", "0.15") or 0.15))
    ap.add_argument("--max-fail-open-rate", type=float, default=float(_env("OFC_CTX_MAX_FAIL_OPEN_RATE", "0.02") or 0.02))
    ap.add_argument("--max-fallback-rate", type=float, default=float(_env("OFC_CTX_MAX_FALLBACK_RATE", "0.25") or 0.25))
    ap.add_argument("--max-bundle-age-sec", type=float, default=float(_env("OFC_CTX_MAX_BUNDLE_AGE_SEC", "21600") or 21600))
    ap.add_argument("--max-writer-lag-sec", type=float, default=float(_env("OFC_CTX_MAX_WRITER_LAG_SEC", "120") or 120))
    ap.add_argument("--min-hold-sec", type=int, default=int(_env("OFC_CTX_ROLLOUT_MIN_HOLD_SEC", "1800") or 1800))
    args = ap.parse_args()

    client = _redis_client(args.redis_url)
    state = _read_hash(client, args.rollout_state_key)
    current_mode = _sanitize_mode(args.current_mode or state.get("current_mode", "shadow"), default="shadow")
    last_change_ms = _to_int(state.get("last_change_ms", 0), 0)

    thresholds = Thresholds(
        min_observations=int(args.min_observations),
        max_shadow_disagree_rate=float(args.max_shadow_disagree_rate),
        max_fail_open_rate=float(args.max_fail_open_rate),
        max_fallback_rate=float(args.max_fallback_rate),
        max_bundle_age_seconds=float(args.max_bundle_age_sec),
        max_writer_lag_seconds=float(args.max_writer_lag_sec),
        min_hold_sec=int(args.min_hold_sec),
    )
    inputs = _load_inputs(client, args)
    decision = compute_rollout_decision(
        current_mode=current_mode,
        last_change_ms=last_change_ms,
        inputs=inputs,
        thresholds=thresholds,
        force_mode=args.force_mode,
    )

    runtime = _read_hash(client, args.runtime_summary_key)
    canary_symbols = _parse_symbols(args.canary_symbols)
    summary = {
        "ts_ms": _now_ms(),
        "apply": int(args.apply),
        "current_mode": decision.current_mode,
        "desired_mode": decision.desired_mode,
        "blocked": int(decision.blocked),
        "blocked_reason": decision.blocked_reason,
        "observations": int(inputs.observations),
        "shadow_disagree_rate": float(inputs.shadow_disagree_rate),
        "fail_open_rate": float(inputs.fail_open_rate),
        "fallback_rate": float(inputs.fallback_rate),
        "bundle_age_seconds": float(inputs.bundle_age_seconds),
        "writer_lag_seconds": float(inputs.writer_lag_seconds),
        "nightly_success": int(inputs.nightly_success),
        "rollback_requested": int(inputs.rollback_requested),
        "canary_symbols_count": len(canary_symbols),
        "runtime_summary_age_seconds": float(runtime.get("state_age_seconds", 0.0) or 0.0),
        "runtime_child_pid": int(_to_int(runtime.get("child_pid", 0), 0)),
        "runtime_child_uptime_seconds": float(runtime.get("child_uptime_seconds", 0.0) or 0.0),
        "runtime_restart_count": int(_to_int(runtime.get("restart_count", 0), 0)),
        "runtime_defer_active": int(_to_int(runtime.get("defer_active", 0), 0)),
        "runtime_cooldown_remaining_seconds": float(runtime.get("cooldown_remaining_seconds", 0.0) or 0.0),
        "runtime_last_restart_reason_kind": str(runtime.get("last_restart_reason_kind", "unknown") or "unknown"),
        "runtime_active_overlay_fingerprint": str(runtime.get("active_overlay_fingerprint", "") or ""),
    }

    if int(args.apply) == 1:
        _write_overlay(args.overlay_env_path, decision.desired_mode, canary_symbols, args.rollback_flag_path)
        if decision.should_set_rollback_flag:
            _touch(args.rollback_flag_path)
        elif decision.should_clear_rollback_flag:
            _rm(args.rollback_flag_path)
        _write_hash(
            client,
            args.rollout_state_key,
            {
                "current_mode": decision.desired_mode,
                "last_change_ms": _now_ms() if decision.desired_mode != decision.current_mode else int(last_change_ms or 0),
                "blocked": int(decision.blocked),
                "blocked_reason": decision.blocked_reason,
                "overlay_env_path": args.overlay_env_path,
                "rollback_flag_path": args.rollback_flag_path,
                "canary_symbols": ",".join(canary_symbols),
                "last_summary": summary,
            },
        )
    _write_hash(client, args.rollout_summary_key, summary)
    print(json.dumps(summary, ensure_ascii=False, separators=(",", ":")))
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
