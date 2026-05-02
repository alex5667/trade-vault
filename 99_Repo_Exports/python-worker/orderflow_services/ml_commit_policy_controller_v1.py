from __future__ import annotations
from utils.time_utils import get_ny_time_millis

import json
import os
import random
import time
from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple

try:  # pragma: no cover
    import redis.asyncio as redis  # type: ignore
except Exception:  # pragma: no cover
    redis = None  # type: ignore

from prometheus_client import Counter, Gauge, Histogram, start_http_server


APPLY_REQUESTS_STREAM = os.getenv(
    "ML_COMMIT_POLICY_INPUT_STREAM",
    "stream:ml:recommendation_apply_requests",
)
COMMIT_REQUESTS_STREAM = os.getenv(
    "ML_COMMIT_POLICY_COMMIT_STREAM",
    "stream:ml:recommendation_commit_requests",
)
AUDIT_STREAM = os.getenv(
    "ML_COMMIT_POLICY_AUDIT_STREAM",
    "stream:ml:recommendation_audit",
)
RESULTS_STREAM = os.getenv(
    "ML_COMMIT_POLICY_RESULTS_STREAM",
    "stream:ml:commit_policy_results",
)
GLOBAL_POLICY_KEY = os.getenv(
    "ML_COMMIT_POLICY_GLOBAL_KEY",
    "cfg:ml:commit_policy:global",
)
ACTION_POLICY_PREFIX = os.getenv(
    "ML_COMMIT_POLICY_ACTION_PREFIX",
    "cfg:ml:commit_policy:action:",
)
STATE_PREFIX = os.getenv(
    "ML_COMMIT_POLICY_STATE_PREFIX",
    "state:ml:commit_policy:",
)
GROUP = os.getenv("ML_COMMIT_POLICY_GROUP", "cg:ml_commit_policy_controller")
CONSUMER = os.getenv("ML_COMMIT_POLICY_CONSUMER", os.getenv("HOSTNAME", "ml-commit-policy-1"))

RUNS = Counter("ml_commit_policy_runs_total", "Controller runs", ["status"])
APPROVED = Counter("ml_commit_policy_commit_approved_total", "Approved commits", ["action"])
BLOCKED = Counter("ml_commit_policy_commit_blocked_total", "Blocked commits", ["action", "reason"])
LAST_RUN = Gauge("ml_commit_policy_last_run_ts_seconds", "Last successful run ts")
QUEUE_LAG_MS = Gauge("ml_commit_policy_queue_lag_ms", "Queue lag ms")
LOOP_SECONDS = Histogram("ml_commit_policy_loop_seconds", "Loop duration")


def _now_ms() -> int:
    return get_ny_time_millis()


def _s(v: Any, d: str = "") -> str:
    return d if v is None else str(v)


def _i(v: Any, d: int = 0) -> int:
    try:
        return int(float(v))
    except Exception:
        return d


def _f(v: Any, d: float = 0.0) -> float:
    try:
        return float(v)
    except Exception:
        return d


def _b(v: Any, d: bool = False) -> bool:
    if v is None:
        return d
    if isinstance(v, bool):
        return v
    s = str(v).strip().lower()
    if s in {"1", "true", "yes", "on", "enabled"}:
        return True
    if s in {"0", "false", "no", "off", "disabled"}:
        return False
    return d


def _stable_share_decision(seed: str, share: float) -> bool:
    if share <= 0.0:
        return False
    if share >= 1.0:
        return True
    h = 1469598103934665603
    for ch in seed.encode("utf-8", errors="ignore"):
        h ^= ch
        h *= 1099511628211
        h &= 0xFFFFFFFFFFFFFFFF
    bucket = (h % 10_000) / 10_000.0
    return bucket < share


@dataclass
class CommitPolicyDecision:
    allow_commit: bool
    reason: str
    policy_mode: str
    cooldown_remaining_sec: int
    commit_stream: str
    dry_run_only: bool


def build_effective_action_policy(
    global_cfg: Dict[str, Any],
    action_cfg: Dict[str, Any],
) -> Dict[str, Any]:
    executor_mode = _s(action_cfg.get("executor_mode", global_cfg.get("executor_mode", "DRY_RUN")), "DRY_RUN").upper()
    return {
        "global_commit_enabled": _b(global_cfg.get("commit_enabled", "0"), False),
        "global_kill_switch": _b(global_cfg.get("kill_switch", "0"), False),
        "kill_reason": _s(global_cfg.get("kill_reason", ""), ""),
        "action_enabled": _b(action_cfg.get("enabled", "0"), False),
        "cooldown_sec": _i(action_cfg.get("cooldown_sec", global_cfg.get("default_cooldown_sec", 3600)), 3600),
        "require_replay_pass": _b(action_cfg.get("require_replay_pass", "1"), True),
        "max_commits_per_hour": _i(action_cfg.get("max_commits_per_hour", 2), 2),
        "canary_share": max(0.0, min(1.0, _f(action_cfg.get("canary_share", 1.0), 1.0))),
        "executor_mode": executor_mode,
        "high_risk_block": _b(action_cfg.get("high_risk_block", "1"), True),
    }


def evaluate_commit_policy(
    *,
    recommendation_id: str,
    action_type: str,
    replay_status: str,
    risk_level: str,
    approvals: int,
    min_approvals: int,
    last_commit_ts_ms: int,
    commits_last_hour: int,
    now_ms: int,
    global_cfg: Dict[str, Any],
    action_cfg: Dict[str, Any],
) -> CommitPolicyDecision:
    policy = build_effective_action_policy(global_cfg, action_cfg)

    if policy["global_kill_switch"]:
        return CommitPolicyDecision(
            allow_commit=False,
            reason=f"global_kill_switch:{policy['kill_reason'] or 'manual'}",
            policy_mode=policy["executor_mode"],
            cooldown_remaining_sec=0,
            commit_stream=COMMIT_REQUESTS_STREAM,
            dry_run_only=True,
        )

    if not policy["global_commit_enabled"]:
        return CommitPolicyDecision(
            allow_commit=False,
            reason="global_commit_disabled",
            policy_mode=policy["executor_mode"],
            cooldown_remaining_sec=0,
            commit_stream=COMMIT_REQUESTS_STREAM,
            dry_run_only=True,
        )

    if not policy["action_enabled"]:
        return CommitPolicyDecision(
            allow_commit=False,
            reason="action_disabled",
            policy_mode=policy["executor_mode"],
            cooldown_remaining_sec=0,
            commit_stream=COMMIT_REQUESTS_STREAM,
            dry_run_only=True,
        )

    if approvals < min_approvals:
        return CommitPolicyDecision(
            allow_commit=False,
            reason="insufficient_approvals",
            policy_mode=policy["executor_mode"],
            cooldown_remaining_sec=0,
            commit_stream=COMMIT_REQUESTS_STREAM,
            dry_run_only=True,
        )

    if policy["require_replay_pass"] and _s(replay_status, "").upper() != "PASS":
        return CommitPolicyDecision(
            allow_commit=False,
            reason="replay_required",
            policy_mode=policy["executor_mode"],
            cooldown_remaining_sec=0,
            commit_stream=COMMIT_REQUESTS_STREAM,
            dry_run_only=True,
        )

    if policy["high_risk_block"] and _s(risk_level, "").lower() in {"high", "critical"}:
        return CommitPolicyDecision(
            allow_commit=False,
            reason="high_risk_blocked",
            policy_mode=policy["executor_mode"],
            cooldown_remaining_sec=0,
            commit_stream=COMMIT_REQUESTS_STREAM,
            dry_run_only=True,
        )

    if commits_last_hour >= policy["max_commits_per_hour"]:
        return CommitPolicyDecision(
            allow_commit=False,
            reason="rate_limit_hour",
            policy_mode=policy["executor_mode"],
            cooldown_remaining_sec=0,
            commit_stream=COMMIT_REQUESTS_STREAM,
            dry_run_only=True,
        )

    elapsed_ms = max(0, now_ms - max(0, last_commit_ts_ms))
    cooldown_ms = max(0, int(policy["cooldown_sec"]) * 1000)
    if cooldown_ms > 0 and last_commit_ts_ms > 0 and elapsed_ms < cooldown_ms:
        return CommitPolicyDecision(
            allow_commit=False,
            reason="cooldown_active",
            policy_mode=policy["executor_mode"],
            cooldown_remaining_sec=max(0, (cooldown_ms - elapsed_ms) // 1000),
            commit_stream=COMMIT_REQUESTS_STREAM,
            dry_run_only=True,
        )

    if not _stable_share_decision(f"{action_type}:{recommendation_id}", float(policy["canary_share"])):
        return CommitPolicyDecision(
            allow_commit=False,
            reason="canary_share_skip",
            policy_mode=policy["executor_mode"],
            cooldown_remaining_sec=0,
            commit_stream=COMMIT_REQUESTS_STREAM,
            dry_run_only=True,
        )

    dry_run_only = policy["executor_mode"] != "COMMIT"
    return CommitPolicyDecision(
        allow_commit=True,
        reason="approved",
        policy_mode=policy["executor_mode"],
        cooldown_remaining_sec=0,
        commit_stream=COMMIT_REQUESTS_STREAM,
        dry_run_only=dry_run_only,
    )


async def _load_hash(r: "redis.Redis", key: str) -> Dict[str, Any]:
    try:
        raw = await r.hgetall(key)
        return {(_s(k)): (_s(v)) for k, v in (raw or {}).items()}
    except Exception:
        return {}


async def _get_state(r: "redis.Redis", action_type: str) -> Tuple[int, int]:
    key = f"{STATE_PREFIX}{action_type}"
    try:
        raw = await r.hgetall(key)
        last_commit_ts_ms = _i(raw.get("last_commit_ts_ms", 0), 0)
        commits_last_hour = _i(raw.get("commits_last_hour", 0), 0)
        window_ts_ms = _i(raw.get("window_ts_ms", 0), 0)
        now_ms = _now_ms()
        if window_ts_ms <= 0 or now_ms - window_ts_ms > 3600_000:
            commits_last_hour = 0
        return last_commit_ts_ms, commits_last_hour
    except Exception:
        return 0, 0


async def _update_state(r: "redis.Redis", action_type: str, now_ms: int) -> None:
    key = f"{STATE_PREFIX}{action_type}"
    try:
        raw = await r.hgetall(key)
        commits_last_hour = _i(raw.get("commits_last_hour", 0), 0)
        window_ts_ms = _i(raw.get("window_ts_ms", 0), 0)
        if window_ts_ms <= 0 or now_ms - window_ts_ms > 3600_000:
            commits_last_hour = 0
            window_ts_ms = now_ms
        commits_last_hour += 1
        await r.hset(
            key,
            mapping={
                "last_commit_ts_ms": str(now_ms),
                "commits_last_hour": str(commits_last_hour),
                "window_ts_ms": str(window_ts_ms),
            }
        )
        await r.expire(key, 7200)
    except Exception:
        return


async def _emit_audit(r: "redis.Redis", payload: Dict[str, Any]) -> None:
    try:
        await r.xadd(AUDIT_STREAM, payload, maxlen=200_000, approximate=True)
    except Exception:
        return


async def _emit_result(r: "redis.Redis", payload: Dict[str, Any]) -> None:
    try:
        await r.xadd(RESULTS_STREAM, payload, maxlen=200_000, approximate=True)
    except Exception:
        return


def _parse_message(msg: Dict[bytes, bytes]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for k, v in msg.items():
        kk = k.decode() if isinstance(k, (bytes, bytearray)) else str(k)
        vv = v.decode() if isinstance(v, (bytes, bytearray)) else v
        out[kk] = vv
    return out


async def main() -> None:  # pragma: no cover
    if redis is None:
        raise RuntimeError("redis.asyncio is required")
    port = _i(os.getenv("ML_COMMIT_POLICY_METRICS_PORT", 9870), 9870)
    start_http_server(port)
    r = redis.from_url(os.getenv("REDIS_URL", "redis://localhost:6379/0"))
    try:
        await r.xgroup_create(APPLY_REQUESTS_STREAM, GROUP, id="0", mkstream=True)
    except Exception:
        pass

    min_approvals = _i(os.getenv("ML_COMMIT_POLICY_MIN_APPROVALS", 1), 1)

    while True:
        t0 = time.perf_counter()
        try:
            resp = await r.xreadgroup(
                GROUP,
                CONSUMER,
                streams={APPLY_REQUESTS_STREAM: ">"},
                count=50,
                block=5000,
            )
            if not resp:
                LAST_RUN.set(time.time())
                LOOP_SECONDS.observe(time.perf_counter() - t0)
                continue

            for _stream, items in resp:
                for msg_id, fields in items:
                    payload = _parse_message(fields)
                    now_ms = _now_ms()
                    ts_ms = _i(payload.get("ts_ms", now_ms), now_ms)
                    QUEUE_LAG_MS.set(max(0, now_ms - ts_ms))

                    recommendation_id = _s(payload.get("recommendation_id", msg_id))
                    action_type = _s(payload.get("action_type", "unknown"))
                    risk_level = _s(payload.get("risk_level", "unknown"))
                    replay_status = _s(payload.get("replay_status", "UNKNOWN"))
                    approvals = _i(payload.get("approvals", 0), 0)
                    global_cfg = await _load_hash(r, GLOBAL_POLICY_KEY)
                    action_cfg = await _load_hash(r, f"{ACTION_POLICY_PREFIX}{action_type}")
                    last_commit_ts_ms, commits_last_hour = await _get_state(r, action_type)
                    decision = evaluate_commit_policy(
                        recommendation_id=recommendation_id,
                        action_type=action_type,
                        replay_status=replay_status,
                        risk_level=risk_level,
                        approvals=approvals,
                        min_approvals=min_approvals,
                        last_commit_ts_ms=last_commit_ts_ms,
                        commits_last_hour=commits_last_hour,
                        now_ms=now_ms,
                        global_cfg=global_cfg,
                        action_cfg=action_cfg,
                    )

                    result_payload = {
                        "ts_ms": str(now_ms),
                        "recommendation_id": recommendation_id,
                        "action_type": action_type,
                        "policy_mode": decision.policy_mode,
                        "allow_commit": "1" if decision.allow_commit else "0",
                        "reason": decision.reason,
                        "cooldown_remaining_sec": str(decision.cooldown_remaining_sec),
                        "dry_run_only": "1" if decision.dry_run_only else "0",
                    }
                    await _emit_result(r, result_payload)

                    audit_payload = {
                        "ts_ms": str(now_ms),
                        "event": "commit_policy_decision",
                        "recommendation_id": recommendation_id,
                        "action_type": action_type,
                        "reason": decision.reason,
                        "policy_mode": decision.policy_mode,
                        "allow_commit": "1" if decision.allow_commit else "0",
                    }
                    await _emit_audit(r, audit_payload)

                    if decision.allow_commit:
                        out = dict(payload)
                        out["commit_policy_reason"] = decision.reason
                        out["executor_mode"] = decision.policy_mode
                        out["dry_run_only"] = "1" if decision.dry_run_only else "0"
                        await r.xadd(COMMIT_REQUESTS_STREAM, out, maxlen=200_000, approximate=True)
                        APPROVED.labels(action=action_type).inc()
                        if not decision.dry_run_only:
                            await _update_state(r, action_type, now_ms)
                        RUNS.labels(status="approved").inc()
                    else:
                        BLOCKED.labels(action=action_type, reason=decision.reason).inc()
                        RUNS.labels(status="blocked").inc()

                    try:
                        await r.xack(APPLY_REQUESTS_STREAM, GROUP, msg_id)
                    except Exception:
                        pass

            LAST_RUN.set(time.time())
            LOOP_SECONDS.observe(time.perf_counter() - t0)
        except Exception:
            RUNS.labels(status="error").inc()
            time.sleep(1.0)


if __name__ == "__main__":  # pragma: no cover
    import asyncio; asyncio.run(main())

