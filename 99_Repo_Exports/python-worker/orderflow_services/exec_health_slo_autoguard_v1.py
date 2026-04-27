#!/usr/bin/env python3
# exec_health_slo_autoguard_v1.py
# P5 AutoGuard: reads metrics:exec_health:slo:last (P4 summary) and applies:
#   - auto-freeze (cfg:orderflow:exec_health:auto_freeze:v1)
#   - optional rollback (cfg:orderflow:overrides:v1:active_sid → prev_sid)
# Triggers:
#   - cross_scope_mode_distinct > 1 sustained for EXEC_HEALTH_AUTOGUARD_MODE_MISMATCH_MINUTES
#   - rollout_drift_instances_total >= threshold sustained for EXEC_HEALTH_AUTOGUARD_DRIFT_MINUTES
# Logic: condition must be sustained; has cooldown; freeze and rollback are idempotent;
#         fail-open when Redis/summary unavailable.
from __future__ import annotations
from utils.time_utils import get_ny_time_millis

import asyncio
import json
import os
import secrets
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Tuple

from services.orderflow.exec_health_freeze_control import (
    build_autoguard_latch_update,
    stringify_mapping,
)
from services.orderflow.exec_health_freeze_service_identity import ensure_service_identity_async
from services.orderflow.exec_health_freeze_reconnect_healing import heal_service_identity_async

try:
    import redis.asyncio as aioredis  # type: ignore
except Exception:  # pragma: no cover
    aioredis = None


def _now_ms() -> int:
    return get_ny_time_millis()


def _i(x: Any, d: int = 0) -> int:
    try:
        return int(float(x))
    except Exception:
        return int(d)


def _b(x: Any) -> bool:
    try:
        if isinstance(x, str):
            return x.strip().lower() in {"1", "true", "yes", "on"}
        return bool(int(x))
    except Exception:
        return False


def _s(x: Any, d: str = "") -> str:
    try:
        return str(x) if x is not None else str(d)
    except Exception:
        return str(d)


@dataclass
class GuardCfg:
    redis_url: str
    summary_key: str       # P4 summary hash: metrics:exec_health:slo:last
    state_key: str         # P5 state hash: metrics:exec_health:slo:autoguard:state
    freeze_key: str        # output freeze key: cfg:orderflow:exec_health:auto_freeze:v1
    control_key: str       # P7 latched control hash: cfg:orderflow:exec_health:freeze_control:v1
    notify_stream: str     # Redis stream for Telegram notifications
    event_stream: str      # P8 audit event stream: ops:exec_health:freeze_events:v1
    loop_s: int            # check interval in seconds
    mode_mismatch_minutes: int      # sustained duration before mode-mismatch trigger
    drift_minutes: int              # sustained duration before drift trigger
    drift_instances_min: int        # minimum drifted instances to count as drift
    freeze_minutes: int             # freeze TTL in minutes
    cooldown_minutes: int           # cooldown after trigger before next trigger allowed
    rollback_enable: bool           # master rollback switch
    rollback_on_mode_mismatch: bool # rollback when mode mismatch is sustained
    rollback_on_drift: bool         # rollback when rollout drift is sustained
    enabled: bool                   # master enable switch (EXEC_HEALTH_AUTOGUARD_ENABLE)

    @staticmethod
    def from_env() -> "GuardCfg":
        return GuardCfg(
            redis_url=os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0"),
            summary_key=os.getenv("EXEC_HEALTH_SLO_SUMMARY_KEY", "metrics:exec_health:slo:last"),
            state_key=os.getenv("EXEC_HEALTH_SLO_AUTOGUARD_STATE_KEY", "metrics:exec_health:slo:autoguard:state"),
            freeze_key=os.getenv("EXEC_HEALTH_AUTO_FREEZE_KEY", "cfg:orderflow:exec_health:auto_freeze:v1"),
            control_key=os.getenv("EXEC_HEALTH_FREEZE_CONTROL_KEY", "cfg:orderflow:exec_health:freeze_control:v1"),
            notify_stream=os.getenv("EXEC_HEALTH_AUTOGUARD_NOTIFY_STREAM", "notify:telegram"),
            event_stream=os.getenv("EXEC_HEALTH_FREEZE_EVENT_STREAM", "ops:exec_health:freeze_events:v1"),
            loop_s=max(5, _i(os.getenv("EXEC_HEALTH_AUTOGUARD_CHECK_EVERY_S", "30"), 30)),
            mode_mismatch_minutes=max(1, _i(os.getenv("EXEC_HEALTH_AUTOGUARD_MODE_MISMATCH_MINUTES", "5"), 5)),
            drift_minutes=max(1, _i(os.getenv("EXEC_HEALTH_AUTOGUARD_DRIFT_MINUTES", "10"), 10)),
            drift_instances_min=max(1, _i(os.getenv("EXEC_HEALTH_AUTOGUARD_DRIFT_INSTANCES_MIN", "1"), 1)),
            freeze_minutes=max(1, _i(os.getenv("EXEC_HEALTH_AUTOGUARD_FREEZE_MINUTES", "30"), 30)),
            cooldown_minutes=max(1, _i(os.getenv("EXEC_HEALTH_AUTOGUARD_COOLDOWN_MINUTES", "30"), 30)),
            rollback_enable=_b(os.getenv("EXEC_HEALTH_AUTOGUARD_ROLLBACK_ENABLE", "1")),
            rollback_on_mode_mismatch=_b(os.getenv("EXEC_HEALTH_AUTOGUARD_ROLLBACK_ON_MODE_MISMATCH", "1")),
            rollback_on_drift=_b(os.getenv("EXEC_HEALTH_AUTOGUARD_ROLLBACK_ON_DRIFT", "0")),
            enabled=_b(os.getenv("EXEC_HEALTH_AUTOGUARD_ENABLE", "1")),
        )


@dataclass
class EvalResult:
    mode_mismatch_active: bool
    rollout_drift_active: bool
    mode_mismatch_since_ts_ms: int   # timestamp when condition first appeared (0 if inactive)
    rollout_drift_since_ts_ms: int   # timestamp when condition first appeared (0 if inactive)
    should_trigger: bool             # True if any condition crossed the sustained threshold
    trigger_reasons: List[str]       # list of active reason labels


def evaluate_autoguard(
    *, summary: Dict[str, Any], prev_state: Dict[str, Any], cfg: GuardCfg, now_ms: int
) -> EvalResult:
    """
    Pure evaluation function (no Redis I/O) — easy to unit-test.
    Reads P4 summary fields and computes whether autoguard should fire.
    """
    cross_scope_mode_distinct = _i(summary.get("cross_scope_mode_distinct"), 0)
    rollout_drift_instances_total = _i(summary.get("rollout_drift_instances_total"), 0)

    mm_active = cross_scope_mode_distinct > 1
    drift_active = rollout_drift_instances_total >= int(cfg.drift_instances_min)

    # Carry forward sustained-since timestamps from previous state; reset when condition clears
    mm_since = _i(prev_state.get("mode_mismatch_since_ts_ms"), 0)
    dr_since = _i(prev_state.get("rollout_drift_since_ts_ms"), 0)

    if mm_active:
        mm_since = mm_since or now_ms   # latch: keep earliest timestamp
    else:
        mm_since = 0                    # reset when condition no longer holds

    if drift_active:
        dr_since = dr_since or now_ms
    else:
        dr_since = 0

    reasons: List[str] = []
    if mm_active and (now_ms - mm_since) >= int(cfg.mode_mismatch_minutes * 60 * 1000):
        reasons.append("cross_scope_mode_mismatch")
    if drift_active and (now_ms - dr_since) >= int(cfg.drift_minutes * 60 * 1000):
        reasons.append("rollout_drift")

    return EvalResult(
        mode_mismatch_active=mm_active,
        rollout_drift_active=drift_active,
        mode_mismatch_since_ts_ms=mm_since,
        rollout_drift_since_ts_ms=dr_since,
        should_trigger=bool(reasons),
        trigger_reasons=reasons,
    )


class AutoGuard:
    """
    Async main service class.
    Reads P4 SLO summary → evaluates conditions → applies freeze/rollback as needed.
    Fail-open: if Redis or summary unavailable, logs nothing and returns.
    """

    def __init__(self, cfg: GuardCfg | None = None):
        self.cfg = cfg or GuardCfg.from_env()
        if aioredis is None:
            raise RuntimeError("redis dependency missing")
        self.r = aioredis.from_url(self.cfg.redis_url, decode_responses=True)
        self._identity_checked = False

    async def _read_hash(self, key: str) -> Dict[str, Any]:
        try:
            return await self.r.hgetall(key) or {}
        except Exception:
            return {}

    async def _notify(self, text: str) -> None:
        """Send notification to Telegram notify stream (best-effort)."""
        try:
            await self.r.xadd(
                self.cfg.notify_stream,
                {"ts_ms": str(_now_ms()), "source": "exec_health_slo_autoguard_v1", "text": text},
                maxlen=5000,
            )
        except Exception:
            pass

    async def _emit_event(self, payload: Dict[str, Any]) -> str:
        """Emit an audit event to the P8 freeze event stream (best-effort).

        Returns the Redis stream event ID (e.g. '1-0') or '' on failure.
        """
        try:
            return str(await self.r.xadd(self.cfg.event_stream, stringify_mapping(payload), maxlen=5000) or "")
        except Exception:
            return ""

    async def _set_state(self, state: Dict[str, Any]) -> None:
        """Persist autoguard state hash with TTL = max(300s, cooldown*3 minutes)."""
        payload = {str(k): str(v) for k, v in state.items()}
        await self.r.hset(self.cfg.state_key, mapping=payload)
        await self.r.expire(self.cfg.state_key, max(300, self.cfg.cooldown_minutes * 180))

    async def _set_control_latch(self, *, now_ms: int, reasons: List[str], freeze_until_ts_ms: int, ack_nonce: str, trigger_event_id: str = "") -> None:
        """Write the P7/P8 latched control hash so raw key deletion cannot bypass the freeze.

        P8: stores the pending ack nonce so the operator thaw CLI can do a CAS check.
        The latch survives the raw TTL key expiry and requires an explicit operator
        ack (exec_health_freeze_override_v1.py thaw) to clear.
        TTL is set to max(24h, freeze_minutes * 24h) to outlast the raw freeze key.
        """
        prev = await self._read_hash(self.cfg.control_key)
        payload = build_autoguard_latch_update(
            prev=prev,
            now_ms=now_ms,
            reasons=list(reasons),
            freeze_until_ts_ms=int(freeze_until_ts_ms),
            ack_nonce=str(ack_nonce),
            trigger_event_id=str(trigger_event_id or ""),
        )
        await self.r.hset(self.cfg.control_key, mapping=stringify_mapping(payload))
        await self.r.expire(self.cfg.control_key, max(86400, self.cfg.freeze_minutes * 86400))

    async def _set_freeze(self, *, now_ms: int, reasons: List[str]) -> None:
        """Write freeze key with TTL = freeze_minutes. Idempotent (overwrite is safe)."""
        until = now_ms + int(self.cfg.freeze_minutes * 60 * 1000)
        payload = {
            "schema_name": "exec_health_auto_freeze",
            "schema_version": 1,
            "ts_ms": now_ms,
            "freeze_active": 1,
            "freeze_reason": ",".join(reasons),
            "freeze_until_ts_ms": until,
        }
        await self.r.set(self.cfg.freeze_key, json.dumps(payload, separators=(",", ":")))
        await self.r.pexpire(self.cfg.freeze_key, int(self.cfg.freeze_minutes * 60 * 1000))

    async def _maybe_rollback(
        self, *, now_ms: int, reasons: List[str], state: Dict[str, Any]
    ) -> Tuple[bool, str, str]:
        """
        Optionally switch cfg:orderflow:overrides:v1:active_sid → prev_sid.
        Returns (did_rollback, from_sid, to_sid).
        Only fires if:
          - EXEC_HEALTH_AUTOGUARD_ROLLBACK_ENABLE=1
          - the relevant reason is configured for rollback
          - active_sid exists, prev_sid exists, and they differ
        """
        if not self.cfg.rollback_enable:
            return False, "", ""
        # Check reason-specific rollback flags
        if ("cross_scope_mode_mismatch" in reasons and not self.cfg.rollback_on_mode_mismatch) or (
            "rollout_drift" in reasons and not self.cfg.rollback_on_drift
        ):
            return False, "", ""

        active_sid = str(await self.r.get("cfg:orderflow:overrides:v1:active_sid") or "")
        prev_sid = str(await self.r.get("cfg:orderflow:overrides:v1:prev_sid") or "")
        if not active_sid or not prev_sid or active_sid == prev_sid:
            return False, active_sid, prev_sid

        # Execute rollback: set active_sid = prev_sid and write rollback marker
        await self.r.set("cfg:orderflow:overrides:v1:active_sid", prev_sid)
        rb = {"ts_ms": now_ms, "reason": ",".join(reasons), "from_sid": active_sid, "to_sid": prev_sid}
        await self.r.set(
            f"cfg:orderflow:overrides:v1:rollback:{active_sid}",
            json.dumps(rb, separators=(",", ":")),
        )
        return True, active_sid, prev_sid

    async def run_once(self) -> None:
        """
        Single evaluation cycle:
          1. Read P4 summary from Redis
          2. Evaluate sustained conditions
          3. Apply freeze + optional rollback if conditions are met and cooldown passed
          4. Persist updated state
        Fail-open: if summary is missing, return silently.
        """
        if not self.cfg.enabled:
            return
        if not self._identity_checked:
            await ensure_service_identity_async(self.r, "exec_health_slo_autoguard_v1")
            self._identity_checked = True
        await heal_service_identity_async(self.r, "exec_health_slo_autoguard_v1")
        now = _now_ms()
        summary = await self._read_hash(self.cfg.summary_key)
        if not summary:
            # P4 summary not yet available — fail-open
            return
        state = await self._read_hash(self.cfg.state_key)

        freeze_until = _i(state.get("freeze_until_ts_ms"), 0)
        cooldown_until = _i(state.get("cooldown_until_ts_ms"), 0)
        freeze_active = 1 if freeze_until > now else 0

        ev = evaluate_autoguard(summary=summary, prev_state=state, cfg=self.cfg, now_ms=now)
        state.update(
            {
                "schema_name": "exec_health_slo_autoguard_state",
                "schema_version": 1,
                "updated_ts_ms": now,
                "mode_mismatch_active": int(ev.mode_mismatch_active),
                "rollout_drift_active": int(ev.rollout_drift_active),
                "mode_mismatch_since_ts_ms": int(ev.mode_mismatch_since_ts_ms),
                "rollout_drift_since_ts_ms": int(ev.rollout_drift_since_ts_ms),
                "last_eval_reasons_json": json.dumps(list(ev.trigger_reasons), ensure_ascii=False),
                "freeze_active": int(freeze_active),
                "freeze_until_ts_ms": int(freeze_until),
                "cooldown_until_ts_ms": int(cooldown_until),
            }
        )

        if ev.should_trigger and now >= cooldown_until:
            # Conditions sustained past threshold and cooldown has elapsed — fire
            freeze_until_ts_ms = int(now + self.cfg.freeze_minutes * 60 * 1000)
            # P8: generate pending ack nonce; emit latch event to audit stream first
            # so the event_id can be stored alongside the nonce in the control hash.
            ack_nonce = secrets.token_hex(16)
            trigger_event_id = await self._emit_event({
                "ts_ms": now,
                "kind": "autoguard_freeze_latch",
                "ack_nonce": ack_nonce,
                "trigger_ts_ms": now,
                "reasons_json": json.dumps(list(ev.trigger_reasons), ensure_ascii=False),
                "freeze_until_ts_ms": freeze_until_ts_ms,
                "source": "exec_health_slo_autoguard_v1",
            })
            await self._set_freeze(now_ms=now, reasons=ev.trigger_reasons)
            # P7/P8: write latched control hash with nonce for CAS check at thaw time
            await self._set_control_latch(now_ms=now, reasons=ev.trigger_reasons, freeze_until_ts_ms=freeze_until_ts_ms, ack_nonce=ack_nonce, trigger_event_id=trigger_event_id)
            did_rb, from_sid, to_sid = await self._maybe_rollback(
                now_ms=now, reasons=ev.trigger_reasons, state=state
            )
            state.update(
                {
                    "freeze_active": 1,
                    "freeze_until_ts_ms": int(freeze_until_ts_ms),
                    "last_trigger_ts_ms": int(now),
                    "last_trigger_reasons_json": json.dumps(list(ev.trigger_reasons), ensure_ascii=False),
                    "last_action": "freeze+rollback" if did_rb else "freeze",
                    # P7/P8: include manual_ack and nonce fields in state hash so fallback read also sees the latch
                    "manual_ack_required": 1,
                    "manual_ack_ts_ms": 0,
                    "manual_ack_operator": "",
                    "manual_ack_reason": "",
                    "manual_ack_ticket": "",
                    "manual_ack_nonce": "",
                    "manual_ack_sig": "",
                    "manual_ack_event_id": "",
                    "expected_ack_nonce": str(ack_nonce),
                    "last_trigger_nonce": str(ack_nonce),
                    "last_trigger_event_id": str(trigger_event_id or ""),
                    "control_source": "autoguard",
                    "effective_freeze_active": 1,
                    "last_trigger_ts_ms": int(now),
                    "last_rollback_ts_ms": int(now if did_rb else _i(state.get("last_rollback_ts_ms"), 0)),
                    "last_rollback_from_sid": str(from_sid or state.get("last_rollback_from_sid") or ""),
                    "last_rollback_to_sid": str(to_sid or state.get("last_rollback_to_sid") or ""),
                    "rollback_total": int(_i(state.get("rollback_total"), 0) + (1 if did_rb else 0)),
                    "cooldown_until_ts_ms": int(now + self.cfg.cooldown_minutes * 60 * 1000),
                }
            )
            await self._notify(
                f"ExecHealth autoguard trigger: reasons={','.join(ev.trigger_reasons)} "
                f"rollback={int(did_rb)} from={from_sid} to={to_sid}"
            )
        elif freeze_until <= now:
            # No active freeze and conditions not triggered — ensure freeze_active=0
            state["freeze_active"] = 0
            state["last_action"] = state.get("last_action") or "idle"

        await self._set_state(state)

    async def run_forever(self) -> None:
        """Main loop: run_once every loop_s seconds. Exceptions are swallowed (fail-open)."""
        while True:
            try:
                await self.run_once()
            except Exception:
                pass
            await asyncio.sleep(self.cfg.loop_s)


async def _main() -> None:
    await AutoGuard().run_forever()


if __name__ == "__main__":
    asyncio.run(_main())
