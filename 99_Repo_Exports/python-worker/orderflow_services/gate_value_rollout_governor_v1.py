"""gate_value_rollout_governor_v1.py — automatic rollout governor.

Drives the gate_value rollout through Stage 3 → 5 → 6_CANDIDATE → 6_ENFORCED
without manual intervention:

  STAGE_3_ACCUMULATING:
    Wait for data accumulation. Numerical gates:
      • XLEN(stream:signals:gated_out_outcomes) growth in window ≥ min_growth
      • XLEN(labels:tb) growth in window ≥ min_growth
      • Stage dwell ≥ STAGE3_MIN_DWELL_H
    LLM consulted; veto holds stage.

  STAGE_5_ADVISORY:
    Autocal is running ENFORCE=0. Numerical gates:
      • Stage dwell ≥ STAGE5_MIN_DURATION_H (default 168h = 7d)
      • Autocal state shows ≥ STAGE5_MIN_GROUPS distinct groups
      • sum(rollback_count) ≤ STAGE5_MAX_ROLLBACK_TOTAL
      • llm_veto_rate ≤ STAGE5_MAX_LLM_VETO_RATE
      • dominant phase fraction (KEEP_CONFIRMED + RELAX_APPLIED) ≥
        STAGE5_MIN_STABLE_FRAC
    LLM consulted; veto holds stage.

  STAGE_6_ENFORCE_CANDIDATE:
    Final canary. Numerical gates:
      • Dwell ≥ STAGE6_CANARY_H (default 24h)
      • No new rollback events since entering this stage
    LLM consulted with HIGH stakes prompt. Veto rolls back to STAGE_5.

  STAGE_6_ENFORCED:
    Governor writes `1` to `cfg:gva:enforce` (autocal honours via
    _resolve_enforce). Daily summary Telegram. Manual rollback only.

State key: `autocal:gate_value:rollout_state` (HMAC-signed, no TTL).
Telegram events: startup, stage_transition, llm_advisory, enforce_flipped,
                 stage_held (LLM veto), rollback.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import logging
import os
import time
from dataclasses import asdict, dataclass, field
from typing import Any

from prometheus_client import Counter, Gauge, start_http_server

log = logging.getLogger("gate_value_rollout_governor")

# ── ENV ───────────────────────────────────────────────────────────────────────


def _env(k: str, d: str = "") -> str:
    return (os.getenv(k, d) or d).strip()


def _env_int(k: str, d: int) -> int:
    try:
        return int(_env(k, str(d)) or d)
    except (TypeError, ValueError):
        return d


def _env_float(k: str, d: float) -> float:
    try:
        return float(_env(k, str(d)) or d)
    except (TypeError, ValueError):
        return d


def _env_bool(k: str, d: bool) -> bool:
    v = _env(k, "1" if d else "0").lower()
    return v in {"1", "true", "yes", "y", "on"}


# ── Constants ─────────────────────────────────────────────────────────────────

STAGE_3 = "STAGE_3_ACCUMULATING"
STAGE_5 = "STAGE_5_ADVISORY"
STAGE_6C = "STAGE_6_ENFORCE_CANDIDATE"
STAGE_6 = "STAGE_6_ENFORCED"

STAGE_LADDER = (STAGE_3, STAGE_5, STAGE_6C, STAGE_6)

STAGE_NEXT = {
    STAGE_3: STAGE_5,
    STAGE_5: STAGE_6C,
    STAGE_6C: STAGE_6,
    STAGE_6: STAGE_6,  # terminal
}

STAGE_PREV = {
    STAGE_6C: STAGE_5,  # only stage that can auto-rollback
}

STABLE_PHASES = {"KEEP_CONFIRMED", "RELAX_APPLIED"}


# ── Config ────────────────────────────────────────────────────────────────────


@dataclass
class Cfg:
    enable: bool
    interval_sec: int

    # Input streams + state keys
    report_key: str
    autocal_state_key: str
    rollout_state_key: str
    enforce_override_key: str
    stream_gated_out_outcomes: str
    stream_labels_tb: str
    stream_ml_confirm: str

    # Stage 3 gates
    stage3_window_h: float
    stage3_min_growth_gated_out: int
    stage3_min_growth_labels_tb: int
    stage3_min_dwell_h: float

    # Stage 5 gates
    stage5_min_duration_h: float
    stage5_min_groups: int
    stage5_max_rollback_total: int
    stage5_max_llm_veto_rate: float
    stage5_min_stable_frac: float

    # Stage 6 candidate gates
    stage6_canary_h: float

    # LLM
    llm_enabled: bool
    llm_timeout_sec: float

    # Notify
    notify_telegram: bool
    notify_stream: str
    daily_summary: bool

    # Transport
    hmac_secret: str
    redis_url: str
    prom_port: int


def load_cfg() -> Cfg:
    return Cfg(
        enable=_env_bool("GVR_ENABLE", True),
        interval_sec=_env_int("GVR_INTERVAL_SEC", 900),
        report_key=_env("GVR_REPORT_KEY", "report:gate_value:latest"),
        autocal_state_key=_env("GVR_AUTOCAL_STATE_KEY", "autocal:gate_value:state"),
        rollout_state_key=_env(
            "GVR_ROLLOUT_STATE_KEY", "autocal:gate_value:rollout_state"
        ),
        enforce_override_key=_env("GVR_ENFORCE_OVERRIDE_KEY", "cfg:gva:enforce"),
        stream_gated_out_outcomes=_env(
            "GVR_STREAM_GATED_OUT_OUTCOMES", "stream:signals:gated_out_outcomes"
        ),
        stream_labels_tb=_env("GVR_STREAM_LABELS_TB", "labels:tb"),
        stream_ml_confirm=_env("GVR_STREAM_ML_CONFIRM", "metrics:ml_confirm"),
        stage3_window_h=_env_float("GVR_STAGE3_WINDOW_H", 6.0),
        stage3_min_growth_gated_out=_env_int("GVR_STAGE3_MIN_GROWTH_GATED_OUT", 500),
        stage3_min_growth_labels_tb=_env_int("GVR_STAGE3_MIN_GROWTH_LABELS_TB", 500),
        stage3_min_dwell_h=_env_float("GVR_STAGE3_MIN_DWELL_H", 6.0),
        stage5_min_duration_h=_env_float("GVR_STAGE5_MIN_DURATION_H", 168.0),
        stage5_min_groups=_env_int("GVR_STAGE5_MIN_GROUPS", 3),
        stage5_max_rollback_total=_env_int("GVR_STAGE5_MAX_ROLLBACK_TOTAL", 3),
        stage5_max_llm_veto_rate=_env_float("GVR_STAGE5_MAX_LLM_VETO_RATE", 0.35),
        stage5_min_stable_frac=_env_float("GVR_STAGE5_MIN_STABLE_FRAC", 0.60),
        stage6_canary_h=_env_float("GVR_STAGE6_CANARY_H", 24.0),
        llm_enabled=_env_bool("GVR_LLM_ENABLED", True),
        llm_timeout_sec=_env_float("GVR_LLM_TIMEOUT_SEC", 30.0),
        notify_telegram=_env_bool("GVR_NOTIFY_TELEGRAM", True),
        notify_stream=_env("GVR_NOTIFY_STREAM", "notify:telegram"),
        daily_summary=_env_bool("GVR_DAILY_SUMMARY", True),
        hmac_secret=(
            _env("GVR_HMAC_SECRET", "")
            or _env("RECS_HMAC_SECRET", "")
            or _env("LAYERS_CAL_HMAC_SECRET", "")
        ),
        redis_url=_env("REDIS_URL", "redis://redis-worker-1:6379/0"),
        prom_port=_env_int("GVR_PROM_PORT", 9143),
    )


# ── Prometheus ────────────────────────────────────────────────────────────────

c_cycles = Counter("gvr_cycles_total", "Governor cycles", ["outcome"])
c_stage_transitions = Counter(
    "gvr_stage_transitions_total",
    "Stage transitions",
    ["from_stage", "to_stage", "trigger"],
)
c_llm_calls = Counter("gvr_llm_calls_total", "LLM advisor calls", ["verdict"])
c_telegram = Counter("gvr_telegram_total", "Telegram XADDs", ["event", "outcome"])
g_current_stage = Gauge("gvr_current_stage", "1 if current", ["stage"])
g_stage_dwell_h = Gauge("gvr_stage_dwell_hours", "Dwell time in current stage")
g_xlen = Gauge("gvr_stream_xlen", "XLEN of monitored streams", ["stream"])
g_growth_window = Gauge(
    "gvr_stream_growth_window",
    "XLEN growth in last window",
    ["stream"],
)
g_autocal_rollback_total = Gauge(
    "gvr_autocal_rollback_total",
    "Sum of rollback_count across autocal groups",
)
g_autocal_stable_frac = Gauge(
    "gvr_autocal_stable_frac",
    "Fraction of groups in KEEP_CONFIRMED or RELAX_APPLIED",
)
g_enforce_flipped = Gauge(
    "gvr_enforce_flipped",
    "1 if governor has flipped cfg:gva:enforce to 1",
)


# ── Telegram ──────────────────────────────────────────────────────────────────


def _send_telegram(r: Any, *, cfg: Cfg, event: str, text: str) -> None:
    if not cfg.notify_telegram:
        return
    try:
        r.xadd(
            cfg.notify_stream,
            {
                "type": "report",
                "subtype": "gate_value_rollout",
                "event": event,
                "ts": str(int(time.time() * 1000)),
                "text": text,
                "parse_mode": "HTML",
            },
            maxlen=5_000,
        )
        c_telegram.labels(event=event, outcome="ok").inc()
    except Exception as e:
        log.warning("telegram (%s) failed: %s", event, e)
        c_telegram.labels(event=event, outcome="error").inc()


def _fmt_startup(cfg: Cfg, stage: str) -> str:
    return (
        "<b>🚦 gate_value rollout governor started</b>\n"
        f"current stage: <b>{stage}</b>\n"
        f"interval=<b>{cfg.interval_sec}s</b> "
        f"llm=<b>{int(cfg.llm_enabled)}</b>\n"
        f"stage3 window/growth=<b>{cfg.stage3_window_h:.0f}h</b>/"
        f"<b>{cfg.stage3_min_growth_gated_out}</b>+<b>{cfg.stage3_min_growth_labels_tb}</b>\n"
        f"stage5 dwell=<b>{cfg.stage5_min_duration_h:.0f}h</b> "
        f"stable_frac=<b>{cfg.stage5_min_stable_frac:.2f}</b>\n"
        f"stage6 canary=<b>{cfg.stage6_canary_h:.0f}h</b>"
    )


def _fmt_stage_transition(prev: str, curr: str, ctx: "Snapshot", reason: str) -> str:
    emoji = "🚦"
    if curr == STAGE_6:
        emoji = "🚨"
    elif curr == STAGE_5:
        emoji = "🟡"
    elif curr == STAGE_6C:
        emoji = "🟠"
    elif curr in {STAGE_PREV.get(prev, "")} or curr == STAGE_5 and prev == STAGE_6C:
        emoji = "↩️"
    return (
        f"<b>{emoji} rollout stage</b> {prev} → <b>{curr}</b>\n"
        f"xlen gated_out=<b>{ctx.xlen_gated_out_outcomes}</b> "
        f"labels=<b>{ctx.xlen_labels_tb}</b> ml_conf=<b>{ctx.xlen_ml_confirm}</b>\n"
        f"growth_window: gated_out=<b>{ctx.growth_gated_out_window}</b> "
        f"labels=<b>{ctx.growth_labels_tb_window}</b>\n"
        f"autocal groups=<b>{ctx.autocal_groups}</b> "
        f"rollback_total=<b>{ctx.autocal_rollback_total}</b> "
        f"stable=<b>{ctx.autocal_stable_frac:.2f}</b>\n"
        f"reason: <i>{(reason or '')[:200]}</i>"
    )


def _fmt_enforce_flipped(ctx: "Snapshot") -> str:
    return (
        "<b>🚨 GVA_ENFORCE FLIPPED → 1</b>\n"
        f"<code>cfg:gva:enforce = 1</code> written to Redis.\n"
        f"autocal will start producing shadow cfg on next cycle.\n"
        f"groups=<b>{ctx.autocal_groups}</b> "
        f"rollback_total=<b>{ctx.autocal_rollback_total}</b> "
        f"stable_frac=<b>{ctx.autocal_stable_frac:.2f}</b>\n"
        "If anything looks wrong, set <code>cfg:gva:enforce</code> to "
        "<code>0</code> and remove key (ENV ENFORCE stays authoritative)."
    )


def _fmt_held(stage: str, reason: str) -> str:
    return (
        f"<b>⏸ rollout held at {stage}</b>\n"
        f"reason: <i>{(reason or '')[:240]}</i>"
    )


def _fmt_llm_advisory(stage: str, proposed: str, advisory: dict[str, Any]) -> str:
    recs = advisory.get("guarded_recommendations") or []
    blocks = advisory.get("blocked_recommendations") or []
    rec_summary = ", ".join(
        f"{(r or {}).get('action', '?')}({(r or {}).get('risk', '?')})"
        for r in recs[:3]
    ) or "none"
    summary = ""
    for r in recs[:1]:
        summary = str((r or {}).get("reason") or "")[:200]
    if not summary:
        for r in blocks[:1]:
            summary = str((r or {}).get("reason") or "")[:200] or "blocked"
    return (
        f"<b>🧠 rollout LLM advisory</b> {stage} → <b>{proposed}</b>\n"
        f"recs: {rec_summary} | blocked=<b>{len(blocks)}</b>\n"
        f"summary: <i>{summary}</i>"
    )


# ── State ─────────────────────────────────────────────────────────────────────


@dataclass
class RolloutState:
    stage: str = STAGE_3
    stage_entry_ms: int = 0
    enforce_flipped_ms: int = 0
    last_daily_summary_ms: int = 0

    # Tracking inputs at stage entry (for growth-rate calc)
    xlen_gated_out_at_entry: int = 0
    xlen_labels_tb_at_entry: int = 0

    # LLM veto tracking (sliding count over last K cycles)
    llm_veto_history: list[int] = field(default_factory=list)


def _load_state(r: Any, key: str) -> RolloutState:
    try:
        raw = r.get(key)
    except Exception as e:
        log.warning("state load failed: %s", e)
        return RolloutState()
    if not raw:
        return RolloutState()
    try:
        payload = json.loads(raw)
    except Exception:
        return RolloutState()
    s = RolloutState()
    s.stage = str(payload.get("stage") or STAGE_3)
    if s.stage not in STAGE_LADDER:
        s.stage = STAGE_3
    s.stage_entry_ms = int(payload.get("stage_entry_ms") or 0)
    s.enforce_flipped_ms = int(payload.get("enforce_flipped_ms") or 0)
    s.last_daily_summary_ms = int(payload.get("last_daily_summary_ms") or 0)
    s.xlen_gated_out_at_entry = int(payload.get("xlen_gated_out_at_entry") or 0)
    s.xlen_labels_tb_at_entry = int(payload.get("xlen_labels_tb_at_entry") or 0)
    hist = payload.get("llm_veto_history") or []
    if isinstance(hist, list):
        s.llm_veto_history = [int(x) for x in hist[-32:]]
    return s


def _hmac_sign(payload: dict[str, Any], secret: str) -> str:
    data = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hmac.new(secret.encode("utf-8"), data, hashlib.sha256).hexdigest()


def _save_state(r: Any, key: str, state: RolloutState, *, secret: str) -> None:
    payload = asdict(state)
    if secret:
        payload["sig"] = _hmac_sign(payload, secret)
    try:
        r.set(key, json.dumps(payload))
    except Exception as e:
        log.warning("state save failed: %s", e)


# ── Snapshot of all inputs at one cycle ──────────────────────────────────────


@dataclass
class Snapshot:
    xlen_gated_out_outcomes: int
    xlen_labels_tb: int
    xlen_ml_confirm: int

    growth_gated_out_window: int
    growth_labels_tb_window: int

    autocal_groups: int
    autocal_rollback_total: int
    autocal_phase_distribution: dict[str, int]
    autocal_stable_frac: float

    llm_veto_rate: float
    days_since_start: float
    stage_dwell_h: float


def _xlen(r: Any, stream: str) -> int:
    try:
        return int(r.xlen(stream))
    except Exception:
        return 0


def _phase_counts(autocal_state: dict[str, Any]) -> tuple[int, int, int, dict[str, int]]:
    """Return (groups, rollback_total, stable_count, phase_dist)."""
    groups_dict = (autocal_state or {}).get("groups") or {}
    if not isinstance(groups_dict, dict):
        return (0, 0, 0, {})
    groups = len(groups_dict)
    rollback_total = 0
    phase_dist: dict[str, int] = {}
    stable = 0
    for _gk, g in groups_dict.items():
        if not isinstance(g, dict):
            continue
        rollback_total += int(g.get("rollback_count") or 0)
        ph = str(g.get("phase") or "")
        phase_dist[ph] = phase_dist.get(ph, 0) + 1
        if ph in STABLE_PHASES:
            stable += 1
    return (groups, rollback_total, stable, phase_dist)


def _load_autocal_state(r: Any, key: str) -> dict[str, Any]:
    try:
        raw = r.get(key)
    except Exception:
        return {}
    if not raw:
        return {}
    try:
        return json.loads(raw)
    except Exception:
        return {}


def _build_snapshot(
    r: Any,
    cfg: Cfg,
    state: RolloutState,
    *,
    now_ms: int,
) -> Snapshot:
    xlen_go = _xlen(r, cfg.stream_gated_out_outcomes)
    xlen_lt = _xlen(r, cfg.stream_labels_tb)
    xlen_ml = _xlen(r, cfg.stream_ml_confirm)

    growth_go = max(0, xlen_go - state.xlen_gated_out_at_entry)
    growth_lt = max(0, xlen_lt - state.xlen_labels_tb_at_entry)

    autocal_state = _load_autocal_state(r, cfg.autocal_state_key)
    groups, rollback_total, stable, dist = _phase_counts(autocal_state)
    stable_frac = (stable / groups) if groups > 0 else 0.0

    hist = state.llm_veto_history
    veto_rate = (sum(hist) / len(hist)) if hist else 0.0
    days_since_start = (
        (now_ms - state.stage_entry_ms) / (24.0 * 3_600_000.0)
        if state.stage_entry_ms > 0
        else 0.0
    )
    dwell_h = (
        (now_ms - state.stage_entry_ms) / 3_600_000.0
        if state.stage_entry_ms > 0
        else 0.0
    )

    return Snapshot(
        xlen_gated_out_outcomes=xlen_go,
        xlen_labels_tb=xlen_lt,
        xlen_ml_confirm=xlen_ml,
        growth_gated_out_window=growth_go,
        growth_labels_tb_window=growth_lt,
        autocal_groups=groups,
        autocal_rollback_total=rollback_total,
        autocal_phase_distribution=dist,
        autocal_stable_frac=stable_frac,
        llm_veto_rate=veto_rate,
        days_since_start=days_since_start,
        stage_dwell_h=dwell_h,
    )


# ── Stage transition logic ───────────────────────────────────────────────────


def _numerical_gates_advance(
    state: RolloutState,
    snap: Snapshot,
    cfg: Cfg,
) -> tuple[bool, list[str]]:
    """Return (ok, fails) — does this stage's numerical gate ladder pass?"""
    fails: list[str] = []
    if state.stage == STAGE_3:
        if snap.stage_dwell_h < cfg.stage3_min_dwell_h:
            fails.append(f"stage3_dwell<{cfg.stage3_min_dwell_h:.0f}h")
        if snap.growth_gated_out_window < cfg.stage3_min_growth_gated_out:
            fails.append(
                f"gated_out_growth<{cfg.stage3_min_growth_gated_out}"
            )
        if snap.growth_labels_tb_window < cfg.stage3_min_growth_labels_tb:
            fails.append(f"labels_tb_growth<{cfg.stage3_min_growth_labels_tb}")
        return (len(fails) == 0, fails)

    if state.stage == STAGE_5:
        if snap.stage_dwell_h < cfg.stage5_min_duration_h:
            fails.append(f"stage5_dwell<{cfg.stage5_min_duration_h:.0f}h")
        if snap.autocal_groups < cfg.stage5_min_groups:
            fails.append(f"autocal_groups<{cfg.stage5_min_groups}")
        if snap.autocal_rollback_total > cfg.stage5_max_rollback_total:
            fails.append(
                f"rollback_total>{cfg.stage5_max_rollback_total}"
            )
        if snap.llm_veto_rate > cfg.stage5_max_llm_veto_rate:
            fails.append(
                f"llm_veto_rate>{cfg.stage5_max_llm_veto_rate:.2f}"
            )
        if snap.autocal_stable_frac < cfg.stage5_min_stable_frac:
            fails.append(
                f"stable_frac<{cfg.stage5_min_stable_frac:.2f}"
            )
        return (len(fails) == 0, fails)

    if state.stage == STAGE_6C:
        if snap.stage_dwell_h < cfg.stage6_canary_h:
            fails.append(f"stage6_canary<{cfg.stage6_canary_h:.0f}h")
        # Also re-check stage 5 stability conditions (sticky):
        if snap.autocal_rollback_total > cfg.stage5_max_rollback_total:
            fails.append("rollback_total_grew_in_canary")
        if snap.autocal_stable_frac < cfg.stage5_min_stable_frac:
            fails.append("stable_frac_dropped_in_canary")
        return (len(fails) == 0, fails)

    return (False, ["stage_terminal"])


def _consult_llm(
    snap: Snapshot,
    state: RolloutState,
    cfg: Cfg,
    *,
    proposed_stage: str,
) -> tuple[dict[str, Any] | None, bool]:
    """Returns (advisory, blocks) — None advisory if LLM disabled."""
    if not cfg.llm_enabled:
        c_llm_calls.labels(verdict="skipped").inc()
        return (None, False)
    try:
        from orderflow_services.gate_value_rollout_llm_advisor import (
            advise_stage_transition,
        )
    except Exception as e:
        log.debug("llm import failed: %s", e)
        c_llm_calls.labels(verdict="skipped").inc()
        return (None, False)
    advisory = advise_stage_transition(
        current_stage=state.stage,
        proposed_stage=proposed_stage,
        stage_dwell_h=snap.stage_dwell_h,
        xlen_gated_out_outcomes=snap.xlen_gated_out_outcomes,
        xlen_labels_tb=snap.xlen_labels_tb,
        xlen_ml_confirm=snap.xlen_ml_confirm,
        growth_gated_out_window=snap.growth_gated_out_window,
        growth_labels_tb_window=snap.growth_labels_tb_window,
        window_hours=cfg.stage3_window_h,
        autocal_groups=snap.autocal_groups,
        autocal_rollback_total=snap.autocal_rollback_total,
        autocal_phase_distribution=snap.autocal_phase_distribution,
        llm_veto_rate=snap.llm_veto_rate,
        days_since_start=snap.days_since_start,
        timeout_sec=cfg.llm_timeout_sec,
    )
    blocks = _advisory_blocks(advisory)
    c_llm_calls.labels(verdict="block" if blocks else "allow").inc()
    return (advisory, blocks)


def _advisory_blocks(advisory: dict[str, Any]) -> bool:
    """Same VETO contract as autocal."""
    if not isinstance(advisory, dict):
        return False
    if advisory.get("blocked_recommendations"):
        return True
    for r in advisory.get("guarded_recommendations") or []:
        if (r or {}).get("action") == "freeze_candidate":
            return True
    return False


def _enter_stage(
    state: RolloutState,
    new_stage: str,
    snap: Snapshot,
    *,
    now_ms: int,
) -> None:
    state.stage = new_stage
    state.stage_entry_ms = now_ms
    state.xlen_gated_out_at_entry = snap.xlen_gated_out_outcomes
    state.xlen_labels_tb_at_entry = snap.xlen_labels_tb


def _flip_enforce(r: Any, cfg: Cfg) -> bool:
    """Write `1` to cfg:gva:enforce. Idempotent: only writes if not already 1."""
    try:
        current = r.get(cfg.enforce_override_key)
        if str(current or "").strip() in {"1", "true", "yes"}:
            return False
        r.set(cfg.enforce_override_key, "1")
        return True
    except Exception as e:
        log.warning("enforce flip failed: %s", e)
        return False


# ── Main cycle ───────────────────────────────────────────────────────────────


def run_once(
    r: Any,
    cfg: Cfg,
    *,
    now_ms: int | None = None,
) -> RolloutState:
    """One governor cycle. Returns the (possibly updated) state."""
    now_ms = int(time.time() * 1000) if now_ms is None else now_ms
    state = _load_state(r, cfg.rollout_state_key)

    if state.stage_entry_ms == 0:
        state.stage_entry_ms = now_ms
        state.xlen_gated_out_at_entry = _xlen(r, cfg.stream_gated_out_outcomes)
        state.xlen_labels_tb_at_entry = _xlen(r, cfg.stream_labels_tb)

    snap = _build_snapshot(r, cfg, state, now_ms=now_ms)

    # Update Prometheus
    try:
        for s in STAGE_LADDER:
            g_current_stage.labels(stage=s).set(1.0 if s == state.stage else 0.0)
        g_stage_dwell_h.set(snap.stage_dwell_h)
        g_xlen.labels(stream=cfg.stream_gated_out_outcomes).set(
            snap.xlen_gated_out_outcomes
        )
        g_xlen.labels(stream=cfg.stream_labels_tb).set(snap.xlen_labels_tb)
        g_xlen.labels(stream=cfg.stream_ml_confirm).set(snap.xlen_ml_confirm)
        g_growth_window.labels(stream=cfg.stream_gated_out_outcomes).set(
            snap.growth_gated_out_window
        )
        g_growth_window.labels(stream=cfg.stream_labels_tb).set(
            snap.growth_labels_tb_window
        )
        g_autocal_rollback_total.set(snap.autocal_rollback_total)
        g_autocal_stable_frac.set(snap.autocal_stable_frac)
        g_enforce_flipped.set(1.0 if state.enforce_flipped_ms > 0 else 0.0)
    except Exception:
        pass

    # Terminal stage: just daily summary if due
    if state.stage == STAGE_6:
        _maybe_send_daily_summary(r, cfg, state, snap, now_ms=now_ms)
        _save_state(r, cfg.rollout_state_key, state, secret=cfg.hmac_secret)
        c_cycles.labels(outcome="terminal").inc()
        return state

    proposed = STAGE_NEXT[state.stage]
    ok, fails = _numerical_gates_advance(state, snap, cfg)

    if not ok:
        # Stage held by numerical gates — no LLM call, no Telegram unless
        # something dramatic (stage 6c rollback case).
        _save_state(r, cfg.rollout_state_key, state, secret=cfg.hmac_secret)
        c_cycles.labels(outcome="held_numerical").inc()
        return state

    # Numerical gates passed → consult LLM
    advisory, blocks = _consult_llm(snap, state, cfg, proposed_stage=proposed)
    state.llm_veto_history.append(1 if blocks else 0)
    state.llm_veto_history = state.llm_veto_history[-32:]

    if advisory is not None:
        _send_telegram(
            r, cfg=cfg, event="llm_advisory",
            text=_fmt_llm_advisory(state.stage, proposed, advisory),
        )

    if blocks:
        # LLM veto: at STAGE_6C this rolls back to STAGE_5; elsewhere just hold.
        if state.stage == STAGE_6C:
            prev_stage = state.stage
            _enter_stage(state, STAGE_PREV[STAGE_6C], snap, now_ms=now_ms)
            c_stage_transitions.labels(
                from_stage=prev_stage, to_stage=state.stage, trigger="llm_veto"
            ).inc()
            _send_telegram(
                r, cfg=cfg, event="rollback",
                text=_fmt_stage_transition(
                    prev_stage, state.stage, snap,
                    reason="LLM veto in STAGE_6_CANDIDATE — rolling back to STAGE_5",
                ),
            )
        else:
            _send_telegram(
                r, cfg=cfg, event="stage_held",
                text=_fmt_held(state.stage, "LLM veto"),
            )
        _save_state(r, cfg.rollout_state_key, state, secret=cfg.hmac_secret)
        c_cycles.labels(outcome="held_llm").inc()
        return state

    # Advance!
    prev_stage = state.stage
    _enter_stage(state, proposed, snap, now_ms=now_ms)
    c_stage_transitions.labels(
        from_stage=prev_stage, to_stage=state.stage, trigger="numerical+llm_ok"
    ).inc()
    _send_telegram(
        r, cfg=cfg, event="stage_transition",
        text=_fmt_stage_transition(
            prev_stage, state.stage, snap, reason="gates passed; LLM not blocking"
        ),
    )

    # Final flip happens when we enter STAGE_6 (i.e., we just advanced FROM 6c TO 6)
    if state.stage == STAGE_6:
        flipped = _flip_enforce(r, cfg)
        if flipped:
            state.enforce_flipped_ms = now_ms
            _send_telegram(
                r, cfg=cfg, event="enforce_flipped",
                text=_fmt_enforce_flipped(snap),
            )

    _save_state(r, cfg.rollout_state_key, state, secret=cfg.hmac_secret)
    c_cycles.labels(outcome="advanced").inc()
    return state


def _maybe_send_daily_summary(
    r: Any,
    cfg: Cfg,
    state: RolloutState,
    snap: Snapshot,
    *,
    now_ms: int,
) -> None:
    if not cfg.daily_summary:
        return
    if state.last_daily_summary_ms > 0 and (now_ms - state.last_daily_summary_ms) < (
        24 * 3_600_000
    ):
        return
    text = (
        "<b>📊 gate_value rollout — daily summary</b>\n"
        f"stage=<b>{state.stage}</b> dwell=<b>{snap.stage_dwell_h:.0f}h</b>\n"
        f"autocal groups=<b>{snap.autocal_groups}</b> "
        f"rollback_total=<b>{snap.autocal_rollback_total}</b> "
        f"stable_frac=<b>{snap.autocal_stable_frac:.2f}</b>\n"
        f"phase_dist: <code>{json.dumps(snap.autocal_phase_distribution)}</code>"
    )
    _send_telegram(r, cfg=cfg, event="daily_summary", text=text)
    state.last_daily_summary_ms = now_ms


# ── Entrypoint ───────────────────────────────────────────────────────────────


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    cfg = load_cfg()
    if not cfg.enable:
        log.warning("GVR_ENABLE=0 — idling")
        while True:
            time.sleep(3600)

    try:
        start_http_server(cfg.prom_port)
        log.info("Prometheus on %d", cfg.prom_port)
    except Exception as e:
        log.warning("prom start failed: %s", e)

    import redis as redis_sync

    r = redis_sync.Redis.from_url(cfg.redis_url, decode_responses=True)
    try:
        r.ping()
    except Exception as e:
        log.warning("redis ping failed: %s", e)

    state = _load_state(r, cfg.rollout_state_key)
    _send_telegram(r, cfg=cfg, event="startup", text=_fmt_startup(cfg, state.stage))

    while True:
        try:
            run_once(r, cfg)
        except Exception as e:
            log.exception("run_once failed: %s", e)
            c_cycles.labels(outcome="error").inc()
        time.sleep(max(60, cfg.interval_sec))


__all__ = [
    "Cfg",
    "RolloutState",
    "Snapshot",
    "STAGE_3",
    "STAGE_5",
    "STAGE_6C",
    "STAGE_6",
    "STAGE_LADDER",
    "STAGE_NEXT",
    "STABLE_PHASES",
    "load_cfg",
    "run_once",
    "_build_snapshot",
    "_numerical_gates_advance",
    "_advisory_blocks",
    "_flip_enforce",
    "_phase_counts",
    "_enter_stage",
    "_load_state",
    "_save_state",
]


if __name__ == "__main__":
    raise SystemExit(main())
