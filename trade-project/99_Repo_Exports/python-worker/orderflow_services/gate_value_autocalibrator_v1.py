"""gate_value_autocalibrator_v1.py — autocal driven by gate_value_reporter.

Reads `report:gate_value:latest` (produced by
python-worker/services/gate_value_reporter), maps the reporter's per-group
decision to a phase ladder, runs numerical safety gates, consults a local
LLM advisor (VETO-only via llm_recommendation_guard_v1), commits a phase
transition into Redis state, optionally writes an "applied" config blob
(SHADOW by default), and notifies Telegram.

Phase ladder per (kind, symbol, horizon) group:
    OBSERVE
        ↓ (reporter=KEEP_GATE & lift_ci_low > min_lift)
    KEEP_CONFIRMED                  ← gate confirmed useful, no change to min_conf
        ↑
    OBSERVE
        ↓ (reporter=RELAX_GATE & ci_high < 0 OR FN rate high)
    RELAX_CANARY                    ← shadow-apply min_conf - relax_step
        ↓ (next cycle confirms RELAX)
    RELAX_APPLIED                   ← writes cfg key (only if ENFORCE)
        ↑ rollback (next cycle says KEEP)
    OBSERVE
        ↓ (reporter=DISABLE_GATE & passed cohort negative)
    DISABLE_CANDIDATE               ← never auto-applied; raises ticket via Telegram

Defaults:
    GVA_ENABLE=1  but
    GVA_ENFORCE=0 → no writes to applied config, only state + Telegram + Prom
    GVA_LLM_ENABLED=1 (read-only; advisor cannot force, only veto)

Telegram events: startup, phase_transition, llm_advisory, disable_candidate.
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

log = logging.getLogger("gate_value_autocalibrator")

# ── ENV helpers ───────────────────────────────────────────────────────────────


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


# ── Config ────────────────────────────────────────────────────────────────────

STATE_KEY_DEFAULT = "autocal:gate_value:state"
APPLIED_KEY_PREFIX_DEFAULT = "cfg:gate_value_autocal:applied"
ENFORCE_OVERRIDE_KEY_DEFAULT = "cfg:gva:enforce"

PHASE_LADDER = (
    "OBSERVE",
    "KEEP_CONFIRMED",
    "RELAX_CANARY",
    "RELAX_APPLIED",
    "DISABLE_CANDIDATE",
    "ROLLED_BACK",
)


@dataclass
class Cfg:
    enable: bool
    enforce: bool
    interval_sec: int
    report_key: str
    state_key: str
    applied_key_prefix: str
    enforce_override_key: str

    # Phase gates
    min_n_passed: int
    min_n_gated_out: int
    min_dwell_h: float
    min_avg_r_lift: float
    max_false_negative_rate: float

    # Action steps (applied as shadow recommendations)
    relax_min_conf_step: float
    tighten_min_conf_step: float
    min_conf_floor: float
    min_conf_ceiling: float

    # LLM
    llm_enabled: bool
    llm_timeout_sec: float
    llm_backend: str

    # Notify / state
    notify_telegram: bool
    notify_stream: str
    hmac_secret: str

    # Bootstrap & report
    redis_url: str
    prom_port: int


def load_cfg() -> Cfg:
    return Cfg(
        enable=_env_bool("GVA_ENABLE", True),
        enforce=_env_bool("GVA_ENFORCE", False),
        interval_sec=_env_int("GVA_INTERVAL_SEC", 900),
        report_key=_env("GVA_REPORT_KEY", "report:gate_value:latest"),
        state_key=_env("GVA_STATE_KEY", STATE_KEY_DEFAULT),
        applied_key_prefix=_env("GVA_APPLIED_KEY_PREFIX", APPLIED_KEY_PREFIX_DEFAULT),
        enforce_override_key=_env("GVA_ENFORCE_OVERRIDE_KEY", ENFORCE_OVERRIDE_KEY_DEFAULT),
        min_n_passed=_env_int("GVA_MIN_N_PASSED", 500),
        min_n_gated_out=_env_int("GVA_MIN_N_GATED_OUT", 500),
        min_dwell_h=_env_float("GVA_MIN_DWELL_H", 12.0),
        min_avg_r_lift=_env_float("GVA_MIN_AVG_R_LIFT", 0.05),
        max_false_negative_rate=_env_float("GVA_MAX_FALSE_NEGATIVE_RATE", 0.25),
        relax_min_conf_step=_env_float("GVA_RELAX_STEP", 0.02),
        tighten_min_conf_step=_env_float("GVA_TIGHTEN_STEP", 0.02),
        min_conf_floor=_env_float("GVA_MIN_CONF_FLOOR", 0.30),
        min_conf_ceiling=_env_float("GVA_MIN_CONF_CEILING", 0.85),
        llm_enabled=_env_bool("GVA_LLM_ENABLED", True),
        llm_timeout_sec=_env_float("GVA_LLM_TIMEOUT_SEC", 30.0),
        llm_backend=_env("GVA_LLM_BACKEND", "auto"),
        notify_telegram=_env_bool("GVA_NOTIFY_TELEGRAM", True),
        notify_stream=_env("GVA_NOTIFY_STREAM", "notify:telegram"),
        hmac_secret=(
            _env("GVA_HMAC_SECRET", "")
            or _env("RECS_HMAC_SECRET", "")
            or _env("LAYERS_CAL_HMAC_SECRET", "")
        ),
        redis_url=_env("REDIS_URL", "redis://redis-worker-1:6379/0"),
        prom_port=_env_int("GVA_PROM_PORT", 9142),
    )


# ── Prometheus metrics ────────────────────────────────────────────────────────

c_cycles = Counter(
    "gva_cycles_total",
    "Autocal run_once cycles",
    ["outcome"],  # ok | report_missing | error
)
c_decisions = Counter(
    "gva_decisions_total",
    "Per-group decisions emitted by autocal",
    ["phase", "action"],
)
c_telegram = Counter(
    "gva_telegram_total",
    "Telegram XADD attempts",
    ["event", "outcome"],
)
c_llm_calls = Counter(
    "gva_llm_calls_total",
    "LLM advisor invocations",
    ["verdict"],  # allow | block | skipped
)
g_groups_total = Gauge(
    "gva_groups_total",
    "Number of groups evaluated this cycle",
)
g_last_cycle_ts = Gauge(
    "gva_last_cycle_ts_seconds",
    "Wall-clock time of last successful cycle",
)
g_phase = Gauge(
    "gva_group_phase",
    "Current phase per group (encoded by index)",
    ["group_key", "phase"],
)


# ── Telegram ──────────────────────────────────────────────────────────────────


def _send_telegram(
    r: Any,
    *,
    cfg: Cfg,
    event: str,
    text: str,
    subtype: str = "gate_value_autocal",
) -> None:
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


def _fmt_startup(cfg: Cfg) -> str:
    return (
        "<b>🟢 gate_value autocal started</b>\n"
        f"enforce=<b>{int(cfg.enforce)}</b> "
        f"llm=<b>{int(cfg.llm_enabled)}</b> ({cfg.llm_backend}) "
        f"interval=<b>{cfg.interval_sec}s</b> "
        f"dwell=<b>{cfg.min_dwell_h:.0f}h</b>\n"
        f"steps relax/tighten=<b>{cfg.relax_min_conf_step:.02f}</b>/"
        f"<b>{cfg.tighten_min_conf_step:.02f}</b> "
        f"floor/ceil=<b>{cfg.min_conf_floor:.02f}</b>/"
        f"<b>{cfg.min_conf_ceiling:.02f}</b>"
    )


def _fmt_phase_transition(d: "GroupDecision") -> str:
    arrow = "→"
    return (
        f"<b>🔄 gate_value phase</b> <code>{d.group_key}</code>\n"
        f"{d.prev_phase} {arrow} <b>{d.phase}</b> "
        f"(reporter=<b>{d.reporter_action}</b>)\n"
        f"passed n=<b>{d.passed_n}</b> avg_r=<b>{d.passed_avg_r:+.3f}</b> | "
        f"gated_out n=<b>{d.gated_out_n}</b> avg_r=<b>{d.gated_out_avg_r:+.3f}</b>\n"
        f"lift=<b>{d.avg_r_lift:+.3f}</b> "
        f"ci=[<b>{d.ci_low:+.3f}</b>, <b>{d.ci_high:+.3f}</b>] "
        f"fn_rate=<b>{d.false_negative_rate:.2f}</b>\n"
        f"reason: <i>{(d.reason or '')[:200]}</i>"
    )


def _fmt_llm_advisory(d: "GroupDecision") -> str:
    rec_summary = ", ".join(
        f"{r.get('action', '?')}({r.get('risk', '?')})"
        for r in (d.llm_guarded or [])[:3]
    ) or "none"
    blocks = len(d.llm_blocked or [])
    return (
        f"<b>🧠 gate_value LLM advisory</b> <code>{d.group_key}</code>\n"
        f"phase {d.prev_phase} → <b>{d.proposed_phase}</b>\n"
        f"recs: {rec_summary} | blocked=<b>{blocks}</b>\n"
        f"summary: <i>{(d.llm_summary or '')[:200]}</i>"
    )


def _fmt_disable_candidate(d: "GroupDecision") -> str:
    return (
        f"<b>⛔ DISABLE_CANDIDATE</b> <code>{d.group_key}</code>\n"
        f"Passed cohort itself is negative (avg_r=<b>{d.passed_avg_r:+.3f}</b>, "
        f"pf=<b>{d.passed_profit_factor:.2f}</b>).\n"
        f"Manual review required — autocal NEVER auto-disables. "
        f"reporter={d.reporter_action}"
    )


# ── State ─────────────────────────────────────────────────────────────────────


def _hmac_sign(payload: dict[str, Any], secret: str) -> str:
    data = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hmac.new(secret.encode("utf-8"), data, hashlib.sha256).hexdigest()


def _load_prev_state(r: Any, state_key: str) -> dict[str, dict[str, Any]]:
    """Return per-group dicts {phase, last_phase_change_ms, rollback_count, ...}."""
    try:
        raw = r.get(state_key)
    except Exception as e:
        log.warning("state load failed: %s", e)
        return {}
    if not raw:
        return {}
    try:
        payload = json.loads(raw)
    except Exception:
        return {}
    groups = payload.get("groups", {})
    return groups if isinstance(groups, dict) else {}


# ── Per-group decision logic ──────────────────────────────────────────────────


@dataclass
class GroupDecision:
    group_key: str
    symbol: str
    kind: str
    horizon_ms: int

    # Inputs from report
    reporter_action: str  # KEEP_GATE / RELAX_GATE / DISABLE_GATE / ...
    passed_n: int
    passed_avg_r: float
    passed_win_rate: float
    passed_profit_factor: float
    gated_out_n: int
    gated_out_avg_r: float
    gated_out_win_rate: float
    gated_out_profit_factor: float
    avg_r_lift: float
    false_negative_rate: float
    ci_low: float
    ci_high: float

    # State
    prev_phase: str = "OBSERVE"
    phase: str = "OBSERVE"
    proposed_phase: str = "OBSERVE"
    dwell_h: float = 0.0
    last_phase_change_ms: int = 0
    rollback_count: int = 0

    # Output
    applied_min_conf_delta: float = 0.0
    reason: str = ""
    llm_summary: str = ""
    llm_guarded: list[dict[str, Any]] = field(default_factory=list)
    llm_blocked: list[dict[str, Any]] = field(default_factory=list)


def _propose_phase(
    reporter_action: str,
    prev_phase: str,
    *,
    ci_low: float,
    ci_high: float,
    fn_rate: float,
    cfg: Cfg,
) -> tuple[str, str]:
    """Return (proposed_phase, reason)."""
    if reporter_action == "DISABLE_GATE":
        return "DISABLE_CANDIDATE", "reporter=DISABLE_GATE"
    if reporter_action == "KEEP_GATE" and ci_low > cfg.min_avg_r_lift:
        return "KEEP_CONFIRMED", f"reporter=KEEP_GATE & ci_low>{cfg.min_avg_r_lift}"
    if reporter_action == "RELAX_GATE" and ci_high < 0.0:
        if prev_phase in {"OBSERVE", "KEEP_CONFIRMED", "ROLLED_BACK"}:
            return "RELAX_CANARY", "reporter=RELAX_GATE & ci_high<0"
        if prev_phase == "RELAX_CANARY":
            return "RELAX_APPLIED", "RELAX_CANARY confirmed by next cycle"
        return prev_phase, "RELAX_GATE but phase already advanced"
    if reporter_action == "RELAX_GATE" and fn_rate > cfg.max_false_negative_rate:
        if prev_phase in {"OBSERVE", "KEEP_CONFIRMED"}:
            return "RELAX_CANARY", f"reporter=RELAX_GATE & fn_rate>{cfg.max_false_negative_rate}"
        return prev_phase, "high fn_rate but phase already advanced"
    if reporter_action == "INSUFFICIENT_DATA":
        return prev_phase, "insufficient_data — hold phase"
    return "OBSERVE", "inconclusive — back to OBSERVE"


def _numerical_gates_pass(
    *,
    passed_n: int,
    gated_out_n: int,
    dwell_h: float,
    cfg: Cfg,
) -> tuple[bool, list[str]]:
    fails: list[str] = []
    if passed_n < cfg.min_n_passed:
        fails.append(f"passed_n<{cfg.min_n_passed}")
    if gated_out_n < cfg.min_n_gated_out:
        fails.append(f"gated_out_n<{cfg.min_n_gated_out}")
    if dwell_h < cfg.min_dwell_h:
        fails.append(f"dwell_h<{cfg.min_dwell_h}")
    return (len(fails) == 0, fails)


def _advisory_blocks_transition(advisory: dict[str, Any]) -> bool:
    """Same VETO contract as edge_directional_bias_autocal_v1.

    Blocked if guard rejected the LLM payload (any blocked_recommendations) OR
    if LLM explicitly requested `freeze_candidate`.
    """
    if not isinstance(advisory, dict):
        return False
    if advisory.get("blocked_recommendations"):
        return True
    for r in advisory.get("guarded_recommendations", []) or []:
        if (r or {}).get("action") == "freeze_candidate":
            return True
    return False


def _group_key(symbol: str, kind: str, horizon_ms: int) -> str:
    return f"{kind}|{symbol}|{int(horizon_ms)}"


def _parse_report_group(grp: dict[str, Any]) -> GroupDecision | None:
    g = grp.get("group", {}) or {}
    p = grp.get("passed", {}) or {}
    o = grp.get("gated_out", {}) or {}
    lift = grp.get("lift", {}) or {}
    ci = grp.get("ci", {}) or {}
    dec = grp.get("decision", {}) or {}

    symbol = str(g.get("symbol") or "")
    kind = str(g.get("kind") or "")
    horizon_ms = int(g.get("horizon_ms") or 0)
    if not symbol or not kind:
        return None

    return GroupDecision(
        group_key=_group_key(symbol, kind, horizon_ms),
        symbol=symbol,
        kind=kind,
        horizon_ms=horizon_ms,
        reporter_action=str(dec.get("action") or "INCONCLUSIVE"),
        passed_n=int(p.get("n") or 0),
        passed_avg_r=float(p.get("avg_r") or 0.0),
        passed_win_rate=float(p.get("win_rate") or 0.0),
        passed_profit_factor=float(p.get("profit_factor") or 0.0),
        gated_out_n=int(o.get("n") or 0),
        gated_out_avg_r=float(o.get("avg_r") or 0.0),
        gated_out_win_rate=float(o.get("win_rate") or 0.0),
        gated_out_profit_factor=float(o.get("profit_factor") or 0.0),
        avg_r_lift=float(lift.get("avg_r_lift") or 0.0),
        false_negative_rate=float(lift.get("false_negative_rate") or 0.0),
        ci_low=float(ci.get("avg_r_lift_p05") or 0.0),
        ci_high=float(ci.get("avg_r_lift_p95") or 0.0),
    )


def _hydrate_prev(
    decision: GroupDecision,
    prev: dict[str, dict[str, Any]],
    *,
    now_ms: int,
) -> None:
    prev_entry = prev.get(decision.group_key) or {}
    decision.prev_phase = str(prev_entry.get("phase") or "OBSERVE")
    decision.phase = decision.prev_phase
    last_change = int(prev_entry.get("last_phase_change_ms") or 0)
    if last_change > 0:
        decision.dwell_h = max(0.0, (now_ms - last_change) / 3_600_000.0)
    else:
        decision.dwell_h = 0.0
    decision.last_phase_change_ms = last_change
    decision.rollback_count = int(prev_entry.get("rollback_count") or 0)


def _maybe_consult_llm(d: GroupDecision, cfg: Cfg) -> None:
    """Populate d.llm_* fields. Returns advisory result side-effect."""
    if not cfg.llm_enabled:
        c_llm_calls.labels(verdict="skipped").inc()
        return
    if d.proposed_phase == d.prev_phase:
        c_llm_calls.labels(verdict="skipped").inc()
        return
    try:
        from orderflow_services.gate_value_llm_advisor import advise_gate_transition
    except Exception as e:
        log.debug("llm advisor import failed: %s", e)
        c_llm_calls.labels(verdict="skipped").inc()
        return

    result = advise_gate_transition(
        group_key=d.group_key,
        current_phase=d.prev_phase,
        proposed_phase=d.proposed_phase,
        decision_action=d.reporter_action,
        passed_n=d.passed_n,
        passed_avg_r=d.passed_avg_r,
        passed_win_rate=d.passed_win_rate,
        passed_profit_factor=d.passed_profit_factor,
        gated_out_n=d.gated_out_n,
        gated_out_avg_r=d.gated_out_avg_r,
        gated_out_win_rate=d.gated_out_win_rate,
        gated_out_profit_factor=d.gated_out_profit_factor,
        avg_r_lift=d.avg_r_lift,
        false_negative_rate=d.false_negative_rate,
        ci_low=d.ci_low,
        ci_high=d.ci_high,
        dwell_h=d.dwell_h,
        timeout_sec=cfg.llm_timeout_sec,
    )
    d.llm_guarded = list(result.get("guarded_recommendations") or [])
    d.llm_blocked = list(result.get("blocked_recommendations") or [])
    d.llm_summary = ""
    for r in d.llm_guarded[:1]:
        d.llm_summary = str((r or {}).get("reason") or "")[:200]
    if not d.llm_summary:
        for r in d.llm_blocked[:1]:
            d.llm_summary = str((r or {}).get("reason") or "")[:200] or "blocked"

    if _advisory_blocks_transition(result):
        c_llm_calls.labels(verdict="block").inc()
        d.reason = (d.reason + "|llm_veto").lstrip("|")
        d.proposed_phase = d.prev_phase
    else:
        c_llm_calls.labels(verdict="allow").inc()


def _phase_to_min_conf_delta(phase: str, cfg: Cfg) -> float:
    """Map phase → recommended Δ to apply to confidence_gate.min_conf.

    Only RELAX_APPLIED produces a non-zero delta; everything else is 0.
    Sign convention: NEGATIVE delta means "loosen the gate"
    (rejected cohort wins were too high), POSITIVE means tighten.
    """
    if phase == "RELAX_APPLIED":
        return -abs(cfg.relax_min_conf_step)
    return 0.0


def _commit_phase(d: GroupDecision, *, now_ms: int) -> None:
    """Commit proposed_phase into phase + bump rollback / dwell."""
    if d.proposed_phase != d.prev_phase:
        d.phase = d.proposed_phase
        d.last_phase_change_ms = now_ms
        d.dwell_h = 0.0
        if d.prev_phase in {"RELAX_APPLIED", "RELAX_CANARY"} and d.phase in {
            "OBSERVE",
            "ROLLED_BACK",
        }:
            d.rollback_count += 1
    else:
        d.phase = d.prev_phase


def _resolve_enforce(r: Any, cfg: Cfg) -> bool:
    """ENFORCE = (Redis override) OR (ENV cfg.enforce).

    The rollout governor writes `1` to `cfg:gva:enforce` when its
    state machine reaches STAGE_6_ENFORCED. We honour that flip without
    requiring a container restart. Redis "1"/"true" wins; missing key or
    Redis error → fall back to cfg.enforce.

    NOTE: an EXPLICIT "0" in Redis does NOT override cfg.enforce=1 — once
    enforcement is enabled via ENV, only ops can turn it off.
    """
    try:
        raw = r.get(cfg.enforce_override_key)
    except Exception as e:
        log.debug("enforce override read failed: %s", e)
        return cfg.enforce
    if raw is None:
        return cfg.enforce
    val = str(raw).strip().lower()
    if val in {"1", "true", "yes", "on"}:
        return True
    return cfg.enforce


def _apply_to_redis(
    r: Any,
    d: GroupDecision,
    *,
    cfg: Cfg,
    now_ms: int,
) -> None:
    """If ENFORCE=1 AND phase=RELAX_APPLIED, write per-group config.

    Otherwise this is a no-op — autocal stays advisory.
    Even when applied, downstream confidence gate code must opt-in to read this
    key — autocalibrator does NOT directly mutate live gate ENV/state.
    """
    delta = _phase_to_min_conf_delta(d.phase, cfg)
    d.applied_min_conf_delta = delta
    if not _resolve_enforce(r, cfg):
        return
    if delta == 0.0:
        return
    payload = {
        "schema_version": 1,
        "ts_ms": now_ms,
        "group_key": d.group_key,
        "phase": d.phase,
        "min_conf_delta": delta,
        "min_conf_floor": cfg.min_conf_floor,
        "min_conf_ceiling": cfg.min_conf_ceiling,
        "reason": d.reason[:240],
        "llm_summary": d.llm_summary[:240],
    }
    if cfg.hmac_secret:
        payload["sig"] = _hmac_sign(payload, cfg.hmac_secret)
    key = f"{cfg.applied_key_prefix}:{d.group_key}"
    try:
        r.set(key, json.dumps(payload), ex=cfg.interval_sec * 6)
    except Exception as e:
        log.warning("applied write failed for %s: %s", key, e)


def _publish_state(
    r: Any,
    decisions: dict[str, GroupDecision],
    *,
    cfg: Cfg,
    now_ms: int,
) -> None:
    groups_payload: dict[str, dict[str, Any]] = {}
    for key, d in decisions.items():
        groups_payload[key] = {
            "phase": d.phase,
            "prev_phase": d.prev_phase,
            "proposed_phase": d.proposed_phase,
            "reporter_action": d.reporter_action,
            "passed_n": d.passed_n,
            "gated_out_n": d.gated_out_n,
            "passed_avg_r": d.passed_avg_r,
            "gated_out_avg_r": d.gated_out_avg_r,
            "avg_r_lift": d.avg_r_lift,
            "false_negative_rate": d.false_negative_rate,
            "ci_low": d.ci_low,
            "ci_high": d.ci_high,
            "dwell_h": d.dwell_h,
            "last_phase_change_ms": d.last_phase_change_ms,
            "rollback_count": d.rollback_count,
            "applied_min_conf_delta": d.applied_min_conf_delta,
            "reason": d.reason,
            "llm_summary": d.llm_summary,
        }
    payload: dict[str, Any] = {
        "schema_version": 1,
        "ts_ms": now_ms,
        "enforce": int(cfg.enforce),
        "llm_enabled": int(cfg.llm_enabled),
        "groups": groups_payload,
    }
    if cfg.hmac_secret:
        payload["sig"] = _hmac_sign(payload, cfg.hmac_secret)
    try:
        r.set(cfg.state_key, json.dumps(payload), ex=cfg.interval_sec * 6)
    except Exception as e:
        log.warning("state publish failed: %s", e)


# ── Main cycle ────────────────────────────────────────────────────────────────


def run_once(
    r: Any,
    cfg: Cfg,
    *,
    now_ms: int | None = None,
) -> dict[str, GroupDecision]:
    """Read report, decide, advise, commit. Returns per-group decisions.

    Pure-ish: only side effects are Redis writes + Prometheus counters +
    optional Telegram XADDs. No mutation of live signal pipeline.
    """
    now_ms = int(time.time() * 1000) if now_ms is None else now_ms

    try:
        raw = r.get(cfg.report_key)
    except Exception as e:
        log.warning("report read failed: %s", e)
        c_cycles.labels(outcome="error").inc()
        return {}
    if not raw:
        c_cycles.labels(outcome="report_missing").inc()
        log.info("report key %s missing — skipping cycle", cfg.report_key)
        return {}

    try:
        report = json.loads(raw)
    except Exception:
        c_cycles.labels(outcome="error").inc()
        log.exception("report parse failed")
        return {}

    prev_state = _load_prev_state(r, cfg.state_key)
    groups_in_report = report.get("groups") or []
    if not isinstance(groups_in_report, list):
        groups_in_report = []

    decisions: dict[str, GroupDecision] = {}

    for grp in groups_in_report:
        d = _parse_report_group(grp)
        if d is None:
            continue
        _hydrate_prev(d, prev_state, now_ms=now_ms)

        proposed, reason = _propose_phase(
            d.reporter_action,
            d.prev_phase,
            ci_low=d.ci_low,
            ci_high=d.ci_high,
            fn_rate=d.false_negative_rate,
            cfg=cfg,
        )
        d.proposed_phase = proposed
        d.reason = reason

        # Numerical gates only apply to actual phase advances.
        if proposed != d.prev_phase and proposed not in {"OBSERVE", "DISABLE_CANDIDATE"}:
            ok, fails = _numerical_gates_pass(
                passed_n=d.passed_n,
                gated_out_n=d.gated_out_n,
                dwell_h=d.dwell_h,
                cfg=cfg,
            )
            if not ok:
                d.proposed_phase = d.prev_phase
                d.reason = (d.reason + "|numerical_gates_fail:" + ",".join(fails))[:200]

        _maybe_consult_llm(d, cfg)
        _commit_phase(d, now_ms=now_ms)
        _apply_to_redis(r, d, cfg=cfg, now_ms=now_ms)

        c_decisions.labels(phase=d.phase, action=d.reporter_action).inc()
        try:
            for ph in PHASE_LADDER:
                g_phase.labels(group_key=d.group_key, phase=ph).set(
                    1.0 if ph == d.phase else 0.0
                )
        except Exception:
            pass

        if d.phase != d.prev_phase:
            _send_telegram(
                r, cfg=cfg, event="phase_transition",
                text=_fmt_phase_transition(d),
            )
            if d.llm_summary or d.llm_guarded or d.llm_blocked:
                _send_telegram(
                    r, cfg=cfg, event="llm_advisory",
                    text=_fmt_llm_advisory(d),
                )
            if d.phase == "DISABLE_CANDIDATE":
                _send_telegram(
                    r, cfg=cfg, event="disable_candidate",
                    text=_fmt_disable_candidate(d),
                )

        decisions[d.group_key] = d

    _publish_state(r, decisions, cfg=cfg, now_ms=now_ms)
    g_groups_total.set(len(decisions))
    g_last_cycle_ts.set(time.time())
    c_cycles.labels(outcome="ok").inc()
    return decisions


# ── Entrypoint ────────────────────────────────────────────────────────────────


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    cfg = load_cfg()

    if not cfg.enable:
        log.warning("GVA_ENABLE=0 — idling")
        while True:
            time.sleep(3600)

    try:
        start_http_server(cfg.prom_port)
        log.info("Prometheus on %d", cfg.prom_port)
    except Exception as e:
        log.warning("Prometheus start failed: %s", e)

    # Import here so the module is testable without redis installed.
    import redis as redis_sync

    r = redis_sync.Redis.from_url(cfg.redis_url, decode_responses=True)
    try:
        r.ping()
    except Exception as e:
        log.warning("redis ping failed at startup: %s", e)

    _send_telegram(r, cfg=cfg, event="startup", text=_fmt_startup(cfg))
    log.info(
        "starting loop: interval=%ds enforce=%s llm=%s",
        cfg.interval_sec, cfg.enforce, cfg.llm_enabled,
    )

    while True:
        try:
            run_once(r, cfg)
        except Exception as e:
            log.exception("run_once failed: %s", e)
            c_cycles.labels(outcome="error").inc()
        time.sleep(max(60, cfg.interval_sec))


# Helpers exported for tests
__all__ = [
    "Cfg",
    "GroupDecision",
    "PHASE_LADDER",
    "load_cfg",
    "run_once",
    "_propose_phase",
    "_numerical_gates_pass",
    "_advisory_blocks_transition",
    "_phase_to_min_conf_delta",
    "_parse_report_group",
    "_hydrate_prev",
    "_commit_phase",
]


if __name__ == "__main__":
    raise SystemExit(main())
