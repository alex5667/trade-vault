"""Core decision logic for the confidence meta-gate.

Pure function: takes a ConfidenceMetaGateInput + MetaGateConfig + artifact,
returns ConfidenceMetaGateOutput. Side effects (Prometheus, Redis XADD) live
in metrics.py and are called by the caller after decide_meta_gate().
"""
from __future__ import annotations

import time
from typing import Iterable

from .canary import canary_bucket as _canary_bucket
from .canary import is_canary_selected as _is_canary_selected
from .config import MetaGateConfig, MetaGateMode
from .dto import ConfidenceMetaGateInput, ConfidenceMetaGateOutput, MetaGateDecisionT
from .model import MetaGateArtifact
from .reason_codes import MetaGateReason


def risk_multiplier_from_p_win(p_win: float, *, min_p_win: float = 0.56) -> float:
    """Conservative sizing hint — never exceeds 1.10× and never bypasses the
    risk engine. Disabled by default during rollout."""
    if p_win < min_p_win:
        return 0.0
    if p_win < 0.60:
        return 0.50
    if p_win < 0.65:
        return 0.75
    if p_win < 0.72:
        return 1.00
    return 1.10


def _expected_r(p_win: float, *, avg_win_r: float = 1.0, avg_loss_r: float = 1.0,
                exec_cost_r: float = 0.0) -> float:
    """E[R] = p·win_r − (1−p)·loss_r − exec_cost_r. Inputs already in R-units."""
    return p_win * avg_win_r - (1.0 - p_win) * avg_loss_r - exec_cost_r


def _exec_cost_r_from_bps(spread_bps: float, slippage_bps: float, fee_bps: float,
                          sl_bps: float) -> float:
    """Convert bp-denominated execution cost into R-units using the SL distance.

    For very small SL distances we cap the resulting cost-R at a sensible
    upper bound to keep E[R] well-defined; the gate already vetoes signals
    with sl_bps==0 via the legacy path so this is only a guard.
    """
    if sl_bps <= 0.0:
        return 0.0
    cost_bps = max(0.0, spread_bps * 0.5) + max(0.0, slippage_bps) + max(0.0, fee_bps)
    raw = cost_bps / sl_bps
    return min(raw, 1.0)


def _fallback_output(
    *, inp: ConfidenceMetaGateInput, cfg: MetaGateConfig, model_ver: str,
    reasons: Iterable[str], started_ns: int,
    mode: MetaGateMode | None = None,
) -> ConfidenceMetaGateOutput:
    decision: MetaGateDecisionT = "FALLBACK_LEGACY"
    latency_ms = (time.perf_counter_ns() - started_ns) / 1e6
    return ConfidenceMetaGateOutput(
        sid=inp.sid,
        model_ver=model_ver,
        mode=(mode or cfg.mode).value,
        decision=decision,
        active=False,
        p_win_raw=0.0,
        p_win_calibrated=0.0,
        p_win_floor=cfg.min_p_win,
        expected_r=0.0,
        expected_edge_bps=inp.expected_edge_bps,
        risk_multiplier=0.0,
        canary_bucket=_canary_bucket(inp.sid, cfg.canary_salt),
        canary_selected=False,
        reason_codes=list(reasons),
        latency_ms=latency_ms,
    )


def decide_meta_gate(
    inp: ConfidenceMetaGateInput,
    cfg: MetaGateConfig,
    artifact: MetaGateArtifact | None,
    *,
    now_ms: int | None = None,
    mode_override: MetaGateMode | None = None,
) -> ConfidenceMetaGateOutput:
    """Compute the meta-gate decision.

    The function is total: every (cfg, artifact) combination resolves to a
    valid output. Caller decides what to do with the output based on mode.

    `mode_override` lets the runtime feed an effective mode (e.g. SHADOW
    forced by the auto-demote watcher) without rebuilding the whole cfg —
    used by `MetaGateRuntime.effective_mode()`. The output's `mode` field
    reflects the override so downstream metrics see what actually drove
    the decision.
    """
    started_ns = time.perf_counter_ns()
    effective_mode = mode_override if mode_override is not None else cfg.mode

    # ── early lifecycle paths ─────────────────────────────────────────
    if not cfg.enabled or effective_mode in (MetaGateMode.OFF, MetaGateMode.LEGACY_ONLY,
                                             MetaGateMode.KILL_SWITCH):
        reason = {
            MetaGateMode.OFF: MetaGateReason.MODE_OFF,
            MetaGateMode.LEGACY_ONLY: MetaGateReason.MODE_LEGACY_ONLY,
            MetaGateMode.KILL_SWITCH: MetaGateReason.MODE_KILL_SWITCH,
        }.get(effective_mode, MetaGateReason.MODE_OFF)
        return _fallback_output(
            inp=inp, cfg=cfg,
            model_ver=(artifact.model_ver if artifact else ""),
            reasons=[reason.value, MetaGateReason.LEGACY_FALLBACK.value],
            started_ns=started_ns,
            mode=effective_mode,
        )

    if artifact is None:
        return _fallback_output(
            inp=inp, cfg=cfg, model_ver="",
            reasons=[MetaGateReason.MODEL_NOT_LOADED.value,
                     MetaGateReason.LEGACY_FALLBACK.value],
            started_ns=started_ns,
            mode=effective_mode,
        )

    # Schema fingerprint must match — otherwise the feature vector means
    # something different than what the model was trained on.
    if inp.feature_cols_hash and inp.feature_cols_hash != artifact.feature_cols_hash:
        return _fallback_output(
            inp=inp, cfg=cfg, model_ver=artifact.model_ver,
            reasons=[MetaGateReason.SCHEMA_MISMATCH.value,
                     MetaGateReason.LEGACY_FALLBACK.value],
            started_ns=started_ns,
            mode=effective_mode,
        )

    now_ref_ms = now_ms if now_ms is not None else int(time.time() * 1000)
    if artifact.training_summary.created_ms > 0:
        age_hours = (now_ref_ms - artifact.training_summary.created_ms) / 1000.0 / 3600.0
        if age_hours > cfg.max_model_age_hours:
            return _fallback_output(
                inp=inp, cfg=cfg, model_ver=artifact.model_ver,
                reasons=[MetaGateReason.MODEL_STALE.value,
                         MetaGateReason.LEGACY_FALLBACK.value],
                started_ns=started_ns,
                mode=effective_mode,
            )

    if (artifact.calibrator.ece is not None
            and artifact.calibrator.ece > cfg.max_calibration_ece):
        return _fallback_output(
            inp=inp, cfg=cfg, model_ver=artifact.model_ver,
            reasons=[MetaGateReason.CALIBRATION_ECE_HIGH.value,
                     MetaGateReason.LEGACY_FALLBACK.value],
            started_ns=started_ns,
            mode=effective_mode,
        )

    # ── score + calibrate ─────────────────────────────────────────────
    try:
        p_raw = artifact.predict_raw(inp.features)
        p_cal = artifact.calibrate(p_raw)
    except Exception:
        # Last-resort fail-open; metrics will flag MODEL_ERROR.
        return _fallback_output(
            inp=inp, cfg=cfg, model_ver=artifact.model_ver,
            reasons=[MetaGateReason.MODEL_ERROR.value,
                     MetaGateReason.LEGACY_FALLBACK.value],
            started_ns=started_ns,
            mode=effective_mode,
        )

    # ── soft-cap / tightening flags (do NOT deny on their own) ───────
    soft_reasons: list[str] = []
    if inp.dq_score < cfg.dq_soft_cap:
        soft_reasons.append(MetaGateReason.DQ_DEGRADED.value)
    if inp.spread_bps > cfg.spread_soft_cap_bps:
        soft_reasons.append(MetaGateReason.SPREAD_SOFT_CAP.value)
    if inp.expected_slippage_bps > cfg.slippage_soft_cap_bps:
        soft_reasons.append(MetaGateReason.SLIPPAGE_SOFT_CAP.value)
    cost_bps = (inp.spread_bps * 0.5 + inp.expected_slippage_bps + inp.fee_bps)
    if cost_bps >= inp.expected_edge_bps:
        soft_reasons.append(MetaGateReason.EXEC_COST_HIGH.value)

    # ── expected R estimate ──────────────────────────────────────────
    # SL distance comes from the model feature vector (canonical name "sl_bps");
    # falls back to expected_edge_bps so E[R] stays interpretable when absent.
    sl_bps_feat = inp.features.get("sl_bps", 0.0) or 0.0
    if sl_bps_feat <= 0.0:
        sl_bps_feat = max(1.0, inp.expected_edge_bps)
    exec_cost_r = _exec_cost_r_from_bps(
        spread_bps=inp.spread_bps,
        slippage_bps=inp.expected_slippage_bps,
        fee_bps=inp.fee_bps,
        sl_bps=sl_bps_feat,
    )
    expected_r = _expected_r(p_cal, exec_cost_r=exec_cost_r)

    thr = artifact.thresholds
    min_p_win = max(cfg.min_p_win, thr.min_p_win)
    min_expected_r = max(cfg.min_expected_r, thr.min_expected_r)
    min_expected_edge_bps = max(cfg.min_expected_edge_bps, thr.min_expected_edge_bps)

    # ── decision rules ───────────────────────────────────────────────
    reasons: list[str] = []
    decision_kind: str
    if p_cal < min_p_win:
        reasons.append(MetaGateReason.P_WIN_BELOW_FLOOR.value)
        decision_kind = "DENY_SOFT"
    elif expected_r < min_expected_r:
        reasons.append(MetaGateReason.EXPECTED_R_BELOW_FLOOR.value)
        decision_kind = "DENY_SOFT"
    elif inp.expected_edge_bps < min_expected_edge_bps:
        reasons.append(MetaGateReason.EXPECTED_EDGE_BELOW_FLOOR.value)
        decision_kind = "DENY_SOFT"
    elif soft_reasons:
        reasons.extend(soft_reasons)
        reasons.append(MetaGateReason.META_ALLOW_TIGHTENED.value)
        decision_kind = "ALLOW_TIGHTENED"
    else:
        reasons.append(MetaGateReason.PROBABILITY_OK.value)
        reasons.append(MetaGateReason.EDGE_OK.value)
        reasons.append(MetaGateReason.META_ALLOW.value)
        decision_kind = "ALLOW"

    # ── canary / mode mapping ────────────────────────────────────────
    bucket = _canary_bucket(inp.sid, cfg.canary_salt)
    selected = False
    active = False
    decision: MetaGateDecisionT

    if effective_mode is MetaGateMode.SHADOW:
        reasons.insert(0, MetaGateReason.MODE_SHADOW.value)
        decision = "SHADOW_ALLOW" if decision_kind in {"ALLOW", "ALLOW_TIGHTENED"} else "SHADOW_DENY"
        active = False
    elif effective_mode is MetaGateMode.CANARY:
        selected = _is_canary_selected(inp.sid, cfg.canary_salt, cfg.canary_share)
        if selected:
            reasons.insert(0, MetaGateReason.MODE_CANARY_SELECTED.value)
            decision = _decision_kind_to_outer(decision_kind)
            active = True
        else:
            reasons.insert(0, MetaGateReason.MODE_CANARY_NOT_SELECTED.value)
            decision = "SHADOW_ALLOW" if decision_kind in {"ALLOW", "ALLOW_TIGHTENED"} else "SHADOW_DENY"
            active = False
    elif effective_mode is MetaGateMode.ENFORCE:
        reasons.insert(0, MetaGateReason.MODE_ENFORCE.value)
        decision = _decision_kind_to_outer(decision_kind)
        active = True
    else:  # safety net — should not be reachable
        reasons.insert(0, MetaGateReason.LEGACY_FALLBACK.value)
        decision = "FALLBACK_LEGACY"
        active = False

    risk_mult = (
        risk_multiplier_from_p_win(p_cal, min_p_win=min_p_win)
        if cfg.risk_mult_enabled else 0.0
    )

    latency_ms = (time.perf_counter_ns() - started_ns) / 1e6
    return ConfidenceMetaGateOutput(
        sid=inp.sid,
        model_ver=artifact.model_ver,
        mode=effective_mode.value,
        decision=decision,
        active=active,
        p_win_raw=p_raw,
        p_win_calibrated=p_cal,
        p_win_floor=min_p_win,
        expected_r=expected_r,
        expected_edge_bps=inp.expected_edge_bps,
        risk_multiplier=risk_mult,
        canary_bucket=bucket,
        canary_selected=selected,
        reason_codes=reasons,
        latency_ms=latency_ms,
    )


def _decision_kind_to_outer(kind: str) -> MetaGateDecisionT:
    if kind == "ALLOW":
        return "ALLOW"
    if kind == "ALLOW_TIGHTENED":
        return "ALLOW_TIGHTENED"
    return "DENY_SOFT"


def _abs(x: float) -> float:
    return -x if x < 0 else x
