"""Prometheus counters + Redis stream emit for the confidence meta-gate.

The metric handles are module-level singletons (idempotent registration) and
the stream XADD is best-effort fire-and-forget — never raised back into the
hot path.
"""
from __future__ import annotations

import contextlib
import json
import logging
from typing import Any

from .config import MetaGateConfig
from .dto import ConfidenceMetaGateInput, ConfidenceMetaGateOutput

log = logging.getLogger("conf_meta_gate.metrics")


# ── Prometheus (lazy / idempotent) ──────────────────────────────────────────
_PROM_INITIALIZED = False
_DECISION_TOTAL = None
_REASON_TOTAL = None
_LATENCY_MS = None
_P_WIN_HIST = None
_EXPECTED_R_HIST = None
_FALLBACK_TOTAL = None
_LEGACY_DISAGREEMENT_TOTAL = None
_CANARY_SELECTED_TOTAL = None
_MODEL_AGE_HOURS_GAUGE = None
_CALIBRATION_ECE_GAUGE = None


def _init_prom() -> None:
    global _PROM_INITIALIZED, _DECISION_TOTAL, _REASON_TOTAL, _LATENCY_MS
    global _P_WIN_HIST, _EXPECTED_R_HIST, _FALLBACK_TOTAL
    global _LEGACY_DISAGREEMENT_TOTAL, _CANARY_SELECTED_TOTAL
    global _MODEL_AGE_HOURS_GAUGE, _CALIBRATION_ECE_GAUGE

    if _PROM_INITIALIZED:
        return
    try:
        from prometheus_client import Counter, Gauge, Histogram

        _DECISION_TOTAL = Counter(
            "conf_meta_gate_decision_total",
            "Confidence meta-gate decisions by mode and active/meta/legacy decision",
            ["mode", "active_decision", "meta_decision", "legacy_decision"],
        )
        _REASON_TOTAL = Counter(
            "conf_meta_gate_reason_total",
            "Confidence meta-gate reason code occurrences",
            ["reason_code"],
        )
        _LATENCY_MS = Histogram(
            "conf_meta_gate_latency_ms",
            "End-to-end decide_meta_gate() latency in milliseconds",
            ["mode"],
            buckets=(0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 25.0, 50.0, 100.0),
        )
        _P_WIN_HIST = Histogram(
            "conf_meta_gate_p_win_calibrated",
            "Calibrated win probability emitted by the meta-gate",
            ["model_ver"],
            buckets=(0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.55, 0.6, 0.65, 0.7, 0.75, 0.8, 0.9, 1.0),
        )
        _EXPECTED_R_HIST = Histogram(
            "conf_meta_gate_expected_r",
            "Expected R-multiple emitted by the meta-gate",
            ["model_ver"],
            buckets=(-1.0, -0.5, -0.2, -0.1, -0.05, 0.0, 0.02, 0.05, 0.1, 0.2, 0.5, 1.0),
        )
        _FALLBACK_TOTAL = Counter(
            "conf_meta_gate_fallback_total",
            "Meta-gate fallbacks (model missing / stale / schema mismatch / ECE high)",
            ["reason"],
        )
        _LEGACY_DISAGREEMENT_TOTAL = Counter(
            "conf_meta_gate_legacy_disagreement_total",
            "Cases where legacy ALLOW/DENY differs from meta ALLOW/DENY",
            ["legacy_decision", "meta_decision"],
        )
        _CANARY_SELECTED_TOTAL = Counter(
            "conf_meta_gate_canary_selected_total",
            "Decisions where the sid landed inside the canary cohort",
            ["selected"],
        )
        _MODEL_AGE_HOURS_GAUGE = Gauge(
            "conf_meta_gate_model_age_hours",
            "Age in hours of the currently loaded meta-gate artifact",
            ["model_ver"],
        )
        _CALIBRATION_ECE_GAUGE = Gauge(
            "conf_meta_gate_calibration_ece",
            "OOS Expected Calibration Error from the meta-gate artifact",
            ["model_ver"],
        )
        _PROM_INITIALIZED = True
    except Exception as e:  # registry conflicts in tests; fail-open
        log.debug("conf_meta_gate Prometheus init failed: %s", e)
        _PROM_INITIALIZED = True  # do not retry


def _meta_kind(decision: str) -> str:
    """Collapse meta decision into ALLOW/DENY/FALLBACK for disagreement counters."""
    if decision in ("ALLOW", "ALLOW_TIGHTENED", "SHADOW_ALLOW"):
        return "ALLOW"
    if decision in ("DENY_SOFT", "SHADOW_DENY"):
        return "DENY"
    return "FALLBACK"


def _legacy_kind(decision: str) -> str:
    return "ALLOW" if (decision or "").upper() == "ALLOW" else "DENY"


def emit_decision(
    inp: ConfidenceMetaGateInput,
    out: ConfidenceMetaGateOutput,
    cfg: MetaGateConfig,
    *,
    active_decision: str,
    redis_client: Any | None = None,
    publish_loop: Any | None = None,
) -> None:
    """Record Prometheus counters + (optionally) push a JSON event to Redis.

    Never raises. `redis_client` is an async or sync Redis instance; when
    given, this function schedules a fire-and-forget XADD to cfg.decision_stream.
    """
    _init_prom()

    meta_kind = _meta_kind(out.decision)
    legacy_kind = _legacy_kind(inp.legacy_decision)
    active_kind = active_decision.upper() if active_decision else legacy_kind

    with contextlib.suppress(Exception):
        if _DECISION_TOTAL is not None:
            _DECISION_TOTAL.labels(
                mode=out.mode,
                active_decision=active_kind,
                meta_decision=meta_kind,
                legacy_decision=legacy_kind,
            ).inc()
    with contextlib.suppress(Exception):
        if _REASON_TOTAL is not None:
            for code in out.reason_codes:
                _REASON_TOTAL.labels(reason_code=code).inc()
    with contextlib.suppress(Exception):
        if _LATENCY_MS is not None:
            _LATENCY_MS.labels(mode=out.mode).observe(out.latency_ms)
    with contextlib.suppress(Exception):
        if _P_WIN_HIST is not None and out.model_ver:
            _P_WIN_HIST.labels(model_ver=out.model_ver).observe(out.p_win_calibrated)
            _EXPECTED_R_HIST.labels(model_ver=out.model_ver).observe(out.expected_r)
    with contextlib.suppress(Exception):
        if out.decision == "FALLBACK_LEGACY" and _FALLBACK_TOTAL is not None:
            # First non-mode reason is the actionable one.
            reason = next(
                (r for r in out.reason_codes if not r.startswith("mode_")
                 and r != "legacy_fallback"),
                "unknown",
            )
            _FALLBACK_TOTAL.labels(reason=reason).inc()
    with contextlib.suppress(Exception):
        if legacy_kind != meta_kind and meta_kind != "FALLBACK" \
                and _LEGACY_DISAGREEMENT_TOTAL is not None:
            _LEGACY_DISAGREEMENT_TOTAL.labels(
                legacy_decision=legacy_kind,
                meta_decision=meta_kind,
            ).inc()
    with contextlib.suppress(Exception):
        if _CANARY_SELECTED_TOTAL is not None and out.mode == "CANARY":
            _CANARY_SELECTED_TOTAL.labels(
                selected="1" if out.canary_selected else "0",
            ).inc()

    _emit_stream(inp, out, cfg, active_decision=active_kind,
                 redis_client=redis_client, publish_loop=publish_loop)


def _emit_stream(
    inp: ConfidenceMetaGateInput,
    out: ConfidenceMetaGateOutput,
    cfg: MetaGateConfig,
    *,
    active_decision: str,
    redis_client: Any | None,
    publish_loop: Any | None,
) -> None:
    if redis_client is None:
        return
    payload = {
        "ts_ms": inp.ts_ms,
        "now_ms": inp.now_ms,
        "sid": inp.sid,
        "symbol": inp.symbol,
        "kind": inp.kind,
        "side": inp.side,
        "mode": out.mode,
        "legacy_decision": inp.legacy_decision,
        "meta_decision": out.decision,
        "active_decision": active_decision,
        "p_win_raw": out.p_win_raw,
        "p_win_calibrated": out.p_win_calibrated,
        "p_win_floor": out.p_win_floor,
        "expected_r": out.expected_r,
        "expected_edge_bps": out.expected_edge_bps,
        "risk_multiplier": out.risk_multiplier,
        "canary_bucket": out.canary_bucket,
        "canary_selected": bool(out.canary_selected),
        "model_ver": out.model_ver,
        "schema_hash": inp.schema_hash,
        "feature_cols_hash": inp.feature_cols_hash,
        "spread_bps": inp.spread_bps,
        "expected_slippage_bps": inp.expected_slippage_bps,
        "fee_bps": inp.fee_bps,
        "dq_score": inp.dq_score,
        "regime": inp.regime,
        "session": inp.session,
        "reason_codes": list(out.reason_codes),
        "latency_ms": round(out.latency_ms, 3),
    }
    if cfg.sample_features_in_stream:
        # Bounded snapshot — only the columns the model declared, so analytics
        # can reproduce p_raw deterministically from this row.
        payload["features"] = dict(inp.features)
    try:
        body = json.dumps(payload, ensure_ascii=False, default=str)
        fields = {"payload": body}
        result = redis_client.xadd(
            cfg.decision_stream, fields, maxlen=50_000, approximate=True,
        )
        # If running async, schedule the coroutine.
        if hasattr(result, "__await__"):
            _schedule_awaitable(result, publish_loop)
    except Exception as e:
        log.debug("conf_meta_gate stream emit failed: %s", e)


def _schedule_awaitable(awaitable: Any, publish_loop: Any) -> None:
    try:
        import asyncio
        loop = publish_loop or asyncio.get_running_loop()
    except RuntimeError:
        return
    try:
        loop.create_task(awaitable)
    except Exception:
        pass


def set_model_health_gauges(model_ver: str, age_hours: float | None, ece: float | None) -> None:
    """Update gauges that track the loaded artifact's freshness/calibration."""
    _init_prom()
    with contextlib.suppress(Exception):
        if _MODEL_AGE_HOURS_GAUGE is not None and age_hours is not None and model_ver:
            _MODEL_AGE_HOURS_GAUGE.labels(model_ver=model_ver).set(age_hours)
    with contextlib.suppress(Exception):
        if _CALIBRATION_ECE_GAUGE is not None and ece is not None and model_ver:
            _CALIBRATION_ECE_GAUGE.labels(model_ver=model_ver).set(ece)
