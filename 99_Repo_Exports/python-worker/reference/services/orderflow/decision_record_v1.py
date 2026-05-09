from domain.evidence_keys import MetaKeys

"""Unified Decision Record (v1).

Implements plan steps:
- P45: create/store a single "decision record" per sid
- P48: explicit Rule↔ML binding (shadow by default)

Storage (Redis):
- decision:{sid} -> JSON string (TTL)
- decisions:final -> stream with a JSON payload field

Design goals:
- Deterministic time: epoch ms
- Fail-open: never break signal emission on recorder issues
- Low overhead: deterministic sampling by sid

This module does NOT change trading behavior by default.
It records both:
- final_actual: what the system actually did (emit/veto)
- final_recommended: what the binding matrix recommends
"""

from __future__ import annotations

import json
import os
import zlib
from typing import Any

from utils.time_utils import get_ny_time_millis


def _env_bool(name: str, default: str = "0") -> bool:
    v = os.getenv(name, default)
    return str(v).strip().lower() in {"1", "true", "yes", "on"}


def _env_float(name: str, default: str) -> float:
    try:
        return float(os.getenv(name, default))
    except Exception:
        return default


def _env_int(name: str, default: str) -> int:
    try:
        return int(float(os.getenv(name, default)))
    except Exception:
        return int(float(default))


def _now_ms() -> int:
    return get_ny_time_millis()


def _stable_sample(s: str, rate: float) -> bool:
    """Deterministic sampler based on crc32(s)."""
    if rate >= 1.0:
        return True
    if rate <= 0.0:
        return False
    h = zlib.crc32(s.encode("utf-8")) & 0xFFFFFFFF
    # map to [0,1)
    x = h / 2**32
    return x < rate

# P62 alias
deterministic_sample = _stable_sample


def _safe_get(d: Any, path: tuple[str, ...], default: Any = None) -> Any:
    cur = d
    for k in path:
        if not isinstance(cur, dict) or k not in cur:
            return default
        cur = cur[k]
    return cur


def _normalize_ml_state(ml: dict[str, Any]) -> str:
    """Best-effort normalize ML decision state."""
    if not isinstance(ml, dict) or not ml:
        return "off"

    # Common hints
    for k in ("state", "decision", "ml_state"):
        v = (ml.get(k, "") or "").strip().lower()
        if v in {"allow", "deny", "abstain", "off", "error"}:
            return v

    mode = (ml.get("mode", "") or "").strip().lower()
    if mode in {"", "off", "disabled", "none"}:
        return "off"

    if ml.get("error") or ml.get("err") or ml.get("exception"):
        return "error"

    if bool(ml.get("abstain", False)) or "abstain" in (ml.get("kind", "") or "").lower():
        return "abstain"

    # Most code paths expose allow=True/False
    if "allow" in ml:
        return "allow" if bool(ml.get("allow")) else "deny"

    return "allow"  # fail-open assumption


from services.orderflow.decision_binding_v1 import bind_rule_ml_v1


def _dq_state_from_indicators(indicators: dict[str, Any]) -> str:
    try:
        dh = float(indicators.get("data_health", 1.0) or 1.0)
    except Exception:
        dh = 1.0
    try:
        book_ok = int(indicators.get("book_health_ok", 1) or 1)
    except Exception:
        book_ok = 1
    try:
        src_ok = int(indicators.get("source_consistency_ok", 1) or 1)
    except Exception:
        src_ok = 1

    # Optional tick_time decisions
    ttd = (indicators.get("tick_time_decision", "") or "").lower()
    if ttd in {"drop", "reject", "quarantine"}:
        return "bad"

    if book_ok == 0 or src_ok == 0:
        return "bad"

    # data_health threshold configurable
    th = _env_float("DECISION_DQ_HEALTH_MIN", "0.70")
    if dh < th:
        return "bad"

    return "ok"


def _drift_state_from_indicators(indicators: dict[str, Any]) -> str:
    # P50/P51: indicators.drift struct
    d = indicators.get("drift")
    if isinstance(d, dict):
        # drift_state_24h: 0, 1, 2 or "ok","warn","block"
        raw = (d.get("drift_state_24h", "") or "").lower()
        if raw in {"0", "ok"}: return "ok"
        if raw in {"1", "warn"}: return "warn"
        if raw in {"2", "block", "fail", "veto"}: return "block"
        if raw: return raw

    # Fallback to direct indicators if not in drift struct
    v = (indicators.get("drift_state", "") or "").lower()
    if v in {"ok", "bad", "na", "block", "warn"}:
        return v
    return "na"


def build_decision_record_v1(
    *,
    runtime: Any,
    signal: dict[str, Any],
    stage: str,
    final_actual: str,
    veto_reason: str = "",
) -> dict[str, Any]:
    """Build a decision record from the enriched signal dict."""

    indicators = signal.get("indicators") if isinstance(signal.get("indicators"), dict) else {}
    ofc = indicators.get("of_confirm") if isinstance(indicators.get("of_confirm"), dict) else {}
    ev = ofc.get("evidence") if isinstance(ofc.get("evidence"), dict) else {}
    ml = ev.get("ml") if isinstance(ev.get("ml"), dict) else {}
    drift = indicators.get("drift") if isinstance(indicators.get("drift"), dict) else {}

    sid = str(signal.get("sid") or signal.get("signal_id") or "").strip()
    symbol = str(signal.get("symbol") or getattr(runtime, "symbol", "") or "").upper()
    ts_ms = int(signal.get("ts_ms") or signal.get("generated_at") or indicators.get("tick_ts") or _now_ms())

    rule_ok = int(indicators.get("of_confirm_ok", ofc.get("ok", 0)) or 0)
    rule_score = float(indicators.get("of_confirm_score", ofc.get("score", 0.0)) or 0.0)
    rule_ok_soft = int(_safe_get(ofc, ("evidence", "ok_soft"), indicators.get("ok_soft", 0)) or 0)

    # scenario + reason
    scenario = str(ofc.get("scenario", "") or indicators.get("scenario", "") or "")
    scenario_v4 = str(ev.get("scenario_v4", "") or indicators.get("scenario_v4", "") or "")
    rule_reason = str(ofc.get("reason", "") or indicators.get("strong_gate_reason", "") or "")

    # ML
    ml_state = _normalize_ml_state(ml)

    dq_state = _dq_state_from_indicators(indicators)
    drift_state = _drift_state_from_indicators(indicators)

    rec = bind_rule_ml_v1(
        rule_ok=rule_ok,
        rule_ok_soft=rule_ok_soft,
        ml_state=ml_state,
        dq_state=dq_state,
        drift_state=drift_state,
    )

    # Keep record compact: store essentials + a pointer to heavy snapshots
    out: dict[str, Any] = {
        "version": 1,
        "ts_ms": int(ts_ms),
        "sid": sid,
        "symbol": symbol,
        "direction": str(signal.get("direction") or signal.get("side") or "").upper(),
        "stage": str(stage),
        "final_actual": str(final_actual),
        "final_veto_reason": (veto_reason or ""),
        "final_recommended": rec.get("action"),
        "final_recommended_soft": int(rec.get("soft", 0) or 0),
        "final_recommended_source": rec.get("source"),
        "final_recommended_reason_code": rec.get("reason_code"),
        "dq_state": dq_state,
        "drift_state": drift_state,
        "drift_psi_max_24h": float(drift.get("psi_max_24h", 0.0) or 0.0),
        "drift_z_max_24h": float(drift.get("feature_drift_max_z_24h", 0.0) or 0.0),
        "drift_top_feature_psi": (drift.get("drift_top_feature_psi", "") or ""),
        "drift_top_feature_z": (drift.get("drift_top_feature_z", "") or ""),
        "drift_last_ts_ms": int(float(drift.get("drift_last_ts_ms", 0) or 0)),
        "binding_recommended_action": rec.get("action"),
        "binding_recommended_reason_code": rec.get("reason_code"),
        "rule": {
            "ok": rule_ok,
            "ok_soft": rule_ok_soft,
            "score": float(rule_score),
            "scenario": scenario,
            "scenario_v4": scenario_v4,
            "reason": rule_reason[:160],
            "have": int(ofc.get("have", indicators.get("strong_gate_have", 0)) or 0),
            "need": int(ofc.get("need", indicators.get("strong_gate_need", 0)) or 0),
            "missing_legs": ev.get("missing_legs") if isinstance(ev.get("missing_legs"), list) else [],
            "gate_bits": int(ofc.get("gate_bits", 0) or 0)
        },
        "ml": {
            "state": ml_state,
            "mode": (ml.get("mode", "") or ""),
            "kind": (ml.get("kind", "") or ""),
            "allow": int(bool(ml.get("allow", True))),
            "bucket": (ml.get("bucket", "") or ""),
            "p_edge": float(ml.get("p_edge", 0.0) or 0.0),
            "p_min": float(ml.get("p_min", 0.0) or 0.0),
            "score": float(ml.get("score", 0.0) or 0.0),
            "floor": float(ml.get("floor", 0.0) or 0.0),
            "latency_us": int(float(ml.get("latency_us", 0) or 0) or 0),
            "model_ver": (ml.get("model_ver", ml.get("ver", "")) or "")
        },
        "inputs": {
            "tick_ts_ms": int(indicators.get("tick_ts", ts_ms) or ts_ms),
            "price": float(indicators.get("price", signal.get("entry", 0.0)) or 0.0),
            "spread_bps": float(indicators.get("spread_bps", 0.0) or 0.0),
            "atr_bps": float(indicators.get("atr_bps", 0.0) or 0.0),
            "exec_risk_bps": float(_safe_get(ofc, ("evidence", "exec_risk_bps"), 0.0) or 0.0),
            "expected_slippage_bps": float(indicators.get("expected_slippage_bps", 0.0) or 0.0),
            "liq_score": float(indicators.get("liq_score", 0.0) or 0.0)
        },
        "conf_cal": {
            "ab_mode": (indicators.get("confidence_cal_ab_mode", "") or ""),
            "p_challenger": float(indicators.get("confidence_cal_p_challenger", 0.0) or 0.0),
            "arm_assigned": (indicators.get("confidence_cal_arm_assigned", "") or ""),
            "arm_taken": (indicators.get("confidence_cal_arm_taken", "") or ""),
            "bucket": int(indicators.get("confidence_cal_bucket", -1) or -1),
            "sticky_key": (indicators.get("confidence_cal_sticky_key", "") or "")[:120],
            "q_champion": float(indicators.get("confidence_cal_champion", indicators.get("confidence_v1", 0.0)) or 0.0),
            "q_challenger": float(indicators.get("confidence_cal_challenger", 0.0) or 0.0),
            "q_final": float(indicators.get("confidence_cal", 0.0) or 0.0),
            "method": (indicators.get("confidence_cal_method", "") or ""),
            "bucket_by": (indicators.get("confidence_cal_bucket_by", "") or ""),
            "bucket_level": (indicators.get("confidence_cal_bucket_level", "") or ""),
            "schema_version": int(indicators.get("confidence_cal_schema_version", 0) or 0),
            "fallback_to_champion": int(indicators.get("confidence_cal_fallback_to_champion", 0) or 0),
            "shadow_delta": float(indicators.get("confidence_cal_shadow_delta", 0.0) or 0.0),
            "shadow_delta_abs": float(indicators.get("confidence_cal_shadow_delta_abs", 0.0) or 0.0),
            "low_conf_would_veto": int(indicators.get("low_conf_would_veto", 0) or 0),
            "low_conf_virtual_pass": int(indicators.get("low_conf_virtual_pass", 0) or 0),
            "is_virtual": int(signal.get("is_virtual", indicators.get("is_virtual", 0)) or 0),
            "virtual_reason": str(indicators.get("virtual_reason", "") or signal.get("virtual_reason", "") or "")[:48],
        },
        "liqmap": {
            "gate": {
                "mode": (indicators.get("liqmap_gate_mode", "") or ""),
                "window": (indicators.get("liqmap_gate_window", "") or ""),
                "veto": int(indicators.get("liqmap_gate_veto", 0) or 0),
                "shadow_veto": int(indicators.get("liqmap_gate_shadow_veto", 0) or 0),
                "reason": (indicators.get("liqmap_gate_veto_reason", "") or ""),
                "rr": float(indicators.get("liqmap_gate_rr", 0.0) or 0.0),
                "risk_bps": float(indicators.get("liqmap_gate_risk_bps", 0.0) or 0.0),
                "reward_bps": float(indicators.get("liqmap_gate_reward_bps", 0.0) or 0.0),
            },
            "w5m": {
                "age_ms": float(indicators.get("liqmap_5m_age_ms", 0.0) or 0.0),
                "near_total_usd": float(indicators.get("liqmap_5m_near_total_usd", 0.0) or 0.0),
                "near_imb": float(indicators.get("liqmap_5m_near_imb", 0.0) or 0.0),
                "dist_up_bps": float(indicators.get("liqmap_5m_dist_up_bps", 0.0) or 0.0),
                "dist_dn_bps": float(indicators.get("liqmap_5m_dist_dn_bps", 0.0) or 0.0),
                "peak_up1_usd": float(indicators.get("liqmap_5m_peak_up1_usd", 0.0) or 0.0),
                "peak_dn1_usd": float(indicators.get("liqmap_5m_peak_dn1_usd", 0.0) or 0.0),
            },
            "w1h": {
                "age_ms": float(indicators.get("liqmap_1h_age_ms", 0.0) or 0.0),
                "near_total_usd": float(indicators.get("liqmap_1h_near_total_usd", 0.0) or 0.0),
                "near_imb": float(indicators.get("liqmap_1h_near_imb", 0.0) or 0.0),
                "dist_up_bps": float(indicators.get("liqmap_1h_dist_up_bps", 0.0) or 0.0),
                "dist_dn_bps": float(indicators.get("liqmap_1h_dist_dn_bps", 0.0) or 0.0),
                "peak_up1_usd": float(indicators.get("liqmap_1h_peak_up1_usd", 0.0) or 0.0),
                "peak_dn1_usd": float(indicators.get("liqmap_1h_peak_dn1_usd", 0.0) or 0.0),
            }
        },
        "meta": {
            "meta_enforce_applied": int(ev.get(MetaKeys.ENFORCE_APPLIED, 0) or 0),
            "meta_enforce_share": float(ev.get(MetaKeys.ENFORCE_SHARE, 1.0) or 1.0),
            "meta_enforce_bucket": (ev.get("meta_enforce_bucket", "") or ""),
            "meta_p": float(ev.get(MetaKeys.P, -1.0) or -1.0),
            "meta_veto": int(ev.get(MetaKeys.VETO, 0) or 0),
        },
        # P68: policy fields (fail-open)
        "policy": {
            "ver": (indicators.get("policy_ver", "") or ""),
            "regime": (indicators.get("policy_regime", "") or ""),
            "reason": (indicators.get("policy_reason", "") or ""),
            "force_rule_strong_only": bool(int(indicators.get("policy_force_rule_strong_only", 0) or 0)),
            "disable_ml_enforce": bool(int(indicators.get("policy_disable_ml_enforce", 0) or 0)),
            "policy_dq_state": (indicators.get("policy_dq_state", indicators.get("dq_state", ""))),
            "policy_drift_state": (indicators.get("policy_drift_state", indicators.get("drift_state", ""))),
            # P69
            "policy_raw_mode": (indicators.get("policy_raw_mode", "")),
            "policy_effective_mode": (indicators.get("policy_effective_mode", "")),
            "policy_hysteresis_debug": (indicators.get("policy_hysteresis_debug", "")),
            "policy_changed": bool(int(indicators.get("policy_changed", 0) or 0)),
        }
    }

    # Optional: attach pointers to heavy snapshots if present
    try:
        if isinstance(signal.get("config_snapshot"), dict):
            out["has_config_snapshot"] = 1
        if isinstance(signal.get("evidence"), dict):
            out["has_evidence"] = 1
    except Exception:
        pass

    return out


async def maybe_write_decision_record_v1(
    *,
    runtime: Any,
    signal: dict[str, Any],
    stage: str,
    final_actual: str,
    veto_reason: str = "",
) -> None:
    """Best-effort write decision record to Redis."""

    if not _env_bool("DECISION_RECORD_ENABLE", "1"):
        return

    sid = str(signal.get("sid") or signal.get("signal_id") or "").strip()
    if not sid:
        return

    rate = _env_float("DECISION_RECORD_SAMPLE", "1.0")
    if not _stable_sample(sid, rate):
        # metrics are optional
        try:
            from services.orderflow.metrics import decision_record_sampled_out_total
            decision_record_sampled_out_total.labels(symbol=(signal.get("symbol") or "unknown"), stage=str(stage)).inc()
        except Exception:
            pass
        return

    r = getattr(runtime, "redis_client", None)
    if r is None:
        # Some deployments keep redis client on publisher.
        r = getattr(getattr(runtime, "publisher", None), "r", None)
    if r is None:
        return

    ttl = _env_int("DECISION_TTL_SEC", "1209600")  # 14d
    maxlen = _env_int("DECISIONS_FINAL_MAXLEN", "200000")
    stream = os.getenv("DECISIONS_FINAL_STREAM", "decisions:final")

    record = build_decision_record_v1(
        runtime=runtime,
        signal=signal,
        stage=stage,
        final_actual=final_actual,
        veto_reason=veto_reason,
    )

    key = f"decision:{record['sid']}"
    payload = json.dumps(record, ensure_ascii=False, separators=(",", ":"), default=str)

    # Try pipeline/transaction for atomicity
    try:
        pipe = r.pipeline()
        pipe.set(key, payload, ex=ttl)
        pipe.xadd(
            stream,
            fields={
                "sid": str(record["sid"]),
                "symbol": str(record["symbol"]),
                "ts_ms": str(record["ts_ms"]),
                "stage": str(record["stage"]),
                "reason_code": (record.get("final_recommended_reason_code") or ""),
                "payload": payload
            },
            maxlen=maxlen,
            approximate=True,
        )
        await pipe.execute()
    except Exception:
        # Fallback sequential
        try:
            await r.set(key, payload, ex=ttl)
        except Exception:
            try:
                from services.orderflow.metrics import decision_record_error_total
                decision_record_error_total.labels(symbol=str(record["symbol"])).inc()
            except Exception:
                pass
        try:
            await r.xadd(
                stream,
                fields={
                    "sid": str(record["sid"]),
                    "symbol": str(record["symbol"]),
                    "ts_ms": str(record["ts_ms"]),
                    "stage": str(record["stage"]),
                    "reason_code": (record.get("final_recommended_reason_code") or ""),
                    "payload": payload
                },
                maxlen=maxlen,
                approximate=True,
            )
        except Exception:
            try:
                from services.orderflow.metrics import decision_record_error_total
                decision_record_error_total.labels(symbol=str(record["symbol"])).inc()
            except Exception:
                pass

    try:
        from services.orderflow.metrics import decision_record_written_total
        decision_record_written_total.labels(
            symbol=str(record["symbol"]),
            stage=str(stage),
            result=str(final_actual),
        ).inc()
    except Exception:
        pass


# P62 Adapters

from typing import TypedDict


class DecisionRecordV1(TypedDict, total=False):
    ver: str
    sid: str
    symbol: str
    tf: str
    strategy: str
    decision_ts_ms: int
    rule_score: float
    rule_ok: bool
    rule_soft: bool
    rule_reason_code_top1: str
    ml_enabled: bool
    ml_state: str
    ml_p_cal: float | None
    ml_model_ver: str
    ml_latency_ms: float | None
    ml_error: str
    dq_state: str
    dq_flags: list[str]
    drift_state: str
    drift_flags: list[str]
    actual_action: str
    actual_reason_code: str
    recommended_action: str
    recommended_reason_code: str
    meta_enforce_cov_bucket: str
    meta_enforce_applied: bool
    payload_summary: dict[str, Any]

    # P68: circuit breaker / policy application (optional)
    policy_ver: str
    policy_regime: str
    policy_reason: str
    policy_force_rule_strong_only: bool
    policy_disable_ml_enforce: bool
    policy_dq_state: str
    policy_drift_state: str

    # P69: Hysteresis fields
    policy_raw_mode: str
    policy_effective_mode: str
    policy_hysteresis_debug: str
    policy_changed: bool


def extract_fields_best_effort(stub: dict[str, Any]) -> dict[str, Any]:
    """Extract decision fields from a loose stub/signal dict."""
    indicators = stub.get("indicators", {})

    # helper
    def _g(k, d=None):
        return indicators.get(k, d)

    # ML
    ml_state = "na"
    # Try to find ml state in indicators
    if "ml_state" in indicators: ml_state = _g("ml_state")
    # Or deep inside of_confirm
    ofc = _g("of_confirm", {})
    ev = ofc.get("evidence", {}) if isinstance(ofc, dict) else {}
    ml = ev.get("ml", {}) if isinstance(ev, dict) else {}
    if ml:
         ml_state = _normalize_ml_state(ml)

    # Rule
    rule_score = float(_g("rule_score", _g("score", 0.0)) or 0.0)
    rule_ok = bool(int(_g("ok", 0) or 0))
    rule_soft = bool(int(_g("soft", 0) or 0))

    # DQ/Drift
    dq_state = _dq_state_from_indicators(indicators)
    drift_state = _drift_state_from_indicators(indicators)

    # Meta Enforce
    meta_bucket = str(_g("meta_enforce_cov_bucket", "unknown"))
    meta_applied = bool(int(_g("meta_enforce_applied", 0) or 0))

    return {
        "rule_score": rule_score,
        "rule_ok": rule_ok,
        "rule_soft": rule_soft,
        "rule_reason_code_top1": str(_g("rule_reason_code_top1", "NA")),
        "ml_enabled": bool(ml),
        "ml_state": ml_state,
        "ml_p_cal": None, # complex to extract without full signal
        "ml_model_ver": (ml.get("ver", "")),
        "ml_latency_ms": None,
        "ml_error": "",
        "dq_state": dq_state,
        "dq_flags": [],
        "drift_state": drift_state,
        "drift_flags": [],
        "meta_enforce_cov_bucket": meta_bucket,
        "meta_enforce_applied": meta_applied,
        # P68: policy fields (fail-open)
        "policy_ver": str(_g("policy_ver", "")),
        "policy_regime": str(_g("policy_regime", "")),
        "policy_reason": str(_g("policy_reason", "")),
        "policy_force_rule_strong_only": bool(int(_g("policy_force_rule_strong_only", 0) or 0)),
        "policy_disable_ml_enforce": bool(int(_g("policy_disable_ml_enforce", 0) or 0)),
        "policy_dq_state": str(_g("policy_dq_state", _g("dq_state", ""))),
        "policy_drift_state": str(_g("policy_drift_state", _g("drift_state", ""))),
        # P69
        "policy_raw_mode": str(_g("policy_raw_mode", "")),
        "policy_effective_mode": str(_g("policy_effective_mode", "")),
        "policy_hysteresis_debug": str(_g("policy_hysteresis_debug", "")),
        "policy_changed": bool(int(_g("policy_changed", 0) or 0)),
    }

async def write_decision_record(redis_client: Any, record: DecisionRecordV1) -> None:
    """Async write for DecisionRecordV1 object."""
    try:
        if not redis_client: return

        ttl = _env_int("DECISION_TTL_SEC", "1209600")
        maxlen = _env_int("DECISIONS_FINAL_MAXLEN", "200000")
        stream = os.getenv("DECISIONS_FINAL_STREAM", "decisions:final")

        sid = record.get("sid", "unknown")
        key = f"decision:{sid}"

        # Serialize
        payload = json.dumps(record, ensure_ascii=False, default=str)

        # We use a reduced field set for the stream to save bandwidth/storage
        stream_fields = {
            "sid": str(sid),
            "symbol": (record.get("symbol", "")),
            "ts_ms": (record.get("decision_ts_ms", 0)),
            "stage": "early_veto", # or extract from record if present
            "reason_code": (record.get("actual_reason_code", "")),
            "payload": payload
        }

        pipe = redis_client.pipeline()
        pipe.set(key, payload, ex=ttl)
        pipe.xadd(stream, stream_fields, maxlen=maxlen, approximate=True)
        await pipe.execute()

    except Exception:
        # Fail open
        pass
