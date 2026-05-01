from __future__ import annotations
"""services.orderflow.evidence_contract

P0 deliverable: a versioned, numeric-only evidence payload for "on-the-wire" exchange
between generators (tick_processor/OFC/ML gate) and consumers (scorer, archivers,
offline evaluation).

Contract guarantees:
  - evidence_map: Dict[str, float] (only finite floats; booleans -> 0.0/1.0)
  - schema_version + producer + sid + ts_event_ms are always present
  - alias mapping is applied for 1–2 releases (backward compatibility)
  - detect -> sanitize -> quarantine -> metrics on unknown/bad keys
"""

from utils.time_utils import get_ny_time_millis

import math
import os
import re
import time
import uuid
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

from pydantic import BaseModel, Field

EVIDENCE_SCHEMA_VERSION: int = 1

_NUM_RE = re.compile(r"^[+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?$")
_KEY_RE = re.compile(r"^[a-z0-9_]{1,64}$")

EVIDENCE_ALLOWLIST: set[str] = {
    "data_health",
    "book_stale_ms",
    "tick_time_age_ms",
    "dq_ok",
    "dq_soft",
    "dq_bad",
    "market_mode_id",
    "reclaim",
    "obi_stable",
    "iceberg_strict",
    "sweep_any",
    "sweep_eqh",
    "sweep_eql",
    "rsi_agree",
    "div_match",
    "div_strength",
    "div_z",
    "weak_progress",
    "progress_z",
    "pressure_sps",
    "spread_z",
    "sweep",       # legacy input only; mapped to sweep_any
    "ice_strict",  # legacy input only; mapped to iceberg_strict
    "conf_rsi_agree",
    "conf_div_match",
    "conf_sweep_eqh",
    "conf_sweep_eql",
    "conf_sweep_any",
    "conf_iceberg_strict",
    "conf_obi_stable",
    "conf_reclaim",
    "conf_weak_progress",
}

EVIDENCE_ALIASES: Dict[str, str] = {
    "ice_strict": "iceberg_strict",
    "iceberg": "iceberg_strict",
    "iceberg_confirm": "iceberg_strict",
    "sweep": "sweep_any",
    "sweep_any": "sweep_any",
    "sweep_eq": "sweep_any",
    "sweep_eq_high": "sweep_eqh",
    "sweep_eq_low": "sweep_eql",
    "div": "div_match",
    "divergence": "div_match",
    "rsi": "rsi_agree",
}

def env(name: str, default: str) -> str:
    v = os.getenv(name)
    return v if v not in (None, "") else default

def env_int(name: str, default: int) -> int:
    v = os.getenv(name)
    return int(v) if v not in (None, "") else default

def _to_float(v: Any) -> Tuple[Optional[float], Optional[str]]:
    if v is None:
        return None, "none"
    if isinstance(v, bool):
        return (1.0 if v else 0.0), None
    if isinstance(v, (int, float)):
        f = float(v)
        if not math.isfinite(f):
            return None, "non_finite"
        return f, None
    if isinstance(v, str):
        s = v.strip()
        if s.lower() in ("true", "false"):
            return (1.0 if s.lower() == "true" else 0.0), None
        if not _NUM_RE.match(s):
            return None, "non_numeric_str"
        try:
            f = float(s)
        except Exception:
            return None, "float_parse"
        if not math.isfinite(f):
            return None, "non_finite"
        return f, None
    return None, "unsupported_type"

def _norm_key(k: Any) -> Tuple[Optional[str], Optional[str]]:
    if k is None:
        return None, "none"
    s = str(k).strip().lower()
    s = s.replace("-", "_").replace(" ", "_")
    s = re.sub(r"[^a-z0-9_]+", "", s)
    if not s:
        return None, "empty"
    if not _KEY_RE.match(s):
        return None, "bad_format"
    return s, None

def _apply_alias(k: str) -> str:
    return EVIDENCE_ALIASES.get(k, k)

def parse_legacy_confirmations(items: Sequence[Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for it in items:
        if it is None:
            continue
        s = str(it).strip()
        if "=" not in s:
            continue
        k, v = s.split("=", 1)
        k0, _ = _norm_key(k)
        if not k0:
            continue
        out[_apply_alias(k0)] = v
    return out

def market_mode_to_id(m: Any) -> Tuple[Optional[float], Optional[str]]:
    if m is None:
        return None, None
    if isinstance(m, (int, float)):
        f = float(m)
        if math.isfinite(f):
            return f, None
        return None, "non_finite"
    if not isinstance(m, str):
        return None, "not_str"
    mm = m.strip().lower()
    if not mm:
        return None, None
    if mm in ("trend", "momentum", "breakout"):
        return 1.0, None
    if mm in ("range", "meanrev", "mean_reversion"):
        return 2.0, None
    if mm in ("neutral", "unknown"):
        return 0.0, None
    return None, "unknown_mode"

def derive_sid(*, sid: Optional[str], symbol: Optional[str], ts_event_ms: Optional[int], direction: Optional[str], entry: Optional[float]) -> str:
    if sid:
        return str(sid)
    sym = symbol or "?"
    ts = int(ts_event_ms or 0)
    d = (direction or "?").upper()
    e = 0.0 if entry is None else float(entry)
    key = f"of:{sym}:{ts}:{d}:{e:.8f}"
    return str(uuid.uuid5(uuid.NAMESPACE_DNS, key))

def validate_ts_event_ms(ts_event_ms: int) -> Optional[str]:
    now = get_ny_time_millis()
    if ts_event_ms < now - 10 * 24 * 3600 * 1000:
        return "too_old"
    if ts_event_ms > now + 2 * 60 * 1000:
        return "too_future"
    return None

class EvidencePayload(BaseModel):
    schema_version: int = Field(default=EVIDENCE_SCHEMA_VERSION, ge=0)
    producer: str = Field(min_length=1, max_length=64)
    sid: str = Field(min_length=1, max_length=128)
    ts_event_ms: int = Field(ge=0)
    evidence_map: Dict[str, float] = Field(default_factory=dict)
    symbol: Optional[str] = None
    tf: Optional[str] = None
    market_mode: Optional[str] = None

@dataclass
class EvidenceNormalizeResult:
    payload: EvidencePayload
    unknown_keys: List[str]
    dropped: Dict[str, str]
    warnings: List[str]

def normalize_evidence_payload(
    *,
    producer: str,
    sid: Optional[str],
    ts_event_ms: int,
    symbol: Optional[str],
    tf: Optional[str],
    direction: Optional[str],
    entry: Optional[float],
    evidence_raw: Optional[Mapping[str, Any]] = None,
    confirmations_legacy: Optional[Sequence[Any]] = None,
    market_mode: Optional[Any] = None,
    strict_unknown: Optional[bool] = None,
    accepted_schema_versions: Optional[Iterable[int]] = None,
) -> EvidenceNormalizeResult:
    strict = strict_unknown
    if strict is None:
        strict = env_int("CONF_EVIDENCE_STRICT_KEYS", 0) == 1

    accepted = set(accepted_schema_versions or [])
    if not accepted:
        accepted = {0, EVIDENCE_SCHEMA_VERSION}

    sid2 = derive_sid(sid=sid, symbol=symbol, ts_event_ms=ts_event_ms, direction=direction, entry=entry)
    unknown_keys: List[str] = []
    dropped: Dict[str, str] = {}
    warnings: List[str] = []

    ts_reason = validate_ts_event_ms(int(ts_event_ms))
    if ts_reason:
        warnings.append(f"bad_ts:{ts_reason}")

    work: Dict[str, Any] = {}
    if evidence_raw:
        for k, v in evidence_raw.items():
            k0, ek = _norm_key(k)
            if k0 is None:
                dropped[str(k)[:64]] = f"bad_key:{ek}"
                continue
            work[_apply_alias(k0)] = v

    if confirmations_legacy:
        for k, v in parse_legacy_confirmations(confirmations_legacy).items():
            work[k] = v

    mm_val = market_mode
    if mm_val is None and isinstance(work.get("market_mode"), (str, int, float)):
        mm_val = work.get("market_mode")
    if "market_mode" in work:
        work.pop("market_mode", None)
    mm_id, mm_err = market_mode_to_id(mm_val)
    if mm_err:
        warnings.append(f"market_mode:{mm_err}")
    if mm_id is not None:
        work["market_mode_id"] = mm_id

    evidence_map: Dict[str, float] = {}
    for k, v in work.items():
        if k in ("schema_version", "producer", "sid", "ts_event_ms"):
            dropped[k] = "reserved_key"
            continue
        is_known = (k in EVIDENCE_ALLOWLIST) or (k in EVIDENCE_ALIASES.values())
        if not is_known:
            unknown_keys.append(k)
            if strict:
                dropped[k] = "unknown_key"
                continue
        fv, ev = _to_float(v)
        if fv is None:
            dropped[k] = f"bad_value:{ev}"
            continue
        evidence_map[k] = fv

    payload = EvidencePayload(
        schema_version=EVIDENCE_SCHEMA_VERSION,
        producer=str(producer),
        sid=sid2,
        ts_event_ms=int(ts_event_ms),
        evidence_map=evidence_map,
        symbol=symbol,
        tf=tf,
        market_mode=str(mm_val) if isinstance(mm_val, str) and mm_val.strip() else None,
    )
    if payload.schema_version not in accepted:
        warnings.append(f"schema_rejected:{payload.schema_version}")
    return EvidenceNormalizeResult(payload=payload, unknown_keys=unknown_keys, dropped=dropped, warnings=warnings)

def make_scores_row(
    *,
    evidence_payload: EvidencePayload,
    confidence_raw: Optional[float],
    confidence_final: Optional[float],
    context: Optional[Mapping[str, Any]] = None,
) -> Dict[str, Any]:
    return {
        "ts_event_ms": evidence_payload.ts_event_ms,
        "sid": evidence_payload.sid,
        "symbol": evidence_payload.symbol,
        "tf": evidence_payload.tf,
        "market_mode": evidence_payload.market_mode,
        "confidence_raw": None if confidence_raw is None else float(confidence_raw),
        "confidence_final": None if confidence_final is None else float(confidence_final),
        "schema_version": evidence_payload.schema_version,
        "producer": evidence_payload.producer,
        "evidence_map": evidence_payload.evidence_map,
        "context": dict(context or {}),
    }
