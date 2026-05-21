from __future__ import annotations

import json
import math
import os
import time
from dataclasses import asdict, dataclass, field
from typing import Any, Literal
import contextlib

try:
    import redis as _redis_lib
except ImportError:  # pragma: no cover
    _redis_lib = None  # type: ignore

try:
    from prometheus_client import Counter
except Exception:  # pragma: no cover
    Counter = None  # type: ignore

PhaseMode = Literal["off", "shadow", "canary", "enforce"]
AtrMode = Literal["legacy", "horizon"]
HorizonBucket = Literal["micro", "short", "medium", "long", "unknown"]
AtrSource = Literal["legacy", "bootstrap", "selector", "manual", "fallback", "unknown"]

_CONTRACT_VER = int(os.getenv("ATR_HORIZON_CONTRACT_VER", "2") or 2)
_PHASE_MODE = (os.getenv("ATR_HORIZON_MODE", "off") or "off").strip().lower()
_EMIT_PAYLOAD_META = os.getenv("ATR_HORIZON_EMIT_PAYLOAD_META", "1") == "1"
_DEFAULT_TF_MS = int(os.getenv("ATR_HORIZON_DEFAULT_TF_MS", "60000") or 60000)
_DEFAULT_WINDOW_N = int(os.getenv("ATR_HORIZON_DEFAULT_WINDOW_N", "14") or 14)
_DEFAULT_BUCKET = (os.getenv("ATR_HORIZON_DEFAULT_BUCKET", "unknown") or "unknown").strip().lower()
_DEFAULT_PROFILE_SOURCE = str(
    os.getenv("ATR_HORIZON_DEFAULT_PROFILE_SOURCE", "static_bootstrap") or "static_bootstrap"
).strip()
_DEFAULT_MAX_SIGNAL_AGE_MS = int(os.getenv("ATR_HORIZON_DEFAULT_MAX_SIGNAL_AGE_MS", "0") or 0)
_POSITION_ATTACH_ENABLED = os.getenv("ATR_HORIZON_POSITION_ATTACH_ENABLED", "1") == "1"
_POSITION_RECOVERY_ENABLED = os.getenv("ATR_HORIZON_POSITION_RECOVERY_ENABLED", "1") == "1"

# Phase 2: runtime ATR selector
_SELECTOR_ENABLED = os.getenv("ATR_HORIZON_SELECTOR_ENABLED", "1") == "1"
_USE_FOR_GATES = os.getenv("ATR_HORIZON_USE_FOR_GATES", "0") == "1"
# Phase 2.1: emit multi-TF candidate map into meta (observe-only)
_EMIT_CANDIDATES = os.getenv("ATR_HORIZON_EMIT_CANDIDATES", "1") == "1"

# Phase 1 Redis profile lookup
_PROFILE_REDIS_LOOKUP = os.getenv("ATR_HORIZON_PROFILE_REDIS_LOOKUP", "1") == "1"
_PROFILE_STALE_MAX_MS = int(
    os.getenv("ATR_HORIZON_PROFILE_STALE_MAX_MS", str(7 * 86_400_000)) or (7 * 86_400_000)
),
_REDIS_URL_FOR_PROFILE = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
_R_PROFILE_SYNC: Any = None  # set lazily

def _safe_counter(name, doc, labels):
    if Counter is None:
        return None
    try:
        return Counter(name, doc, labels)
    except ValueError:
        return None

_M_EMITTED = _safe_counter(
    "trade_horizon_contract_emitted_total",
    "Phase0 horizon contract emitted into payload",
    ["source", "phase_mode"],
)

_M_REASON = _safe_counter(
    "trade_horizon_reason_total",
    "Phase0 horizon contract reason",
    ["reason_code"],
)

_M_POS_ATTACH = _safe_counter(
    "trade_horizon_position_attach_total",
    "Phase0.2 horizon contract attached to PositionState",
    ["source"],
)

_M_POS_RECOVER = _safe_counter(
    "trade_horizon_position_recover_total",
    "Phase0.2 horizon contract recovered into PositionState",
    ["source"],
)

_M_POS_MISSING = _safe_counter(
    "trade_horizon_position_missing_total",
    "Phase0.2 missing horizon contract during attach/recovery",
    ["source"],
)


def _safe_float(v: Any, default: float = 0.0) -> float:
    try:
        x = float(v)
        return x if math.isfinite(x) else default
    except Exception:
        return default


def _safe_int(v: Any, default: int = 0) -> int:
    try:
        return int(v)
    except Exception:
        return default


def _ensure_dict(v: Any) -> dict[str, Any]:
    return dict(v) if isinstance(v, dict) else {}


# ---------------------------------------------------------------------------
# Phase 1: Redis profile lookup helpers
# ---------------------------------------------------------------------------

def _get_profile_redis() -> Any | None:
    """Lazy singleton Redis client for profile lookup. Returns None on failure."""
    global _R_PROFILE_SYNC
    if _R_PROFILE_SYNC is not None:
        return _R_PROFILE_SYNC if _R_PROFILE_SYNC is not False else None
    if _redis_lib is None:
        _R_PROFILE_SYNC = False
        return None
    try:
        _R_PROFILE_SYNC = _redis_lib.Redis.from_url(_REDIS_URL_FOR_PROFILE, decode_responses=True)
        return _R_PROFILE_SYNC
    except Exception:
        _R_PROFILE_SYNC = False
        return None


def _profile_redis_key(source: str, symbol: str, kind: str, regime: str) -> str:
    return f"cfg:horizon:profile:{source}:{symbol}:{kind}:{regime}"


def _load_calibrated_horizon_profile(
    *,
    source: str,
    symbol: str,
    kind: str,
    regime: str,
    now_ms: int,
) -> dict[str, Any] | None:
    """
    Try to load a calibrated profile from Redis.
    Fallback chain: exact -> scenario('na') -> symbol default.
    Returns None when the feature is disabled, Redis is unavailable, or no
    fresh key exists at any level.
    Fail-open: any exception → None (static bootstrap used instead).
    """
    if not _PROFILE_REDIS_LOOKUP:
        return None
    r = _get_profile_redis()
    if r is None:
        return None

    source  = (source or "CryptoOrderFlow")
    symbol  = (symbol or "").upper()
    kind    = (kind or "default").lower()
    regime  = (regime or "na").lower()

    keys_levels = [
        (_profile_redis_key(source, symbol, kind, regime),    "exact"),
        (_profile_redis_key(source, symbol, kind, "na"),      "scenario"),
        (_profile_redis_key(source, symbol, "default", "na"), "default"),
    ]
    for key, level in keys_levels:
        try:
            raw = r.get(key)
            if not raw:
                continue
            obj = json.loads(raw)
            if not isinstance(obj, dict):
                continue
            updated_at_ms = _safe_int(obj.get("updated_at_ms"), 0)
            if updated_at_ms > 0 and (now_ms - updated_at_ms) > _PROFILE_STALE_MAX_MS:  # type: ignore
                try:
                    from services.horizon_profile_bootstrap_service import _M_LOOKUP_STALE
                    _M_LOOKUP_STALE.inc()
                except Exception:
                    pass
                continue
            try:
                from services.horizon_profile_bootstrap_service import _M_LOOKUP_HIT
                _M_LOOKUP_HIT.labels(level=level).inc()
            except Exception:
                pass
            return obj
        except Exception:
            continue

    try:
        from services.horizon_profile_bootstrap_service import _M_LOOKUP_MISS
        _M_LOOKUP_MISS.inc()
    except Exception:
        pass
    return None


def _merge_meta_contract(dst_meta: dict[str, Any], src_meta: dict[str, Any]) -> dict[str, Any]:
    out = dict(dst_meta or {})
    if "contract_ver" in src_meta and not out.get("contract_ver"):
        out["contract_ver"] = src_meta.get("contract_ver")
    if isinstance(src_meta.get("horizon"), dict) and not isinstance(out.get("horizon"), dict):
        out["horizon"] = dict(src_meta.get("horizon") or {})
    if isinstance(src_meta.get("atr_profile"), dict) and not isinstance(out.get("atr_profile"), dict):
        out["atr_profile"] = dict(src_meta.get("atr_profile") or {})
    return out


def _coerce_contract_from_signal_payload(payload: dict[str, Any]) -> dict[str, Any]:
    """
    Accept both canonical nested contract:
      payload.meta.contract_ver / payload.meta.horizon / payload.meta.atr_profile
    and flat aliases persisted in signal_payload for back-compat.
    """
    payload = _ensure_dict(payload)
    meta = _ensure_dict(payload.get("meta"))
    horizon = _ensure_dict(meta.get("horizon"))
    atr_profile = _ensure_dict(meta.get("atr_profile"))
    contract_ver = _safe_int(meta.get("contract_ver") or payload.get("contract_ver"), 0)

    if not horizon:
        if any(k in payload for k in ("hold_target_ms", "alpha_half_life_ms", "max_signal_age_ms", "risk_horizon_bucket")):
            horizon = {
                "hold_target_ms": _safe_int(payload.get("hold_target_ms"), 0),
                "alpha_half_life_ms": _safe_int(payload.get("alpha_half_life_ms"), 0),
                "max_signal_age_ms": _safe_int(payload.get("max_signal_age_ms"), 0),
                "risk_horizon_bucket": (payload.get("risk_horizon_bucket") or "unknown"),
                "profile_source": str(payload.get("horizon_profile_source") or _DEFAULT_PROFILE_SOURCE),
                "profile_conf": _safe_float(payload.get("horizon_profile_conf"), 0.0),
                "reason_code": (payload.get("horizon_reason_code") or "HZ_STATIC_BOOTSTRAP"),
                "reason_details": _ensure_dict(payload.get("horizon_reason_details")),
            }

    if not atr_profile:
        if any(k in payload for k in ("atr_tf_ms", "atr_age_ms", "atr_source", "atr_mode")):
            atr_profile = {
                "mode": (payload.get("atr_mode") or "legacy"),
                "atr_value": _safe_float(payload.get("atr") or payload.get("atr_value"), 0.0),
                "atr_tf_ms": _safe_int(payload.get("atr_tf_ms"), _DEFAULT_TF_MS),
                "atr_window_n": _safe_int(payload.get("atr_window_n"), _DEFAULT_WINDOW_N),
                "atr_age_ms": _safe_int(payload.get("atr_age_ms"), 0),
                "atr_source": (payload.get("atr_source") or "legacy"),
                "atr_regime_value": _safe_float(payload.get("atr_regime_value"), 0.0),
                "atr_trail_value": _safe_float(payload.get("atr_trail_value"), 0.0),
                "atr_regime_tf_ms": _safe_int(payload.get("atr_regime_tf_ms"), 0),
                "atr_trail_tf_ms": _safe_int(payload.get("atr_trail_tf_ms"), 0),
                "atr_pct": _safe_float(payload.get("atr_pct"), 0.0),
                "vol_ratio_fast_slow": _safe_float(payload.get("vol_ratio_fast_slow"), 1.0),
                "vol_ratio_z": _safe_float(payload.get("vol_ratio_z"), 0.0),
            }

    if contract_ver <= 0 and (horizon or atr_profile):
        contract_ver = _CONTRACT_VER

    return {
        "contract_ver": int(contract_ver or 0),
        "horizon": horizon,
        "atr_profile": atr_profile,
    }


def _apply_position_aliases(pos: Any, contract: dict[str, Any]) -> bool:
    hz = _ensure_dict(contract.get("horizon"))
    ap = _ensure_dict(contract.get("atr_profile"))
    ver = _safe_int(contract.get("contract_ver"), 0)
    if ver <= 0 and not hz and not ap:
        return False

    pos.horizon_contract_ver = int(ver or _CONTRACT_VER)
    pos.hold_target_ms = _safe_int(hz.get("hold_target_ms"), getattr(pos, "hold_target_ms", 0))
    pos.alpha_half_life_ms = _safe_int(hz.get("alpha_half_life_ms"), getattr(pos, "alpha_half_life_ms", 0))
    pos.max_signal_age_ms = _safe_int(hz.get("max_signal_age_ms"), getattr(pos, "max_signal_age_ms", 0))
    pos.risk_horizon_bucket = str(hz.get("risk_horizon_bucket") or getattr(pos, "risk_horizon_bucket", "unknown") or "unknown")
    pos.horizon_profile_source = str(hz.get("profile_source") or getattr(pos, "horizon_profile_source", _DEFAULT_PROFILE_SOURCE))
    pos.horizon_profile_conf = _safe_float(hz.get("profile_conf"), getattr(pos, "horizon_profile_conf", 0.0))
    pos.horizon_reason_code = str(hz.get("reason_code") or getattr(pos, "horizon_reason_code", "HZ_STATIC_BOOTSTRAP"))
    pos.horizon_reason_details = _ensure_dict(hz.get("reason_details"))

    pos.atr_mode = str(ap.get("mode") or getattr(pos, "atr_mode", "legacy"))
    pos.atr_tf_ms = _safe_int(ap.get("atr_tf_ms"), getattr(pos, "atr_tf_ms", _DEFAULT_TF_MS))
    pos.atr_window_n = _safe_int(ap.get("atr_window_n"), getattr(pos, "atr_window_n", _DEFAULT_WINDOW_N))
    pos.atr_age_ms = _safe_int(ap.get("atr_age_ms"), getattr(pos, "atr_age_ms", 0))
    pos.atr_source = str(ap.get("atr_source") or getattr(pos, "atr_source", "legacy"))
    pos.atr_regime_value = _safe_float(ap.get("atr_regime_value"), getattr(pos, "atr_regime_value", 0.0))
    pos.atr_trail_value = _safe_float(ap.get("atr_trail_value"), getattr(pos, "atr_trail_value", 0.0))
    pos.atr_regime_tf_ms = _safe_int(ap.get("atr_regime_tf_ms"), getattr(pos, "atr_regime_tf_ms", 0))
    pos.atr_trail_tf_ms = _safe_int(ap.get("atr_trail_tf_ms"), getattr(pos, "atr_trail_tf_ms", 0))
    pos.atr_pct = _safe_float(ap.get("atr_pct"), getattr(pos, "atr_pct", 0.0))
    pos.vol_ratio_fast_slow = _safe_float(ap.get("vol_ratio_fast_slow"), getattr(pos, "vol_ratio_fast_slow", 1.0))
    pos.vol_ratio_z = _safe_float(ap.get("vol_ratio_z"), getattr(pos, "vol_ratio_z", 0.0))

    pos.horizon_contract = {"contract_ver": int(ver or _CONTRACT_VER), "horizon": hz, "atr_profile": ap}
    return True


def stamp_position_from_signal_payload(pos: Any, payload: dict[str, Any], *, source: str = "signal_open") -> bool:
    """
    Phase 0.2:
      - ensure PositionState.signal_payload carries canonical contract
      - apply convenience attrs onto PositionState for lifecycle / recovery / monitoring
    """
    if not _POSITION_ATTACH_ENABLED:
        return False

    if pos is None:
        return False
    payload = _ensure_dict(payload)
    sp = getattr(pos, "signal_payload", None)
    if not isinstance(sp, dict):
        sp = {}
        pos.signal_payload = sp

    payload_contract = _coerce_contract_from_signal_payload(payload)
    if not payload_contract.get("contract_ver") and not payload_contract.get("horizon") and not payload_contract.get("atr_profile"):
        if _M_POS_MISSING is not None:
            with contextlib.suppress(Exception):
                _M_POS_MISSING.labels(source=(source or "signal_open")).inc()
        return False

    sp_meta = _ensure_dict(sp.get("meta"))
    payload_meta = _ensure_dict(payload.get("meta"))
    payload_meta = _merge_meta_contract(payload_meta, {
        "contract_ver": payload_contract.get("contract_ver"),
        "horizon": payload_contract.get("horizon"),
        "atr_profile": payload_contract.get("atr_profile"),
    })
    sp["meta"] = _merge_meta_contract(sp_meta, payload_meta)

    hz = _ensure_dict(sp["meta"].get("horizon"))
    ap = _ensure_dict(sp["meta"].get("atr_profile"))
    sp.setdefault("contract_ver", _safe_int(sp["meta"].get("contract_ver"), _CONTRACT_VER))
    sp.setdefault("hold_target_ms", _safe_int(hz.get("hold_target_ms"), 0))
    sp.setdefault("alpha_half_life_ms", _safe_int(hz.get("alpha_half_life_ms"), 0))
    sp.setdefault("max_signal_age_ms", _safe_int(hz.get("max_signal_age_ms"), 0))
    sp.setdefault("risk_horizon_bucket", (hz.get("risk_horizon_bucket") or "unknown"))
    sp.setdefault("horizon_profile_source", str(hz.get("profile_source") or _DEFAULT_PROFILE_SOURCE))
    sp.setdefault("horizon_profile_conf", _safe_float(hz.get("profile_conf"), 0.0))
    sp.setdefault("horizon_reason_code", (hz.get("reason_code") or "HZ_STATIC_BOOTSTRAP"))
    sp.setdefault("atr_mode", (ap.get("mode") or "legacy"))
    sp.setdefault("atr_value", _safe_float(ap.get("atr_value"), 0.0))
    sp.setdefault("atr_tf_ms", _safe_int(ap.get("atr_tf_ms"), _DEFAULT_TF_MS))
    sp.setdefault("atr_window_n", _safe_int(ap.get("atr_window_n"), _DEFAULT_WINDOW_N))
    sp.setdefault("atr_age_ms", _safe_int(ap.get("atr_age_ms"), 0))
    sp.setdefault("atr_source", (ap.get("atr_source") or "legacy"))
    sp.setdefault("atr_pct", _safe_float(ap.get("atr_pct"), 0.0))
    sp.setdefault("vol_ratio_fast_slow", _safe_float(ap.get("vol_ratio_fast_slow"), 1.0))
    sp.setdefault("vol_ratio_z", _safe_float(ap.get("vol_ratio_z"), 0.0))

    ok = _apply_position_aliases(pos, {
        "contract_ver": _safe_int(sp["meta"].get("contract_ver"), _CONTRACT_VER),
        "horizon": hz,
        "atr_profile": ap,
    })
    if ok and _M_POS_ATTACH is not None:
        with contextlib.suppress(Exception):
            _M_POS_ATTACH.labels(source=(source or "signal_open")).inc()
    return ok


def hydrate_position_from_signal_payload(pos: Any, *, source: str = "recovery") -> bool:
    """
    Rebuild PositionState convenience attrs from persisted signal_payload.
    Fail-open: returns False when contract is missing.
    """
    if not _POSITION_RECOVERY_ENABLED:
        return False
    if pos is None:
        return False
    sp = getattr(pos, "signal_payload", None)
    if not isinstance(sp, dict):
        if _M_POS_MISSING is not None:
            with contextlib.suppress(Exception):
                _M_POS_MISSING.labels(source=(source or "recovery")).inc()
        return False

    contract = _coerce_contract_from_signal_payload(sp)
    ok = _apply_position_aliases(pos, contract)
    if ok:
        if _M_POS_RECOVER is not None:
            with contextlib.suppress(Exception):
                _M_POS_RECOVER.labels(source=(source or "recovery")).inc()
    else:
        if _M_POS_MISSING is not None:
            with contextlib.suppress(Exception):
                _M_POS_MISSING.labels(source=(source or "recovery")).inc()
    return ok


@dataclass(frozen=True)
class ATRProfileV1:
    mode: AtrMode
    atr_value: float
    atr_tf_ms: int
    atr_window_n: int
    atr_age_ms: int
    atr_source: AtrSource
    atr_regime_value: float = 0.0
    atr_trail_value: float = 0.0
    atr_regime_tf_ms: int = 0
    atr_trail_tf_ms: int = 0
    atr_pct: float = 0.0
    vol_ratio_fast_slow: float = 1.0
    vol_ratio_z: float = 0.0


@dataclass(frozen=True)
class HorizonProfileV1:
    contract_ver: int
    phase_mode: PhaseMode
    hold_target_ms: int
    alpha_half_life_ms: int
    max_signal_age_ms: int
    risk_horizon_bucket: HorizonBucket
    profile_source: str
    profile_conf: float = 0.0
    reason_code: str = "HZ_STATIC_BOOTSTRAP"
    reason_details: dict[str, Any] = field(default_factory=dict)


def build_phase0_horizon_profile(
    *,
    source: str = "CryptoOrderFlow",
    symbol: str,
    kind: str,
    regime: str,
    now_ms: int,
) -> dict[str, Any]:
    """
    Build horizon profile dict.

    Phase 1: first tries to load a Redis-calibrated profile (history-based).
    Falls back to static bootstrap zeros when Redis key is missing or stale.
    """
    calibrated = _load_calibrated_horizon_profile(
        source=source,
        symbol=symbol,
        kind=kind,
        regime=regime,
        now_ms=now_ms,
    ),
    if calibrated and isinstance(calibrated, dict):
        # Overlay mandatory contract fields so consumers can rely on them.
        calibrated["contract_ver"] = _CONTRACT_VER
        calibrated["phase_mode"] = (
            _PHASE_MODE if _PHASE_MODE in {"off", "shadow", "canary", "enforce"} else "off"
        ),
        return calibrated

    # Static bootstrap fallback (Phase 0 / no history yet)
    hp = HorizonProfileV1(
        contract_ver=_CONTRACT_VER,
        phase_mode=_PHASE_MODE if _PHASE_MODE in {"off", "shadow", "canary", "enforce"} else "off",  # type: ignore
        hold_target_ms=0,
        alpha_half_life_ms=0,
        max_signal_age_ms=_DEFAULT_MAX_SIGNAL_AGE_MS,
        risk_horizon_bucket=_DEFAULT_BUCKET if _DEFAULT_BUCKET in {"micro", "short", "medium", "long", "unknown"} else "unknown",  # type: ignore
        profile_source=_DEFAULT_PROFILE_SOURCE,
        profile_conf=0.0,
        reason_code="HZ_STATIC_BOOTSTRAP",
        reason_details={
            "symbol": (symbol or "").upper(),
            "kind": (kind or "unknown"),
            "regime": (regime or "unknown"),
            "ts_ms": int(now_ms),
        },
    ),
    return asdict(hp)  # type: ignore


def build_phase0_atr_profile(
    *,
    atr_value: float,
    price: float,
    atr_age_ms: int,
) -> dict[str, Any]:
    """Legacy builder — kept for back-compat callers outside attach_phase0_contract."""
    atr_pct = float(atr_value / price) if price > 0.0 else 0.0
    ap = ATRProfileV1(
        mode="legacy",
        atr_value=float(atr_value),
        atr_tf_ms=_DEFAULT_TF_MS,
        atr_window_n=_DEFAULT_WINDOW_N,
        atr_age_ms=max(0, int(atr_age_ms)),
        atr_source="legacy",
        atr_regime_value=float(atr_value),
        atr_trail_value=float(atr_value),
        atr_regime_tf_ms=_DEFAULT_TF_MS,
        atr_trail_tf_ms=_DEFAULT_TF_MS,
        atr_pct=atr_pct,
        vol_ratio_fast_slow=1.0,
        vol_ratio_z=0.0,
    ),
    return asdict(ap)  # type: ignore


def build_runtime_atr_profile(
    *,
    signal: dict[str, Any],
    price: float,
    hold_target_ms: int,
    alpha_half_life_ms: int,
    now_ms: int,
) -> dict[str, Any]:
    """
    Phase 2 builder: delegates to runtime selector when enabled.
    Always fail-open to legacy ATRProfileV1 on any error.
    """
    if _SELECTOR_ENABLED:
        try:
            from services.atr_runtime_selector import select_runtime_atr_profile
            return select_runtime_atr_profile(  # type: ignore
                signal=signal,
                price=price,
                hold_target_ms=hold_target_ms,
                alpha_half_life_ms=alpha_half_life_ms,
                now_ms=now_ms,
            ),
        except Exception:
            pass
    # Last-ditch fallback: classic legacy scalar
    atr_value = _safe_float(
        signal.get("atr")
        or _ensure_dict(signal.get("indicators")).get("atr")
        or 0.0,
        0.0,
    ),
    atr_pct = float(atr_value / price) if price > 0.0 else 0.0  # type: ignore
    ap = ATRProfileV1(
        mode="legacy",
        atr_value=float(atr_value),  # type: ignore
        atr_tf_ms=_DEFAULT_TF_MS,
        atr_window_n=_DEFAULT_WINDOW_N,
        atr_age_ms=0,
        atr_source="legacy",
        atr_regime_value=float(atr_value),  # type: ignore
        atr_trail_value=float(atr_value),  # type: ignore
        atr_regime_tf_ms=_DEFAULT_TF_MS,
        atr_trail_tf_ms=_DEFAULT_TF_MS,
        atr_pct=atr_pct,
        vol_ratio_fast_slow=1.0,
        vol_ratio_z=0.0,
    ),
    return asdict(ap)  # type: ignore


def attach_phase0_contract(signal: dict[str, Any], *, symbol: str, source: str) -> dict[str, Any]:
    """
    Idempotent phase-0 contract attachment.
    Does NOT modify trading semantics.
    """
    if not _EMIT_PAYLOAD_META:
        return signal
    if not isinstance(signal, dict):
        return signal

    meta = _ensure_dict(signal.get("meta"))
    signal["meta"] = meta

    # Already attached -> keep caller fields untouched.
    if (
        meta.get("contract_ver") == _CONTRACT_VER
        and isinstance(meta.get("horizon"), dict)
        and isinstance(meta.get("atr_profile"), dict)
    ):
        return signal

    now_ms = _safe_int(
        signal.get("ts_ms") or signal.get("tick_ts") or int(time.time() * 1000),
        int(time.time() * 1000),
    ),
    indicators = _ensure_dict(signal.get("indicators"))
    price = _safe_float(
        signal.get("entry_price")
        or signal.get("entry")
        or signal.get("price")
        or signal.get("entry_px")
        or indicators.get("mid_px")
        or 0.0,
        0.0,
    ),
    regime = str(
        meta.get("regime")
        or indicators.get("regime")
        or signal.get("regime")
        or "unknown"
    ).lower()
    kind = str(signal.get("kind") or signal.get("reason") or "unknown").lower()

    meta.setdefault("contract_ver", _CONTRACT_VER)

    # Phase 2.1: populate atr_candidates observe-only feed before selector runs.
    if _EMIT_CANDIDATES and "atr_candidates" not in meta:
        try:
            from services.atr_candidate_provider import get_atr_candidate_provider
            _provider = get_atr_candidate_provider()
            meta["atr_candidates"] = _provider.collect(
                signal=signal,
                symbol=str(symbol or signal.get("symbol") or "").upper(),
                now_ms=now_ms,  # type: ignore
            ),
        except Exception:
            pass

    meta.setdefault("horizon", build_phase0_horizon_profile(
        source=(source or "CryptoOrderFlow"),
        symbol=str(symbol or signal.get("symbol") or "").upper(),
        kind=kind,
        regime=regime,
        now_ms=now_ms,  # type: ignore
    ))

    # Phase 2: use runtime selector; reads hold/decay from the horizon just built.
    hz = _ensure_dict(meta.get("horizon"))
    hold_target_ms_hz = _safe_int(hz.get("hold_target_ms"), 0)
    alpha_half_life_ms_hz = _safe_int(hz.get("alpha_half_life_ms"), 0)
    meta.setdefault("atr_profile", build_runtime_atr_profile(
        signal=signal,
        price=price,  # type: ignore
        hold_target_ms=hold_target_ms_hz,
        alpha_half_life_ms=alpha_half_life_ms_hz,
        now_ms=now_ms,  # type: ignore
    ))

    # Legacy aliases for future phases / diagnostics.
    signal.setdefault("hold_target_ms", _safe_int(meta["horizon"].get("hold_target_ms"), 0))
    signal.setdefault("alpha_half_life_ms", _safe_int(meta["horizon"].get("alpha_half_life_ms"), 0))
    signal.setdefault("max_signal_age_ms", _safe_int(meta["horizon"].get("max_signal_age_ms"), 0))
    signal.setdefault("risk_horizon_bucket", str(meta["horizon"].get("risk_horizon_bucket") or "unknown"))
    signal.setdefault("atr_tf_ms", _safe_int(meta["atr_profile"].get("atr_tf_ms"), _DEFAULT_TF_MS))
    signal.setdefault("atr_age_ms", _safe_int(meta["atr_profile"].get("atr_age_ms"), 0))
    signal.setdefault("atr_source", str(meta["atr_profile"].get("atr_source") or "unknown"))
    # Phase 2 new aliases
    signal.setdefault("atr_mode", str(meta["atr_profile"].get("mode") or "legacy"))
    signal.setdefault("atr_value", _safe_float(meta["atr_profile"].get("atr_value"), 0.0))
    signal.setdefault("atr_pct", _safe_float(meta["atr_profile"].get("atr_pct"), 0.0))
    signal.setdefault("vol_ratio_fast_slow", _safe_float(meta["atr_profile"].get("vol_ratio_fast_slow"), 1.0))
    signal.setdefault("vol_ratio_z", _safe_float(meta["atr_profile"].get("vol_ratio_z"), 0.0))

    # Phase 2.3: push canonical feature aliases into indicators for ML v5 serving path.
    # These populate vol_ratio and vol_ratio_z (MLFeatureSchemaV5OF.num_keys) so the
    # model receives them without changes to execution / trailing gates.
    try:
        inds = signal.setdefault("indicators", {})
        if isinstance(inds, dict):
            inds.setdefault("vol_ratio", _safe_float(meta["atr_profile"].get("vol_ratio_fast_slow"), 1.0))
            inds.setdefault("vol_ratio_z", _safe_float(meta["atr_profile"].get("vol_ratio_z"), 0.0))
            inds.setdefault("atr_selected_tf_ms", _safe_int(meta["atr_profile"].get("atr_tf_ms"), _DEFAULT_TF_MS))
            inds.setdefault("atr_selected_age_ms", _safe_int(meta["atr_profile"].get("atr_age_ms"), 0))
            inds.setdefault("atr_selected_pct", _safe_float(meta["atr_profile"].get("atr_pct"), 0.0))
    except Exception:
        pass  # fail-open: alias bridge must never break signal publishing


    # Important: by default _USE_FOR_GATES=False → legacy signal["atr"] is NOT touched.
    # Only explicit ENV flag lets selector feed existing DQ/gate path.
    if _USE_FOR_GATES:
        try:
            new_atr = _safe_float(meta["atr_profile"].get("atr_value"), 0.0)
            if new_atr > 0.0:
                signal["atr"] = new_atr
        except Exception:
            pass

    if _M_EMITTED is not None:
        try:
            _M_EMITTED.labels(
                source=(source or "unknown"),
                phase_mode=str(meta["horizon"].get("phase_mode") or "off"),
            ).inc()
            _M_REASON.labels(  # type: ignore
                reason_code=str(meta["horizon"].get("reason_code") or "HZ_STATIC_BOOTSTRAP"),
            ).inc()
        except Exception:
            pass

    return signal


def extract_horizon_contract_from_payload(payload: dict[str, Any]) -> dict[str, Any]:
    """
    Stable DB snapshot for trades_closed / analytics.
    """
    if not isinstance(payload, dict):
        return {}
    meta = _ensure_dict(payload.get("meta"))
    horizon = _ensure_dict(meta.get("horizon"))
    atr_profile = _ensure_dict(meta.get("atr_profile"))
    if not horizon and not atr_profile and not meta.get("contract_ver"):
        return {}
    return {
        "contract_ver": _safe_int(meta.get("contract_ver"), _CONTRACT_VER),
        "horizon": horizon,
        "atr_profile": atr_profile,
    }


def extract_horizon_bucket(contract: dict[str, Any]) -> str:
    if not isinstance(contract, dict):
        return ""
    hz = _ensure_dict(contract.get("horizon"))
    return (hz.get("risk_horizon_bucket") or "")


def extract_atr_tf_ms(contract: dict[str, Any]) -> int:
    if not isinstance(contract, dict):
        return 0
    ap = _ensure_dict(contract.get("atr_profile"))
    return _safe_int(ap.get("atr_tf_ms"), 0)


# ---------------------------------------------------------------------------
# Phase 0.3 — scalar projection helpers
# ---------------------------------------------------------------------------

POSITION_HORIZON_SCALAR_KEYS: tuple = (
    "contract_ver",
    "hold_target_ms",
    "alpha_half_life_ms",
    "max_signal_age_ms",
    "risk_horizon_bucket",
    "horizon_profile_source",
    "horizon_profile_conf",
    "horizon_reason_code",
    "atr_mode",
    "atr_value",
    "atr_tf_ms",
    "atr_window_n",
    "atr_age_ms",
    "atr_regime_value",
    "atr_trail_value",
    "atr_regime_tf_ms",
    "atr_trail_tf_ms",
    "atr_source",
    "atr_pct",
    "vol_ratio_fast_slow",
    "vol_ratio_z",
),


def extract_position_horizon_scalars(obj: Any) -> dict[str, Any]:
    """
    Produce first-class scalar fields for Redis hash / events / analytics.
    Reads convenience attrs set by Phase 0.2 (_apply_position_aliases).
    Fail-open, deterministic.
    """
    return {
        "contract_ver": _safe_int(
            getattr(obj, "horizon_contract_ver", None) or getattr(obj, "contract_ver", None),
            _CONTRACT_VER,
        ),
        "hold_target_ms": _safe_int(getattr(obj, "hold_target_ms", None), 0),
        "alpha_half_life_ms": _safe_int(getattr(obj, "alpha_half_life_ms", None), 0),
        "max_signal_age_ms": _safe_int(getattr(obj, "max_signal_age_ms", None), 0),
        "risk_horizon_bucket": str(getattr(obj, "risk_horizon_bucket", None) or "unknown"),
        "horizon_profile_source": str(getattr(obj, "horizon_profile_source", None) or _DEFAULT_PROFILE_SOURCE),
        "horizon_profile_conf": _safe_float(getattr(obj, "horizon_profile_conf", None), 0.0),
        "horizon_reason_code": str(getattr(obj, "horizon_reason_code", None) or "HZ_STATIC_BOOTSTRAP"),
        "atr_mode": str(getattr(obj, "atr_mode", None) or "legacy"),
        "atr_value": _safe_float(
            getattr(obj, "atr_value", None) or getattr(obj, "atr", None),
            0.0,
        ),
        "atr_tf_ms": _safe_int(getattr(obj, "atr_tf_ms", None), _DEFAULT_TF_MS),
        "atr_window_n": _safe_int(getattr(obj, "atr_window_n", None), _DEFAULT_WINDOW_N),
        "atr_age_ms": _safe_int(getattr(obj, "atr_age_ms", None), 0),
        "atr_source": str(getattr(obj, "atr_source", None) or "legacy"),
        "atr_pct": _safe_float(getattr(obj, "atr_pct", None), 0.0),
        "vol_ratio_fast_slow": _safe_float(getattr(obj, "vol_ratio_fast_slow", None), 1.0),
        "vol_ratio_z": _safe_float(getattr(obj, "vol_ratio_z", None), 0.0),
        "atr_regime_value": _safe_float(getattr(obj, "atr_regime_value", None), 0.0),
        "atr_trail_value": _safe_float(getattr(obj, "atr_trail_value", None), 0.0),
        "atr_regime_tf_ms": _safe_int(getattr(obj, "atr_regime_tf_ms", None), 0),
        "atr_trail_tf_ms": _safe_int(getattr(obj, "atr_trail_tf_ms", None), 0),
    }


_M_SCALAR_RECOVER = _safe_counter(
    "trade_horizon_position_scalar_recover_total",
    "Phase 0.3 horizon scalars restored directly from Redis hash fields",
    ["source"],
)

_M_SCALAR_MISSING = _safe_counter(
    "trade_horizon_position_scalar_missing_total",
    "Phase 0.3 position had no horizon scalar fields in Redis hash",
    ["source"],
)

_M_CLOSED_STAMPED = _safe_counter(
    "trade_horizon_closed_scalar_stamped_total",
    "Phase 0.3 horizon scalars stamped onto TradeClosed",
    [],
)


def apply_position_horizon_scalars_from_hash(pos: Any, h: dict[str, Any], *, source: str = "hash") -> bool:
    """
    Rebuild scalar attrs directly from Redis hash fields.
    Phase 0.3 fallback: recovery does NOT depend only on signal_payload JSON.
    Fail-open.
    """
    if pos is None or not isinstance(h, dict):
        return False
    # Check if there are any horizon scalars in hash at all
    has_any = any(h.get(k) for k in ("risk_horizon_bucket", "hold_target_ms", "atr_tf_ms", "contract_ver"))
    if not has_any:
        if _M_SCALAR_MISSING is not None:
            with contextlib.suppress(Exception):
                _M_SCALAR_MISSING.labels(source=str(source)).inc()
        return False
    try:
        pos.horizon_contract_ver = _safe_int(h.get("contract_ver"), _CONTRACT_VER)
        pos.hold_target_ms = _safe_int(h.get("hold_target_ms"), 0)
        pos.alpha_half_life_ms = _safe_int(h.get("alpha_half_life_ms"), 0)
        pos.max_signal_age_ms = _safe_int(h.get("max_signal_age_ms"), 0)
        pos.risk_horizon_bucket = (h.get("risk_horizon_bucket") or "unknown")
        pos.horizon_profile_source = str(h.get("horizon_profile_source") or _DEFAULT_PROFILE_SOURCE)
        pos.horizon_profile_conf = _safe_float(h.get("horizon_profile_conf"), 0.0)
        pos.horizon_reason_code = (h.get("horizon_reason_code") or "HZ_STATIC_BOOTSTRAP")
        pos.atr_mode = (h.get("atr_mode") or "legacy")
        pos.atr_value = _safe_float(h.get("atr_value") or h.get("atr"), 0.0)
        pos.atr_tf_ms = _safe_int(h.get("atr_tf_ms"), _DEFAULT_TF_MS)
        pos.atr_window_n = _safe_int(h.get("atr_window_n"), _DEFAULT_WINDOW_N)
        pos.atr_age_ms = _safe_int(h.get("atr_age_ms"), 0)
        pos.atr_source = (h.get("atr_source") or "legacy")
        pos.atr_pct = _safe_float(h.get("atr_pct"), 0.0)
        pos.vol_ratio_fast_slow = _safe_float(h.get("vol_ratio_fast_slow"), 1.0)
        pos.vol_ratio_z = _safe_float(h.get("vol_ratio_z"), 0.0)
        if _M_SCALAR_RECOVER is not None:
            with contextlib.suppress(Exception):
                _M_SCALAR_RECOVER.labels(source=str(source)).inc()
        return True
    except Exception:
        return False


def stamp_closed_trade_horizon_from_position(pos: Any, closed: Any) -> bool:
    """
    Copy first-class scalar horizon/ATR fields from PositionState → TradeClosed.
    Call this inside _stamp_closed_trade_meta() before save_closed().
    Fail-open.
    """
    if pos is None or closed is None:
        return False
    scalars = extract_position_horizon_scalars(pos)
    try:
        for k, v in scalars.items():
            setattr(closed, k, v)
        # Keep a copy in signal_payload too for back-compat / replay.
        sp = getattr(closed, "signal_payload", None)
        if not isinstance(sp, dict):
            sp = {}
            closed.signal_payload = sp
        sp.setdefault("_horizon_scalars", scalars)
        if _M_CLOSED_STAMPED is not None:
            with contextlib.suppress(Exception):
                _M_CLOSED_STAMPED.inc()
        return True
    except Exception:
        return False


def build_horizon_event_scalars(obj: Any) -> dict[str, Any]:
    """
    Compact payload fragment for OPEN/CLOSED events.
    Subset of extract_position_horizon_scalars — only fields useful for event consumers.
    """
    s = extract_position_horizon_scalars(obj)
    return {
        "contract_ver": s["contract_ver"],
        "risk_horizon_bucket": s["risk_horizon_bucket"],
        "hold_target_ms": s["hold_target_ms"],
        "alpha_half_life_ms": s["alpha_half_life_ms"],
        "max_signal_age_ms": s["max_signal_age_ms"],
        "atr_tf_ms": s["atr_tf_ms"],
        "atr_age_ms": s["atr_age_ms"],
        "atr_source": s["atr_source"],
        "atr_pct": s["atr_pct"],
        "vol_ratio_fast_slow": s["vol_ratio_fast_slow"],
        "vol_ratio_z": s["vol_ratio_z"],
    }
