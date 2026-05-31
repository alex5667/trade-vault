from __future__ import annotations

"""python-worker/core/horizon_contract.py

Phase 0 — Единый horizon-aware контракт для всего пайплайна.

Цель Phase 0:
  - Ввести canonical объекты ATRProfileV1 / HorizonProfileV1 / SignalRiskProfileV1.
  - НЕ менять торговую логику, ATR TF selection, SL/TP, trailing.
  - Предоставить deterministic bootstrap-builders для Phase 0 (режим mode=legacy).
  - Все USE_FOR_* флаги выключены → только emit-флаги активны.

Backward compatibility:
  - ctx.atr остаётся legacy alias.
  - ctx.atr_profile / ctx.horizon_profile — новые canonical поля.
  - signal_id НЕ изменяется (новые поля не входят в dedup-base).
"""


import os
from dataclasses import dataclass, field
from typing import Any, Literal
import contextlib

# ─── Canonical type aliases ────────────────────────────────────────────────────

HorizonBucket = Literal["micro", "short", "medium", "long", "unknown"]
AtrSource = Literal["legacy", "bootstrap", "selector", "manual", "fallback", "unknown"]
AtrMode = Literal["legacy", "horizon"]
PhaseMode = Literal["off", "shadow", "canary", "enforce"]

# ─── Contract version ──────────────────────────────────────────────────────────

HORIZON_CONTRACT_VER: int = 2


# ─── Reason codes (Phase 0 canonical set) ─────────────────────────────────────

class HorizonReasonCode:
    """Short, indexable reason codes for horizon profile decisions.

    These are written to diagnostics stream and payload meta.
    reason_details (JSON) carries unstructured context — not used for branching.
    """
    # Horizon
    HZ_OK = "HZ_OK"
    HZ_STATIC_BOOTSTRAP = "HZ_STATIC_BOOTSTRAP"
    HZ_HISTORY_PROFILE = "HZ_HISTORY_PROFILE"
    HZ_FALLBACK_UNKNOWN_KIND = "HZ_FALLBACK_UNKNOWN_KIND"
    HZ_FALLBACK_UNKNOWN_REGIME = "HZ_FALLBACK_UNKNOWN_REGIME"
    HZ_LOW_SAMPLE_PROFILE = "HZ_LOW_SAMPLE_PROFILE"
    HZ_MISSING_PROFILE = "HZ_MISSING_PROFILE"
    HZ_PROFILE_STALE = "HZ_PROFILE_STALE"
    HZ_MAX_SIGNAL_AGE_EXCEEDED = "HZ_MAX_SIGNAL_AGE_EXCEEDED"

    # ATR
    ATR_OK = "ATR_OK"
    ATR_LEGACY_ALIAS = "ATR_LEGACY_ALIAS"
    ATR_SELECTOR_PENDING = "ATR_SELECTOR_PENDING"
    ATR_PROFILE_MISSING = "ATR_PROFILE_MISSING"
    ATR_PROFILE_STALE = "ATR_PROFILE_STALE"
    ATR_HORIZON_MISMATCH = "ATR_HORIZON_MISMATCH"
    ATR_SOURCE_FALLBACK = "ATR_SOURCE_FALLBACK"
    ATR_WINDOW_INVALID = "ATR_WINDOW_INVALID"
    ATR_TF_UNSUPPORTED = "ATR_TF_UNSUPPORTED"

    # DQ (future-reserved)
    DQ_BOOK_STALE_FOR_HORIZON = "DQ_BOOK_STALE_FOR_HORIZON"
    DQ_ATR_STALE_FOR_HORIZON = "DQ_ATR_STALE_FOR_HORIZON"
    DQ_ATR_UNAVAILABLE = "DQ_ATR_UNAVAILABLE"
    DQ_TICK_GAP_CRITICAL = "DQ_TICK_GAP_CRITICAL"
    DQ_SIGNAL_TOO_OLD = "DQ_SIGNAL_TOO_OLD"


RC = HorizonReasonCode  # shorthand


# ─── Canonical profile dataclasses ────────────────────────────────────────────

@dataclass(frozen=True)
class ATRProfileV1:
    """Canonical ATR profile snapshot at decision time.

    Phase 0: mode is always 'legacy', selects from ctx.atr.
    Phase 1+: mode becomes 'horizon' with intelligent TF selector.

    atr_value — canonical ATR used for SL/TP logic (backward compat: == ctx.atr).
    """
    mode: AtrMode                   # "legacy" | "horizon"
    atr_value: float                # canonical ATR for stop logic
    atr_tf_ms: int                  # chosen TF in ms (Phase 0: 60000)
    atr_window_n: int               # e.g. 14
    atr_age_ms: int                 # freshness at decision time
    atr_source: AtrSource           # "legacy" | "bootstrap" | "selector" | ...

    # Reserved for future regime/trailing split
    atr_regime_value: float = 0.0
    atr_trail_value: float = 0.0
    atr_regime_tf_ms: int = 0
    atr_trail_tf_ms: int = 0

    # Derived / enrichment fields
    atr_pct: float = 0.0            # atr_value / price
    vol_ratio_fast_slow: float = 0.0
    vol_ratio_z: float = 0.0

    def to_dict(self) -> dict[str, Any]:
        return {
            "mode": self.mode,
            "atr_value": self.atr_value,
            "atr_tf_ms": self.atr_tf_ms,
            "atr_window_n": self.atr_window_n,
            "atr_age_ms": self.atr_age_ms,
            "atr_source": self.atr_source,
            "atr_regime_value": self.atr_regime_value,
            "atr_trail_value": self.atr_trail_value,
            "atr_regime_tf_ms": self.atr_regime_tf_ms,
            "atr_trail_tf_ms": self.atr_trail_tf_ms,
            "atr_pct": self.atr_pct,
            "vol_ratio_fast_slow": self.vol_ratio_fast_slow,
            "vol_ratio_z": self.vol_ratio_z,
        }


@dataclass(frozen=True)
class HorizonProfileV1:
    """Canonical horizon profile snapshot at decision time.

    Phase 0: profile_source='static_bootstrap', phase_mode='off'.
    Phase 1+: profile resolved from history/Redis config.

    contract_ver — always 2 for new consumers.
    """
    contract_ver: int               # = 2
    phase_mode: PhaseMode           # "off" | "shadow" | "canary" | "enforce"
    hold_target_ms: int
    alpha_half_life_ms: int
    max_signal_age_ms: int
    risk_horizon_bucket: HorizonBucket
    profile_source: str             # "static_bootstrap" | "history" | "fallback"
    profile_conf: float = 0.0       # 0..1
    reason_code: str = RC.HZ_STATIC_BOOTSTRAP
    reason_details: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "phase_mode": self.phase_mode,
            "hold_target_ms": self.hold_target_ms,
            "alpha_half_life_ms": self.alpha_half_life_ms,
            "max_signal_age_ms": self.max_signal_age_ms,
            "risk_horizon_bucket": self.risk_horizon_bucket,
            "profile_source": self.profile_source,
            "profile_conf": self.profile_conf,
            "reason_code": self.reason_code,
            "reason_details": dict(self.reason_details),
        }


@dataclass(frozen=True)
class SignalRiskProfileV1:
    """Combined risk profile snapshot — canonical serializable structure.

    Carries both horizon and ATR profiles for a signal at decision time.
    Designed for payload/meta, DB persistence, and diagnostics enrichment.
    """
    horizon: HorizonProfileV1
    atr: ATRProfileV1
    rr_target: float = 0.0
    sl_atr_mult: float = 0.0
    tp1_atr_mult: float = 0.0
    tp2_atr_mult: float = 0.0

    def to_dict(self) -> dict[str, Any]:
        return {
            "contract_ver": self.horizon.contract_ver,
            "horizon": self.horizon.to_dict(),
            "atr_profile": self.atr.to_dict(),
            "rr_target": self.rr_target,
            "sl_atr_mult": self.sl_atr_mult,
            "tp1_atr_mult": self.tp1_atr_mult,
            "tp2_atr_mult": self.tp2_atr_mult,
        }


# ─── ENV config ───────────────────────────────────────────────────────────────

class HorizonEnvConfig:
    """Read-once ENV config for Phase 0 horizon contract.

    All USE_FOR_* flags are OFF in Phase 0.
    All EMIT_* flags are ON (observability only).
    """

    @staticmethod
    def _bool(key: str, default: bool = True) -> bool:
        v = os.getenv(key, "1" if default else "0").strip().lower()
        return v in {"1", "true", "yes", "on"}

    @staticmethod
    def _int(key: str, default: int) -> int:
        try:
            return int(os.getenv(key, str(default)))
        except (TypeError, ValueError):
            return default

    @staticmethod
    def _str(key: str, default: str) -> str:
        return os.getenv(key, default).strip()

    # Master switch
    @classmethod
    def mode(cls) -> str:
        return cls._str("ATR_HORIZON_MODE", "off")

    @classmethod
    def contract_ver(cls) -> int:
        return cls._int("ATR_HORIZON_CONTRACT_VER", HORIZON_CONTRACT_VER)

    # Phase 0 defaults
    @classmethod
    def default_tf_ms(cls) -> int:
        return cls._int("ATR_HORIZON_DEFAULT_TF_MS", 60_000)

    @classmethod
    def default_window_n(cls) -> int:
        return cls._int("ATR_HORIZON_DEFAULT_WINDOW_N", 14)

    @classmethod
    def default_bucket(cls) -> str:
        return cls._str("ATR_HORIZON_DEFAULT_BUCKET", "unknown")

    @classmethod
    def default_profile_source(cls) -> str:
        return cls._str("ATR_HORIZON_DEFAULT_PROFILE_SOURCE", "static_bootstrap")

    # Emit flags (all ON by default in Phase 0)
    @classmethod
    def emit_payload_meta(cls) -> bool:
        return cls._bool("ATR_HORIZON_EMIT_PAYLOAD_META", True)

    @classmethod
    def emit_diag(cls) -> bool:
        return cls._bool("ATR_HORIZON_EMIT_DIAG", True)

    @classmethod
    def emit_metrics(cls) -> bool:
        return cls._bool("ATR_HORIZON_EMIT_METRICS", True)

    @classmethod
    def enable_ctx_aliases(cls) -> bool:
        return cls._bool("ATR_HORIZON_ENABLE_CTX_ALIASES", True)

    # Use flags (all OFF in Phase 0 — trading logic UNCHANGED)
    @classmethod
    def use_for_gates(cls) -> bool:
        return cls._bool("ATR_HORIZON_USE_FOR_GATES", False)

    @classmethod
    def use_for_execution(cls) -> bool:
        return cls._bool("ATR_HORIZON_USE_FOR_EXECUTION", False)

    @classmethod
    def use_for_trailing(cls) -> bool:
        return cls._bool("ATR_HORIZON_USE_FOR_TRAILING", False)

    @classmethod
    def use_for_ml(cls) -> bool:
        return cls._bool("ATR_HORIZON_USE_FOR_ML", False)


_ENV = HorizonEnvConfig  # shorthand


# ─── Phase 0 bootstrap builders ───────────────────────────────────────────────

def build_phase0_horizon_profile(
    *,
    symbol: str,
    kind: str,
    regime: str,
    now_ts_ms: int,
) -> HorizonProfileV1:
    """Deterministic Phase 0 horizon profile.

    Returns a zero-valued static bootstrap profile (phase_mode='off').
    Phase 1 will replace this with actual history-based profile resolution.

    IMPORTANT: This function must be deterministic — same inputs → same output.
    """
    return HorizonProfileV1(
        contract_ver=HORIZON_CONTRACT_VER,
        phase_mode="off",
        hold_target_ms=0,
        alpha_half_life_ms=0,
        max_signal_age_ms=0,
        risk_horizon_bucket="unknown",  # type: ignore[arg-type]
        profile_source=_ENV.default_profile_source(),
        profile_conf=0.0,
        reason_code=RC.HZ_STATIC_BOOTSTRAP,
        reason_details={
            "symbol": symbol,
            "kind": kind,
            "regime": regime,
            "ts_ms": now_ts_ms,
        },
    )


def build_phase0_atr_profile(
    *,
    atr_value: float,
    price: float,
    atr_age_ms: int,
) -> ATRProfileV1:
    """Deterministic Phase 0 ATR profile (legacy mode alias).

    Maps ctx.atr → ATRProfileV1 without changing ATR TF selection.
    Phase 1 will introduce intelligent TF selection via ATRHorizonSelector.

    Price=0 safe: atr_pct defaults to 0.0.
    """
    atr_value = float(atr_value or 0.0)
    price = float(price or 0.0)
    atr_age_ms = max(0, int(atr_age_ms or 0))
    atr_pct = (atr_value / price) if price > 0 and atr_value > 0 else 0.0

    return ATRProfileV1(
        mode="legacy",
        atr_value=atr_value,
        atr_tf_ms=_ENV.default_tf_ms(),
        atr_window_n=_ENV.default_window_n(),
        atr_age_ms=atr_age_ms,
        atr_source="legacy",
        atr_regime_value=atr_value,
        atr_trail_value=atr_value,
        atr_regime_tf_ms=_ENV.default_tf_ms(),
        atr_trail_tf_ms=_ENV.default_tf_ms(),
        atr_pct=atr_pct,
        vol_ratio_fast_slow=1.0,
        vol_ratio_z=0.0,
    )


def build_phase0_risk_profile(
    *,
    ctx: Any,
    symbol: str,
    kind: str,
    regime: str,
    now_ts_ms: int,
    sl_atr_mult: float = 0.0,
    tp1_atr_mult: float = 0.0,
    tp2_atr_mult: float = 0.0,
    rr_target: float = 0.0,
) -> SignalRiskProfileV1:
    """Build complete Phase 0 risk profile from ctx.

    Reads ctx.atr (legacy) and ctx.price to compute ATRProfileV1.
    Reads ctx.ts (or now_ts_ms) as decision timestamp.
    """
    atr_val = float(getattr(ctx, "atr", 0.0) or 0.0)
    price = float(getattr(ctx, "price", 0.0) or 0.0)

    # atr_age_ms: approximate from ctx if available
    atr_age_ms = int(getattr(ctx, "atr_age_ms", 0) or 0)

    horizon = build_phase0_horizon_profile(
        symbol=symbol,
        kind=kind,
        regime=regime,
        now_ts_ms=now_ts_ms,
    )
    atr_profile = build_phase0_atr_profile(
        atr_value=atr_val,
        price=price,
        atr_age_ms=atr_age_ms,
    )
    return SignalRiskProfileV1(
        horizon=horizon,
        atr=atr_profile,
        rr_target=float(rr_target),
        sl_atr_mult=float(sl_atr_mult),
        tp1_atr_mult=float(tp1_atr_mult),
        tp2_atr_mult=float(tp2_atr_mult),
    )


# ─── ctx enrichment (fail-open) ───────────────────────────────────────────────

def attach_phase0_profiles_to_ctx(
    ctx: Any,
    *,
    symbol: str,
    kind: str,
    regime: str,
    now_ts_ms: int,
    sl_atr_mult: float = 0.0,
    tp1_atr_mult: float = 0.0,
    tp2_atr_mult: float = 0.0,
    rr_target: float = 0.0,
) -> SignalRiskProfileV1 | None:
    """Best-effort: attach atr_profile and horizon_profile to ctx.

    IMPORTANT:
      - ctx.atr is NOT modified (legacy alias preserved).
      - If ATR_HORIZON_ENABLE_CTX_ALIASES=0, skips ctx attachment.
      - Never raises — fail-open for production safety.

    Returns the SignalRiskProfileV1 (or None on failure).
    """
    try:
        risk_profile = build_phase0_risk_profile(
            ctx=ctx,
            symbol=symbol,
            kind=kind,
            regime=regime,
            now_ts_ms=now_ts_ms,
            sl_atr_mult=sl_atr_mult,
            tp1_atr_mult=tp1_atr_mult,
            tp2_atr_mult=tp2_atr_mult,
            rr_target=rr_target,
        )

        if _ENV.enable_ctx_aliases():
            # Attach canonical profiles to ctx (fail-open per field)
            try:
                object.__setattr__(ctx, "atr_profile", risk_profile.atr)
            except (AttributeError, TypeError):
                with contextlib.suppress(Exception):
                    ctx.atr_profile = risk_profile.atr

            try:
                object.__setattr__(ctx, "horizon_profile", risk_profile.horizon)
            except (AttributeError, TypeError):
                with contextlib.suppress(Exception):
                    ctx.horizon_profile = risk_profile.horizon

            # Compatibility aliases
            _attach_compat_aliases(ctx, risk_profile)

        return risk_profile
    except Exception:
        return None


def _attach_compat_aliases(ctx: Any, rp: SignalRiskProfileV1) -> None:
    """Attach flat compatibility aliases to ctx (fail-open per field)."""
    aliases = {
        "atr_tf_ms": rp.atr.atr_tf_ms,
        "atr_age_ms": rp.atr.atr_age_ms,
        "atr_source": rp.atr.atr_source,
        "hold_target_ms": rp.horizon.hold_target_ms,
        "alpha_half_life_ms": rp.horizon.alpha_half_life_ms,
        "max_signal_age_ms": rp.horizon.max_signal_age_ms,
        "risk_horizon_bucket": rp.horizon.risk_horizon_bucket,
        "vol_ratio_fast_slow": rp.atr.vol_ratio_fast_slow,
        "vol_ratio_z": rp.atr.vol_ratio_z,
    }
    for attr, val in aliases.items():
        try:
            object.__setattr__(ctx, attr, val)
        except (AttributeError, TypeError):
            with contextlib.suppress(Exception):
                setattr(ctx, attr, val)


# ─── Payload meta builder ─────────────────────────────────────────────────────

def build_horizon_meta_for_payload(
    risk_profile: SignalRiskProfileV1,
    *,
    existing_meta: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Merge horizon/atr_profile into existing signal meta dict.

    Called from orchestrator._build_payload() when ATR_HORIZON_EMIT_PAYLOAD_META=1.

    IMPORTANT:
      - Does NOT override existing meta keys (preserves sl_mode, sl_atr_mult, etc.).
      - Adds: contract_ver, horizon, atr_profile.
      - signal_id is NOT touched.
    """
    if not _ENV.emit_payload_meta():
        return dict(existing_meta or {})

    meta = dict(existing_meta or {})
    rp_dict = risk_profile.to_dict()

    # contract_ver
    meta["contract_ver"] = rp_dict["contract_ver"]

    # horizon (nested)
    meta["horizon"] = rp_dict["horizon"]

    # atr_profile (nested)
    meta["atr_profile"] = rp_dict["atr_profile"]

    return meta


# ─── Diagnostics trace enrichment ─────────────────────────────────────────────

def build_horizon_trace_fragment(risk_profile: SignalRiskProfileV1) -> dict[str, Any]:
    """Build the 'horizon' and 'atr_profile' fragment for diagnostics trace.

    Added to trace['horizon'] and trace['atr_profile'] in diagnostics stream.
    """
    return {
        "horizon": {
            "hold_target_ms": risk_profile.horizon.hold_target_ms,
            "alpha_half_life_ms": risk_profile.horizon.alpha_half_life_ms,
            "max_signal_age_ms": risk_profile.horizon.max_signal_age_ms,
            "risk_horizon_bucket": risk_profile.horizon.risk_horizon_bucket,
            "reason_code": risk_profile.horizon.reason_code,
        },
        "atr_profile": {
            "atr_value": risk_profile.atr.atr_value,
            "atr_tf_ms": risk_profile.atr.atr_tf_ms,
            "atr_age_ms": risk_profile.atr.atr_age_ms,
            "atr_source": risk_profile.atr.atr_source,
        },
    }
