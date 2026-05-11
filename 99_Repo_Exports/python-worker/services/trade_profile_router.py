"""
trade_profile_router.py
=======================
Единый маршрутизатор торгового профиля.

Выбирает профиль по ключу (symbol, regime_bucket, kind).
Приоритет: symbol-specific → regime×kind default → global fallback.

Redis-конфиг (опционально):
  cfg:trade_profile:{scope}:{regime_bucket}:{kind}
  scope = symbol (BTCUSDT) или "default"

ENV:
  TRADE_PROFILE_ROUTER_ENABLED=1        — глобальный выключатель
  TRADE_PROFILE_MODE=SHADOW|ENFORCE     — shadow: логировать без применения
  TRADE_PROFILE_CANARY_SHARE_TREND=0.10 — доля canary для trend (0.0–1.0)
  TRADE_PROFILE_CANARY_SHARE_RANGE=0.05
  TRADE_PROFILE_CANARY_SHARE_THIN=0.00
"""
from __future__ import annotations

import hashlib
import logging
import os
from dataclasses import dataclass, field
from typing import Any, Mapping, Optional, Sequence

logger = logging.getLogger("trade_profile_router")

# ---------------------------------------------------------------------------
# DTO
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class TradeProfile:
    """Immutable profile binding all trade parameters for a given context."""
    name: str
    regime_bucket: str                   # trend | range | thin | mixed
    allowed_kinds: Sequence[str]
    deny_kinds: Sequence[str] = field(default_factory=tuple)

    # Edge / confidence thresholds
    min_p_edge: float = 0.55
    min_confidence: float = 0.57
    max_expected_slippage_bps: float = 15.0

    # Zone distance cap (bps) per symbol class
    # Used by SmtCoherenceGate / smt_entry_candidate to gate zone proximity
    max_zone_bp_majors: float = 10.0   # BTC/ETH/SOL
    max_zone_bp_alts: float = 14.0     # liquid alts
    max_zone_bp_memes: float = 18.0    # meme/low-cap

    # Stop multiplier (ATR) per symbol class
    stop_atr_mult_majors: float = 1.0
    stop_atr_mult_alts: float = 1.1
    stop_atr_mult_memes: float = 1.25

    # TP parameters
    tp_rr: str = "1.2,2.0,3.0"
    tp1_atr_mult: float = 0.9
    trailing_profile: str = "wide_swing"

    # Execution / risk
    execution_policy: str = "SAFETY_FIRST"   # SAFETY_FIRST | MAKER_FIRST
    # Per-tier risk multipliers (tier_A = premium liquidity, tier_B = standard, tier_C = low-liq)
    risk_multiplier_tier_a: float = 1.0
    risk_multiplier_tier_b: float = 0.75
    risk_multiplier_tier_c: float = 0.40
    min_net_edge_bps: float = 2.0
    mode: str = "LIVE"                       # LIVE | SHADOW_BY_DEFAULT

    reason_code: str = ""

    # ------------------------------------------------------------------
    # Convenience accessors
    # ------------------------------------------------------------------

    def risk_multiplier_for_tier(self, tier: str) -> float:
        """Return risk multiplier for a symbol tier (A/B/C). Defaults to tier_B."""
        t = tier.upper().strip()
        if t == "A":
            return self.risk_multiplier_tier_a
        if t == "C":
            return self.risk_multiplier_tier_c
        return self.risk_multiplier_tier_b

    def max_zone_bp_for_class(self, symbol_class: str) -> float:
        """Return max zone distance (bps) for a symbol class (majors/alts/memes)."""
        c = symbol_class.lower().strip()
        if c == "majors":
            return self.max_zone_bp_majors
        if c == "memes":
            return self.max_zone_bp_memes
        return self.max_zone_bp_alts

    def stop_atr_mult_for_class(self, symbol_class: str) -> float:
        """Return stop ATR multiplier for a symbol class (majors/alts/memes)."""
        c = symbol_class.lower().strip()
        if c == "majors":
            return self.stop_atr_mult_majors
        if c == "memes":
            return self.stop_atr_mult_memes
        return self.stop_atr_mult_alts

    @property
    def risk_multiplier(self) -> float:
        """Backward-compat: returns tier_B as default scalar."""
        return self.risk_multiplier_tier_b


@dataclass
class ProfileDecision:
    """Result returned by TradeProfileRouter.route()."""
    allowed: bool
    profile: TradeProfile
    reason_code: str
    regime_bucket: str
    is_canary: bool = False
    mode: str = "LIVE"          # LIVE | SHADOW


# ---------------------------------------------------------------------------
# Tiered-value helper (supports flat scalar OR nested dict from Redis/YAML)
# ---------------------------------------------------------------------------

def _get_tiered(raw: dict, key: str, sub_key: str, default: float) -> float:
    """
    Supports two formats from Redis/YAML:

    Flat:   {"stop_atr_mult": 1.1}   → returns 1.1 for all sub_keys
    Tiered: {"stop_atr_mult": {"majors": 1.1, "alts": 1.25, "memes": 1.4}}
            {"risk_multiplier": {"tier_A": 0.70, "tier_B": 0.50, "tier_C": 0.25}}
    """
    val = raw.get(key)
    if val is None:
        return default
    if isinstance(val, dict):
        return float(val.get(sub_key, default))
    try:
        return float(val)
    except (TypeError, ValueError):
        return default


# ---------------------------------------------------------------------------
# Built-in profile catalogue
# ---------------------------------------------------------------------------

_BUILTIN_PROFILES: dict[str, TradeProfile] = {
    "trend_breakout_v1": TradeProfile(
        name="trend_breakout_v1",
        regime_bucket="trend",
        allowed_kinds=("breakout", "obi_spike", "extreme"),
        deny_kinds=(),
        min_p_edge=0.56,
        min_confidence=0.58,
        max_expected_slippage_bps=14.0,
        # zone cap
        max_zone_bp_majors=12.0,
        max_zone_bp_alts=16.0,
        max_zone_bp_memes=22.0,
        # stop
        stop_atr_mult_majors=0.85,
        stop_atr_mult_alts=0.95,
        stop_atr_mult_memes=1.10,
        tp_rr="1.2,2.0,3.0",
        tp1_atr_mult=0.9,
        trailing_profile="trend_runner_v1",
        execution_policy="MAKER_FIRST",
        # risk per tier
        risk_multiplier_tier_a=1.10,
        risk_multiplier_tier_b=1.00,
        risk_multiplier_tier_c=0.60,
        min_net_edge_bps=3.0,
        mode="LIVE",
        reason_code="trend_breakout_v1",
    ),
    "range_absorption_v1": TradeProfile(
        name="range_absorption_v1",
        regime_bucket="range",
        allowed_kinds=("absorption",),
        deny_kinds=("breakout",),
        min_p_edge=0.62,
        min_confidence=0.64,
        max_expected_slippage_bps=14.0,
        # zone cap
        max_zone_bp_majors=10.0,
        max_zone_bp_alts=14.0,
        max_zone_bp_memes=18.0,
        # stop
        stop_atr_mult_majors=1.10,
        stop_atr_mult_alts=1.25,
        stop_atr_mult_memes=1.40,
        tp_rr="0.8,1.3,2.0",
        tp1_atr_mult=0.55,
        trailing_profile="range_lock_v1",
        execution_policy="SAFETY_FIRST",
        # risk per tier
        risk_multiplier_tier_a=0.70,
        risk_multiplier_tier_b=0.50,
        risk_multiplier_tier_c=0.25,
        min_net_edge_bps=2.5,
        mode="LIVE",
        reason_code="range_absorption_v1",
    ),
    "thin_defensive_v1": TradeProfile(
        name="thin_defensive_v1",
        regime_bucket="thin",
        allowed_kinds=("extreme",),
        deny_kinds=(),
        min_p_edge=0.70,
        min_confidence=0.72,
        max_expected_slippage_bps=10.0,
        max_zone_bp_majors=8.0,
        max_zone_bp_alts=12.0,
        max_zone_bp_memes=16.0,
        stop_atr_mult_majors=1.30,
        stop_atr_mult_alts=1.40,
        stop_atr_mult_memes=1.60,
        tp_rr="1.0,1.6,2.2",
        tp1_atr_mult=0.8,
        trailing_profile="wide_swing",
        execution_policy="SAFETY_FIRST",
        risk_multiplier_tier_a=0.40,
        risk_multiplier_tier_b=0.30,
        risk_multiplier_tier_c=0.15,
        min_net_edge_bps=5.0,
        mode="SHADOW_BY_DEFAULT",
        reason_code="thin_defensive_v1",
    ),
    "high_vol_breakout_v1": TradeProfile(
        name="high_vol_breakout_v1",
        regime_bucket="mixed",
        allowed_kinds=("breakout", "extreme"),
        deny_kinds=(),
        min_p_edge=0.60,
        min_confidence=0.62,
        max_expected_slippage_bps=18.0,
        max_zone_bp_majors=14.0,
        max_zone_bp_alts=18.0,
        max_zone_bp_memes=24.0,
        stop_atr_mult_majors=1.30,
        stop_atr_mult_alts=1.40,
        stop_atr_mult_memes=1.60,
        tp_rr="1.2,2.2,3.2",
        tp1_atr_mult=1.0,
        trailing_profile="vol_runner_v1",
        execution_policy="SAFETY_FIRST",
        risk_multiplier_tier_a=0.85,
        risk_multiplier_tier_b=0.75,
        risk_multiplier_tier_c=0.40,
        min_net_edge_bps=3.5,
        mode="LIVE",
        reason_code="high_vol_breakout_v1",
    ),
    # Fallback / conservative default
    "default_v1": TradeProfile(
        name="default_v1",
        regime_bucket="mixed",
        allowed_kinds=("breakout", "absorption", "obi_spike", "extreme", "continuation", "reversal"),
        deny_kinds=(),
        min_p_edge=0.55,
        min_confidence=0.57,
        max_expected_slippage_bps=20.0,
        max_zone_bp_majors=15.0,
        max_zone_bp_alts=20.0,
        max_zone_bp_memes=25.0,
        stop_atr_mult_majors=0.95,
        stop_atr_mult_alts=1.05,
        stop_atr_mult_memes=1.20,
        tp_rr="1.0,1.8,2.8",
        tp1_atr_mult=0.8,
        trailing_profile="wide_swing",
        execution_policy="SAFETY_FIRST",
        risk_multiplier_tier_a=1.00,
        risk_multiplier_tier_b=0.80,
        risk_multiplier_tier_c=0.45,
        min_net_edge_bps=2.0,
        mode="LIVE",
        reason_code="default_v1",
    ),
}

# regime_bucket → profile name
_REGIME_PROFILE_MAP: dict[str, str] = {
    "trend": "trend_breakout_v1",
    "range": "range_absorption_v1",
    "thin":  "thin_defensive_v1",
    "mixed": "default_v1",
}

# ---------------------------------------------------------------------------
# Canary share helpers
# ---------------------------------------------------------------------------

def _canary_share(regime_bucket: str) -> float:
    """Returns configured canary share [0.0, 1.0] for a regime bucket."""
    env_map = {
        "trend": "TRADE_PROFILE_CANARY_SHARE_TREND",
        "range": "TRADE_PROFILE_CANARY_SHARE_RANGE",
        "thin":  "TRADE_PROFILE_CANARY_SHARE_THIN",
        "mixed": "TRADE_PROFILE_CANARY_SHARE_MIXED",
    }
    key = env_map.get(regime_bucket, "TRADE_PROFILE_CANARY_SHARE_MIXED")
    try:
        val = float(os.getenv(key, "0.0"))
        return max(0.0, min(1.0, val))
    except (ValueError, TypeError):
        return 0.0


def _stable_bucket_01(symbol: str, regime_bucket: str, salt: str = "tpr-v1") -> float:
    """Deterministic float in [0, 1) for canary assignment."""
    raw = f"{salt}|{symbol}|{regime_bucket}"
    h = hashlib.sha1(raw.encode("utf-8", errors="ignore")).digest()
    v = (h[0] << 8) | h[1]
    return v / 65536.0


# ---------------------------------------------------------------------------
# Router
# ---------------------------------------------------------------------------

class TradeProfileRouter:
    """
    Маршрутизирует входящий сигнал к конкретному TradeProfile.

    Логика выбора:
      1. Проверить symbol-specific override в Redis (если Redis доступен)
      2. Выбрать профиль по (regime_bucket, kind) из встроенного каталога
      3. Применить canary-долю (is_canary)
      4. Вернуть ProfileDecision с allowed / SHADOW / LIVE
    """

    def __init__(self) -> None:
        self._enabled = int(os.getenv("TRADE_PROFILE_ROUTER_ENABLED", "1")) == 1
        self._global_mode = os.getenv("TRADE_PROFILE_MODE", "SHADOW").upper()  # SHADOW | ENFORCE

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def route(
        self,
        *,
        symbol: str,
        regime_bucket: str,
        kind: str,
        overrides: Optional[Mapping[str, Any]] = None,
    ) -> ProfileDecision:
        """
        Выбрать профиль и вернуть решение.

        Args:
            symbol:        Торговый символ (BTCUSDT, 1000PEPEUSDT, …)
            regime_bucket: Результат regime_group() — trend|range|thin|mixed
            kind:          Тип сигнала — breakout|absorption|obi_spike|extreme|…
            overrides:     Дополнительные параметры из Redis (опционально)

        Returns:
            ProfileDecision
        """
        if not self._enabled:
            return ProfileDecision(
                allowed=True,
                profile=_BUILTIN_PROFILES["default_v1"],
                reason_code="router_disabled",
                regime_bucket=regime_bucket,
                mode="LIVE",
            )

        profile = self._select_profile(symbol=symbol, regime_bucket=regime_bucket, kind=kind, overrides=overrides)
        decision = self._apply_gates(symbol=symbol, kind=kind, profile=profile, regime_bucket=regime_bucket)
        return decision

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _select_profile(
        self,
        *,
        symbol: str,
        regime_bucket: str,
        kind: str,
        overrides: Optional[Mapping[str, Any]],
    ) -> TradeProfile:
        """Select the most-specific matching profile."""
        # Symbol-specific override from passed-in Redis snapshot
        if overrides:
            sym_key = f"cfg:trade_profile:{symbol}:{regime_bucket}:{kind}"
            if sym_key in overrides:
                return self._profile_from_override(overrides[sym_key], fallback=regime_bucket)
            default_key = f"cfg:trade_profile:default:{regime_bucket}:{kind}"
            if default_key in overrides:
                return self._profile_from_override(overrides[default_key], fallback=regime_bucket)

        # Regime bucket default
        profile_name = _REGIME_PROFILE_MAP.get(regime_bucket, "default_v1")
        return _BUILTIN_PROFILES.get(profile_name, _BUILTIN_PROFILES["default_v1"])

    @staticmethod
    def _profile_from_override(raw: Any, fallback: str) -> TradeProfile:
        """Build TradeProfile from Redis-loaded dict. Falls back to default on parse errors."""
        if not isinstance(raw, dict):
            return _BUILTIN_PROFILES.get(_REGIME_PROFILE_MAP.get(fallback, "default_v1"), _BUILTIN_PROFILES["default_v1"])
        try:
            return TradeProfile(
                name=str(raw.get("name", "custom_v1")),
                regime_bucket=str(raw.get("regime_bucket", fallback)),
                allowed_kinds=tuple(raw.get("allowed_kinds", [])),
                deny_kinds=tuple(raw.get("deny_kinds", [])),
                min_p_edge=float(raw.get("min_p_edge", 0.55)),
                min_confidence=float(raw.get("min_confidence", 0.57)),
                max_expected_slippage_bps=float(raw.get("max_expected_slippage_bps", 15.0)),
                # zone cap — accept flat value OR per-class dict
                max_zone_bp_majors=float(_get_tiered(raw, "max_zone_bp", "majors", 10.0)),
                max_zone_bp_alts=float(_get_tiered(raw, "max_zone_bp", "alts", 14.0)),
                max_zone_bp_memes=float(_get_tiered(raw, "max_zone_bp", "memes", 18.0)),
                # stop atr mult
                stop_atr_mult_majors=float(_get_tiered(raw, "stop_atr_mult", "majors", 1.0)),
                stop_atr_mult_alts=float(_get_tiered(raw, "stop_atr_mult", "alts", 1.1)),
                stop_atr_mult_memes=float(_get_tiered(raw, "stop_atr_mult", "memes", 1.25)),
                tp_rr=str(raw.get("tp_rr", "1.2,2.0,3.0")),
                tp1_atr_mult=float(raw.get("tp1_atr_mult", 0.9)),
                trailing_profile=str(raw.get("trailing_profile", "wide_swing")),
                execution_policy=str(raw.get("execution_policy", "SAFETY_FIRST")),
                # risk per tier
                risk_multiplier_tier_a=float(_get_tiered(raw, "risk_multiplier", "tier_A", 1.0)),
                risk_multiplier_tier_b=float(_get_tiered(raw, "risk_multiplier", "tier_B", 0.75)),
                risk_multiplier_tier_c=float(_get_tiered(raw, "risk_multiplier", "tier_C", 0.40)),
                min_net_edge_bps=float(raw.get("min_net_edge_bps", 2.0)),
                mode=str(raw.get("mode", "LIVE")),
                reason_code=str(raw.get("reason_code", "redis_override")),
            )
        except Exception as exc:
            logger.warning("TradeProfileRouter: override parse error: %s", exc)
            return _BUILTIN_PROFILES["default_v1"]

    def _apply_gates(
        self,
        *,
        symbol: str,
        kind: str,
        profile: TradeProfile,
        regime_bucket: str,
    ) -> ProfileDecision:
        """Apply kind-allow/deny gates and canary logic."""
        # Deny-list check
        if profile.deny_kinds and kind in profile.deny_kinds:
            return ProfileDecision(
                allowed=False,
                profile=profile,
                reason_code="kind_denied_for_profile",
                regime_bucket=regime_bucket,
                mode=self._effective_mode(profile),
            )

        # Allowed-kinds check (empty = allow all)
        if profile.allowed_kinds and kind not in profile.allowed_kinds:
            return ProfileDecision(
                allowed=False,
                profile=profile,
                reason_code="kind_not_allowed_for_regime",
                regime_bucket=regime_bucket,
                mode=self._effective_mode(profile),
            )

        # Canary assignment
        share = _canary_share(regime_bucket)
        bucket = _stable_bucket_01(symbol, regime_bucket)
        is_canary = bucket < share

        effective_mode = self._effective_mode(profile)
        if effective_mode == "SHADOW" and not is_canary:
            return ProfileDecision(
                allowed=True,
                profile=profile,
                reason_code="shadow_mode",
                regime_bucket=regime_bucket,
                is_canary=False,
                mode="SHADOW",
            )

        return ProfileDecision(
            allowed=True,
            profile=profile,
            reason_code=profile.reason_code or "profile_matched",
            regime_bucket=regime_bucket,
            is_canary=is_canary,
            mode=effective_mode,
        )

    def _effective_mode(self, profile: TradeProfile) -> str:
        """Resolve final mode: profile.mode overrides global only if SHADOW_BY_DEFAULT."""
        if profile.mode == "SHADOW_BY_DEFAULT":
            return "SHADOW"
        if self._global_mode == "SHADOW":
            return "SHADOW"
        return "LIVE"


# ---------------------------------------------------------------------------
# Signal meta builder
# ---------------------------------------------------------------------------

def build_signal_profile_meta(
    decision: ProfileDecision,
    *,
    symbol_tier: str = "B",       # A | B | C
    symbol_class: str = "alts",   # majors | alts | memes
    realized_vol_bps: float = 0.0,
    target_vol_bps: float = 0.0,
) -> dict[str, Any]:
    """
    Строит секцию ``meta`` для signal payload.

    Все поля попадают в enriched_signal["meta"]["trade_profile_*"] и
    downstream в smt_entry_candidate, signal_pipeline, TradeMonitor.

    Поля:
      trade_profile, profile_reason, risk_multiplier,
      execution_policy, trailing_profile,
      stop_atr_mult (class-specific),
      max_zone_bp   (class-specific),
      tp_rr, tp1_atr_mult,
      min_p_edge, min_confidence, min_net_edge_bps,
      profile_mode, is_canary, regime_bucket.

    risk_multiplier выбирается по tier (A/B/C) + vol-scaling.
    stop_atr_mult и max_zone_bp — по symbol_class (majors/alts/memes).
    """
    profile = decision.profile

    # --- risk_multiplier: tier → vol-scaling ---
    rm = profile.risk_multiplier_for_tier(symbol_tier)
    if realized_vol_bps > 0 and target_vol_bps > 0:
        vol_scale = max(0.50, min(1.20, target_vol_bps / realized_vol_bps))
        rm = rm * vol_scale
    rm = round(max(0.10, min(2.0, rm)), 4)

    # --- class-specific params ---
    stop_mult = profile.stop_atr_mult_for_class(symbol_class)
    zone_bp   = profile.max_zone_bp_for_class(symbol_class)

    return {
        "trade_profile":    profile.name,
        "profile_reason":   decision.reason_code,
        "risk_multiplier":  rm,
        "execution_policy": profile.execution_policy,
        "trailing_profile": profile.trailing_profile,
        "stop_atr_mult":    stop_mult,
        "max_zone_bp":      zone_bp,
        "tp_rr":            profile.tp_rr,
        "tp1_atr_mult":     profile.tp1_atr_mult,
        "min_p_edge":       profile.min_p_edge,
        "min_confidence":   profile.min_confidence,
        "min_net_edge_bps": profile.min_net_edge_bps,
        "profile_mode":     decision.mode,
        "is_canary":        decision.is_canary,
        "regime_bucket":    decision.regime_bucket,
    }



