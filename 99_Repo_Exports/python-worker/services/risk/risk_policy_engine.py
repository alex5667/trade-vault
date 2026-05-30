from __future__ import annotations

"""Unified risk policy engine used before a signal is published for execution.

Design goals
------------
1. Deterministic: same inputs must produce the same decision.
2. Explainable: the engine always returns structured reasons and a full snapshot.
3. Safe by default: baseline leverage / risk budget defaults are intentionally conservative.
4. Backward compatible: aliases at the bottom preserve the previous portfolio-risk API.

The engine combines three layers:
- portfolio caps (daily loss, total exposure, symbol exposure, cluster exposure)
- tier policy (A/B/C with maker allowance, leverage, confidence floor, watchdog timeout)
- per-trade sizing (equity, stop distance, volatility, spread and expected slippage)

Decision levels (string constants exported for use by callers):
  ALLOW           — trade is fully approved at computed notional
  ALLOW_TIGHTENED — trade allowed but notional is reduced by risk multiplier
  DENY_SOFT       — confidence/slippage/cost issue; no trade this tick
  DENY_HARD       — exposure cap, equity missing, or concurrent positions exceeded
  FORCE_FLATTEN   — daily drawdown limit breached or kill switch activated

All ENV-configurable limits have safe production-grade defaults.

ENV (global limits):
  RISK_KILL_SWITCH                  (default 0)
  RISK_KILL_SWITCH_FORCE_FLATTEN    (default 1)
  RISK_MAX_DAILY_LOSS_PCT           (default 2.0)
  RISK_MAX_TOTAL_EXPOSURE_RATIO     (default 2.25)
  RISK_MAX_CLUSTER_EXPOSURE_RATIO   (default 1.15)
  RISK_INFRA_DEGRADED_MULTIPLIER    (default 0.50)
  RISK_HIGH_VOL_MULTIPLIER          (default 0.65)
  RISK_MIN_STOP_DISTANCE_BPS        (default 8.0)
  RISK_VOLATILITY_STOP_WEIGHT       (default 0.50)
  RISK_SOFT_SPREAD_BPS_CAP          (default 10.0)
  RISK_HARD_SPREAD_BPS_CAP          (default 25.0)
  RISK_TIER_A_SYMBOLS               (default BTCUSDT,ETHUSDT)
  RISK_TIER_B_SYMBOLS               (default SOLUSDT,XRPUSDT,BNBUSDT,…)

ENV (per-tier limits, X = A | B | C):
  RISK_TIER_X_MAX_LEVERAGE
  RISK_TIER_X_MIN_CONFIDENCE
  RISK_TIER_X_MAKER_ALLOWED
  RISK_TIER_X_SLIPPAGE_BPS_CAP
  RISK_TIER_X_WATCHDOG_TIMEOUT_MS
  RISK_TIER_X_MAX_SYMBOL_EXPOSURE_RATIO
  RISK_TIER_X_MAX_CONCURRENT_POSITIONS
  RISK_TIER_X_BASE_RISK_PCT
"""

import os
import time
from collections.abc import Iterable
from dataclasses import dataclass, field
from typing import Any

try:
    from prometheus_client import REGISTRY, Counter, Gauge, Histogram
except Exception:  # pragma: no cover
    Counter = Gauge = Histogram = None  # type: ignore
    REGISTRY = None  # type: ignore


def _metric(factory, name: str, *args, **kwargs):
    """Idempotent Prometheus metric factory — returns existing metric if already registered."""
    if factory is None:
        return None
    try:
        return factory(name, *args, **kwargs)
    except ValueError:
        return getattr(REGISTRY, "_names_to_collectors", {}).get(name) if REGISTRY is not None else None


TRADE_RISK_LEVEL = _metric(
    Gauge,
    "trade_risk_level",
    "Risk-engine decision level for the next trade (0=ALLOW,1=TIGHTENED,2=DENY_SOFT,3=DENY_HARD,4=FORCE_FLATTEN).",
    ["symbol"],
)
TRADE_RISK_DENY_TOTAL = _metric(
    Counter,
    "trade_risk_deny_total",
    "Number of risk-engine denials for pre-trade publication.",
    ["symbol", "reason", "level"],
)
TRADE_RISK_FORCE_FLATTEN_TOTAL = _metric(
    Counter,
    "trade_risk_force_flatten_total",
    "Number of times the risk engine requested forced flatten instead of new risk.",
    ["symbol", "reason"],
)
TRADE_PORTFOLIO_TOTAL_EXPOSURE_RATIO = _metric(
    Gauge,
    "trade_portfolio_total_exposure_ratio",
    "Current total notional exposure divided by equity, before applying the next trade.",
)
TRADE_PORTFOLIO_SYMBOL_EXPOSURE_RATIO = _metric(
    Gauge,
    "trade_portfolio_symbol_exposure_ratio",
    "Current symbol notional exposure divided by equity, before applying the next trade.",
    ["symbol"],
)
TRADE_PORTFOLIO_CLUSTER_EXPOSURE_RATIO = _metric(
    Gauge,
    "trade_portfolio_cluster_exposure_ratio",
    "Current cluster notional exposure divided by equity, before applying the next trade.",
    ["cluster"],
)
TRADE_RISK_RECOMMENDED_NOTIONAL_USD = _metric(
    Gauge,
    "trade_risk_recommended_notional_usd",
    "Recommended notional after the risk engine applies budget and cap logic.",
    ["symbol"],
)
TRADE_RISK_LEVERAGE_CAP = _metric(
    Gauge,
    "trade_risk_leverage_cap",
    "Leverage cap selected by the risk engine for the next trade.",
    ["symbol", "tier"],
)
TRADE_RISK_MIN_CONFIDENCE_REQUIRED = _metric(
    Gauge,
    "trade_risk_min_confidence_required",
    "Minimum confidence required by the tier policy for the next trade.",
    ["symbol", "tier"],
)
TRADE_RISK_MAKER_ALLOWED = _metric(
    Gauge,
    "trade_risk_maker_allowed",
    "Whether maker policy is allowed by the current tier / regime / infra state.",
    ["symbol", "tier"],
)

# P4.5 metrics: decision volume, latency distribution, clamp rate, confidence denials
TRADE_RISK_DECISION_TOTAL = _metric(
    Counter,
    "trade_risk_decision_total",
    "Total number of risk-engine decisions.",
    ["tier", "level"],
)
TRADE_RISK_DECISION_LATENCY_MS = _metric(
    Histogram,
    "trade_risk_decision_latency_ms",
    "Risk-engine decision latency in milliseconds.",
    ["tier", "level"],
    buckets=(1, 2, 5, 10, 20, 50, 100, 250, 500, 1000),
)
TRADE_RISK_SYMBOL_UNREGISTERED_TOTAL = _metric(
    Counter,
    "trade_risk_symbol_unregistered_total",
    "Counter of symbols that fallback to Tier C without explicit mapping.",
    ["symbol"],
)
TRADE_RISK_CLAMP_TOTAL = _metric(
    Counter,
    "trade_risk_clamp_total",
    "Number of times requested notional was clamped by the risk engine.",
    ["tier"],
)
TRADE_RISK_CONFIDENCE_DENY_TOTAL = _metric(
    Counter,
    "trade_risk_confidence_deny_total",
    "Number of denials caused by confidence floor by tier.",
    ["tier"],
)
# E rollout 2026-05-18 SHADOW counter: counts decisions that would have been
# ALLOWED if RISK_TIER_<T>_MIN_CONFIDENCE_SHADOW were enforced instead of the
# current floor. Active only when the shadow env is set below the live floor.
# Use for "what-if" analysis before promoting a relaxed threshold to canary.
TRADE_RISK_SHADOW_RELAX_WOULD_ALLOW_TOTAL = _metric(
    Counter,
    "trade_risk_shadow_relax_would_allow_total",
    "Decisions denied by current floor that would be allowed at the shadow floor.",
    ["tier"],
)

# Decision level string constants
RISK_ALLOW = "ALLOW"
RISK_ALLOW_TIGHTENED = "ALLOW_TIGHTENED"
RISK_DENY_SOFT = "DENY_SOFT"
RISK_DENY_HARD = "DENY_HARD"
RISK_FORCE_FLATTEN = "FORCE_FLATTEN"


# ── Internal helpers ──────────────────────────────────────────────────────────

def _f(v: Any, default: float = 0.0) -> float:
    """Safe float cast with fallback."""
    try:
        return float(v)
    except Exception:
        return default


def _i_env(name: str, default: int) -> int:
    """Read integer from ENV with safe fallback."""
    try:
        return int(float(os.getenv(name, str(default))))
    except Exception:
        return default


def _f_env(name: str, default: float) -> float:
    """Read float from ENV with safe fallback."""
    try:
        return float(os.getenv(name, str(default)))
    except Exception:
        return default


def _b_env(name: str, default: bool) -> bool:
    """Read bool from ENV with safe fallback."""
    raw = os.getenv(name)
    if raw is None:
        return default
    return str(raw).strip().lower() in {"1", "true", "yes", "on"}


def _tier(v: Any) -> str:
    """Normalise tier string: A/B/C only, default B."""
    s = (v or "").strip().upper()
    return s if s in {"A", "B", "C"} else "B"


def _level_num(level: str) -> int:
    """Convert level string to Prometheus gauge numeric value."""
    return {
        RISK_ALLOW: 0,
        RISK_ALLOW_TIGHTENED: 1,
        RISK_DENY_SOFT: 2,
        RISK_DENY_HARD: 3,
        RISK_FORCE_FLATTEN: 4,
    }.get(level, 3)


# ── Tier policy ───────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class TierPolicy:
    """Per-tier execution and risk constraints. Immutable; built by RiskPolicyLimits.tier_policy()."""
    name: str                       # "A", "B", or "C"
    leverage_cap: float             # Max leverage allowed for this tier
    min_confidence: float           # Minimum signal confidence required (0–1)
    maker_allowed: bool             # Whether maker-first execution is allowed
    slippage_bps_cap: float         # Hard slippage cap (bps) for this tier
    watchdog_timeout_ms: int        # Max allowed order live time (ms)
    max_symbol_exposure_ratio: float  # Max single-symbol notional / equity for this tier
    max_concurrent_positions: int   # Max open positions on this symbol simultaneously
    base_risk_pct: float            # Base risk budget as % of equity per trade

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "leverage_cap": float(self.leverage_cap),
            "min_confidence": float(self.min_confidence),
            "maker_allowed": bool(self.maker_allowed),
            "slippage_bps_cap": float(self.slippage_bps_cap),
            "watchdog_timeout_ms": int(self.watchdog_timeout_ms),
            "max_symbol_exposure_ratio": float(self.max_symbol_exposure_ratio),
            "max_concurrent_positions": int(self.max_concurrent_positions),
            "base_risk_pct": float(self.base_risk_pct),
        }


# ── Global limits ─────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class RiskPolicyLimits:
    """Immutable global risk limit configuration. Build via from_env() or set per-test.

    Intentionally conservative defaults — avoids universal 100x / 5% risk presets.
    """
    max_daily_loss_pct: float = 2.0             # Force-flatten threshold (absolute %)
    max_total_exposure_ratio: float = 2.25       # Max portfolio notional / equity
    max_cluster_exposure_ratio: float = 1.15     # Max cluster notional / equity
    infra_degraded_risk_multiplier: float = 0.50  # Risk budget multiplier when infra is degraded
    high_vol_risk_multiplier: float = 0.65       # Risk budget multiplier in high-vol regime
    kill_switch: bool = False                    # Global kill switch (disables all new trades)
    kill_switch_force_flatten: bool = True       # On kill_switch, force-flatten vs deny-hard
    min_stop_distance_bps: float = 8.0           # Floor for stop-distance used in sizing
    volatility_stop_weight: float = 0.50         # Weight of volatility_bps in effective stop
    soft_spread_bps_cap: float = 10.0            # Soft spread cap → ALLOW_TIGHTENED (tighten size)
    hard_spread_bps_cap: float = 25.0            # Hard spread cap → DENY_HARD
    target_volatility_bps: float = 45.0         # Vol-target anchor for sizing tighten/expand
    max_net_beta_long_ratio: float = 1.50       # Net long beta exposure / equity
    max_net_beta_short_ratio: float = 1.50      # Net short beta exposure / equity
    leader_override_enable: bool = True         # Tighten/deny alts when BTC/ETH leader is stressed
    leader_override_drawdown_bps: float = 250.0 # Trigger threshold for leader stress
    leader_override_alt_tighten_multiplier: float = 0.50
    news_blackout_force_deny: bool = True
    # Symbol lists defining tiers (uppercase); anything else → Tier C
    tier_a_symbols: tuple = ("BTCUSDT", "ETHUSDT")
    tier_b_symbols: tuple = (
        "SOLUSDT", "XRPUSDT", "BNBUSDT", "ADAUSDT", "DOGEUSDT",
        "LTCUSDT", "BCHUSDT"
    )

    @classmethod
    def from_env(cls) -> RiskPolicyLimits:
        """Construct limits from environment variables with safe fallbacks."""
        def _f_l(name: str, default: float) -> float:
            try:
                return float(os.getenv(name, str(default)))
            except Exception:
                return default

        def _b_l(name: str, default: bool) -> bool:
            v = os.getenv(name)
            if v is None:
                return default
            return str(v).strip().lower() in {"1", "true", "yes", "on"}

        def _csv(name: str, default: Iterable[str]) -> tuple:
            raw = os.getenv(name)
            if raw is None:
                return tuple(default)
            values = tuple(str(v).strip().upper() for v in raw.split(",") if str(v).strip())
            return values or tuple(default)

        return cls(
            max_daily_loss_pct=_f_l("RISK_MAX_DAILY_LOSS_PCT", cls.max_daily_loss_pct),
            max_total_exposure_ratio=_f_l("RISK_MAX_TOTAL_EXPOSURE_RATIO", cls.max_total_exposure_ratio),
            max_cluster_exposure_ratio=_f_l("RISK_MAX_CLUSTER_EXPOSURE_RATIO", cls.max_cluster_exposure_ratio),
            infra_degraded_risk_multiplier=_f_l("RISK_INFRA_DEGRADED_MULTIPLIER", cls.infra_degraded_risk_multiplier),
            high_vol_risk_multiplier=_f_l("RISK_HIGH_VOL_MULTIPLIER", cls.high_vol_risk_multiplier),
            kill_switch=_b_l("RISK_KILL_SWITCH", cls.kill_switch),
            kill_switch_force_flatten=_b_l("RISK_KILL_SWITCH_FORCE_FLATTEN", cls.kill_switch_force_flatten),
            min_stop_distance_bps=_f_l("RISK_MIN_STOP_DISTANCE_BPS", cls.min_stop_distance_bps),
            volatility_stop_weight=_f_l("RISK_VOLATILITY_STOP_WEIGHT", cls.volatility_stop_weight),
            soft_spread_bps_cap=_f_l("RISK_SOFT_SPREAD_BPS_CAP", cls.soft_spread_bps_cap),
            hard_spread_bps_cap=_f_l("RISK_HARD_SPREAD_BPS_CAP", cls.hard_spread_bps_cap),
            target_volatility_bps=_f_l("RISK_TARGET_VOLATILITY_BPS", cls.target_volatility_bps),
            max_net_beta_long_ratio=_f_l("RISK_MAX_NET_BETA_LONG_RATIO", cls.max_net_beta_long_ratio),
            max_net_beta_short_ratio=_f_l("RISK_MAX_NET_BETA_SHORT_RATIO", cls.max_net_beta_short_ratio),
            leader_override_enable=_b_l("RISK_LEADER_OVERRIDE_ENABLE", cls.leader_override_enable),
            leader_override_drawdown_bps=_f_l("RISK_LEADER_OVERRIDE_DRAWDOWN_BPS", cls.leader_override_drawdown_bps),
            leader_override_alt_tighten_multiplier=_f_l("RISK_LEADER_OVERRIDE_ALT_TIGHTEN_MULTIPLIER", cls.leader_override_alt_tighten_multiplier),
            news_blackout_force_deny=_b_l("RISK_NEWS_BLACKOUT_FORCE_DENY", cls.news_blackout_force_deny),
            tier_a_symbols=_csv("RISK_TIER_A_SYMBOLS", cls.tier_a_symbols),
            tier_b_symbols=_csv("RISK_TIER_B_SYMBOLS", cls.tier_b_symbols),
        )

    def tier_policy(self, tier: str) -> TierPolicy:
        """Build TierPolicy for the given tier name (A/B/C). All values are ENV-overridable."""
        t = _tier(tier)
        # Safer defaults than legacy universal 100x / 5% risk settings.
        if t == "A":
            return TierPolicy(
                name="A",
                leverage_cap=_f_env("RISK_TIER_A_MAX_LEVERAGE", 10.0),
                min_confidence=_f_env("RISK_TIER_A_MIN_CONFIDENCE", 0.55),
                maker_allowed=_b_env("RISK_TIER_A_MAKER_ALLOWED", True),
                slippage_bps_cap=_f_env("RISK_TIER_A_SLIPPAGE_BPS_CAP", 12.0),
                watchdog_timeout_ms=_i_env("RISK_TIER_A_WATCHDOG_TIMEOUT_MS", 4000),
                max_symbol_exposure_ratio=_f_env("RISK_TIER_A_MAX_SYMBOL_EXPOSURE_RATIO", 0.70),
                max_concurrent_positions=_i_env("RISK_TIER_A_MAX_CONCURRENT_POSITIONS", 3),
                base_risk_pct=_f_env("RISK_TIER_A_BASE_RISK_PCT", 0.75),
            )
        if t == "B":
            return TierPolicy(
                name="B",
                leverage_cap=_f_env("RISK_TIER_B_MAX_LEVERAGE", 5.0),
                min_confidence=_f_env("RISK_TIER_B_MIN_CONFIDENCE", 0.60),
                maker_allowed=_b_env("RISK_TIER_B_MAKER_ALLOWED", True),
                slippage_bps_cap=_f_env("RISK_TIER_B_SLIPPAGE_BPS_CAP", 18.0),
                watchdog_timeout_ms=_i_env("RISK_TIER_B_WATCHDOG_TIMEOUT_MS", 3500),
                max_symbol_exposure_ratio=_f_env("RISK_TIER_B_MAX_SYMBOL_EXPOSURE_RATIO", 0.45),
                max_concurrent_positions=_i_env("RISK_TIER_B_MAX_CONCURRENT_POSITIONS", 2),
                base_risk_pct=_f_env("RISK_TIER_B_BASE_RISK_PCT", 0.45),
            )
        # Tier C — memes / thin books / noisy alts — most conservative defaults
        return TierPolicy(
            name="C",
            leverage_cap=_f_env("RISK_TIER_C_MAX_LEVERAGE", 3.0),
            min_confidence=_f_env("RISK_TIER_C_MIN_CONFIDENCE", 0.68),
            maker_allowed=_b_env("RISK_TIER_C_MAKER_ALLOWED", False),
            slippage_bps_cap=_f_env("RISK_TIER_C_SLIPPAGE_BPS_CAP", 10.0),
            watchdog_timeout_ms=_i_env("RISK_TIER_C_WATCHDOG_TIMEOUT_MS", 2500),
            max_symbol_exposure_ratio=_f_env("RISK_TIER_C_MAX_SYMBOL_EXPOSURE_RATIO", 0.20),
            max_concurrent_positions=_i_env("RISK_TIER_C_MAX_CONCURRENT_POSITIONS", 1),
            base_risk_pct=_f_env("RISK_TIER_C_BASE_RISK_PCT", 0.25),
        )


# ── Data classes ──────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class RiskPosition:
    """A single open position in the portfolio."""
    symbol: str
    notional_usd: float
    side: str = "LONG"
    cluster: str = "default"
    tier: str = "B"
    beta: float = 1.0


@dataclass(frozen=True)
class RiskPolicyInput:
    """All inputs needed for risk evaluation of a new trade signal."""
    symbol: str
    cluster: str
    tier: str
    requested_notional_usd: float
    current_positions: list[RiskPosition] = field(default_factory=list)
    equity_usd: float = 0.0                # Account equity (used for ratio computation)
    daily_pnl_pct: float = 0.0             # Today's realized PnL as % of equity (< 0 = loss)
    stop_distance_bps: float = 0.0         # Planned stop distance in bps (for per-trade sizing)
    volatility_bps: float = 0.0            # Realized volatility proxy (e.g. ATR in bps)
    spread_bps: float = 0.0                # Estimated market spread at signal time
    expected_slippage_bps: float = 0.0     # Expected slippage from historical calibration
    expected_edge_bps: float = 0.0         # Gross expected edge before costs
    fee_bps: float = 0.0                   # Round-trip or entry+exit fee estimate
    confidence: float = 0.0               # Signal confidence score (0–1); 0 = not provided
    maker_policy_requested: bool = False   # Caller requests maker-first execution policy
    infra_degraded: bool = False           # True if DQ hard-veto or Redis lag is elevated
    high_vol: bool = False                 # True if current regime is high-volatility
    kill_switch: bool = False              # Per-signal kill switch (e.g. from upstream)
    net_beta: float = 1.0                  # Beta of the new trade versus leader basket
    leader_symbol: str = "BTCUSDT"        # Leader market used for alt overrides
    leader_drawdown_bps: float = 0.0       # Recent leader drawdown magnitude in bps
    news_blackout: bool = False            # External news/high-vol blackout flag
    shadow_only: bool = False              # Publish shadow-only, do not trade live


@dataclass(frozen=True)
class RiskPolicyDecision:
    """Result of evaluate_risk_policy(). Immutable; serializable via to_dict()."""
    level: str                    # ALLOW | ALLOW_TIGHTENED | DENY_SOFT | DENY_HARD | FORCE_FLATTEN
    allow_trade_publish: bool
    adjusted_notional_usd: float  # Effective notional after risk budget + caps (0 if denied)
    leverage_cap: float           # Max leverage for this symbol tier
    risk_multiplier: float        # 1.0 = full budget, < 1.0 = tightened
    reasons: list[str]
    snapshot: dict[str, Any]
    tier_policy: TierPolicy
    maker_policy_allowed: bool    # Whether maker execution is allowed this tick
    min_confidence_required: float
    watchdog_timeout_ms: int
    effective_execution_policy: str  # "MAKER_FIRST" | "SAFETY_FIRST"

    def to_dict(self) -> dict[str, Any]:
        return {
            "level": self.level,
            "allow_trade_publish": bool(self.allow_trade_publish),
            "adjusted_notional_usd": float(self.adjusted_notional_usd),
            "leverage_cap": float(self.leverage_cap),
            "risk_multiplier": float(self.risk_multiplier),
            "reasons": list(self.reasons),
            "snapshot": dict(self.snapshot),
            "tier_policy": self.tier_policy.to_dict(),
            "maker_policy_allowed": bool(self.maker_policy_allowed),
            "min_confidence_required": float(self.min_confidence_required),
            "watchdog_timeout_ms": int(self.watchdog_timeout_ms),
            "effective_execution_policy": str(self.effective_execution_policy),
        }


# ── Tier inference ────────────────────────────────────────────────────────────

def infer_symbol_tier(symbol: Any, limits: RiskPolicyLimits | None = None) -> str:
    """Infer the risk tier for a symbol.

    Tier A: BTC / ETH (deep liquid markets)
    Tier B: major alts (SOLUSDT, XRPUSDT, …)
    Tier C: memes / thin books / noisy alts → defaults to C for anything unknown

    Uses RISK_TIER_A_SYMBOLS / RISK_TIER_B_SYMBOLS ENV vars via limits.
    """
    lim = limits or RiskPolicyLimits.from_env()
    sym = (symbol or "").strip().upper()
    if sym in set(lim.tier_a_symbols):
        return "A"
    if sym in set(lim.tier_b_symbols):
        return "B"
    # Known meme / leveraged-token patterns → explicit Tier C
    if sym.endswith("USDT") and (
        sym.startswith("1000") or
        "PEPE" in sym or "BONK" in sym or "WIF" in sym or "FLOKI" in sym
    ):
        return "C"
    # Default: treat unknowns as Tier C (conservative)
    import logging
    logging.getLogger("trade.risk").warning(f"[Risk] Symbol {sym} not in any tier, falling back to C")
    if TRADE_RISK_SYMBOL_UNREGISTERED_TOTAL:
        TRADE_RISK_SYMBOL_UNREGISTERED_TOTAL.labels(symbol=sym).inc()
    return "C"


def _count_positions(positions: Iterable[RiskPosition], *, symbol: str) -> int:
    """Count open positions (non-zero notional) for a given symbol."""
    return sum(1 for p in positions if str(p.symbol).upper() == symbol and abs(_f(p.notional_usd)) > 0)


# ── Core engine ───────────────────────────────────────────────────────────────

def evaluate_risk_policy(inp: RiskPolicyInput, limits: RiskPolicyLimits | None = None) -> RiskPolicyDecision:
    """Evaluate portfolio risk and return a publish decision.

    P4.5: records decision_latency_ms and clamp_ratio in the returned snapshot.

    Evaluation order (highest priority first):
      1. Kill switch → FORCE_FLATTEN or DENY_HARD
      2. Daily loss limit → FORCE_FLATTEN
      3. Concurrent position cap (tier) → DENY_SOFT
      4. Missing equity → DENY_HARD
      5. Total portfolio exposure cap → DENY_HARD
      6. Symbol exposure cap (tier) → DENY_HARD
      7. Cluster exposure cap → DENY_HARD
      8. Confidence below tier floor → DENY_SOFT
      9. Spread hard cap → DENY_HARD
     10. Slippage hard cap → DENY_HARD
     11. Tightening multipliers (infra_degraded, high_vol, soft caps) → ALLOW_TIGHTENED
     12. Per-trade risk sizing: risk budget / effective stop fraction
     13. Cap sizing against leverage, symbol, cluster, total residual caps

    Key: if requested_notional is too large, the engine SHRINKS if first rather
    than issuing a hard-deny. Only if the risk budget results in 0 residual capacity
    does the engine return DENY_SOFT (no_residual_risk_capacity).

    Pure function — no side effects except Prometheus metric updates.
    """
    started = time.perf_counter()  # P4.5: decision latency measurement
    lim = limits or RiskPolicyLimits.from_env()
    symbol = str(inp.symbol or "UNKNOWN").upper()
    # Auto-infer tier if not explicitly set or invalid
    tier = _tier(inp.tier) if str(inp.tier or "").strip() else infer_symbol_tier(symbol, lim)
    cluster = str(inp.cluster or symbol)
    tier_policy = lim.tier_policy(tier)
    equity = max(0.0, _f(inp.equity_usd))
    req = max(0.0, _f(inp.requested_notional_usd))
    positions = list(inp.current_positions or [])

    # ── Current portfolio exposure aggregates ─────────────────────────────────
    total_exposure = sum(abs(_f(p.notional_usd)) for p in positions)
    symbol_exposure = sum(abs(_f(p.notional_usd)) for p in positions if str(p.symbol).upper() == symbol)
    cluster_exposure = sum(abs(_f(p.notional_usd)) for p in positions if str(p.cluster or "") == cluster)
    symbol_position_count = _count_positions(positions, symbol=symbol)
    net_long_beta_notional = sum(max(0.0, _f(p.notional_usd)) * abs(_f(getattr(p, 'beta', 1.0))) for p in positions if str(p.side).upper() == 'LONG')
    net_short_beta_notional = sum(max(0.0, _f(p.notional_usd)) * abs(_f(getattr(p, 'beta', 1.0))) for p in positions if str(p.side).upper() == 'SHORT')

    # Pre-trade exposure ratios (without the new trade)
    total_ratio = (total_exposure / equity) if equity > 0 else 0.0
    symbol_ratio = (symbol_exposure / equity) if equity > 0 else 0.0
    cluster_ratio = (cluster_exposure / equity) if equity > 0 else 0.0

    # Update pre-trade Prometheus gauges
    if TRADE_PORTFOLIO_TOTAL_EXPOSURE_RATIO:
        TRADE_PORTFOLIO_TOTAL_EXPOSURE_RATIO.set(total_ratio)
    if TRADE_PORTFOLIO_SYMBOL_EXPOSURE_RATIO:
        TRADE_PORTFOLIO_SYMBOL_EXPOSURE_RATIO.labels(symbol=symbol).set(symbol_ratio)
    if TRADE_PORTFOLIO_CLUSTER_EXPOSURE_RATIO:
        TRADE_PORTFOLIO_CLUSTER_EXPOSURE_RATIO.labels(cluster=cluster).set(cluster_ratio)

    level = RISK_ALLOW
    allow = True
    risk_multiplier = 1.0
    reasons: list[str] = []

    # W2: entry_slippage_cap autocal override (AUTOCAL_ENTRY_SLIP_CAP_READ_ENABLED=0 default)
    _effective_slippage_cap = tier_policy.slippage_bps_cap
    try:
        from services.entry_slippage_cap_runtime_overrides import get_cap_bps as _get_slip_cap
        _cal_slip = _get_slip_cap(symbol)
        if _cal_slip and _cal_slip > 0:
            _effective_slippage_cap = _cal_slip
    except Exception:
        pass

    # W2: daily_dd_tier autocal override (AUTOCAL_DAILY_DD_TIER_READ_ENABLED=0 default)
    _effective_daily_loss_pct = abs(lim.max_daily_loss_pct)
    try:
        from services.daily_dd_tier_runtime_overrides import get_reader as _get_dd_reader
        _dd_rdr = _get_dd_reader()
        if _dd_rdr is not None:
            _cal_hard = _dd_rdr.get_hard_limit(tier, "*")
            if _cal_hard and _cal_hard > 0:
                _effective_daily_loss_pct = _cal_hard
    except Exception:
        pass

    # Maker policy: tier must allow it AND infra must not be degraded
    maker_allowed = bool(tier_policy.maker_allowed) and not bool(inp.infra_degraded)
    effective_execution_policy = "MAKER_FIRST" if (maker_allowed and bool(inp.maker_policy_requested)) else "SAFETY_FIRST"

    # ── Priority 1: Kill switch ───────────────────────────────────────────────
    if lim.kill_switch or bool(inp.kill_switch):
        level = RISK_FORCE_FLATTEN if lim.kill_switch_force_flatten else RISK_DENY_HARD
        allow = False
        reasons.append("kill_switch")

    # ── Priority 2: Daily loss limit (force-flatten) ──────────────────────────
    elif _f(inp.daily_pnl_pct) <= -_effective_daily_loss_pct:
        level = RISK_FORCE_FLATTEN
        allow = False
        reasons.append("daily_loss_limit")

    # ── Priority 2b: News blackout / external hard veto ──────────────────────
    if allow and bool(inp.news_blackout) and bool(lim.news_blackout_force_deny):
        level = RISK_DENY_HARD
        allow = False
        reasons.append("news_blackout")

    # ── Priority 3: Tier concurrent position cap ──────────────────────────────
    if allow and symbol_position_count >= tier_policy.max_concurrent_positions:
        level = RISK_DENY_SOFT
        allow = False
        reasons.append("tier_max_concurrent_positions")

    # ── Priority 4: Missing equity ────────────────────────────────────────────
    if allow and equity <= 0:
        level = RISK_DENY_HARD
        allow = False
        reasons.append("missing_equity")

    # ── Priority 5–7: Exposure caps ───────────────────────────────────────────
    # Check current (pre-trade) ratios, not projected.
    # The sizing step below handles capping gracefully without hard-denying.
    confidence = _f(inp.confidence)
    if allow and total_ratio > lim.max_total_exposure_ratio:
        level = RISK_DENY_HARD
        allow = False
        reasons.append("total_exposure_cap")
    if allow and symbol_ratio > tier_policy.max_symbol_exposure_ratio:
        level = RISK_DENY_HARD
        allow = False
        reasons.append("symbol_exposure_cap")
    if allow and cluster_ratio > lim.max_cluster_exposure_ratio:
        level = RISK_DENY_HARD
        allow = False
        reasons.append("cluster_exposure_cap")
    if allow and equity > 0:
        new_trade_beta = abs(_f(inp.net_beta) or 1.0) * max(0.0, req)
        if str(getattr(inp, 'side', 'LONG')).upper() == 'LONG':
            projected_long_ratio = (net_long_beta_notional + new_trade_beta) / equity
            if projected_long_ratio > lim.max_net_beta_long_ratio:
                level = RISK_DENY_SOFT
                allow = False
                reasons.append("net_long_beta_cap")
        else:
            projected_short_ratio = (net_short_beta_notional + new_trade_beta) / equity
            if projected_short_ratio > lim.max_net_beta_short_ratio:
                level = RISK_DENY_SOFT
                allow = False
                reasons.append("net_short_beta_cap")

    # ── Priority 8: Confidence below tier floor ───────────────────────────────
    # shadow_only=True (virtual/paper signals) bypass the confidence floor —
    # they are analytics-only and must not be silently dropped before calibration data is captured.
    if allow and confidence and confidence < float(tier_policy.min_confidence) and not inp.shadow_only:
        level = RISK_DENY_SOFT
        allow = False
        reasons.append("confidence_below_tier_floor")
        if TRADE_RISK_CONFIDENCE_DENY_TOTAL:  # P4.5: count confidence-floor denials by tier
            TRADE_RISK_CONFIDENCE_DENY_TOTAL.labels(tier=tier).inc()
        # E shadow: would this have passed under a relaxed floor?
        shadow_floor = _f_env(f"RISK_TIER_{tier}_MIN_CONFIDENCE_SHADOW", float(tier_policy.min_confidence))
        if (
            shadow_floor < float(tier_policy.min_confidence)
            and confidence >= shadow_floor
            and TRADE_RISK_SHADOW_RELAX_WOULD_ALLOW_TOTAL
        ):
            TRADE_RISK_SHADOW_RELAX_WOULD_ALLOW_TOTAL.labels(tier=tier).inc()

    # ── Priority 9–10: Market cost hard caps ──────────────────────────────────
    spread_bps = _f(inp.spread_bps)
    expected_slippage_bps = _f(inp.expected_slippage_bps)
    if allow and spread_bps > lim.hard_spread_bps_cap:
        level = RISK_DENY_HARD
        allow = False
        reasons.append("spread_hard_cap")
    if allow and expected_slippage_bps > _effective_slippage_cap:
        level = RISK_DENY_HARD
        allow = False
        reasons.append("slippage_hard_cap")
    gross_edge_bps = _f(inp.expected_edge_bps)
    fee_bps = _f(inp.fee_bps)
    cost_bps = spread_bps + expected_slippage_bps + fee_bps
    net_edge_bps = gross_edge_bps - cost_bps
    if allow and gross_edge_bps > 0 and net_edge_bps <= 0:
        level = RISK_DENY_SOFT
        allow = False
        reasons.append("edge_negative_after_cost")

    # ── Priority 11: Tightening multipliers (stack multiplicatively) ──────────
    if allow:
        if inp.infra_degraded:
            risk_multiplier *= max(0.10, min(1.0, lim.infra_degraded_risk_multiplier))
            reasons.append("infra_degraded_tightened")
        if inp.high_vol:
            risk_multiplier *= max(0.10, min(1.0, lim.high_vol_risk_multiplier))
            reasons.append("high_vol_tightened")
        vol_bps = max(0.0, _f(inp.volatility_bps))
        if vol_bps > 0 and lim.target_volatility_bps > 0:
            vol_target_mult = max(0.25, min(1.25, lim.target_volatility_bps / vol_bps))
            risk_multiplier *= vol_target_mult
            if vol_target_mult < 0.999:
                reasons.append("volatility_targeting_tightened")
        if lim.leader_override_enable and tier in {'B', 'C'} and abs(_f(inp.leader_drawdown_bps)) >= abs(lim.leader_override_drawdown_bps):
            risk_multiplier *= max(0.10, min(1.0, lim.leader_override_alt_tighten_multiplier))
            reasons.append("leader_override_tightened")
        if spread_bps > lim.soft_spread_bps_cap:
            risk_multiplier *= 0.75
            reasons.append("spread_soft_cap")
        if expected_slippage_bps > (_effective_slippage_cap * 0.66):
            risk_multiplier *= 0.75
            reasons.append("slippage_soft_cap")
        if not maker_allowed and bool(inp.maker_policy_requested):
            # Record that maker was requested but is not allowed — no size penalty
            reasons.append("maker_policy_disabled_for_tier_or_infra")
            effective_execution_policy = "SAFETY_FIRST"
        if risk_multiplier < 0.999 and level == RISK_ALLOW:
            level = RISK_ALLOW_TIGHTENED

    # ── Priority 12–13: Per-trade risk sizing ─────────────────────────────────
    # risk_budget_usd = equity * (base_risk_pct / 100) * risk_multiplier
    # effective_stop_frac = max(stop_bps, vol_component_bps) / 10000
    # model_notional = risk_budget / effective_stop_frac
    # Final notional = min(requested, model_notional, leverage_cap, residual caps)
    #
    # Critical: this means oversized requested_notional is SHRUNK, not denied.
    stop_distance_bps = max(_f(inp.stop_distance_bps), lim.min_stop_distance_bps)
    volatility_component_bps = max(0.0, _f(inp.volatility_bps) * max(0.0, lim.volatility_stop_weight))
    effective_stop_bps = max(stop_distance_bps, volatility_component_bps, lim.min_stop_distance_bps)
    effective_stop_frac = max(effective_stop_bps / 10000.0, 0.0001)
    base_risk_usd = equity * (tier_policy.base_risk_pct / 100.0)
    budget_risk_usd = base_risk_usd * risk_multiplier if allow else 0.0
    model_notional_usd = budget_risk_usd / effective_stop_frac if effective_stop_frac > 0 else 0.0

    # Residual capacity from caps
    leverage_notional_cap = equity * tier_policy.leverage_cap
    residual_symbol_cap = max(0.0, (tier_policy.max_symbol_exposure_ratio * equity) - symbol_exposure)
    residual_total_cap = max(0.0, (lim.max_total_exposure_ratio * equity) - total_exposure)
    residual_cluster_cap = max(0.0, (lim.max_cluster_exposure_ratio * equity) - cluster_exposure)
    max_position_notional_usd = max(
        0.0, min(leverage_notional_cap, residual_symbol_cap, residual_total_cap, residual_cluster_cap)
    )

    # Final notional: use requested if provided, otherwise use model sizing
    requested_target_notional = req if req > 0 else model_notional_usd
    adjusted = min(requested_target_notional, model_notional_usd, max_position_notional_usd) if allow else 0.0

    if allow and adjusted <= 0.0:
        # No residual capacity — deny soft (not hard, as this is transient)
        level = RISK_DENY_SOFT
        allow = False
        reasons.append("no_residual_risk_capacity")
    elif allow and requested_target_notional > adjusted:
        # Notional was clamped (shrunk) by risk engine — not denied, but flagged
        reasons.append("notional_clamped_by_risk_engine")
        if TRADE_RISK_CLAMP_TOTAL:  # P4.5: count clamp events by tier
            TRADE_RISK_CLAMP_TOTAL.labels(tier=tier).inc()
        if level == RISK_ALLOW:
            level = RISK_ALLOW_TIGHTENED

    snapshot = {
        "net_long_beta_notional": float(net_long_beta_notional),
        "net_short_beta_notional": float(net_short_beta_notional),
        "symbol": symbol,
        "cluster": cluster,
        "tier": tier,
        "equity_usd": float(equity),
        "daily_pnl_pct": float(_f(inp.daily_pnl_pct)),
        "requested_notional_usd": float(req),
        "adjusted_notional_usd": float(adjusted),
        "base_risk_usd": float(base_risk_usd),
        "budget_risk_usd": float(budget_risk_usd),
        "effective_stop_bps": float(effective_stop_bps),
        "spread_bps": float(spread_bps),
        "expected_slippage_bps": float(expected_slippage_bps),
        "expected_edge_bps": float(gross_edge_bps),
        "fee_bps": float(fee_bps),
        "cost_bps": float(cost_bps),
        "net_edge_bps": float(net_edge_bps),
        "confidence": float(confidence),
        "total_exposure_ratio": float(total_ratio),
        "symbol_exposure_ratio": float(symbol_ratio),
        "cluster_exposure_ratio": float(cluster_ratio),
        "max_position_notional_usd": float(max_position_notional_usd),
        "symbol_position_count": int(symbol_position_count),
        "maker_policy_requested": bool(inp.maker_policy_requested),
        "maker_policy_allowed": bool(maker_allowed),
        "infra_degraded": bool(inp.infra_degraded),
        "high_vol": bool(inp.high_vol),
        "leader_symbol": str(inp.leader_symbol or ''),
        "leader_drawdown_bps": float(_f(inp.leader_drawdown_bps)),
        "news_blackout": bool(inp.news_blackout),
        "kill_switch": bool(lim.kill_switch or inp.kill_switch),
        # P4.5: clamp_ratio — 1.0 means no clamping, <1.0 means notional was shrunk
        "clamp_ratio": float((adjusted / requested_target_notional) if requested_target_notional > 0 else 1.0),
    }

    # Update Prometheus metrics
    if TRADE_RISK_LEVEL:
        TRADE_RISK_LEVEL.labels(symbol=symbol).set(_level_num(level))
    if TRADE_RISK_RECOMMENDED_NOTIONAL_USD:
        TRADE_RISK_RECOMMENDED_NOTIONAL_USD.labels(symbol=symbol).set(float(adjusted))
    if TRADE_RISK_LEVERAGE_CAP:
        TRADE_RISK_LEVERAGE_CAP.labels(symbol=symbol, tier=tier).set(float(tier_policy.leverage_cap))
    if TRADE_RISK_MIN_CONFIDENCE_REQUIRED:
        TRADE_RISK_MIN_CONFIDENCE_REQUIRED.labels(symbol=symbol, tier=tier).set(float(tier_policy.min_confidence))
    if TRADE_RISK_MAKER_ALLOWED:
        TRADE_RISK_MAKER_ALLOWED.labels(symbol=symbol, tier=tier).set(1.0 if maker_allowed else 0.0)
    # P4.5: decision volume counter and latency histogram
    if TRADE_RISK_DECISION_TOTAL:
        TRADE_RISK_DECISION_TOTAL.labels(tier=tier, level=level).inc()
    latency_ms = max(0.0, (time.perf_counter() - started) * 1000.0)
    snapshot['decision_latency_ms'] = float(latency_ms)
    if TRADE_RISK_DECISION_LATENCY_MS:
        TRADE_RISK_DECISION_LATENCY_MS.labels(tier=tier, level=level).observe(float(latency_ms))
    if level in {RISK_DENY_SOFT, RISK_DENY_HARD} and TRADE_RISK_DENY_TOTAL:
        for reason in sorted(set(reasons)):
            TRADE_RISK_DENY_TOTAL.labels(symbol=symbol, reason=reason, level=level).inc()
    if level == RISK_FORCE_FLATTEN and TRADE_RISK_FORCE_FLATTEN_TOTAL:
        for reason in sorted(set(reasons)):
            TRADE_RISK_FORCE_FLATTEN_TOTAL.labels(symbol=symbol, reason=reason).inc()

    return RiskPolicyDecision(
        level=level,
        allow_trade_publish=bool(allow),
        adjusted_notional_usd=float(adjusted),
        leverage_cap=float(tier_policy.leverage_cap),
        risk_multiplier=float(risk_multiplier if allow else 0.0),
        reasons=sorted(set(reasons)),
        snapshot=snapshot,
        tier_policy=tier_policy,
        maker_policy_allowed=bool(maker_allowed),
        min_confidence_required=float(tier_policy.min_confidence),
        watchdog_timeout_ms=int(tier_policy.watchdog_timeout_ms),
        effective_execution_policy=effective_execution_policy,
    )


# ── Backward-compatibility aliases for the previous portfolio-risk module ────
# Old code importing from portfolio_risk_engine (or risk_policy_engine) via the
# wrapper continues to work unchanged.
PortfolioPosition = RiskPosition
PortfolioRiskInput = RiskPolicyInput
PortfolioRiskLimits = RiskPolicyLimits
PortfolioRiskDecision = RiskPolicyDecision


def evaluate_portfolio_risk(inp: RiskPolicyInput, limits: RiskPolicyLimits | None = None) -> RiskPolicyDecision:
    """Backward-compatible alias for evaluate_risk_policy()."""
    return evaluate_risk_policy(inp, limits)
