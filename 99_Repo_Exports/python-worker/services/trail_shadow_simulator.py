"""
Trail Shadow Simulator — virtual P&L A/B test for calibrated vs actual trailing params.

For each closed trade in the lookback window, simulates where the calibrated
trailing stop *would have* exited and computes the virtual P&L.
Compares virtual vs actual to produce a per-symbol recommendation.

Key pattern: trail:shadow:{symbol}:{regime}
Fail-open: never raises on Redis/data errors.
"""
from __future__ import annotations
from utils.time_utils import get_ny_time_millis

import math
import os
import time
from dataclasses import dataclass, asdict
from typing import Any, Dict, List, Optional, Sequence

from common.log import setup_logger

logger = setup_logger("TrailShadowSimulator")

EPS = 1e-9

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _sf(v: Any, default: float = 0.0) -> float:
    try:
        if v is None:
            return float(default)
        return float(str(v).strip())
    except Exception:
        return float(default)


def _env_bool(name: str, default: bool) -> bool:
    v = (os.getenv(name, "1" if default else "0") or "").strip().lower()
    return v in {"1", "true", "yes", "on"}


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class ShadowSimConfig:
    enabled: bool
    atr_fallback_bps: float
    key_prefix: str
    calib_prefix: str
    ttl_sec: int

    @classmethod
    def from_env(cls) -> "ShadowSimConfig":
        return cls(
            enabled=_env_bool("TRAIL_SHADOW_ENABLED", True)
            atr_fallback_bps=float(os.getenv("TRAIL_SHADOW_ATR_FALLBACK_BPS", "50") or 50)
            key_prefix=os.getenv("TRAIL_SHADOW_KEY_PREFIX", "trail:shadow") or "trail:shadow"
            calib_prefix=os.getenv("TRAIL_CALIB_KEY_PREFIX", "trail:calib") or "trail:calib"
            ttl_sec=int(os.getenv("TRAIL_CALIB_TTL_SEC", str(48 * 3600)) or 48 * 3600)
        )


# ---------------------------------------------------------------------------
# Default ATR (same as calibrator)
# ---------------------------------------------------------------------------

DEFAULT_ATR_BPS: Dict[str, float] = {
    "BTCUSDT": 30.0
    "ETHUSDT": 45.0
    "SOLUSDT": 80.0
    "XRPUSDT": 60.0
    "BNBUSDT": 40.0
    "DOGEUSDT": 100.0
    "1000PEPEUSDT": 120.0
    "1000BONKUSDT": 110.0
    "1000SHIBUSDT": 90.0
    "1000FLOKIUSDT": 95.0
    "XAUUSDT": 25.0
    "WIFUSDT": 100.0
    "SUIUSDT": 85.0
    "APTUSDT": 75.0
    "ARBUSDT": 70.0
}


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

@dataclass
class ShadowSimResult:
    """Shadow simulation result for one symbol × regime."""
    symbol: str
    regime: str
    n_trades: int
    actual_avg_pnl_r: float       # real avg P&L per trade in R
    shadow_avg_pnl_r: float       # virtual P&L with calibrated params in R
    delta_pnl_r: float            # shadow - actual
    actual_win_rate: float        # actual fraction of profitable trades
    shadow_win_rate: float        # virtual fraction of profitable trades
    shadow_sharpe: float          # Sharpe-like ratio of shadow P&L
    recommendation: str           # "BETTER" | "WORSE" | "NEUTRAL"
    computed_at_ms: int

    def to_redis_mapping(self) -> Dict[str, str]:
        return {k: str(v) for k, v in asdict(self).items()}


@dataclass
class _TradeForSim:
    """Internal trade data needed for shadow simulation."""
    symbol: str
    regime: str
    pnl_net: float
    one_r_money: float
    mfe_pnl: float
    giveback: float
    entry_price: float
    notional: float
    trailing_started: bool


# ---------------------------------------------------------------------------
# Core simulation logic (pure, no I/O)
# ---------------------------------------------------------------------------

def simulate_shadow_exit_r(
    mfe_r: float
    actual_pnl_r: float
    callback_r: float
    min_profit_lock_r: float
    activate_offset_r: float
) -> float:
    """
    Simulate where calibrated trailing stop would have exited.

    Args:
        mfe_r: maximum favorable excursion in R
        actual_pnl_r: actual P&L in R (for fallback when trail wouldn't engage)
        callback_r: calibrated callback distance in R
        min_profit_lock_r: minimum locked profit in R before trail activates
        activate_offset_r: activation offset in R

    Returns:
        Shadow exit P&L in R units.
    """
    activation_threshold = min_profit_lock_r + activate_offset_r

    if mfe_r < activation_threshold:
        # Trail would NOT have engaged → exit = actual exit
        return actual_pnl_r

    # Trail engaged: exit at MFE minus callback, but at least min_profit_lock
    shadow_exit_r = max(mfe_r - callback_r, min_profit_lock_r)
    return shadow_exit_r


def compute_shadow_results(
    trades: List[_TradeForSim]
    callback_atr_mult: float
    activate_offset_bps: float
    min_profit_lock_r: float
    atr_bps: float
) -> Optional[ShadowSimResult]:
    """
    Compute shadow simulation for a bucket of trades.

    Pure function — no Redis I/O.
    """
    if not trades or len(trades) < 5:
        return None

    symbol = trades[0].symbol
    regime = trades[0].regime

    actual_pnl_r_list: List[float] = []
    shadow_pnl_r_list: List[float] = []

    for t in trades:
        if t.one_r_money < EPS or t.notional < EPS:
            continue

        # Convert to R-based metrics
        actual_pnl_r = t.pnl_net / t.one_r_money
        mfe_r = t.mfe_pnl / t.one_r_money if t.mfe_pnl > 0 else 0.0

        # Compute callback in R: callback_bps = callback_atr_mult × ATR_bps
        # one_r_bps = one_r_money / notional × 10000
        one_r_bps = (t.one_r_money / t.notional) * 10_000.0
        if one_r_bps < EPS:
            actual_pnl_r_list.append(actual_pnl_r)
            shadow_pnl_r_list.append(actual_pnl_r)
            continue

        callback_bps = callback_atr_mult * atr_bps
        callback_r = callback_bps / one_r_bps

        activate_offset_r = activate_offset_bps / one_r_bps

        shadow_exit_r = simulate_shadow_exit_r(
            mfe_r=mfe_r
            actual_pnl_r=actual_pnl_r
            callback_r=callback_r
            min_profit_lock_r=min_profit_lock_r
            activate_offset_r=activate_offset_r
        )

        actual_pnl_r_list.append(actual_pnl_r)
        shadow_pnl_r_list.append(shadow_exit_r)

    if not actual_pnl_r_list:
        return None

    n = len(actual_pnl_r_list)
    actual_avg = sum(actual_pnl_r_list) / n
    shadow_avg = sum(shadow_pnl_r_list) / n
    delta = shadow_avg - actual_avg

    actual_wins = sum(1 for x in actual_pnl_r_list if x > EPS)
    shadow_wins = sum(1 for x in shadow_pnl_r_list if x > EPS)

    # Sharpe-like: mean / stdev of shadow P&L
    if n > 1:
        mean_s = shadow_avg
        var_s = sum((x - mean_s) ** 2 for x in shadow_pnl_r_list) / (n - 1)
        std_s = math.sqrt(var_s) if var_s > 0 else 1.0
        sharpe = mean_s / std_s
    else:
        sharpe = 0.0

    # Recommendation logic
    if delta > 0.05:
        recommendation = "BETTER"
    elif delta < -0.05:
        recommendation = "WORSE"
    else:
        recommendation = "NEUTRAL"

    return ShadowSimResult(
        symbol=symbol
        regime=regime
        n_trades=n
        actual_avg_pnl_r=round(actual_avg, 6)
        shadow_avg_pnl_r=round(shadow_avg, 6)
        delta_pnl_r=round(delta, 6)
        actual_win_rate=round(actual_wins / n, 4)
        shadow_win_rate=round(shadow_wins / n, 4)
        shadow_sharpe=round(sharpe, 4)
        recommendation=recommendation
        computed_at_ms=get_ny_time_millis()
    )


# ---------------------------------------------------------------------------
# Simulator engine (with Redis I/O)
# ---------------------------------------------------------------------------

class TrailShadowSimulator:
    """
    Reads calibrated params from trail:calib:{symbol}:{regime} and
    trades from TrailPostAnalyzer's parsed output to compute virtual P&L.
    Writes results to trail:shadow:{symbol}:{regime}.
    """

    def __init__(self, redis_client: Any, *, cfg: Optional[ShadowSimConfig] = None):
        self.redis = redis_client
        self.cfg = cfg or ShadowSimConfig.from_env()

    def run(
        self
        trades_by_bucket: Dict[str, List[Any]]
    ) -> List[ShadowSimResult]:
        """
        Run shadow simulation for all buckets.

        Args:
            trades_by_bucket: dict of "SYMBOL:regime" -> list of _ParsedTrade
                (from TrailPostAnalyzer._load_trades, grouped).

        Returns:
            List of ShadowSimResult.
        """
        if not self.cfg.enabled:
            logger.info("Shadow simulator disabled (TRAIL_SHADOW_ENABLED=0)")
            return []

        results: List[ShadowSimResult] = []

        for key, raw_trades in trades_by_bucket.items():
            parts = key.rsplit(":", 1)
            if len(parts) < 2:
                continue
            symbol, regime = parts[0], parts[1]

            # Read calibrated params for this bucket
            calib = self._read_calib_params(symbol, regime)
            if not calib:
                logger.debug("No calib params for %s:%s — skipping shadow", symbol, regime)
                continue

            # Convert to sim trades
            sim_trades = []
            for t in raw_trades:
                sim_trades.append(_TradeForSim(
                    symbol=getattr(t, "symbol", symbol)
                    regime=getattr(t, "regime", regime)
                    pnl_net=getattr(t, "pnl_net", 0.0)
                    one_r_money=getattr(t, "one_r_money", 0.0)
                    mfe_pnl=getattr(t, "mfe_pnl", 0.0)
                    giveback=getattr(t, "giveback", 0.0)
                    entry_price=getattr(t, "entry_price", 0.0)
                    notional=getattr(t, "notional", 0.0)
                    trailing_started=getattr(t, "trailing_started", False)
                ))

            atr_bps = DEFAULT_ATR_BPS.get(symbol, self.cfg.atr_fallback_bps)

            result = compute_shadow_results(
                trades=sim_trades
                callback_atr_mult=calib["callback_atr_mult"]
                activate_offset_bps=calib["activate_offset_bps"]
                min_profit_lock_r=calib["min_profit_lock_r"]
                atr_bps=atr_bps
            )

            if result:
                results.append(result)
                self._write_to_redis(result)

        logger.info("Shadow simulation complete: %d buckets", len(results))
        return results

    def _read_calib_params(self, symbol: str, regime: str) -> Optional[Dict[str, float]]:
        """Read calibrated params from trail:calib:{symbol}:{regime}."""
        if self.redis is None:
            return None
        key = f"{self.cfg.calib_prefix}:{symbol}:{regime}"
        try:
            h = self.redis.hgetall(key)
            if not h:
                return None
            cb = _sf(h.get("callback_atr_mult"))
            if cb < EPS:
                return None
            return {
                "callback_atr_mult": cb
                "activate_offset_bps": _sf(h.get("activate_offset_bps"), 5.0)
                "min_profit_lock_r": _sf(h.get("min_profit_lock_r"), 0.1)
            }
        except Exception as e:
            logger.error("Failed to read calib params %s: %s", key, e)
            return None

    def _write_to_redis(self, result: ShadowSimResult) -> None:
        """Write shadow result to Redis."""
        if self.redis is None:
            return
        key = f"{self.cfg.key_prefix}:{result.symbol}:{result.regime}"
        try:
            pipe = self.redis.pipeline(transaction=False)
            pipe.hset(key, mapping=result.to_redis_mapping())
            if self.cfg.ttl_sec > 0:
                pipe.expire(key, self.cfg.ttl_sec)
            pipe.execute()
            logger.debug("Wrote %s (%d trades, delta=%.4fR)", key, result.n_trades, result.delta_pnl_r)
        except Exception as e:
            logger.error("Failed to write %s: %s", key, e)

    # ------------------------------------------------------------------
    # Telegram formatting
    # ------------------------------------------------------------------

    @staticmethod
    def format_telegram_report(results: List[ShadowSimResult]) -> str:
        """Format shadow simulation results for Telegram."""
        if not results:
            return ""

        lines = ["📊 <b>Shadow A/B Comparison</b> (calibrated vs actual)\n"]

        for r in sorted(results, key=lambda x: x.symbol):
            emoji = "✅" if r.recommendation == "BETTER" else "⚠️" if r.recommendation == "WORSE" else "🔄"
            lines.append(
                f"  {r.symbol}: actual={r.actual_avg_pnl_r:+.3f}R → "
                f"shadow={r.shadow_avg_pnl_r:+.3f}R "
                f"(Δ={r.delta_pnl_r:+.3f}R) {emoji}"
            )
            lines.append(
                f"    WR: {r.actual_win_rate*100:.0f}%→{r.shadow_win_rate*100:.0f}% | "
                f"sharpe={r.shadow_sharpe:.2f} | n={r.n_trades}"
            )

        # Summary
        better = sum(1 for r in results if r.recommendation == "BETTER")
        worse = sum(1 for r in results if r.recommendation == "WORSE")
        neutral = sum(1 for r in results if r.recommendation == "NEUTRAL")
        avg_delta = sum(r.delta_pnl_r for r in results) / len(results)

        lines.append(
            f"\n📈 Summary: {better}✅ {neutral}🔄 {worse}⚠️ | "
            f"avg_Δ={avg_delta:+.3f}R"
        )

        return "\n".join(lines)
