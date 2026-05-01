from __future__ import annotations
"""
Trail Post-Analyzer — MFE/MAE post-analysis per symbol × regime.

Reads closed trades from `trades:closed` stream, computes:
  - trail_giveback_bps: BPS given back from MFE to actual exit
  - trail_vs_tp2_delta_r: P&L delta trailing vs hypothetical TP2
  - trail_hit_rate: % of trades improved by trailing
  - optimal_callback_bps: MFE-based optimal callback per bucket
  - MFE distribution (p25/p50/p75)
  - MAE after MFE peak (p75) — input for calibrator

Key pattern: trail:analysis:{symbol}:{regime}
TTL: configurable (default 48h)

GPU-accelerated: uses CuPy for array operations when available (CUDA).
Fallback: numpy on CPU if CuPy not installed or no GPU.
Fail-open: never raises on Redis/data errors.
"""
from utils.time_utils import get_ny_time_millis

import math
import os
import time
from dataclasses import dataclass, asdict
from typing import Any, Dict, List, Optional, Sequence

from common.log import setup_logger

logger = setup_logger("TrailPostAnalyzer")

EPS = 1e-9

# ---------------------------------------------------------------------------
# GPU / CPU array backend (CuPy preferred, numpy fallback)
# ---------------------------------------------------------------------------
try:
    import cupy as xp  # type: ignore[import-untyped]
    _GPU = True
    logger.info("CuPy available — trail post-analysis will run on GPU")
except ImportError:
    import numpy as xp  # type: ignore[no-redef]
    _GPU = False
    logger.info("CuPy not available — trail post-analysis will run on CPU (numpy)")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _env_bool(name: str, default: bool) -> bool:
    v = (os.getenv(name, "1" if default else "0") or "").strip().lower()
    return v in {"1", "true", "yes", "on"}


def _sf(v: Any, default: float = 0.0) -> float:
    try:
        if v is None:
            return float(default)
        return float(str(v).strip())
    except Exception:
        return float(default)


def _si(v: Any, default: int = 0) -> int:
    try:
        if v is None:
            return int(default)
        return int(float(str(v).strip()))
    except Exception:
        return int(default)


def _sb(v: Any) -> bool:
    s = str(v or "").strip().lower()
    return s in ("1", "true", "yes", "y", "on")


def _canon_regime(v: Any) -> str:
    if v is None:
        return "na"
    if isinstance(v, str):
        s = v.strip().lower()
        return s if s else "na"
    try:
        s = str(getattr(v, "name", None) or getattr(v, "value", None) or v).strip().lower()
        return s if s else "na"
    except Exception:
        return "na"


def _quantile(values: Sequence[float], q: float) -> float:
    if not values:
        return 0.0
    arr = xp.array(values, dtype=xp.float64)
    result = float(xp.percentile(arr, q * 100.0))
    return result


def _median(values: Sequence[float]) -> float:
    if not values:
        return 0.0
    arr = xp.array(values, dtype=xp.float64)
    return float(xp.median(arr))


def _pstdev(values: Sequence[float]) -> float:
    if len(values) < 2:
        return 0.0
    arr = xp.array(values, dtype=xp.float64)
    return float(xp.std(arr))


def _gpu_bucket_stats(values: Sequence[float]) -> Dict[str, float]:
    """Compute all stats for a values array on GPU in one pass."""
    if not values:
        return {"mean": 0.0, "median": 0.0, "std": 0.0, "p25": 0.0, "p50": 0.0, "p75": 0.0}
    arr = xp.array(values, dtype=xp.float64)
    return {
        "mean": float(xp.mean(arr)),
        "median": float(xp.median(arr)),
        "std": float(xp.std(arr)),
        "p25": float(xp.percentile(arr, 25)),
        "p50": float(xp.percentile(arr, 50)),
        "p75": float(xp.percentile(arr, 75)),
    }


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class TrailAnalyzerConfig:
    enabled: bool
    lookback_days: int
    min_n: int
    notify: bool
    key_prefix: str
    ttl_sec: int

    @classmethod
    def from_env(cls) -> "TrailAnalyzerConfig":
        return cls(
            enabled=_env_bool("TRAIL_ANALYZER_ENABLED", True),
            lookback_days=_si(os.getenv("TRAIL_ANALYZER_LOOKBACK_DAYS", "7"), 7),
            min_n=_si(os.getenv("TRAIL_ANALYZER_MIN_N", "50"), 50),
            notify=_env_bool("TRAIL_ANALYZER_NOTIFY", True),
            key_prefix=os.getenv("TRAIL_ANALYZER_KEY_PREFIX", "trail:analysis") or "trail:analysis",
            ttl_sec=_si(os.getenv("TRAIL_CALIB_TTL_SEC", str(48 * 3600)), 48 * 3600),
        )


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

@dataclass
class TrailAnalysisBucket:
    """Post-analysis results for one symbol × regime bucket."""
    symbol: str
    regime: str
    n_total: int
    n_trailing: int
    # Giveback metrics
    avg_giveback_bps: float
    avg_giveback_r: float
    median_giveback_r: float
    # Trail vs TP2
    trail_vs_tp2_delta_r: float  # avg(pnl_net - pnl_if_fixed_exit) / one_r
    trail_hit_rate: float        # fraction of trades where trailing improved P&L
    # Optimal callback (for calibrator)
    optimal_callback_bps: float  # P75(MAE_after_MFE) × safety_mult
    # MFE distribution (in R)
    mfe_p25_r: float
    mfe_p50_r: float
    mfe_p75_r: float
    # MAE after MFE peak (bps) — how much price retraces after peak
    mae_after_mfe_p75_bps: float
    # Confidence
    confidence: float
    # Timestamp
    computed_at_ms: int

    def to_redis_mapping(self) -> Dict[str, str]:
        d: Dict[str, str] = {}
        for k, v in asdict(self).items():
            d[k] = str(v)
        return d


@dataclass
class _ParsedTrade:
    """Internal parsed trade for analysis."""
    symbol: str
    regime: str
    pnl_net: float
    one_r_money: float
    mfe_pnl: float        # max favorable excursion in USD
    giveback: float        # MFE - actual exit PnL (USD)
    pnl_if_fixed_exit: float  # hypothetical TP2 exit PnL
    trailing_started: bool
    entry_price: float
    qty: float
    notional: float
    exit_ts_ms: int
    close_reason: str


# ---------------------------------------------------------------------------
# Analysis engine
# ---------------------------------------------------------------------------

class TrailPostAnalyzer:
    """
    Reads trades:closed stream, computes MFE/MAE analysis per symbol × regime.
    Writes results to Redis hashes: trail:analysis:{symbol}:{regime}.
    """

    def __init__(self, redis_client: Any, *, cfg: Optional[TrailAnalyzerConfig] = None):
        self.redis = redis_client
        self.cfg = cfg or TrailAnalyzerConfig.from_env()

    def run(self, symbols: Optional[List[str]] = None) -> List[TrailAnalysisBucket]:
        """
        Main entry point. Returns list of analysis buckets.

        Args:
            symbols: if None, analyze all symbols found in stream.
        """
        if not self.cfg.enabled:
            logger.info("Trail post-analyzer disabled (TRAIL_ANALYZER_ENABLED=0)")
            return []

        trades = self._load_trades()
        if not trades:
            logger.warning("No trades loaded from trades:closed")
            return []

        # Filter by symbols if specified
        if symbols:
            syms = {s.upper() for s in symbols}
            trades = [t for t in trades if t.symbol in syms]

        # Group by symbol × regime
        buckets: Dict[str, List[_ParsedTrade]] = {}
        for t in trades:
            key = f"{t.symbol}:{t.regime}"
            buckets.setdefault(key, []).append(t)

        results: List[TrailAnalysisBucket] = []
        for key, bucket_trades in buckets.items():
            bucket = self._analyze_bucket(bucket_trades)
            if bucket:
                results.append(bucket)
                self._write_to_redis(bucket)

        logger.info(
            "Trail post-analysis complete: %d buckets from %d trades",
            len(results), len(trades),
        )
        return results

    def _load_trades(self) -> List[_ParsedTrade]:
        """Load trades from trades:closed stream within lookback window."""
        if self.redis is None:
            return []

        lookback_ms = self.cfg.lookback_days * 86400 * 1000
        from_ts_ms = get_ny_time_millis() - lookback_ms
        min_id = f"{from_ts_ms}-0"

        # Try hydrator for compact stream support
        try:
            from services.trade_closed_hydrator import hydrate_trade_closed_batch
            has_hydrator = True
        except ImportError:
            has_hydrator = False
            hydrate_trade_closed_batch = None

        trades: List[_ParsedTrade] = []
        CHUNK = 2000
        MAX_SCAN = 500_000
        last_id = "+"
        total_scanned = 0

        while total_scanned < MAX_SCAN:
            try:
                entries = self.redis.xrevrange("trades:closed", max=last_id, min=min_id, count=CHUNK)
            except Exception as e:
                logger.error("Redis error reading trades:closed: %s", e)
                break

            if not entries:
                break

            # Hydrate batch
            raw_items = []
            for _id, fields in entries:
                raw_items.append(self._norm_map(fields or {}))

            if has_hydrator and hydrate_trade_closed_batch:
                try:
                    hydrated = hydrate_trade_closed_batch(
                        self.redis, raw_items,
                        require_closed=False, merge_precedence="hash",
                    )
                except Exception:
                    hydrated = raw_items
            else:
                hydrated = raw_items

            for fields in hydrated:
                t = self._parse_trade(fields)
                if t and t.one_r_money > EPS:
                    trades.append(t)

            total_scanned += len(entries)
            if len(entries) < CHUNK:
                break

            oldest_id = entries[-1][0]
            last_id = f"({oldest_id}"

        logger.info("Loaded %d valid trades (scanned %d entries)", len(trades), total_scanned)
        return trades

    def _parse_trade(self, fields: Dict[str, str]) -> Optional[_ParsedTrade]:
        """Parse a single trade from stream fields."""
        symbol = (fields.get("symbol") or "").upper()
        if not symbol:
            return None

        exit_ts = _si(fields.get("exit_ts_ms") or fields.get("closed_time"), 0)
        if exit_ts <= 0:
            return None

        pnl_net = _sf(fields.get("pnl_net"))
        one_r = _sf(fields.get("one_r_money"))
        mfe_pnl = _sf(fields.get("mfe_pnl"))
        giveback = _sf(fields.get("giveback"))
        pnl_if_fixed = _sf(fields.get("pnl_if_fixed_exit") or fields.get("pnl_fixed_exit"))
        entry_price = _sf(fields.get("entry_price") or fields.get("entry_px"))
        qty = _sf(fields.get("lot") or fields.get("qty"))
        notional = _sf(fields.get("notional_usd") or fields.get("notional"))

        if notional <= EPS and entry_price > EPS and qty > EPS:
            notional = entry_price * abs(qty)

        # Regime
        regime = _canon_regime(fields.get("regime") or fields.get("market_regime"))

        # Trailing detection
        t_started = _sb(fields.get("trailing_started"))
        t_moves = _si(fields.get("trailing_moves_count") or fields.get("trailing_moves"))
        cr = (fields.get("close_reason") or "").upper()
        is_trail_exit = "TRAIL" in cr
        trailing = t_started or t_moves > 0 or is_trail_exit

        # Reconstruct one_r if missing (from SL)
        if one_r <= EPS and entry_price > EPS and qty > EPS:
            sl = _sf(fields.get("sl") or fields.get("sl_price") or fields.get("stop_loss"))
            if sl > EPS:
                one_r = abs(entry_price - sl) * abs(qty)

        return _ParsedTrade(
            symbol=symbol,
            regime=regime,
            pnl_net=pnl_net,
            one_r_money=one_r,
            mfe_pnl=mfe_pnl,
            giveback=giveback,
            pnl_if_fixed_exit=pnl_if_fixed,
            trailing_started=trailing,
            entry_price=entry_price,
            qty=qty,
            notional=notional,
            exit_ts_ms=exit_ts,
            close_reason=cr,
        )

    def _analyze_bucket(self, trades: List[_ParsedTrade]) -> Optional[TrailAnalysisBucket]:
        """Analyze one symbol × regime bucket. GPU-vectorized."""
        if not trades:
            return None

        symbol = trades[0].symbol
        regime = trades[0].regime
        n_total = len(trades)
        n_trailing = sum(1 for t in trades if t.trailing_started)

        if n_total < max(self.cfg.min_n, 5):
            logger.debug("Skipping %s:%s — only %d trades (min=%d)", symbol, regime, n_total, self.cfg.min_n)
            return None

        # ---- Vectorized: move all trade data to GPU arrays in one shot ----
        pnl_net = xp.array([t.pnl_net for t in trades], dtype=xp.float64)
        one_r = xp.array([t.one_r_money for t in trades], dtype=xp.float64)
        mfe_pnl = xp.array([t.mfe_pnl for t in trades], dtype=xp.float64)
        giveback = xp.array([t.giveback for t in trades], dtype=xp.float64)
        pnl_if_fixed = xp.array([t.pnl_if_fixed_exit for t in trades], dtype=xp.float64)
        notional = xp.array([t.notional for t in trades], dtype=xp.float64)

        # Mask: valid one_r
        valid = one_r > EPS

        # ---- Giveback BPS (where notional > 0 and giveback > 0) ----
        gb_bps_mask = valid & (notional > EPS) & (giveback > EPS)
        gb_bps_arr = xp.where(gb_bps_mask, (giveback / xp.maximum(notional, EPS)) * 10_000.0, xp.nan)
        gb_bps_finite = gb_bps_arr[gb_bps_mask & (gb_bps_arr > 0) & (gb_bps_arr < 50_000)]

        # ---- Giveback R ----
        gb_r_arr = xp.where(valid & (giveback > 0), giveback / xp.maximum(one_r, EPS), 0.0)
        gb_r_finite = gb_r_arr[valid & xp.isfinite(gb_r_arr) & (gb_r_arr < 100)]

        # ---- Trail vs TP2 delta R ----
        has_pnl = valid & ((xp.abs(pnl_if_fixed) > EPS) | (xp.abs(pnl_net) > EPS))
        delta_r_arr = xp.where(has_pnl, (pnl_net - pnl_if_fixed) / xp.maximum(one_r, EPS), xp.nan)
        delta_r_valid = delta_r_arr[has_pnl & xp.isfinite(delta_r_arr) & (xp.abs(delta_r_arr) < 100)]

        # ---- MFE R ----
        mfe_r_arr = xp.where(valid & (mfe_pnl > 0), mfe_pnl / xp.maximum(one_r, EPS), 0.0)
        mfe_r_valid = mfe_r_arr[valid & xp.isfinite(mfe_r_arr) & (mfe_r_arr > 0) & (mfe_r_arr < 100)]

        # ---- MAE after MFE (BPS) ----
        mae_mask = valid & (notional > EPS) & (mfe_pnl > EPS) & (giveback > 0)
        mae_bps_arr = xp.where(mae_mask, (giveback / xp.maximum(notional, EPS)) * 10_000.0, xp.nan)
        mae_bps_valid = mae_bps_arr[mae_mask & xp.isfinite(mae_bps_arr) & (mae_bps_arr > 0) & (mae_bps_arr < 50_000)]

        # ---- Compute aggregates (all on GPU) ----
        avg_giveback_bps = float(xp.mean(gb_bps_finite)) if len(gb_bps_finite) > 0 else 0.0
        avg_giveback_r = float(xp.mean(gb_r_finite)) if len(gb_r_finite) > 0 else 0.0
        median_giveback_r = float(xp.median(gb_r_finite)) if len(gb_r_finite) > 0 else 0.0

        avg_trail_vs_tp2 = float(xp.mean(delta_r_valid)) if len(delta_r_valid) > 0 else 0.0
        better_count = int(xp.sum(delta_r_valid > EPS)) if len(delta_r_valid) > 0 else 0
        trail_hit_rate = better_count / max(len(delta_r_valid), 1)

        # Optimal callback = P75(MAE) × safety_mult
        safety_mult = float(os.getenv("TRAIL_CALIB_SAFETY_MULT", "1.2") or 1.2)
        mae_p75 = float(xp.percentile(mae_bps_valid, 75)) if len(mae_bps_valid) > 0 else 0.0
        optimal_callback_bps = mae_p75 * safety_mult

        # MFE quantiles
        mfe_p25 = float(xp.percentile(mfe_r_valid, 25)) if len(mfe_r_valid) > 0 else 0.0
        mfe_p50 = float(xp.percentile(mfe_r_valid, 50)) if len(mfe_r_valid) > 0 else 0.0
        mfe_p75 = float(xp.percentile(mfe_r_valid, 75)) if len(mfe_r_valid) > 0 else 0.0

        # Confidence
        n_factor = min(1.0, n_total / (self.cfg.min_n * 3))
        vol_penalty = float(xp.std(gb_r_finite)) / 2.0 if len(gb_r_finite) > 1 else 0.5
        confidence = max(0.0, min(1.0, n_factor / (1.0 + vol_penalty)))

        return TrailAnalysisBucket(
            symbol=symbol,
            regime=regime,
            n_total=n_total,
            n_trailing=n_trailing,
            avg_giveback_bps=round(avg_giveback_bps, 4),
            avg_giveback_r=round(avg_giveback_r, 6),
            median_giveback_r=round(median_giveback_r, 6),
            trail_vs_tp2_delta_r=round(avg_trail_vs_tp2, 6),
            trail_hit_rate=round(trail_hit_rate, 4),
            optimal_callback_bps=round(optimal_callback_bps, 4),
            mfe_p25_r=round(mfe_p25, 6),
            mfe_p50_r=round(mfe_p50, 6),
            mfe_p75_r=round(mfe_p75, 6),
            mae_after_mfe_p75_bps=round(mae_p75, 4),
            confidence=round(confidence, 4),
            computed_at_ms=get_ny_time_millis(),
        )

    def _write_to_redis(self, bucket: TrailAnalysisBucket) -> None:
        """Write analysis bucket to Redis hash."""
        if self.redis is None:
            return
        key = f"{self.cfg.key_prefix}:{bucket.symbol}:{bucket.regime}"
        try:
            pipe = self.redis.pipeline(transaction=False)
            pipe.hset(key, mapping=bucket.to_redis_mapping())
            if self.cfg.ttl_sec > 0:
                pipe.expire(key, self.cfg.ttl_sec)
            pipe.execute()
            logger.debug("Wrote %s (%d trades)", key, bucket.n_total)
        except Exception as e:
            logger.error("Failed to write %s: %s", key, e)

    @staticmethod
    def _norm_map(m: Dict[str, Any]) -> Dict[str, str]:
        out: Dict[str, str] = {}
        for k, v in (m or {}).items():
            if v is None:
                continue
            out[str(k)] = str(v)
        return out

    # ------------------------------------------------------------------
    # Telegram report formatting
    # ------------------------------------------------------------------

    @staticmethod
    def format_telegram_report(buckets: List[TrailAnalysisBucket]) -> str:
        """Format analysis results for Telegram."""
        if not buckets:
            return "📊 <b>Trail Analysis Report</b>\n\nNo data — insufficient trades."

        lines = ["📊 <b>Trail Analysis Report</b> (post-analysis)\n"]

        # Group by symbol
        by_sym: Dict[str, List[TrailAnalysisBucket]] = {}
        for b in buckets:
            by_sym.setdefault(b.symbol, []).append(b)

        for sym in sorted(by_sym.keys()):
            lines.append(f"\n<b>{sym}</b>:")
            for b in sorted(by_sym[sym], key=lambda x: x.regime):
                emoji = "✅" if b.trail_vs_tp2_delta_r > 0.02 else "⚠️" if b.trail_vs_tp2_delta_r < -0.02 else "🔄"
                lines.append(
                    f"  {b.regime}: hit={b.trail_hit_rate * 100:.0f}%, "
                    f"gb={b.avg_giveback_bps:.1f}bps, "
                    f"vs_TP2={b.trail_vs_tp2_delta_r:+.2f}R, "
                    f"n={b.n_total} {emoji}"
                )
                lines.append(
                    f"    MFE(R): p25={b.mfe_p25_r:.2f} p50={b.mfe_p50_r:.2f} p75={b.mfe_p75_r:.2f} | "
                    f"opt_cb={b.optimal_callback_bps:.1f}bps"
                )

        # Summary
        trend_buckets = [b for b in buckets if b.regime in ("trend", "trending")]
        range_buckets = [b for b in buckets if b.regime in ("range", "mean_revert", "choppy")]
        if trend_buckets:
            avg_trend = sum(b.trail_vs_tp2_delta_r for b in trend_buckets) / len(trend_buckets)
            lines.append(f"\n📈 Trend avg: {avg_trend:+.3f}R")
        if range_buckets:
            avg_range = sum(b.trail_vs_tp2_delta_r for b in range_buckets) / len(range_buckets)
            lines.append(f"📉 Range avg: {avg_range:+.3f}R")

        return "\n".join(lines)
