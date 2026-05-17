from __future__ import annotations

"""
core/v13_runtime_tracker.py
===========================
Self-contained real-time computation tracker for v13_of indicator groups.

Provides two update hooks:
  - on_tick(price, qty, side, ts_ms, *, book_mid, book_state)  — per aggTrade
  - on_bar_close(bar)  — per microbar close

Populates instance attributes consumed by `v13_of_features.py` via
  `getattr(runtime.v13_tracker, attr, 0.0)`.

Design:
  - All rolling buffers are bounded deques.
  - All computations are fail-open: exceptions → keep previous value.
  - PIN EM is cached with 5s TTL to avoid CPU spikes.
  - ADF is cached with 5s TTL (statsmodels is heavy).
  - No external IO — uses only in-memory buffers.
"""


import math
import time
from collections import deque
from dataclasses import dataclass
from typing import Any
import contextlib

try:
    import numpy as np
except ImportError:
    np = None  # type: ignore


# ═══════════════════════════════════════════════════════════════════════════════
# Constants
# ═══════════════════════════════════════════════════════════════════════════════

_LN2 = math.log(2.0)
_BAR_WINDOW = 20       # rolling window for OHLC bars
_TICK_WINDOW = 100     # rolling window for tick-level buffers
_PIN_CACHE_TTL_S = 5   # PIN EM recompute interval
_ADF_CACHE_TTL_S = 5   # ADF test recompute interval


# ═══════════════════════════════════════════════════════════════════════════════
# Helper: OHLC bar struct
# ═══════════════════════════════════════════════════════════════════════════════

@dataclass
class _OHLCBar:
    o: float = 0.0
    h: float = 0.0
    l: float = 0.0
    c: float = 0.0
    volume: float = 0.0
    ts_ms: int = 0


# ═══════════════════════════════════════════════════════════════════════════════
# Main Tracker
# ═══════════════════════════════════════════════════════════════════════════════

class V13RuntimeTracker:
    """Computes v13_of runtime attributes from rolling tick/bar buffers."""

    __slots__ = (
        # Group NA — OHLC volatility
        "garman_klass_vol", "parkinson_vol", "yang_zhang_vol", "vol_of_vol",
        # Group NB — academic liquidity
        "amihud_illiquidity", "corwin_schultz_spread", "hasbrouck_info_share",
        "depth_resilience_half_life",
        # Group NC — flow toxicity
        "pin_estimate", "kyle_lambda_buy", "kyle_lambda_sell",
        "aggressive_sweep_ratio",
        # Group NE — entropy
        "price_entropy_50", "order_size_gini", "mutual_info_price_volume",
        # Group NF — mean reversion
        "half_life_mean_reversion", "adf_pvalue_50", "mid_vwap_diff_std",
        # Rolling buffers
        "_bar_buf", "_return_buf", "_volume_buf", "_side_buf",
        "_price_buf", "_size_buf", "_vol_bps_buf",
        "_buy_vol_buf", "_sell_vol_buf",
        "_mid_vwap_diff_buf",
        # Depth resilience tracking
        "_depth_pre_sweep", "_depth_post_times", "_depth_recovery_buf",
        # Sweep tracking
        "_sweep_level_count", "_total_trade_count",
        # Caches
        "_pin_cache_ts", "_adf_cache_ts",
        "_last_book_mid",
    )

    def __init__(self) -> None:
        # ── Group NA: OHLC Volatility ──
        self.garman_klass_vol: float = 0.0
        self.parkinson_vol: float = 0.0
        self.yang_zhang_vol: float = 0.0
        self.vol_of_vol: float = 0.0

        # ── Group NB: Academic Liquidity ──
        self.amihud_illiquidity: float = 0.0
        self.corwin_schultz_spread: float = 0.0
        self.hasbrouck_info_share: float = 0.0
        self.depth_resilience_half_life: float = 0.0

        # ── Group NC: Flow Toxicity ──
        self.pin_estimate: float = 0.0
        self.kyle_lambda_buy: float = 0.0
        self.kyle_lambda_sell: float = 0.0
        self.aggressive_sweep_ratio: float = 0.0

        # ── Group NE: Entropy / Information Theory ──
        self.price_entropy_50: float = 0.0
        self.order_size_gini: float = 0.0
        self.mutual_info_price_volume: float = 0.0

        # ── Group NF: Mean Reversion / Stationarity ──
        self.half_life_mean_reversion: float = 0.0
        self.adf_pvalue_50: float = 0.0
        self.mid_vwap_diff_std: float = 0.0

        # ── Rolling buffers ──
        self._bar_buf: deque[_OHLCBar] = deque(maxlen=_BAR_WINDOW)
        self._return_buf: deque[float] = deque(maxlen=_TICK_WINDOW)
        self._volume_buf: deque[float] = deque(maxlen=_TICK_WINDOW)
        self._side_buf: deque[int] = deque(maxlen=_TICK_WINDOW)  # +1 buy, -1 sell
        self._price_buf: deque[float] = deque(maxlen=_TICK_WINDOW)
        self._size_buf: deque[float] = deque(maxlen=_TICK_WINDOW)
        self._vol_bps_buf: deque[float] = deque(maxlen=_BAR_WINDOW)
        self._buy_vol_buf: deque[float] = deque(maxlen=_TICK_WINDOW)
        self._sell_vol_buf: deque[float] = deque(maxlen=_TICK_WINDOW)
        self._mid_vwap_diff_buf: deque[float] = deque(maxlen=50)

        # Depth resilience
        self._depth_pre_sweep: float = 0.0
        self._depth_post_times: deque[tuple[int, float]] = deque(maxlen=20)
        self._depth_recovery_buf: deque[float] = deque(maxlen=20)

        # Sweep level tracking
        self._sweep_level_count: int = 0
        self._total_trade_count: int = 0

        # Cache timestamps
        self._pin_cache_ts: float = 0.0
        self._adf_cache_ts: float = 0.0
        self._last_book_mid: float = 0.0

    # ───────────────────────────────────────────────────────────────────────────
    # Public interface: on_tick
    # ───────────────────────────────────────────────────────────────────────────

    def on_tick(
        self,
        price: float,
        qty: float,
        side: str,  # "BUY" or "SELL"
        ts_ms: int,
        *,
        book_mid: float = 0.0,
        levels_crossed: int = 0,
        book_depth_near: float = 0.0,
    ) -> None:
        """Process a single aggTrade event.

        Args:
            price:  trade price
            qty:    trade quantity (base)
            side:   "BUY" or "SELL"
            ts_ms:  server timestamp (epoch ms)
            book_mid:  mid-price at trade time (for return computation)
            levels_crossed:  how many book levels this trade crossed (for sweep)
            book_depth_near: near-touch depth USD (for resilience)
        """
        try:
            s = 1 if side.upper() == "BUY" else -1

            # Populate rolling buffers
            self._price_buf.append(price)
            self._size_buf.append(qty)
            self._side_buf.append(s)
            self._volume_buf.append(qty * price)  # notional USD
            self._total_trade_count += 1

            if s > 0:
                self._buy_vol_buf.append(qty * price)
                self._sell_vol_buf.append(0.0)
            else:
                self._buy_vol_buf.append(0.0)
                self._sell_vol_buf.append(qty * price)

            # Return in bps
            if len(self._price_buf) >= 2:
                prev_px = self._price_buf[-2]
                if prev_px > 0:
                    ret_bps = (price - prev_px) / prev_px * 10_000.0
                    self._return_buf.append(ret_bps)

            if book_mid > 0:
                self._last_book_mid = book_mid

            # Sweep level tracking
            if levels_crossed >= 3:
                self._sweep_level_count += 1

            # ── Per-tick computations ──
            self._compute_entropy()
            self._compute_gini()

            # Periodic heavy computations
            now = time.monotonic()
            if now - self._pin_cache_ts > _PIN_CACHE_TTL_S:
                self._pin_cache_ts = now
                self._compute_pin()
                self._compute_kyle_split()
                self._compute_mutual_info()

            if now - self._adf_cache_ts > _ADF_CACHE_TTL_S:
                self._adf_cache_ts = now
                self._compute_half_life()
                self._compute_adf()

            # Bookkeeping
            self._compute_aggressive_sweep_ratio()

        except Exception:
            pass  # fail-open

    # ───────────────────────────────────────────────────────────────────────────
    # Public interface: on_bar_close
    # ───────────────────────────────────────────────────────────────────────────

    def on_bar_close(self, bar: Any) -> None:
        """Process a microbar close event.

        Args:
            bar: MicroBar with o/h/l/c/vol/end_ts_ms attributes
        """
        try:
            o = float(getattr(bar, "open", 0.0) or 0.0)
            h = float(getattr(bar, "high", 0.0) or 0.0)
            l = float(getattr(bar, "low", 0.0) or 0.0)
            c = float(getattr(bar, "close", 0.0) or 0.0)
            vol = float(getattr(bar, "vol", 0.0) or getattr(bar, "volume", 0.0) or 0.0)
            ts_ms = int(getattr(bar, "end_ts_ms", 0) or 0)

            if o <= 0 or h <= 0 or l <= 0 or c <= 0:
                return

            self._bar_buf.append(_OHLCBar(o=o, h=h, l=l, c=c, volume=vol, ts_ms=ts_ms))

            # ── Group NA: OHLC volatility ──
            self._compute_ohlc_vol()

            # ── Group NB: Amihud + Corwin-Schultz ──
            self._compute_amihud()
            self._compute_corwin_schultz()
            self._compute_hasbrouck()

            # ── Group NF: mid-vwap diff std ──
            try:
                mid = self._last_book_mid if self._last_book_mid > 0 else c
                vwap = float(getattr(bar, "vwap", 0.0) or 0.0)
                if vwap > 0 and mid > 0:
                    self._mid_vwap_diff_buf.append(mid - vwap)
                    if len(self._mid_vwap_diff_buf) >= 5:
                        arr = list(self._mid_vwap_diff_buf)
                        mean = sum(arr) / len(arr)
                        var = sum((x - mean) ** 2 for x in arr) / len(arr)
                        self.mid_vwap_diff_std = math.sqrt(max(0.0, var))
            except Exception:
                pass

        except Exception:
            pass  # fail-open

    # ───────────────────────────────────────────────────────────────────────────
    # Public interface: forward_to_runtime
    # ───────────────────────────────────────────────────────────────────────────

    def forward_to_runtime(self, runtime: Any) -> None:
        """Copy all computed attributes to the runtime object."""
        try:
            for attr in (
                "garman_klass_vol", "parkinson_vol", "yang_zhang_vol", "vol_of_vol",
                "amihud_illiquidity", "corwin_schultz_spread", "hasbrouck_info_share",
                "depth_resilience_half_life",
                "pin_estimate", "kyle_lambda_buy", "kyle_lambda_sell",
                "aggressive_sweep_ratio",
                "price_entropy_50", "order_size_gini", "mutual_info_price_volume",
                "half_life_mean_reversion", "adf_pvalue_50", "mid_vwap_diff_std",
            ):
                with contextlib.suppress(Exception):
                    setattr(runtime, attr, getattr(self, attr, 0.0))
        except Exception:
            pass

    # ───────────────────────────────────────────────────────────────────────────
    # Public interface: snapshot()
    # ───────────────────────────────────────────────────────────────────────────

    def snapshot(self) -> dict[str, float]:
        """Return v13_of NA/NB/NC/NE/NF keys + derived NC/NF helpers.

        All values are floats; missing/uncomputed = 0.0. Designed to merge into
        the outbound `indicators` dict so the v13_of feature schema sees real
        values instead of vectorizer defaults (train/serve skew fix).

        Does NOT include Group NX interactions — those need outer indicators
        (spread_bps, funding_rate, OI delta). Use `compute_interactions()` for
        the NX-keys at the call site that has the indicators dict.
        """
        try:
            # Derived: lambda_asym = (buy - sell) / (buy + sell), clamped to [-1, 1].
            lb = float(getattr(self, "kyle_lambda_buy", 0.0) or 0.0)
            ls = float(getattr(self, "kyle_lambda_sell", 0.0) or 0.0)
            denom = lb + ls
            lambda_asym = (lb - ls) / denom if denom > 1e-12 else 0.0
            lambda_asym = max(-1.0, min(1.0, lambda_asym))

            # Derived: toxicity_regime_score = pin × (1 + sweep ratio).
            # Bounded composite — high PIN + high sweep = toxic regime.
            pin = float(getattr(self, "pin_estimate", 0.0) or 0.0)
            sweep = float(getattr(self, "aggressive_sweep_ratio", 0.0) or 0.0)
            toxicity_regime_score = max(0.0, min(1.0, pin * (1.0 + sweep)))

            # Derived: zscore_mid_to_vwap from rolling mid_vwap diff buffer.
            zscore_mid_to_vwap = 0.0
            try:
                buf = list(self._mid_vwap_diff_buf)
                if len(buf) >= 5:
                    mean = sum(buf) / len(buf)
                    std = self.mid_vwap_diff_std
                    if std > 1e-12:
                        zscore_mid_to_vwap = (buf[-1] - mean) / std
                        zscore_mid_to_vwap = max(-10.0, min(10.0, zscore_mid_to_vwap))
            except Exception:
                pass

            return {
                # Group NA — OHLC volatility
                "garman_klass_vol": self.garman_klass_vol,
                "parkinson_vol": self.parkinson_vol,
                "yang_zhang_vol": self.yang_zhang_vol,
                "vol_of_vol": self.vol_of_vol,
                # Group NB — academic liquidity
                "amihud_illiquidity": self.amihud_illiquidity,
                "corwin_schultz_spread": self.corwin_schultz_spread,
                "hasbrouck_info_share": self.hasbrouck_info_share,
                "depth_resilience_half_life": self.depth_resilience_half_life,
                # Group NC — flow toxicity
                "pin_estimate": self.pin_estimate,
                "aggressive_sweep_ratio": self.aggressive_sweep_ratio,
                "lambda_asym": lambda_asym,
                "toxicity_regime_score": toxicity_regime_score,
                # Group NE — entropy
                "price_entropy_50": self.price_entropy_50,
                "order_size_gini": self.order_size_gini,
                "mutual_info_price_volume": self.mutual_info_price_volume,
                # Group NF — mean reversion
                "half_life_mean_reversion": self.half_life_mean_reversion,
                "adf_pvalue_50": self.adf_pvalue_50,
                "zscore_mid_to_vwap": zscore_mid_to_vwap,
            }
        except Exception:
            return {}

    @staticmethod
    def compute_interactions(
        snap: dict[str, float], indicators: dict[str, Any] | None
    ) -> dict[str, float]:
        """Compute v13_of Group NX interaction features.

        Args:
            snap: result of `snapshot()` (provides amihud, depth_resil, entropy,
                  aggressive_sweep_ratio).
            indicators: outer indicators dict providing spread_bps, funding_rate,
                        open_interest_delta, hurst_exp_50, vol_regime_code, vpin.

        Returns:
            dict with up to 5 NX-keys. Missing source ⇒ key omitted (the
            vectorizer will impute 0.0).
        """
        out: dict[str, float] = {}
        if not snap:
            return out
        ind = indicators or {}

        try:
            depth_resil = float(snap.get("depth_resilience_half_life", 0.0) or 0.0)
            sweep = float(snap.get("aggressive_sweep_ratio", 0.0) or 0.0)
            out["depth_resil_x_sweep"] = depth_resil * sweep
        except Exception:
            pass

        try:
            entropy = float(snap.get("price_entropy_50", 0.0) or 0.0)
            spread_bps = float(ind.get("spread_bps", 0.0) or 0.0)
            out["entropy_x_spread"] = entropy * spread_bps
        except Exception:
            pass

        try:
            amihud = float(snap.get("amihud_illiquidity", 0.0) or 0.0)
            oi_delta = ind.get("open_interest_delta")
            if oi_delta is not None:
                out["amihud_x_oi_delta"] = amihud * float(oi_delta)
        except Exception:
            pass

        try:
            hurst = ind.get("hurst_exp_50")
            vol_regime = ind.get("vol_regime_code")
            if hurst is not None and vol_regime is not None:
                out["hurst_x_vol_regime"] = float(hurst) * float(vol_regime)
        except Exception:
            pass

        try:
            vpin = ind.get("vpin")
            funding = ind.get("funding_rate")
            if vpin is not None and funding is not None:
                f = float(funding)
                sign = 1.0 if f > 0 else (-1.0 if f < 0 else 0.0)
                out["vpin_x_funding"] = float(vpin) * sign
        except Exception:
            pass

        return out

    # ═══════════════════════════════════════════════════════════════════════════
    # Group NA: OHLC Volatility (called on bar close)
    # ═══════════════════════════════════════════════════════════════════════════

    def _compute_ohlc_vol(self) -> None:
        """Compute Garman-Klass, Parkinson, Yang-Zhang volatility and vol-of-vol."""
        bars = list(self._bar_buf)
        n = len(bars)
        if n < 3:
            return

        try:
            gk_sum = 0.0
            pk_sum = 0.0
            closes = []
            overnight_vars = []

            for i, b in enumerate(bars):
                if b.h <= 0 or b.l <= 0 or b.o <= 0 or b.c <= 0:
                    continue

                log_hl = math.log(b.h / b.l)
                log_co = math.log(b.c / b.o)

                # Garman-Klass: 0.5 * ln(H/L)² - (2ln2-1) * ln(C/O)²
                gk_sum += 0.5 * log_hl ** 2 - (2 * _LN2 - 1) * log_co ** 2

                # Parkinson: ln(H/L)²
                pk_sum += log_hl ** 2

                closes.append(b.c)
                if i > 0:
                    prev_c = bars[i - 1].c
                    if prev_c > 0:
                        overnight_vars.append(math.log(b.o / prev_c) ** 2)

            if n > 1:
                self.garman_klass_vol = math.sqrt(max(0.0, gk_sum / n))
                self.parkinson_vol = math.sqrt(max(0.0, pk_sum / (4.0 * n * _LN2)))
            else:
                self.garman_klass_vol = 0.0
                self.parkinson_vol = 0.0

            # Yang-Zhang: σ² = σ_overnight² + k * σ_RS² + (1-k) * σ_cc²
            if len(overnight_vars) >= 2 and len(closes) >= 2:
                k = 0.34 / (1.34 + (n + 1) / (n - 1)) if n > 1 else 0.34
                sigma_o2 = sum(overnight_vars) / len(overnight_vars)
                # Close-to-close variance
                log_ret = [math.log(closes[i] / closes[i - 1]) for i in range(1, len(closes)) if closes[i - 1] > 0]
                if log_ret:
                    mean_lr = sum(log_ret) / len(log_ret)
                    sigma_cc2 = sum((r - mean_lr) ** 2 for r in log_ret) / len(log_ret)
                else:
                    sigma_cc2 = 0.0
                # Rogers-Satchell variance
                rs_sum = 0.0
                rs_cnt = 0
                for b in bars:
                    if b.h > 0 and b.l > 0 and b.o > 0 and b.c > 0:
                        rs_sum += (math.log(b.h / b.c) * math.log(b.h / b.o) +
                                   math.log(b.l / b.c) * math.log(b.l / b.o))
                        rs_cnt += 1
                sigma_rs2 = rs_sum / rs_cnt if rs_cnt > 0 else 0.0

                yz_var = sigma_o2 + k * sigma_rs2 + (1 - k) * sigma_cc2
                self.yang_zhang_vol = math.sqrt(max(0.0, yz_var))
            else:
                self.yang_zhang_vol = 0.0

            # Vol-of-vol: StdDev of realized_vol rolling values
            realized_vol = self.garman_klass_vol * 10_000.0  # convert to bps
            self._vol_bps_buf.append(realized_vol)
            if len(self._vol_bps_buf) >= 5:
                arr = list(self._vol_bps_buf)
                m = sum(arr) / len(arr)
                v = sum((x - m) ** 2 for x in arr) / len(arr)
                self.vol_of_vol = math.sqrt(max(0.0, v))

        except Exception:
            pass

    # ═══════════════════════════════════════════════════════════════════════════
    # Group NB: Academic Liquidity
    # ═══════════════════════════════════════════════════════════════════════════

    def _compute_amihud(self) -> None:
        """Amihud Illiquidity = mean(|return| / volume_USD)."""
        bars = list(self._bar_buf)
        if len(bars) < 3:
            return
        try:
            ratios = []
            for i in range(1, len(bars)):
                c_prev = bars[i - 1].c
                c_curr = bars[i].c
                vol = bars[i].volume * bars[i].c  # notional
                if c_prev > 0 and vol > 1e-6:
                    ret = abs(c_curr / c_prev - 1.0)
                    ratios.append(ret / vol)
            if ratios:
                self.amihud_illiquidity = sum(ratios) / len(ratios)
        except Exception:
            pass

    def _compute_corwin_schultz(self) -> None:
        """Corwin-Schultz implied spread from consecutive H/L bars."""
        bars = list(self._bar_buf)
        if len(bars) < 3:
            return
        try:
            spreads = []
            for i in range(1, len(bars)):
                h1, l1 = bars[i - 1].h, bars[i - 1].l
                h2, l2 = bars[i].h, bars[i].l
                if h1 <= 0 or l1 <= 0 or h2 <= 0 or l2 <= 0:
                    continue
                h_max = max(h1, h2)
                l_min = min(l1, l2)
                beta = (math.log(h1 / l1) ** 2 + math.log(h2 / l2) ** 2)
                gamma = math.log(h_max / l_min) ** 2
                # alpha = (sqrt(2*beta) - sqrt(beta)) / (3 - 2*sqrt(2)) - sqrt(gamma / (3 - 2*sqrt(2)))
                denom = 3 - 2 * math.sqrt(2)
                if denom == 0:
                    continue
                alpha = (math.sqrt(2 * beta) - math.sqrt(beta)) / denom - math.sqrt(gamma / denom)
                spread = 2.0 * (math.exp(alpha) - 1.0) / (1.0 + math.exp(alpha))
                spreads.append(max(0.0, spread * 10_000.0))  # bps
            if spreads:
                self.corwin_schultz_spread = sum(spreads) / len(spreads)
        except Exception:
            pass

    def _compute_hasbrouck(self) -> None:
        """Hasbrouck information share: variance of permanent component / total variance."""
        prices = list(self._price_buf)
        if len(prices) < 20 or np is None:
            return
        try:
            p = np.array(prices[-50:])
            dp = np.diff(p)
            if len(dp) < 10:
                return
            # Decompose: permanent = cumulative trend, transient = noise
            # Simple VAR(1) approximation
            dp1 = dp[:-1]
            dp2 = dp[1:]
            cov = np.cov(dp1, dp2)[0, 1] if len(dp1) > 2 else 0.0
            var_total = np.var(dp)
            if var_total > 1e-12:
                # Permanent variance ~ total variance + 2*cov(lag1)
                var_perm = max(0.0, var_total + 2 * cov)
                self.hasbrouck_info_share = min(1.0, var_perm / var_total)
            else:
                self.hasbrouck_info_share = 0.0
        except Exception:
            pass

    # ═══════════════════════════════════════════════════════════════════════════
    # Group NC: Flow Toxicity
    # ═══════════════════════════════════════════════════════════════════════════

    def _compute_pin(self) -> None:
        """Simplified PIN estimation via buy/sell volume EM."""
        buy_vols = list(self._buy_vol_buf)
        sell_vols = list(self._sell_vol_buf)
        if len(buy_vols) < 20:
            return
        try:
            B = sum(buy_vols)
            S = sum(sell_vols)
            total = B + S
            if total < 1e-6:
                self.pin_estimate = 0.0
                return

            # Simplified PIN: |B - S| / (B + S)
            # This is the volume-based PIN proxy (Easley et al. simplified)
            self.pin_estimate = abs(B - S) / total
        except Exception:
            pass

    def _compute_kyle_split(self) -> None:
        """Split Kyle's lambda by buy/sell side."""
        prices = list(self._price_buf)
        buy_vols = list(self._buy_vol_buf)
        sell_vols = list(self._sell_vol_buf)
        if len(prices) < 20 or np is None:
            return
        try:
            p = np.array(prices[-50:])
            b = np.array(buy_vols[-50:])
            s = np.array(sell_vols[-50:])

            dp = np.diff(p)
            b_t = b[1:]
            s_t = s[1:]

            # Buy lambda: cov(dp, sqrt(buy_vol)) / var(sqrt(buy_vol))
            b_flow = np.sqrt(np.abs(b_t))
            s_flow = np.sqrt(np.abs(s_t))

            if np.var(b_flow) > 1e-12:
                self.kyle_lambda_buy = float(np.clip(
                    np.cov(dp, b_flow)[0, 1] / np.var(b_flow), 0.0, 100.0
                ))
            else:
                self.kyle_lambda_buy = 0.0

            if np.var(s_flow) > 1e-12:
                self.kyle_lambda_sell = float(np.clip(
                    np.cov(dp, s_flow)[0, 1] / np.var(s_flow), 0.0, 100.0
                ))
            else:
                self.kyle_lambda_sell = 0.0

        except Exception:
            pass

    def _compute_aggressive_sweep_ratio(self) -> None:
        """Ratio of sweep trades (crossing 3+ levels) to total trades."""
        try:
            if self._total_trade_count > 0:
                self.aggressive_sweep_ratio = self._sweep_level_count / self._total_trade_count
            else:
                self.aggressive_sweep_ratio = 0.0
        except Exception:
            pass

    # ═══════════════════════════════════════════════════════════════════════════
    # Group NE: Entropy / Information Theory
    # ═══════════════════════════════════════════════════════════════════════════

    def _compute_entropy(self) -> None:
        """Shannon entropy of binned returns (50 ticks, 10 bins)."""
        if len(self._return_buf) < 20:
            return
        try:
            rets = list(self._return_buf)[-50:]
            n = len(rets)
            if n < 10:
                return

            # Bin returns into 10 equal-width bins
            mn, mx = min(rets), max(rets)
            if mx - mn < 1e-12:
                self.price_entropy_50 = 0.0
                return

            n_bins = 10
            counts = [0] * n_bins
            for r in rets:
                idx = min(n_bins - 1, int((r - mn) / (mx - mn) * n_bins))
                counts[idx] += 1

            # Shannon entropy
            entropy = 0.0
            for c in counts:
                if c > 0:
                    p = c / n
                    entropy -= p * math.log2(p)

            self.price_entropy_50 = entropy
        except Exception:
            pass

    def _compute_gini(self) -> None:
        """Gini coefficient of trade sizes in window."""
        sizes = list(self._size_buf)
        if len(sizes) < 10:
            return
        try:
            arr = sorted(sizes)
            n = len(arr)
            total = sum(arr)
            if total < 1e-12:
                self.order_size_gini = 0.0
                return

            # Gini = (2 * Σ(i * y_i) / (n * Σ(y_i))) - (n+1)/n
            weighted_sum = sum((i + 1) * v for i, v in enumerate(arr))
            gini = (2.0 * weighted_sum) / (n * total) - (n + 1) / n
            self.order_size_gini = max(0.0, min(1.0, gini))
        except Exception:
            pass

    def _compute_mutual_info(self) -> None:
        """Mutual information between returns and volume (discretized)."""
        if len(self._return_buf) < 20 or len(self._volume_buf) < 20:
            return
        try:
            n = min(len(self._return_buf), len(self._volume_buf), 100)
            rets = list(self._return_buf)[-n:]
            vols = list(self._volume_buf)[-n:]

            if n < 15:
                return

            # Discretize into 5 bins each
            n_bins = 5

            def _bin(values: list[float]) -> list[int]:
                mn, mx = min(values), max(values)
                if mx - mn < 1e-12:
                    return [0] * len(values)
                out = []
                for v in values:
                    idx = min(n_bins - 1, int((v - mn) / (mx - mn) * n_bins))
                    out.append(idx)
                return out

            r_bins = _bin(rets)
            v_bins = _bin(vols)

            # Joint and marginal counts
            joint = {}
            r_counts = [0] * n_bins
            v_counts = [0] * n_bins
            for rb, vb in zip(r_bins, v_bins):
                key = (rb, vb)
                joint[key] = joint.get(key, 0) + 1
                r_counts[rb] += 1
                v_counts[vb] += 1

            # MI = Σ p(x,y) * log2(p(x,y) / (p(x)*p(y)))
            mi = 0.0
            for (rb, vb), c in joint.items():
                p_xy = c / n
                p_x = r_counts[rb] / n
                p_y = v_counts[vb] / n
                if p_x > 0 and p_y > 0 and p_xy > 0:
                    mi += p_xy * math.log2(p_xy / (p_x * p_y))

            self.mutual_info_price_volume = max(0.0, mi)
        except Exception:
            pass

    # ═══════════════════════════════════════════════════════════════════════════
    # Group NF: Mean Reversion / Stationarity
    # ═══════════════════════════════════════════════════════════════════════════

    def _compute_half_life(self) -> None:
        """Half-life of mean reversion from Ornstein-Uhlenbeck fit."""
        prices = list(self._price_buf)
        if len(prices) < 20 or np is None:
            return
        try:
            p = np.array(prices[-50:])
            # OU: dp = θ(μ - p)dt + σdW
            # Regression: ΔP = α + β*P_{t-1} + ε, half_life = -ln(2)/β
            dp = np.diff(p)
            p_lag = p[:-1]

            if len(dp) < 10:
                return

            # OLS: β = cov(dp, p_lag) / var(p_lag)
            var_p = np.var(p_lag)
            if var_p < 1e-12:
                self.half_life_mean_reversion = 0.0
                return

            beta = np.cov(dp, p_lag)[0, 1] / var_p

            if beta >= 0:
                # Not mean-reverting
                self.half_life_mean_reversion = 0.0
            else:
                hl = -_LN2 / beta
                self.half_life_mean_reversion = float(min(max(hl, 0.0), 10_000.0))
        except Exception:
            pass

    def _compute_adf(self) -> None:
        """ADF test p-value (cached). Fail-open to 1.0 (no stationarity)."""
        prices = list(self._price_buf)
        if len(prices) < 20:
            return
        try:
            # Try statsmodels ADF
            from statsmodels.tsa.stattools import adfuller
            result = adfuller(prices[-50:], maxlag=5, regression="c", autolag=None)
            self.adf_pvalue_50 = float(result[1])  # p-value
        except ImportError:
            # statsmodels not available — fallback: use OU beta significance
            if np is None:
                return
            try:
                p = np.array(prices[-50:])
                dp = np.diff(p)
                p_lag = p[:-1]
                var_p = np.var(p_lag)
                if var_p > 1e-12:
                    beta = np.cov(dp, p_lag)[0, 1] / var_p
                    # Rough approximation: more negative beta → lower p-value
                    self.adf_pvalue_50 = min(1.0, max(0.0, 1.0 + beta * 10.0))
                else:
                    self.adf_pvalue_50 = 1.0
            except Exception:
                self.adf_pvalue_50 = 1.0
        except Exception:
            self.adf_pvalue_50 = 1.0
