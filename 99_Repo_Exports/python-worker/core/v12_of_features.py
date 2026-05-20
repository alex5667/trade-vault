from __future__ import annotations

"""
core/v12_of_features.py
=======================
Compute functions for the 21 new v12_of indicator keys (Groups MA–MX).

Design principles:
  - Every function is FAIL-OPEN: exceptions return 0.0.
  - Train == Serve: same code runs in tick_processor.py and offline dataset builder.
  - All keys match ml_feature_schema_v12_of.py exactly.
  - No heavy imports; only stdlib math + existing runtime attributes.
"""


from typing import Any
import contextlib

# ---------------------------------------------------------------------------
# Group MA — Microstructure / Trade-by-trade
# Keys: trade_arrival_rate_hz, large_trade_ratio, tick_direction_run,
#       trade_size_entropy
# ---------------------------------------------------------------------------

def compute_group_ma(runtime: Any, now_ms: int, indicators: dict[str, Any]) -> dict[str, float]:
    """
    Group MA: per-tick microstructure features derived from rolling trade stats
    stored in SymbolRuntime.

    Runtime attributes consumed (all fail-open to 0.0 if absent):
      - trade_arrival_rate_hz : float — trades/second in rolling window
                                fallback indicators[book_rate_hz] (event rate
                                Hz — semantically closest signal in payload)
      - large_trade_ratio     : float — share of notional > 3σ in window
                                (no fallback — needs raw trade tracker;
                                vectorizes to 0.0)
      - tick_direction_run    : int   — max consecutive same-sign tick runs;
                                fallback obi_stable_secs * book_rate_hz
                                (≈ ticks during direction-stable window)
      - trade_size_entropy    : float — Shannon entropy of trade size
                                distribution (no fallback)
    """
    out: dict[str, float] = {}

    # trade_arrival_rate_hz — runtime → book_rate_hz fallback
    try:
        v = float(getattr(runtime, "trade_arrival_rate_hz", 0.0) or 0.0)
        if v == 0.0:
            v = float(indicators.get("book_rate_hz", 0.0) or 0.0)
        out["trade_arrival_rate_hz"] = v
    except Exception:
        out["trade_arrival_rate_hz"] = 0.0

    # large_trade_ratio — runtime only (raw trade tracker required)
    try:
        out["large_trade_ratio"] = float(
            getattr(runtime, "large_trade_ratio", 0.0) or 0.0
        )
    except Exception:
        out["large_trade_ratio"] = 0.0

    # tick_direction_run — runtime → indicators-fallback (audit 2026-05-19
    # Phase 7). signal_pipeline.publish_signal pre-populates this with a
    # per-symbol signal-direction-run counter (capped at 50). It's a
    # signal-granularity proxy (vs original tick-granularity) — values
    # naturally land in expected [1, 20] range. Old obi_stable_secs×book_rate_hz
    # proxy removed: produced 25-1200 (OOD for ML inputs).
    try:
        v = float(getattr(runtime, "tick_direction_run", 0) or 0)
        if v == 0.0:
            v = float(indicators.get("tick_direction_run", 0.0) or 0.0)
        out["tick_direction_run"] = v
    except Exception:
        out["tick_direction_run"] = 0.0

    # trade_size_entropy — runtime only (raw trade tracker required)
    try:
        out["trade_size_entropy"] = float(
            getattr(runtime, "trade_size_entropy", 0.0) or 0.0
        )
    except Exception:
        out["trade_size_entropy"] = 0.0

    return out


# ---------------------------------------------------------------------------
# Group MB — Order Book Dynamics
# Keys: quote_stuffing_score, depth_migration_bps, level2_wap_divergence,
#       bid_ask_queue_imbalance
#
# Note: depth_migration_bps (raw snapshot) from book tracker; the EMA
# version (depth_migration_bps_ema) lives in runtime:crossasset via go-worker.
# Both are available.
# ---------------------------------------------------------------------------

def compute_group_mb(runtime: Any, now_ms: int, indicators: dict[str, Any]) -> dict[str, float]:
    """
    Group MB: order book dynamics.

    Runtime attributes (primary) + indicators-fallback (audit 2026-05-19):
      - quote_stuffing_score   : float — cancel_50ms / quote_50ms
      - depth_migration_bps    : float — best bid/ask shift velocity (bps/tick)
      - level2_wap_divergence  : float — WAP(5L) - mid_price in bps;
                                  fallback indicators[micro_mid_shift_vel_bps_s]
      - bid_ask_queue_imbalance: float — (best_bid_qty - best_ask_qty) / total_best_qty;
                                  fallback indicators[depth_imbalance_5] (top-5 imbalance)
    """
    out: dict[str, float] = {}

    for key in (
        "quote_stuffing_score",
        "depth_migration_bps",
        "level2_wap_divergence",
        "bid_ask_queue_imbalance",
    ):
        try:
            out[key] = float(getattr(runtime, key, 0.0) or 0.0)
        except Exception:
            out[key] = 0.0

    # Indicators-fallback when runtime trackers are unwired (TickProcessor lives
    # in reference/ — BookProcessor in prod doesn't update these runtime attrs).
    if out["bid_ask_queue_imbalance"] == 0.0:
        try:
            v = indicators.get("depth_imbalance_5")
            if v is not None:
                out["bid_ask_queue_imbalance"] = float(v)
        except Exception:
            pass
    if out["level2_wap_divergence"] == 0.0:
        try:
            v = indicators.get("micro_mid_shift_vel_bps_s")
            if v is not None:
                out["level2_wap_divergence"] = float(v)
        except Exception:
            pass

    return out


# ---------------------------------------------------------------------------
# Group MC — Temporal / Seasonality
# Keys: minutes_to_funding, session_overlap_flag, time_since_last_liq_ms
# ---------------------------------------------------------------------------

# Funding timestamps are every 8h: 00:00, 08:00, 16:00 UTC
_FUNDING_INTERVAL_MS = 8 * 3600 * 1000


def _next_funding_ts_ms(now_ms: int) -> int:
    """Compute next 8h UTC funding boundary in epoch ms."""
    interval = _FUNDING_INTERVAL_MS
    return now_ms + (interval - now_ms % interval)


# Session overlap windows in UTC hours (start_h, end_h, inclusive-exclusive)
_SESSION_OVERLAPS = [
    (8, 12),   # London ∩ Asia tail (~08:00–12:00 UTC)
    (13, 17),  # London ∩ NY (~13:00–17:00 UTC)
]


def _is_session_overlap(now_ms: int) -> float:
    """Return 1.0 if current UTC time is in a known high-volume session overlap window."""
    try:
        hour_utc = (now_ms // 3_600_000) % 24
        for start_h, end_h in _SESSION_OVERLAPS:
            if start_h <= hour_utc < end_h:
                return 1.0
    except Exception:
        pass
    return 0.0


_LIQ_STALE_SENTINEL_MS = 100_000_000  # 100M ms = ~28h → producer's "never seen" sentinel


def compute_group_mc(runtime: Any, now_ms: int, indicators: dict[str, Any]) -> dict[str, float]:
    """
    Group MC: temporal/seasonality features.

    minutes_to_funding  — derived deterministically from now_ms (Train==Serve ✓)
    session_overlap_flag— derived from UTC hour (Train==Serve ✓)
    time_since_last_liq_ms — runtime.liq_last_ts_ms (updated when liq_usd>0);
                             fallback indicators[liq_book_stale_ms] when < sentinel
    """
    out: dict[str, float] = {}

    # minutes_to_funding (always computable)
    try:
        next_fund_ms = _next_funding_ts_ms(int(now_ms))
        out["minutes_to_funding"] = float(max(0.0, next_fund_ms - now_ms) / 60_000.0)
    except Exception:
        out["minutes_to_funding"] = 0.0

    # session_overlap_flag (always computable)
    try:
        out["session_overlap_flag"] = _is_session_overlap(int(now_ms))
    except Exception:
        out["session_overlap_flag"] = 0.0

    # time_since_last_liq_ms — runtime first, then indicators fallback
    try:
        liq_last_ts = int(getattr(runtime, "liq_last_ts_ms", 0) or 0)
        if liq_last_ts > 0:
            out["time_since_last_liq_ms"] = float(max(0.0, now_ms - liq_last_ts))
        else:
            # Indicators-fallback (audit 2026-05-19): liq_book_stale_ms reflects
            # book staleness vs last liquidation update; clip 1e8 sentinel
            # ("never seen") to 0 so ML doesn't get a magic-number outlier.
            try:
                raw = float(indicators.get("liq_book_stale_ms", 0.0) or 0.0)
                out["time_since_last_liq_ms"] = raw if 0.0 < raw < _LIQ_STALE_SENTINEL_MS else 0.0
            except Exception:
                out["time_since_last_liq_ms"] = 0.0
    except Exception:
        out["time_since_last_liq_ms"] = 0.0

    return out


# ---------------------------------------------------------------------------
# Group MD — Cross-Asset / Macro
# Keys: eth_btc_corr_5m, perp_spot_basis_bps, stable_coin_flow_delta
#
# All sourced from runtime:crossasset:{SYM} Redis Hash via maybe_load_crossasset().
# Fail-open to 0.0.
# ---------------------------------------------------------------------------

def compute_group_md(runtime: Any, now_ms: int, indicators: dict[str, Any]) -> dict[str, float]:
    """
    Group MD: cross-asset macro features loaded from go-worker → Redis hash.
    SymbolRuntime.maybe_load_crossasset() populates the runtime attributes every ~5s.

    Indicators-fallback (audit 2026-05-19):
      - perp_spot_basis_bps falls back to indicators[basis_bps] (canonical
        alias from derivatives_context_collector_v1).
    """
    out: dict[str, float] = {}

    for key in (
        "eth_btc_corr_5m",
        "perp_spot_basis_bps",
        "stable_coin_flow_delta",
    ):
        try:
            out[key] = float(getattr(runtime, key, 0.0) or 0.0)
        except Exception:
            out[key] = 0.0

    if out["perp_spot_basis_bps"] == 0.0:
        try:
            v = indicators.get("basis_bps")
            if v is not None:
                out["perp_spot_basis_bps"] = float(v)
        except Exception:
            pass

    return out


# ---------------------------------------------------------------------------
# Group ME — Self-Referential / Meta-Signal
# Keys: signal_frequency_1h, last_trade_outcome_raw, calibration_age_ms
# ---------------------------------------------------------------------------

def compute_group_me(runtime: Any, now_ms: int, indicators: dict[str, Any]) -> dict[str, float]:
    """
    Group ME: meta-signal / self-referential features.

    Indicators-fallback (audit 2026-05-19) used when runtime trackers are
    unwired:
      signal_frequency_1h   — indicators['signal_frequency_1h'] (populated by
                              signal_pipeline.publish_signal counter), then
                              runtime.signal_count_1h
      last_trade_outcome_raw— runtime.last_trade_pnl_bps
                              (no indicator equivalent; needs trade_close hook)
      calibration_age_ms    — runtime.abs_lvl_calib_last_ts_ms → fallback
                              indicators['atr_age_ms'] (ATR is also a
                              periodic calibrator; same age semantics)
    """
    out: dict[str, float] = {}

    # signal_frequency_1h — prefer indicators (populated by signal_pipeline counter)
    try:
        v = indicators.get("signal_frequency_1h")
        if v not in (None, 0, 0.0):
            out["signal_frequency_1h"] = float(v)
        else:
            out["signal_frequency_1h"] = float(
                getattr(runtime, "signal_count_1h", 0) or 0
            )
    except Exception:
        out["signal_frequency_1h"] = 0.0

    # last_trade_outcome_raw — runtime → indicators fallback (audit 2026-05-19
    # Phase 4: trade-close pipeline writes to Redis trades:closed, not to
    # runtime. signal_pipeline.publish_signal() reads it from there and
    # injects into indicators before compute_group_me runs.
    try:
        v_rt = float(getattr(runtime, "last_trade_pnl_bps", 0.0) or 0.0)
        if v_rt != 0.0:
            out["last_trade_outcome_raw"] = v_rt
        else:
            try:
                v_ind = float(indicators.get("last_trade_outcome_raw", 0.0) or 0.0)
                out["last_trade_outcome_raw"] = v_ind
            except Exception:
                out["last_trade_outcome_raw"] = 0.0
    except Exception:
        out["last_trade_outcome_raw"] = 0.0

    # calibration_age_ms — runtime → indicators fallback (atr_age_ms)
    try:
        calib_ts = int(getattr(runtime, "abs_lvl_calib_last_ts_ms", 0) or 0)
        if calib_ts > 0:
            out["calibration_age_ms"] = float(max(0.0, now_ms - calib_ts))
        else:
            # ATR is recomputed each bar; its age is a reasonable calibration
            # freshness proxy on the same ms scale.
            try:
                v = float(indicators.get("atr_age_ms", 0.0) or 0.0)
                out["calibration_age_ms"] = v if v > 0 else 0.0
            except Exception:
                out["calibration_age_ms"] = 0.0
    except Exception:
        out["calibration_age_ms"] = 0.0

    return out


# ---------------------------------------------------------------------------
# Group MX — Derived interaction features (computed from existing indicators)
# Keys: spread_percentile_rank_1d, cvd_divergence_from_price,
#       order_imbalance_momentum, atr_percentile_rank_30d
# ---------------------------------------------------------------------------

def compute_group_mx(runtime: Any, now_ms: int, indicators: dict[str, Any]) -> dict[str, float]:
    """
    Group MX: derived features computed from runtime rolling state + indicators.

    spread_percentile_rank_1d  — runtime.spread_bps_rank_1d  [0.0, 1.0]
    cvd_divergence_from_price  — sign(cvd_slope) ≠ sign(momentum_10s): 0.0 or 1.0
    order_imbalance_momentum   — delta of ofi over last N ticks (first derivative of OFI)
    atr_percentile_rank_30d    — runtime.atr_bps_rank_30d  [0.0, 1.0]
    """
    out: dict[str, float] = {}

    # spread_percentile_rank_1d — runtime tracker first; indicators-fallback
    # (audit 2026-05-19): if v15_of TCA rolling p95 is published, approximate
    # rank as clamp(spread_bps / p95_threshold) → ~0.95 at p95, scales linearly
    # below; clamped at 1.0 above. NOT a true rank but bounded [0, 1] proxy
    # on the same scale (informative for ML, train==serve via same formula).
    try:
        rank = float(getattr(runtime, "spread_bps_rank_1d", 0.0) or 0.0)
        if rank == 0.0:
            cur = float(indicators.get("spread_bps", 0.0) or 0.0)
            p95 = float(
                indicators.get("spread_p95_bps_symbol_kind_session", 0.0)
                or 0.0
            )
            if cur > 0.0 and p95 > 0.0:
                rank = min(1.0, (cur / p95) * 0.95)
        out["spread_percentile_rank_1d"] = rank
    except Exception:
        out["spread_percentile_rank_1d"] = 0.0

    # cvd_divergence_from_price: 1.0 if sign(cvd_slope/ema_delta) ≠ sign(price momentum)
    # Indicators-fallback (audit 2026-05-19): cvd_slope/momentum_10s absent in
    # active producer; substitute cvd_ema sign × delta_z sign — same intent
    # (cumulative volume direction vs short-term price direction).
    try:
        cvd_slope = float(indicators.get("cvd_slope", 0.0) or 0.0)
        mom = float(indicators.get("momentum_10s", 0.0) or 0.0)
        if cvd_slope == 0.0:
            cvd_slope = float(indicators.get("cvd_ema", 0.0) or 0.0)
        if mom == 0.0:
            mom = float(indicators.get("delta_z", 0.0) or 0.0)
        if cvd_slope != 0.0 and mom != 0.0:
            diverge = 1.0 if (cvd_slope > 0) != (mom > 0) else 0.0
        else:
            diverge = 0.0
        out["cvd_divergence_from_price"] = diverge
    except Exception:
        out["cvd_divergence_from_price"] = 0.0

    # order_imbalance_momentum: Δofi (current - previous ofi in runtime)
    # Indicators-fallback (audit 2026-05-19): canonical `ofi` absent in
    # producer naming; fall back to `ofi_ml_norm` (normalized OFI for ML).
    try:
        ofi_now = float(
            indicators.get("ofi", indicators.get("ofi_ml_norm", 0.0)) or 0.0
        )
        ofi_prev = float(getattr(runtime, "ofi_prev_tick", 0.0) or 0.0)
        out["order_imbalance_momentum"] = ofi_now - ofi_prev
        # Update runtime for next tick (best-effort, fail-silent)
        with contextlib.suppress(Exception):
            runtime.ofi_prev_tick = ofi_now
    except Exception:
        out["order_imbalance_momentum"] = 0.0

    # atr_percentile_rank_30d — runtime tracker first; indicators-fallback
    # (audit 2026-05-19): atr_bps_th is the regime-floor threshold (~p50-p75
    # in practice). Use clamp(atr_bps / (2 * atr_bps_th)) so threshold-level
    # atr lands at ~0.5 and elevated regimes saturate to 1.0. Bounded [0, 1].
    try:
        rank = float(getattr(runtime, "atr_bps_rank_30d", 0.0) or 0.0)
        if rank == 0.0:
            atr = float(indicators.get("atr_bps", 0.0) or 0.0)
            atr_th = float(indicators.get("atr_bps_th", 0.0) or 0.0)
            if atr > 0.0 and atr_th > 0.0:
                rank = min(1.0, atr / (2.0 * atr_th))
        out["atr_percentile_rank_30d"] = rank
    except Exception:
        out["atr_percentile_rank_30d"] = 0.0

    return out


# ---------------------------------------------------------------------------
# Master injection entry point
# ---------------------------------------------------------------------------

def inject_v12_of_features(
    *,
    runtime: Any,
    now_ms: int,
    indicators: dict[str, Any],
) -> None:
    """
    Compute and inject all 21 v12_of new indicator keys into `indicators`.

    Called from TickProcessor after the v10_of Group 2E block.
    Fail-open: any exception in a group is silently swallowed; keys default to 0.0.

    Groups:
      MA — Microstructure trade-by-trade    (4 keys)
      MB — Order Book Dynamics              (4 keys)
      MC — Temporal / Seasonality           (3 keys)
      MD — Cross-Asset Macro                (3 keys)
      ME — Meta-Signal / Self-referential   (3 keys)
      MX — Derived interaction              (4 keys)
    """
    _DEFAULTS: dict[str, float] = {
        # MA
        "trade_arrival_rate_hz": 0.0,
        "large_trade_ratio": 0.0,
        "tick_direction_run": 0.0,
        "trade_size_entropy": 0.0,
        # MB
        "quote_stuffing_score": 0.0,
        "depth_migration_bps": 0.0,
        "level2_wap_divergence": 0.0,
        "bid_ask_queue_imbalance": 0.0,
        # MC
        "minutes_to_funding": 0.0,
        "session_overlap_flag": 0.0,
        "time_since_last_liq_ms": 0.0,
        # MD
        "eth_btc_corr_5m": 0.0,
        "perp_spot_basis_bps": 0.0,
        "stable_coin_flow_delta": 0.0,
        # ME
        "signal_frequency_1h": 0.0,
        "last_trade_outcome_raw": 0.0,
        "calibration_age_ms": 0.0,
        # MX
        "spread_percentile_rank_1d": 0.0,
        "cvd_divergence_from_price": 0.0,
        "order_imbalance_momentum": 0.0,
        "atr_percentile_rank_30d": 0.0,
    }
    # Pre-set defaults so keys always exist even if a group raises
    for k, v in _DEFAULTS.items():
        indicators.setdefault(k, v)

    _groups = [
        compute_group_ma,
        compute_group_mb,
        compute_group_mc,
        compute_group_md,
        compute_group_me,
        compute_group_mx,
    ]
    for fn in _groups:
        try:
            result = fn(runtime, now_ms, indicators)
            indicators.update(result)
        except Exception:
            pass  # fail-open: defaults already set above


# ---------------------------------------------------------------------------
# Completeness check (import-time guard; caught by unit tests)
# ---------------------------------------------------------------------------

_V12_OF_NEW_KEYS = frozenset(_DEFAULTS for _DEFAULTS in [  # type: ignore[assignment]
    {
        "trade_arrival_rate_hz", "large_trade_ratio", "tick_direction_run", "trade_size_entropy",
        "quote_stuffing_score", "depth_migration_bps", "level2_wap_divergence", "bid_ask_queue_imbalance",
        "minutes_to_funding", "session_overlap_flag", "time_since_last_liq_ms",
        "eth_btc_corr_5m", "perp_spot_basis_bps", "stable_coin_flow_delta",
        "signal_frequency_1h", "last_trade_outcome_raw", "calibration_age_ms",
        "spread_percentile_rank_1d", "cvd_divergence_from_price", "order_imbalance_momentum", "atr_percentile_rank_30d",
    }
][0])  # flatten to frozenset
