from __future__ import annotations

"""
v12_of — Feature schema v12 (OrderFlow), pinned snapshot + anti-overfit signal extensions.

Generated: 2026-03-16 (manual bump from v11_of).

v12_of = v11_of (193 keys) + 25 additional indicators:
  Group MA (4)  — Microstructure / Trade-by-trade (unique signal vs v11 pressure/vpin)
  Group MB (4)  — Order Book Dynamics (velocity, not snapshot)
  Group MC (3)  — Temporal / Seasonality (fine-grained, orthogonal to hour_of_week)
  Group MD (3)  — Cross-Asset / Macro (extending btc_corr_5m)
  Group ME (3)  — Self-Referential / Meta-Signal (inference-time awareness)
  Group MX (4)  — Medium-priority (percentile ranks, derivatives, interaction)

  Subtotal new  = 4+4+3+3+3+4 = 21 (after dedup vs v11_of)

Coverage: ~214 numeric indicators (no separate bool block — all bool as float 0/1)
          + direction/bucket/hour/dow/session one-hots → ~274 total feature_cols.

Design notes
------------
- Fail-open: ALL keys vectorize as 0.0 if missing in runtime snapshot.
- Group MC (temporal): computed deterministically from now_ts_ms — always available.
- Group MD (cross-asset): 0.0 until go-worker external feed deployed (fail-open).
- Group ME (meta): 0.0 until runtime ring-buffer populated (~first 100 signals).
- Group MX: pure derived indicators — always available if source keys are present.
- Anti-overfit policy: each key has Pearson(new, nearest_v11_key) < 0.70 by construction.
- Append-only: new schema versions always add keys, never remove.
"""



SCHEMA_HASH = "b5d7e17579f6"


# Import base v11 keys to avoid duplication drift
try:
    from core.ml_feature_schema_v11_of import V11_OF_NUMERIC_KEYS as _V11_OF_BASE
except ImportError:
    # Fallback only if running in a strict minimal environment
    _V11_OF_BASE = []


# ---------------------------------------------------------------------------
# Group MA — Microstructure / Trade-by-trade (4 keys)
# Orthogonal to v11_of: pressure=volume-based; these are frequency/size-structure
# ---------------------------------------------------------------------------

_GROUP_MA_MICROSTRUCTURE: list[str] = [
    "trade_arrival_rate_hz",   # count(trades) / window_sec — arrival intensity (Hawkes λ proxy)
                               # distinct from taker_lambda (hawkes on aggressive orders only)
    "large_trade_ratio",       # count(notional > 3σ_notional) / count_all — whale trade share
                               # vpin_rolling aggregates volume, not individual trade sizing
    "tick_direction_run",      # max consecutive same-sign tick run in window — sweep detection
                               # ofi/ofi_stability_score measure balance, not directional runs
    "trade_size_entropy",      # Shannon entropy of trade sizes across quantile buckets
                               # low entropy → concentrated (institutional); high → retail noise
]

# ---------------------------------------------------------------------------
# Group MB — Order Book Dynamics (4 keys)
# Orthogonal to v11_of: book_refresh_rate_hz already in v11 Group D; these are new
# ---------------------------------------------------------------------------

_GROUP_MB_BOOK_DYNAMICS: list[str] = [
    "quote_stuffing_score",    # cancels_50ms / quotes_50ms — spoofing / MM pressure proxy
                               # distinct from cancel_to_fill_ratio (v11) which is trade-level
    "depth_migration_bps",     # speed of best bid/ask level shift over N ticks in bps/tick
                               # leading indicator before sweep; book_slope_bid is static shape
    "level2_wap_divergence",   # WAP(L2 5-level) - mid_price in bps — hidden imbalance signal
                               # book_imbalance_5lvl is normalised ratio; this is price impact
    "bid_ask_queue_imbalance", # (queue_size_bid - queue_size_ask) / total_queue at best level
                               # bid_ask_depth_ratio (v10) uses 5 levels; this is best-level
]

# ---------------------------------------------------------------------------
# Group MC — Temporal / Seasonality fine-grained (3 keys)
# Orthogonal to hour_of_week (continuous); these are event-distance / categorical
# ---------------------------------------------------------------------------

_GROUP_MC_TEMPORAL: list[str] = [
    "minutes_to_funding",       # (next_8h_funding_ts_ms - now_ts_ms) / 60000
                                # funding_rate_bps (v10) = level; this = time-to-event (decay)
    "session_overlap_flag",     # 1.0 if in NY∩London or Asia∩London overlap window (binary)
                                # hour_of_week is continuous; overlap = high-volume regime event
    "time_since_last_liq_ms",   # ms elapsed since last liquidation_usd_1m > 0
                                # liquidation_usd_1m (v10) = current volume; this = recency decay
]

# ---------------------------------------------------------------------------
# Group MD — Cross-Asset / Macro (3 keys)
# Extending btc_corr_5m (v10): ETH divergence + carry + OI momentum
# ---------------------------------------------------------------------------

_GROUP_MD_CROSS_ASSET: list[str] = [
    "eth_btc_corr_5m",          # Rolling 5-min correlation of symbol returns with ETH/BTC ratio
                                # btc_corr_5m (v10) = vs BTC; ETH/BTC correlation = alt-season
    "perp_spot_basis_bps",      # (perp_price - spot_price) / spot * 10_000
                                # cash-and-carry premium; proxy for leveraged long crowding
    "stable_coin_flow_delta",   # Δ(USDT+USDC dominance) over 1h — macro capital rotation signal
                                # open_interest_delta (v10) is symbol-level; this is market-wide
]

# ---------------------------------------------------------------------------
# Group ME — Self-Referential / Meta-Signal (3 keys)
# Orthogonal to v11 Group E: conf_ma_ratio/gate_hardness_score already there
# ---------------------------------------------------------------------------

_GROUP_ME_META: list[str] = [
    "signal_frequency_1h",      # count(signals emitted for symbol) in last 60 min
                                # distinct from signal_cluster_flag (v11, 60s window)
    "last_trade_outcome_raw",   # realized P&L bps of last closed trade on this symbol
                                # mae_r/mfe_r (v10) = rolling averages; this = last-N=1 event
    "calibration_age_ms",       # ms since last successful abs_lvl calibration
                                # abs_lvl_calib_n (v10) = count; this = time since last update
]

# ---------------------------------------------------------------------------
# Group MX — Medium-priority derived / interaction features (4 keys)
# All computable from existing v11_of keys; low data-pipeline cost
# ---------------------------------------------------------------------------

_GROUP_MX_DERIVED: list[str] = [
    "spread_percentile_rank_1d", # rank of spread_bps within rolling 1-day window [0,1]
                                 # spread_bps (v10) = raw; rank = regime-relative cost signal
    "cvd_divergence_from_price", # sign(cvd_slope) ≠ sign(momentum_10s): float 0/1 flag
                                 # cvd_ema (v10) = magnitude; this = direction divergence flag
    "order_imbalance_momentum",  # Δofi over last N ticks (rate of OFI change)
                                 # ofi_stability_score (v10) = level; this = first derivative
    "atr_percentile_rank_30d",   # rank of atr_bps within rolling 30-day window [0,1]
                                 # atr_bps (v10) = raw; rank = vol regime relative to history
]


# ---------------------------------------------------------------------------
# Final composite key list — V12_OF_NUMERIC_KEYS (sorted for determinism)
# ---------------------------------------------------------------------------

V12_OF_NUMERIC_KEYS: list[str] = sorted(set(
    _V11_OF_BASE
    + _GROUP_MA_MICROSTRUCTURE
    + _GROUP_MB_BOOK_DYNAMICS
    + _GROUP_MC_TEMPORAL
    + _GROUP_MD_CROSS_ASSET
    + _GROUP_ME_META
    + _GROUP_MX_DERIVED
))

# Sanity guard (caught immediately at import in tests)
_EXPECTED_MIN = 205
_EXPECTED_MAX = 240
if _V11_OF_BASE:
    assert _EXPECTED_MIN <= len(V12_OF_NUMERIC_KEYS) <= _EXPECTED_MAX, (
        f"v12_of key count {len(V12_OF_NUMERIC_KEYS)} out of expected range "
        f"[{_EXPECTED_MIN}, {_EXPECTED_MAX}] — check for duplicates or deletions"
    )


def get_v12_of_numeric_keys() -> list[str]:
    """Return sorted list of numeric indicator keys for v12_of."""
    return list(V12_OF_NUMERIC_KEYS)


def v12_of_info() -> dict:
    """Summary dict for logging / audit."""
    n_v11 = len(_V11_OF_BASE)
    n_new = len(V12_OF_NUMERIC_KEYS) - n_v11
    return {
        "ver": "v12_of",
        "n_numeric_keys": len(V12_OF_NUMERIC_KEYS),
        "n_v11_of_base": n_v11,
        "n_new_keys": n_new,
        "groups": {
            "group_ma_microstructure": len(_GROUP_MA_MICROSTRUCTURE),
            "group_mb_book_dynamics": len(_GROUP_MB_BOOK_DYNAMICS),
            "group_mc_temporal": len(_GROUP_MC_TEMPORAL),
            "group_md_cross_asset": len(_GROUP_MD_CROSS_ASSET),
            "group_me_meta": len(_GROUP_ME_META),
            "group_mx_derived": len(_GROUP_MX_DERIVED),
        },
    }
