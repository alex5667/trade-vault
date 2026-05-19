from __future__ import annotations

"""v15_of — v14_of (359 keys) + 156 additions from Phase 8.2/8.3/8.4/8.5/
P1/P2/P3/4.x already emitted by core/external_features_payload_v1.py but
absent from v14_of.

Append-only: v14_of ⊆ v15_of. The v14_of canary remains valid; v15_of is a
new schema that requires its own training + Redis pin
(cfg:feature_registry:edge_stack:v15_of) before any rollout.

Created: 2026-05-18 — closes schema-gap identified in
audit_v14_of_schema_gap_fixes_2026_05_18.

Groups added on top of v14_of
-----------------------------
P82 (Phase 8.2 — time/gate/sentiment basics, 7 keys)
P83 (Phase 8.3 — taker ratio + force-order notionals, 7 keys)
P84 (Phase 8.4 — Hawkes/VPIN raw intensities + composites, 27 keys)
P85_XV (cross-venue Binance↔Bybit sanity, 4 keys)
P85_XVI (CoinGecko macro context, 7 keys)
P85_XVII (Deribit extended — options OI + per-symbol basis, 3 keys)
P85_XVIII (DefiLlama slow-regime context, 5 keys)
P1_DERIBIT (Deribit term structure tenor + put/call, 8 keys)
P1_BREADTH (5m breadth volume + z, 2 keys)
P1_RELSTR (symbol relative strength vs btc/market/sector, 3 keys)
P2_BYBIT (Bybit cross-venue ingest, 6 keys)
P3_FG (Fear & Greed delta, 1 key)
P3_CP (CoinPaprika fallback, 4 keys)
P3_CMC (CoinMarketCap fallback, 4 keys)
P3_DL_EXT (DefiLlama extended, 4 keys)
DERIV_BASE (funding/OI/basis/liq base from ctx:deriv, 23 keys)
PIT_PRIORS (PIT priors session + 7d rolling + 30d MAE/MFE, 18 keys)
MACRO_CAL (macro event calendar proximity, 3 keys)
ROLL_VPIN (VPIN 1m/5m + slope, 3 keys)
SECTOR_AGG (sector aggregation across symbols, 2 keys)
LIQMAP_ALIAS (liq heatmap density/distance aliases, 4 keys)
LOB_ADD_RATE (limit_add Hawkes split + added-rate EMA, 6 keys per spec; 0 new)
LEADER_FLAGS (BTC/ETH leader confirm + direction conflict, 2 keys)
TIME_CYC (cyclical hour/dow encoding, 4 keys)
PRIOR_STALE (PIT prior staleness 0/1 + age ms, 2 keys)
"""


SCHEMA_HASH = "v15of_v14base_p82_p83_p84_p85_p1_p2_p3_2026_05_18"


# ──────────────────────────────────────────────────────────────────────────────
# Group P82 — Phase 8.2 cyclical time + sector breadth basics
# ──────────────────────────────────────────────────────────────────────────────
_GROUP_P82_TIME_CYC: list[str] = [
    "hour_sin", "hour_cos", "dow_sin", "dow_cos",
    "sector_breadth_1m",
    "prior_stale", "prior_stale_ms",
]

# ──────────────────────────────────────────────────────────────────────────────
# Group P83 — Phase 8.3 taker/top-trader ratios + force-order per-side
# ──────────────────────────────────────────────────────────────────────────────
_GROUP_P83_TAKER_FORCE: list[str] = [
    "taker_buy_sell_ratio",
    "taker_buy_sell_ratio_z",
    "top_trader_long_short_ratio",
    "force_order_long_notional_1m",
    "force_order_short_notional_1m",
    "force_order_cluster_score",
    "futures_crowding_score",
]

# ──────────────────────────────────────────────────────────────────────────────
# Group P84 — Phase 8.4 Hawkes/VPIN raw intensities + derived composites
# ──────────────────────────────────────────────────────────────────────────────
_GROUP_P84_HAWKES_VPIN: list[str] = [
    "hawkes_dt_s",
    "hawkes_taker_buy_lam", "hawkes_taker_sell_lam",
    "hawkes_cancel_bid_lam", "hawkes_cancel_ask_lam",
    "hawkes_limit_add_lam",
    "hawkes_limit_add_bid_lam", "hawkes_limit_add_ask_lam",
    "hawkes_limit_add_imbalance",
    "hawkes_taker_lam", "hawkes_cancel_lam", "hawkes_churn_lam",
    "hawkes_S_taker_buy", "hawkes_S_taker_sell",
    "hawkes_S_cancel_bid", "hawkes_S_cancel_ask",
    "hawkes_S_limit_add",
    "hawkes_buy_sell_lam_ratio", "hawkes_cancel_imbalance",
    "added_bid_rate_ema", "added_ask_rate_ema", "added_total_rate_ema",
    "vpin_tox_ema", "vpin_tox_z",
    "vpin_tox_1m", "vpin_tox_5m", "vpin_tox_slope",
]

# ──────────────────────────────────────────────────────────────────────────────
# Group P85_XV — cross-venue sanity (Binance ↔ Bybit divergence)
# ──────────────────────────────────────────────────────────────────────────────
_GROUP_P85_XV_CROSS_VENUE: list[str] = [
    "cross_venue_agree_score",
    "cross_venue_dislocation_bps",
    "cross_venue_dislocation_z",
    "binance_local_noise_score",
]

# ──────────────────────────────────────────────────────────────────────────────
# Group P85_XVI — CoinGecko macro context
# ──────────────────────────────────────────────────────────────────────────────
_GROUP_P85_XVI_COINGECKO: list[str] = [
    "cg_btc_dom_pct", "cg_stable_dom_pct", "cg_btc_dom_mom",
    "cg_global_turnover", "cg_symbol_rank",
    "cg_rel_strength_btc_1h", "cg_volume_mcap_ratio",
]

# ──────────────────────────────────────────────────────────────────────────────
# Group P85_XVII — Deribit extended (options OI + per-symbol perp basis)
# ──────────────────────────────────────────────────────────────────────────────
_GROUP_P85_XVII_DERIBIT_EXT: list[str] = [
    "deribit_btc_options_oi_usd",
    "deribit_eth_options_oi_usd",
    "deribit_perp_basis_bps",
]

# ──────────────────────────────────────────────────────────────────────────────
# Group P85_XVIII — DefiLlama slow-regime context
# ──────────────────────────────────────────────────────────────────────────────
_GROUP_P85_XVIII_DEFILLAMA: list[str] = [
    "dl_stablecoin_mcap_usd",
    "dl_stablecoin_mcap_delta_1d",
    "dl_stablecoin_risk_regime_code",
    "dl_eth_tvl_usd",
    "dl_eth_dex_vol_delta_1d_pct",
]

# ──────────────────────────────────────────────────────────────────────────────
# Group P1_DERIBIT — Deribit term structure (tenor IV + put/call ratios)
# ──────────────────────────────────────────────────────────────────────────────
_GROUP_P1_DERIBIT_TERM: list[str] = [
    "deribit_btc_iv_7d", "deribit_btc_iv_30d",
    "deribit_eth_iv_7d", "deribit_eth_iv_30d",
    "deribit_iv_term_structure_7d_30d",
    "deribit_put_call_ratio",
    "deribit_options_oi_call_put_ratio",
    "deribit_event_vol_premium_score",
]

# ──────────────────────────────────────────────────────────────────────────────
# Group P1_BREADTH — 5m market breadth volume + z-score
# ──────────────────────────────────────────────────────────────────────────────
_GROUP_P1_BREADTH_5M: list[str] = [
    "market_breadth_vol_5m",
    "market_breadth_volume_z",
]

# ──────────────────────────────────────────────────────────────────────────────
# Group P1_RELSTR — symbol relative strength vs btc/market/sector
# ──────────────────────────────────────────────────────────────────────────────
_GROUP_P1_RELSTR: list[str] = [
    "symbol_rel_strength_vs_btc_1m",
    "symbol_rel_strength_vs_market_1m",
    "symbol_rel_strength_vs_sector_1m",
]

# ──────────────────────────────────────────────────────────────────────────────
# Group P2_BYBIT — Bybit cross-venue ingest
# ──────────────────────────────────────────────────────────────────────────────
_GROUP_P2_BYBIT: list[str] = [
    "bybit_funding_rate",
    "bybit_ret_1m",
    "bybit_oi_delta_5m",
    "bybit_taker_buy_sell_ratio",
    "binance_bybit_price_diff_bps",
    "binance_bybit_oi_divergence",
]

# ──────────────────────────────────────────────────────────────────────────────
# Group P3_FG — Fear & Greed delta
# ──────────────────────────────────────────────────────────────────────────────
_GROUP_P3_FG_DELTA: list[str] = [
    "fear_greed_delta_1d",
]

# ──────────────────────────────────────────────────────────────────────────────
# Group P3_CP — CoinPaprika fallback feed
# ──────────────────────────────────────────────────────────────────────────────
_GROUP_P3_COINPAPRIKA: list[str] = [
    "cp_btc_dom_pct",
    "cp_symbol_ret_7d",
    "cp_volume_mcap_ratio",
    "cp_market_cap_rank",
]

# ──────────────────────────────────────────────────────────────────────────────
# Group P3_CMC — CoinMarketCap fallback feed
# ──────────────────────────────────────────────────────────────────────────────
_GROUP_P3_COINMARKETCAP: list[str] = [
    "cmc_btc_dom_pct",
    "cmc_total_mcap_usd",
    "cmc_total_volume_usd",
    "cmc_active_cryptos",
]

# ──────────────────────────────────────────────────────────────────────────────
# Group P3_DL_EXT — DefiLlama extended
# ──────────────────────────────────────────────────────────────────────────────
_GROUP_P3_DEFILLAMA_EXT: list[str] = [
    "dl_dex_volume_spike_z",
    "dl_eth_fees_24h_usd",
    "dl_eth_fees_revenue_momentum",
    "dl_perps_oi_delta_1d_pct",
]

# ──────────────────────────────────────────────────────────────────────────────
# Group DERIV_BASE — funding/OI/basis/liq base from ctx:deriv:{symbol}
# ──────────────────────────────────────────────────────────────────────────────
_GROUP_DERIV_BASE: list[str] = [
    "funding_rate", "funding_rate_z",
    "oi_notional_usd", "open_interest_z",
    "oi_delta_5m", "oi_delta_1m", "oi_accel", "oi_delta_z",
    "basis_bps", "premium_index_bps", "premium_index_z",
    "basis_pressure_score",
    "liq_long_notional_1m", "liq_short_notional_1m",
    "liq_long_notional_5m", "liq_short_notional_5m",
    "liq_imbalance_1m", "liq_imbalance_5m", "liq_imbalance_z",
    "long_short_ratio_z",
    "sector_breadth_ret_24h", "sector_breadth_vol_z",
    "sector_breadth_score",
]

# ──────────────────────────────────────────────────────────────────────────────
# Group PIT_PRIORS — PIT priors (session + 7d rolling + 30d MAE/MFE)
# ──────────────────────────────────────────────────────────────────────────────
_GROUP_PIT_PRIORS: list[str] = [
    "prior_winrate_symbol_kind_session",
    "prior_ev_r_symbol_kind_session",
    "prior_ev_r_median",
    "prior_sample_count_log",
    "prior_age_ms",
    "prior_profit_factor",
    "prior_sl_hit_rate",
    "prior_r_std",
    "prior_winrate_symbol_kind_7d",
    "prior_ev_r_symbol_kind_7d",
    "prior_profit_factor_symbol_kind_7d",
    "prior_sl_hit_rate_symbol_kind_7d",
    "prior_tp1_hit_rate_symbol_kind_7d",
    "prior_samples_symbol_kind_7d",
    "prior_winrate_symbol_kind_session_7d",
    "prior_median_mae_r_winners_30d",
    "prior_p90_mae_r_winners_30d",
    "prior_median_mfe_r_30d",
    "prior_giveback_p75_30d",
]

# ──────────────────────────────────────────────────────────────────────────────
# Group MACRO_CAL — macro event calendar proximity
# ──────────────────────────────────────────────────────────────────────────────
_GROUP_MACRO_CAL: list[str] = [
    "macro_event_severity",
    "minutes_to_macro_event",
    "minutes_after_macro_event",
]

# ──────────────────────────────────────────────────────────────────────────────
# Group SECTOR_AGG — cross-symbol sector aggregation
# ──────────────────────────────────────────────────────────────────────────────
_GROUP_SECTOR_AGG: list[str] = [
    "sector_delta_z_median",
    "sector_obi_median",
]

# ──────────────────────────────────────────────────────────────────────────────
# Group LIQMAP_ALIAS — liq heatmap aliases (density / cluster distance)
# ──────────────────────────────────────────────────────────────────────────────
_GROUP_LIQMAP_ALIAS: list[str] = [
    "liq_cluster_dist_above_bps",
    "liq_cluster_dist_below_bps",
    "liq_heatmap_density_above",
    "liq_heatmap_density_below",
]

# ──────────────────────────────────────────────────────────────────────────────
# Group LEADER_FLAGS — BTC/ETH leader confirm + direction conflict
# ──────────────────────────────────────────────────────────────────────────────
_GROUP_LEADER_FLAGS: list[str] = [
    "leader_btc_eth_confirm",
    "leader_direction_conflict",
]

# ──────────────────────────────────────────────────────────────────────────────
# Group BREADTH_RET — per-segment breadth and 1m/5m total returns
# ──────────────────────────────────────────────────────────────────────────────
_GROUP_BREADTH_RET: list[str] = [
    "market_breadth_ret_1m", "market_breadth_ret_5m",
    "major_breadth_1m", "major_ret_1m",
    "meme_breadth_1m", "meme_ret_1m",
    "alt_breadth_1m", "alt_ret_1m",
    "alt_breadth_5m", "alt_ret_5m",
    "sector_breadth_5m",
]


# ---------------------------------------------------------------------------
# Final composite key list — V15_OF_NUMERIC_KEYS (sorted for determinism)
# ---------------------------------------------------------------------------

def _v14_base() -> list[str]:
    from core.ml_feature_schema_v14_of import V14_OF_NUMERIC_KEYS
    return list(V14_OF_NUMERIC_KEYS)


_ALL_GROUPS = (
    _GROUP_P82_TIME_CYC,
    _GROUP_P83_TAKER_FORCE,
    _GROUP_P84_HAWKES_VPIN,
    _GROUP_P85_XV_CROSS_VENUE,
    _GROUP_P85_XVI_COINGECKO,
    _GROUP_P85_XVII_DERIBIT_EXT,
    _GROUP_P85_XVIII_DEFILLAMA,
    _GROUP_P1_DERIBIT_TERM,
    _GROUP_P1_BREADTH_5M,
    _GROUP_P1_RELSTR,
    _GROUP_P2_BYBIT,
    _GROUP_P3_FG_DELTA,
    _GROUP_P3_COINPAPRIKA,
    _GROUP_P3_COINMARKETCAP,
    _GROUP_P3_DEFILLAMA_EXT,
    _GROUP_DERIV_BASE,
    _GROUP_PIT_PRIORS,
    _GROUP_MACRO_CAL,
    _GROUP_SECTOR_AGG,
    _GROUP_LIQMAP_ALIAS,
    _GROUP_LEADER_FLAGS,
    _GROUP_BREADTH_RET,
)


def _build_keys() -> list[str]:
    base = _v14_base()
    new: list[str] = []
    for grp in _ALL_GROUPS:
        new.extend(grp)
    return sorted(set(base) | set(new))


V15_OF_NUMERIC_KEYS: list[str] = _build_keys()


# Hard invariant: pinned count. v14_of base + dedup'd new groups.
# Bump _EXPECTED_KEYS when intentionally adding/removing groups and refresh
# SCHEMA_HASH accordingly. Catches accidental drift.
_EXPECTED_KEYS = 515
assert len(V15_OF_NUMERIC_KEYS) == _EXPECTED_KEYS, (
    f"v15_of key count drift: got {len(V15_OF_NUMERIC_KEYS)}, expected {_EXPECTED_KEYS}. "
    f"If this is intentional, bump _EXPECTED_KEYS and update SCHEMA_HASH."
)


def get_v15_of_numeric_keys() -> list[str]:
    """Return sorted list of numeric indicator keys for v15_of."""
    return list(V15_OF_NUMERIC_KEYS)


def v15_of_info() -> dict:
    """Summary dict for logging / audit."""
    n_v14 = len(_v14_base())
    n_new = len(V15_OF_NUMERIC_KEYS) - n_v14
    return {
        "ver": "v15_of",
        "n_numeric_keys": len(V15_OF_NUMERIC_KEYS),
        "n_v14_of_base": n_v14,
        "n_new_keys": n_new,
        "groups": {
            "p82_time_cyc": len(_GROUP_P82_TIME_CYC),
            "p83_taker_force": len(_GROUP_P83_TAKER_FORCE),
            "p84_hawkes_vpin": len(_GROUP_P84_HAWKES_VPIN),
            "p85_xv_cross_venue": len(_GROUP_P85_XV_CROSS_VENUE),
            "p85_xvi_coingecko": len(_GROUP_P85_XVI_COINGECKO),
            "p85_xvii_deribit_ext": len(_GROUP_P85_XVII_DERIBIT_EXT),
            "p85_xviii_defillama": len(_GROUP_P85_XVIII_DEFILLAMA),
            "p1_deribit_term": len(_GROUP_P1_DERIBIT_TERM),
            "p1_breadth_5m": len(_GROUP_P1_BREADTH_5M),
            "p1_relstr": len(_GROUP_P1_RELSTR),
            "p2_bybit": len(_GROUP_P2_BYBIT),
            "p3_fg_delta": len(_GROUP_P3_FG_DELTA),
            "p3_coinpaprika": len(_GROUP_P3_COINPAPRIKA),
            "p3_coinmarketcap": len(_GROUP_P3_COINMARKETCAP),
            "p3_defillama_ext": len(_GROUP_P3_DEFILLAMA_EXT),
            "deriv_base": len(_GROUP_DERIV_BASE),
            "pit_priors": len(_GROUP_PIT_PRIORS),
            "macro_cal": len(_GROUP_MACRO_CAL),
            "sector_agg": len(_GROUP_SECTOR_AGG),
            "liqmap_alias": len(_GROUP_LIQMAP_ALIAS),
            "leader_flags": len(_GROUP_LEADER_FLAGS),
            "breadth_ret": len(_GROUP_BREADTH_RET),
        },
    }
