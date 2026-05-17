from __future__ import annotations

"""
v14_of — Feature schema v14 (OrderFlow), pinned snapshot.

Generated: 2026-05-13 (manual bump from v13_of).
Extended:  2026-05-16 — +20 external-data keys (composites + breadth + deribit + sentiment).

v14_of = v13_of (~242 keys) + 16 + 20 = ~278 additional indicators:
  Group OG (16) — OrderFlow Rule-Gate Consensus
                  (of_score components, have/need legs, contributions,
                   reason codes, gate bits, strong-need flags)
  Group OE (59) — External Data (composites + cross-source joiners)
                  - 5 composites from ctx:deriv:{symbol}
                  - 5 live market breadth from runtime:breadth
                  - 7 Deribit IV/funding/regime from ctx:deribit:global
                  - 1 num + 2 bool-as-float Fear&Greed from ctx:sentiment:global
                  - 1 OI notional z-score
                  - 11 granular breadth v2 (1m/5m + segment returns)
                  - 2 breadth volume 5m (P1)
                  - 3 symbol relative strength vs market/btc/sector (P1)
                  - 8 Deribit term structure IV + put/call ratios (P1)

  Subtotal new  = 16 (og_*) + 59 (oe_*) + 4 (xv_*) + 7 (xvi_*) + 3 (xvii_*) + 5 (xviii_*) = 94 (no key collisions with v13)

Coverage: ~278 numeric indicators (no separate bool block — all bool as float 0/1)
          + direction/bucket/hour/dow/session one-hots.

Design notes
------------
- Fail-open: ALL keys vectorize as 0.0 if missing in runtime snapshot.
- Group OG (rule consensus): mirrors `of_confirm_engine` decision artifacts
  (dec.have, dec.need, contrib dict, need_reason). Population is added in a
  separate change; until then keys vectorize to 0.0 (no model failure).
- Anti-overfit policy: each key has Pearson(new, nearest_v13_key) < 0.70 by
  design — of_score_final (v9_of+) is the aggregated post-clip score; og_*
  surface the pre-aggregation structure (which leg fired, by how much).
- Naming: `og_` prefix everywhere to guarantee zero collision with existing
  keys (`of_score_final`, `weak_progress`, `strong_gate_have/need`, etc.).
- Append-only: new schema versions always add keys, never remove.

Phase / rollout
---------------
Phase 0 (this file): schema declaration + registry mapping. No prod env switch.
Phase 1: `of_confirm_engine` writes og_* keys into `indicators` dict before
         XADD to `signals:of:inputs`; dataset builder picks them up automatically.
Phase 2: offline train baseline LR + GBDT challenger; compare to v13_of champion.
Phase 3: canary (BTCUSDT/ETHUSDT/SOLUSDT) shadow → enforce when metrics pass.
"""


SCHEMA_HASH = "v14of_og16_oe61_xv4_cg7_dx3_dl9_cp4_cmc4_bybit6_2026_05_16"


# Import base v13 keys to avoid duplication drift
try:
    from core.ml_feature_schema_v13_of import V13_OF_NUMERIC_KEYS as _V13_OF_BASE
except ImportError:
    _V13_OF_BASE = []


# ---------------------------------------------------------------------------
# Group OG — OrderFlow Rule-Gate Consensus (16 keys)
#
# Source artifacts (of_confirm_engine.py):
#   dec.have                            — number of legs satisfied
#   dec.need                            — number of legs required
#   dec.need_reason                     — reason_code string ("rev_dz_strong", ...)
#   contrib (dict)                      — per-leg contribution weights
#   nd.need_rev, nd.need_cont, nd.reason (strong_need_same_tick)
#   weak_progress (int 0/1)
#
# Orthogonality vs v13_of:
#   - of_score_final / of_score_final_raw (v9_of+, already in v13) = aggregated score
#   - og_*                                                         = decomposition
# ---------------------------------------------------------------------------

_GROUP_OG_RULE_CONSENSUS: list[str] = [
    # Gate progress (raw legs)
    "og_have",                   # int → float: legs currently satisfied (dec.have)
    "og_need",                   # int → float: legs required for confirm (dec.need)
    "og_have_minus_need",        # have - need (negative = gap; 0 = passed; positive = surplus)
    "og_ok",                     # float 0/1: gate passed (dec.have >= dec.need)
    "og_score_minus_threshold",  # of_score_final - legacy_of_score_min  (margin vs symbol min)

    # Per-leg contributions (from contrib dict in of_confirm_engine)
    # Each = weight * leg_score, normalized to [0, 1] (or 0 if leg absent)
    "og_contrib_z",              # delta_z component
    "og_contrib_wp",             # weak_progress component
    "og_contrib_reclaim",        # reclaim component
    "og_contrib_obi",            # OBI / book-pressure component
    "og_contrib_iceberg",        # iceberg / hidden-liquidity component
    "og_contrib_absorption",     # absorption / fp_edge component

    # Gate structure
    "og_gate_bits_count",        # popcount of active gate bits in current tick (int → float)

    # Strong-need policy (strong_need_same_tick)
    "og_strong_need_rev",        # int → float 0/1: reversal-strong-need fired this tick
    "og_strong_need_cont",       # int → float 0/1: continuation-strong-need fired this tick

    # Categorical / progress flags
    "og_weak_progress_any",      # int → float 0/1: any weak-progress leg present (mirror of weak_progress)
    "og_reason_code_id",         # stable hash(need_reason) % 64 (categorical encoded as small int → float)
]


# ---------------------------------------------------------------------------
# Group OE — External Data (61 keys: 32 original + 13 P1 + 11 rolling PIT + 3 macro + 1 P3 + 1 prior_stale)
#
# Source artifacts:
#   - ctx:deriv:{symbol}       (Python derivatives_context_collector_v1, REST)
#   - runtime:breadth          (Go binance_miniticker_breadth_ws, ~1Hz WS)
#   - ctx:deribit:global       (Go deribit_scheduler, ~60s)
#   - ctx:sentiment:global     (Go sentiment_scheduler / alternative.me, daily)
#
# Population: of_confirm_engine.py — Phase 7.9b (composites) and Phase 8.1
# (joiners) — already writes these names into `indicators_with_v4` via setdefault.
# All bool kept as float 0/1 to fit the no-bool-block design of v13_of+.
# Lag-guards: DERIV_CTX_MAX_LAG_MS (60s), BREADTH_MAX_LAG_MS (10s),
# DERIBIT_MAX_LAG_MS (120s), SENTIMENT_MAX_LAG_MS (7200s) — stale → 0.0.
# ---------------------------------------------------------------------------

_GROUP_OE_EXTERNAL_DATA: list[str] = [
    # --- 5 composites from ctx:deriv:{symbol} ---
    "taker_buy_sell_imbalance",
    "force_order_imbalance_1m",
    "oi_confirmation_score",      # sign(oi_delta_5m)*sign(funding_rate_z)
    "squeeze_risk_score",         # |funding_z|*|ls_z| when both>1.5, cap 25
    "liq_impulse_score",          # |liq_imbalance_z| when >2.0

    # --- 5 live market breadth from runtime:breadth ---
    "market_breadth_ret_24h",
    "market_breadth_vol_z",
    "btc_leader_ret_breadth",
    "eth_leader_ret_breadth",
    "breadth_leader_confirm",

    # --- 7 Deribit context from ctx:deribit:global ---
    "deribit_btc_iv_proxy",
    "deribit_eth_iv_proxy",
    "deribit_btc_iv_z",
    "deribit_eth_iv_z",
    "deribit_btc_funding_8h",
    "deribit_eth_funding_8h",
    "deribit_vol_regime_code",    # normal=0, elevated=1, extreme=2

    # --- 1 num + 2 bool-as-float Fear&Greed ---
    "fear_greed_index",
    "fear_greed_regime_extreme_fear",   # 0/1 (index<25)
    "fear_greed_regime_extreme_greed",  # 0/1 (index>75)

    # --- 1 OI notional z-score (Phase 8.4+) ---
    "open_interest_z",   # robust z-score of oi_notional_usd level (not delta)

    # --- 11 granular breadth (Phase breadth-v2: 1m/5m + major/meme/alt segments) ---
    "market_breadth_ret_1m",    # avg 1-min return, all tracked USDT perps
    "market_breadth_ret_5m",    # avg 5-min return, all tracked USDT perps
    "major_breadth_1m",         # fraction positive 1m, top-cap segment
    "major_ret_1m",             # avg 1-min return, top-cap segment
    "meme_breadth_1m",          # fraction positive 1m, meme segment
    "meme_ret_1m",              # avg 1-min return, meme segment
    "alt_breadth_1m",           # fraction positive 1m, alt segment
    "alt_ret_1m",               # avg 1-min return, alt segment
    "alt_breadth_5m",           # fraction positive 5m, alt segment
    "alt_ret_5m",               # avg 5-min return, alt segment
    "sector_breadth_score",     # 0.5*breadth_1m + 0.3*major_breadth_1m + 0.2*alt_breadth_1m

    # --- Phase P1: 5-min breadth volume + z-score ---
    "market_breadth_vol_5m",    # rolling 5-min quote-vol delta (cumulative 24h vol diff)
    "market_breadth_volume_z",  # robust z-score of vol_5m vs 60-tick history

    # --- Phase P1: symbol relative strength vs market / BTC / sector ---
    "symbol_rel_strength_vs_btc_1m",    # rel_ret_1m_vs_btc (identity alias)
    "symbol_rel_strength_vs_market_1m", # sym_ret_1m - market_breadth_ret_1m
    "symbol_rel_strength_vs_sector_1m", # sym_ret_1m - segment_ret_1m (major/meme/alt)

    # --- Phase P1: Deribit term structure (tenor-bucketed IV + ratios) ---
    "deribit_btc_iv_7d",                  # OI-weighted BTC IV for ≤7 DTE options
    "deribit_btc_iv_30d",                 # OI-weighted BTC IV for 8-30 DTE options
    "deribit_eth_iv_7d",                  # OI-weighted ETH IV for ≤7 DTE options
    "deribit_eth_iv_30d",                 # OI-weighted ETH IV for 8-30 DTE options
    "deribit_iv_term_structure_7d_30d",   # btc_iv_7d / btc_iv_30d (near/far ratio; 1=flat)
    "deribit_put_call_ratio",             # total BTC put OI / call OI
    "deribit_options_oi_call_put_ratio",  # total BTC call OI / put OI (inverse)
    "deribit_event_vol_premium_score",    # max(0, iv_7d/iv_30d - 1) event-premium proxy

    # --- Phase 4.10: Rolling PIT priors (pit_priors_rolling_v1) ---
    # Source: pit_priors:rolling:{7d|30d}:{symbol}:{kind}:{session|all}
    # Service: pit_priors_rolling_v1.py (hourly, embargo 1h, min 20 samples)
    "prior_winrate_symbol_kind_7d",       # rolling 7d win rate (all sessions)
    "prior_ev_r_symbol_kind_7d",          # rolling 7d expected value R (all sessions)
    "prior_profit_factor_symbol_kind_7d", # rolling 7d profit factor (gross win / loss)
    "prior_sl_hit_rate_symbol_kind_7d",   # rolling 7d SL hit rate (all sessions)
    "prior_tp1_hit_rate_symbol_kind_7d",  # rolling 7d TP1 hit rate among winners
    "prior_samples_symbol_kind_7d",       # log1p(sample_count) — log-scale
    "prior_winrate_symbol_kind_session_7d", # rolling 7d winrate, session-bucketed
    "prior_median_mae_r_winners_30d",     # median MAE/R on winning trades, 30d
    "prior_p90_mae_r_winners_30d",        # p90 MAE/R on winning trades (drawdown risk)
    "prior_median_mfe_r_30d",             # median peak gain/R, 30d
    "prior_giveback_p75_30d",             # p75 giveback (MFE - close) / R, 30d

    # --- Phase 4.12: Macro event calendar proximity ---
    # Source: ctx:macro:global (services/macro_calendar_scheduler.py, 60s)
    # Events: FOMC, CPI, NFP, PCE (HIGH=2), PPI (MEDIUM=1)
    # Active window: ±MACRO_ACTIVE_WINDOW_MIN (default 120 min) around event
    "macro_event_severity",          # 0=none, 1=medium, 2=high (nearest active event)
    "minutes_to_macro_event",        # minutes until next event (cap 10080=1wk)
    "minutes_after_macro_event",     # minutes since last event (cap 10080=1wk)

    # --- Phase P3: Fear & Greed delta (1d change) ---
    # Source: ctx:sentiment:global.fear_greed_delta_1d (Go SentimentScheduler)
    "fear_greed_delta_1d",           # daily change in FNG index

    # --- PIT prior staleness flag (mirrors v5_of bool block as float 0/1) ---
    # Source: of_confirm_engine.py — set when prior age exceeds PIT_PRIOR_STALE_MS.
    # Why: v14_of has no separate bool block; without this the model loses the
    # only signal that a PIT prior is unreliable (forces blind reliance on the
    # numeric prior_age_ms which is monotone-stale, not regime-stale).
    "prior_stale",                   # bool→float 0/1: 1 if PIT prior is stale
]


# ---------------------------------------------------------------------------
# Group XV — Cross-Venue Sanity (4 keys, added 2026-05-16)
#
# Source: ctx:crossvenue:{symbol} (JSON, Go crossvenue aggregator)
# Venues: OKX spot + Kraken spot + Coinbase spot (BTC/ETH/SOL only)
# Stale guard: CROSSVENUE_MAX_LAG_MS (30s default); quality_status=STALE → 0
# Population: of_confirm_engine.py — Phase 8.5
# ---------------------------------------------------------------------------

_GROUP_XV_CROSS_VENUE: list[str] = [
    "cross_venue_agree_score",      # [0,1] fraction of venues agreeing on direction
    "cross_venue_dislocation_bps",  # max-min mid across venues (basis points)
    "cross_venue_dislocation_z",    # robust-z of venue dislocation
    "binance_local_noise_score",    # [0,1] composite: disloc_z/3 × (1-agree); 0 when stale
]


# ---------------------------------------------------------------------------
# Group XVI — CoinGecko Macro Context (7 keys, added 2026-05-16)
#
# Source:
#   runtime:coingecko:global         (Go coingecko_scheduler, ~30s)
#   runtime:coingecko:market:{sym}   (Go coingecko_scheduler, ~60s)
# Stale guard: CG_MAX_LAG_MS (600s default)
# Population: of_confirm_engine.py — Phase 8.5
# ---------------------------------------------------------------------------

_GROUP_XVI_COINGECKO_MACRO: list[str] = [
    # Global dominance snapshot (runtime:coingecko:global)
    "cg_btc_dom_pct",           # BTC global market cap dominance (%)
    "cg_stable_dom_pct",        # Stablecoin global dominance (%)
    "cg_btc_dom_mom",           # BTC dominance momentum (24h change in pct)
    "cg_global_turnover",       # Global volume / mcap ratio
    # Per-symbol snapshot (runtime:coingecko:market:{symbol})
    "cg_symbol_rank",           # Symbol market cap rank (lower = larger)
    "cg_rel_strength_btc_1h",   # Symbol return vs BTC over 1h
    "cg_volume_mcap_ratio",     # Symbol 24h volume / market cap ratio
]


# ---------------------------------------------------------------------------
# Group XVII — Deribit Extended (3 keys, added 2026-05-16)
#
# Source:
#   ctx:deribit:global    (already read in Phase 8.1) — options OI aggregates
#   ctx:deribit:{symbol}  (JSON, Go deribit_scheduler) — per-symbol perp basis
# Only BTC/ETH have per-symbol Deribit context; others default to 0.
# Population: of_confirm_engine.py — Phase 8.5
# ---------------------------------------------------------------------------

_GROUP_XVII_DERIBIT_EXT: list[str] = [
    "deribit_btc_options_oi_usd",  # BTC options open interest (USD billions)
    "deribit_eth_options_oi_usd",  # ETH options open interest (USD billions)
    "deribit_perp_basis_bps",      # Per-symbol perp basis bps (0 for non-BTC/ETH)
]


# ---------------------------------------------------------------------------
# Group XVIII — DefiLlama Slow-Regime (5 keys, added 2026-05-16)
#
# Source:
#   runtime:defillama:stablecoins   (Go defillama_scheduler, ~900s HSET)
#   runtime:defillama:chain:Ethereum (Go defillama_scheduler, ~900s HSET)
#   runtime:defillama:dexs:Ethereum  (Go defillama_scheduler, ~300s HSET)
# Stale guard: DL_MAX_LAG_MS (1800s default) — slow-regime, not intraday gate.
# All features default to 0.0 when stale; used only as risk-regime modifiers.
# Population: of_confirm_engine.py — Phase 8.5
# ---------------------------------------------------------------------------

_GROUP_XVIII_DEFILLAMA: list[str] = [
    "dl_stablecoin_mcap_usd",        # Total stablecoin mcap (USD trillions = /1e12)
    "dl_stablecoin_mcap_delta_1d",   # Absolute 1d change in stablecoin mcap (USD)
    "dl_stablecoin_risk_regime_code",# 0=neutral, 1=risk_on, -1=risk_off
    "dl_eth_tvl_usd",                # Ethereum TVL (USD billions = /1e9)
    "dl_eth_dex_vol_delta_1d_pct",   # Ethereum DEX volume 1d delta (%)
    # Phase P3 DefiLlama extended (4 new, all from existing Go sources)
    "dl_dex_volume_spike_z",         # Ethereum DEX volume z-score vs 7d baseline
    "dl_eth_fees_24h_usd",           # Ethereum protocol fees 24h (USD millions = /1e6)
    "dl_eth_fees_revenue_momentum",  # fees_7d_ma / fees_30d_ma - 1 (momentum ratio)
    "dl_perps_oi_delta_1d_pct",      # DefiLlama cross-chain perps OI 1d change (%)
]


# ---------------------------------------------------------------------------
# Group XIX — CoinPaprika Fallback (4 keys, added 2026-05-16)
#
# Source:
#   runtime:provider:coinpaprika:global       (Go ProviderFallbackScheduler, ~300s)
#   runtime:provider:coinpaprika:market:{sym} (Go ProviderFallbackScheduler, ~300s)
# Stale guard: CP_MAX_LAG_MS (900s default).
# ---------------------------------------------------------------------------

_GROUP_XIX_COINPAPRIKA: list[str] = [
    "cp_btc_dom_pct",         # BTC market cap dominance (%)
    "cp_symbol_ret_7d",       # Symbol 7d return (%)
    "cp_volume_mcap_ratio",   # Symbol 24h volume / market cap ratio
    "cp_market_cap_rank",     # Symbol market cap rank (lower = larger)
]


# ---------------------------------------------------------------------------
# Group XX — CoinMarketCap Fallback (4 keys, added 2026-05-16)
#
# Source:
#   runtime:provider:coinmarketcap:global (Go ProviderFallbackScheduler, ~300s)
# Stale guard: CMC_MAX_LAG_MS (900s default).
# ---------------------------------------------------------------------------

_GROUP_XX_CMC: list[str] = [
    "cmc_btc_dom_pct",         # BTC market cap dominance (%)
    "cmc_total_mcap_usd",      # Total crypto market cap (USD trillions = /1e12)
    "cmc_total_volume_usd",    # Total 24h volume (USD billions = /1e9)
    "cmc_active_cryptos",      # Number of active cryptocurrencies listed
]


# ---------------------------------------------------------------------------
# Group XXI — Bybit Cross-Venue (6 keys, added 2026-05-16)
#
# Source:
#   runtime:bybit:{symbol} HASH (Go bybit_features_collector, ~15s poll)
# Covers top-5 symbols: BTCUSDT/ETHUSDT/SOLUSDT/BNBUSDT/XRPUSDT.
# Stale guard: BYBIT_MAX_LAG_MS (120s default).
# ---------------------------------------------------------------------------

_GROUP_XXI_BYBIT: list[str] = [
    "bybit_funding_rate",              # Bybit current funding rate
    "bybit_ret_1m",                    # Bybit 1-min price return (rolling)
    "bybit_oi_delta_5m",               # Bybit OI 5-min delta (USD)
    "bybit_taker_buy_sell_ratio",      # Bybit account buy/sell ratio (5min period)
    "binance_bybit_price_diff_bps",    # (binance_mid - bybit_last) / binance_mid * 10000
    "binance_bybit_oi_divergence",     # bybit_oi_delta_5m - binance_oi_delta_5m
]


# ---------------------------------------------------------------------------
# Final composite key list — V14_OF_NUMERIC_KEYS (sorted for determinism)
# ---------------------------------------------------------------------------

V14_OF_NUMERIC_KEYS: list[str] = sorted(set(
    _V13_OF_BASE
    + _GROUP_OG_RULE_CONSENSUS
    + _GROUP_OE_EXTERNAL_DATA
    + _GROUP_XV_CROSS_VENUE
    + _GROUP_XVI_COINGECKO_MACRO
    + _GROUP_XVII_DERIBIT_EXT
    + _GROUP_XVIII_DEFILLAMA
    + _GROUP_XIX_COINPAPRIKA
    + _GROUP_XX_CMC
    + _GROUP_XXI_BYBIT
))

# Sanity guard (caught immediately at import in tests)
_EXPECTED_MIN = 245
_EXPECTED_MAX = 380  # raised to accommodate P3/P2 additions (+15 P3 + 6 P2)
if _V13_OF_BASE:
    assert _EXPECTED_MIN <= len(V14_OF_NUMERIC_KEYS) <= _EXPECTED_MAX, (
        f"v14_of key count {len(V14_OF_NUMERIC_KEYS)} out of expected range "
        f"[{_EXPECTED_MIN}, {_EXPECTED_MAX}] — check for duplicates or deletions"
    )

# Hard guard: new groups must not collide with v13_of base keys.
_OG_COLLISIONS = set(_GROUP_OG_RULE_CONSENSUS) & set(_V13_OF_BASE)
assert not _OG_COLLISIONS, (
    f"v14_of OG group collides with v13_of base keys: {sorted(_OG_COLLISIONS)}"
)
_OE_COLLISIONS = set(_GROUP_OE_EXTERNAL_DATA) & set(_V13_OF_BASE)
assert not _OE_COLLISIONS, (
    f"v14_of OE group collides with v13_of base keys: {sorted(_OE_COLLISIONS)}"
)
_XV_COLLISIONS = set(_GROUP_XV_CROSS_VENUE) & set(_V13_OF_BASE)
assert not _XV_COLLISIONS, (
    f"v14_of XV group collides with v13_of base keys: {sorted(_XV_COLLISIONS)}"
)
_XVI_COLLISIONS = set(_GROUP_XVI_COINGECKO_MACRO) & set(_V13_OF_BASE)
assert not _XVI_COLLISIONS, (
    f"v14_of XVI group collides with v13_of base keys: {sorted(_XVI_COLLISIONS)}"
)
_XVII_COLLISIONS = set(_GROUP_XVII_DERIBIT_EXT) & set(_V13_OF_BASE)
assert not _XVII_COLLISIONS, (
    f"v14_of XVII group collides with v13_of base keys: {sorted(_XVII_COLLISIONS)}"
)
_XVIII_COLLISIONS = set(_GROUP_XVIII_DEFILLAMA) & set(_V13_OF_BASE)
assert not _XVIII_COLLISIONS, (
    f"v14_of XVIII group collides with v13_of base keys: {sorted(_XVIII_COLLISIONS)}"
)
_XIX_COLLISIONS = set(_GROUP_XIX_COINPAPRIKA) & set(_V13_OF_BASE)
assert not _XIX_COLLISIONS, (
    f"v14_of XIX group collides with v13_of base keys: {sorted(_XIX_COLLISIONS)}"
)
_XX_COLLISIONS = set(_GROUP_XX_CMC) & set(_V13_OF_BASE)
assert not _XX_COLLISIONS, (
    f"v14_of XX group collides with v13_of base keys: {sorted(_XX_COLLISIONS)}"
)
_XXI_COLLISIONS = set(_GROUP_XXI_BYBIT) & set(_V13_OF_BASE)
assert not _XXI_COLLISIONS, (
    f"v14_of XXI group collides with v13_of base keys: {sorted(_XXI_COLLISIONS)}"
)


def get_v14_of_numeric_keys() -> list[str]:
    """Return sorted list of numeric indicator keys for v14_of."""
    return list(V14_OF_NUMERIC_KEYS)


def v14_of_info() -> dict:
    """Summary dict for logging / audit."""
    n_v13 = len(_V13_OF_BASE)
    n_new = len(V14_OF_NUMERIC_KEYS) - n_v13
    return {
        "ver": "v14_of",
        "schema_hash": SCHEMA_HASH,
        "n_numeric_keys": len(V14_OF_NUMERIC_KEYS),
        "n_v13_of_base": n_v13,
        "n_new_keys": n_new,
        "groups": {
            "group_og_rule_consensus": len(_GROUP_OG_RULE_CONSENSUS),
            "group_oe_external_data": len(_GROUP_OE_EXTERNAL_DATA),
            "group_xv_cross_venue": len(_GROUP_XV_CROSS_VENUE),
            "group_xvi_coingecko_macro": len(_GROUP_XVI_COINGECKO_MACRO),
            "group_xvii_deribit_ext": len(_GROUP_XVII_DERIBIT_EXT),
            "group_xviii_defillama": len(_GROUP_XVIII_DEFILLAMA),
            "group_xix_coinpaprika": len(_GROUP_XIX_COINPAPRIKA),
            "group_xx_cmc": len(_GROUP_XX_CMC),
            "group_xxi_bybit": len(_GROUP_XXI_BYBIT),
        },
    }
