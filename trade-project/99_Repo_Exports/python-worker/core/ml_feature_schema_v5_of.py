from __future__ import annotations

"""ML feature schema v5 (OrderFlow).

This schema is a strict superset of MLFeatureSchemaV4OF.

Goal:
  - keep v4_of stable for deployed models
  - introduce v5_of for training/online gating with extra low-latency microstructure features

Design constraints:
  - deterministic order of features
  - low-latency (only features that already exist in indicators, or are computed in cheap book_microstructure_v4)
  - backward compatibility: v4_of unchanged
"""


import hashlib
import json
import os
from dataclasses import dataclass

from core.ml_feature_schema_v4_of import MLFeatureSchemaV4OF

SCHEMA_HASH = "c3e1a7f29d50"  # Phase 4.10: +11 rolling PIT priors (266 num + 34 bool = 300 total)



@dataclass
class MLFeatureSchemaV5OF(MLFeatureSchemaV4OF):
    """v5_of = v4_of + extra microstructure/regime/fill features.

    Notes:
      - extras are appended to preserve v4 feature order
      - do not remove/reorder existing keys without bumping schema version
    """

    def __post_init__(self) -> None:  # noqa: D401
        super().__post_init__()

        extra_num: list[str] = [
            # Vol regime (more informative than raw fast/slow)
            "vol_ratio",
            "vol_ratio_z",

            # Execution/fill proxy
            "fill_prob_proxy",
            "eta_fill_sec",
            "fill_prob_p_base",
            "fill_prob_p_wait",
            "exec_fill_pen",
            "max_expected_slippage_bps_eff",

            # LOB pressure (already produced under lob_* keys)
            "lob_qi_mean",
            "lob_qi_max_abs",
            "lob_qi_slope",
            "lob_micro_mid_div_bps",
            "lob_micro_shift_bps",
            "lob_depth_slope_imb",
            "lob_depth_convexity_imb",
            "lob_dw_obi_z",
            "lob_dw_obi_stability_score",
            "lob_dw_obi_stable_secs",

            # Cheap multilevel depth/imbalance/OFI (added to book_microstructure_v4)
            "depth_total_5",
            "depth_imbalance_5",
            "depth_top5_sum",
            "qimb_wmean",
            "qimb_l1",
            "qimb_l5",
            "qimb_slope",
            "ofi_ml_norm",
            "ofi_ml_wsum",

            # ---------------------------------------------------------------
            # [Phase 6] Horizon-aware + ATR metrics
            # These are normalized so the model sees relative magnitudes.
            # Fail-open: missing values map to 0.0 by default in vectorize().
            # ---------------------------------------------------------------
            # ATR selection metadata from ATRCache.get_with_meta()
            # atr_tf_ms: selected ATR timeframe in ms (normalized / 900000 = 0..n)
            #   0=1m, 0.33=5m, 1.0=15m, 4.0=1h, 16.0=4h, 96.0=24h etc.
            #   NOTE: raw ms value — model learns schedule-relative importance
            "atr_tf_ms",
            # atr_stop_pct: ATR / entry_price * 100 (risk in %)
            #   Typical range: 0.1% (BTC quiet) to 5%+ (alt coins, news)
            "atr_stop_pct",
            # atr_regime_pct: atr_bps / atr_bps_threshold (regime-relative vol)
            #   > 1.0 = current vol exceeds regime floor, < 1.0 = below regime
            "atr_regime_pct",

            # Horizon contract fields (normalized to fraction of 1 hour)
            # hold_target_ms_norm: hold_target_ms / 3_600_000 (0 = unknown)
            "hold_target_ms_norm",
            # alpha_half_life_ms_norm: alpha_half_life_ms / 3_600_000 (0 = unknown)
            "alpha_half_life_ms_norm",

            # Vol ratio from horizon contract
            # vol_ratio_fast_slow: fast_vol / slow_vol (should be ~1.0 at equilibrium)
            "vol_ratio_fast_slow",

            # max_signal_age_ratio: (now_ms - signal_ts_ms) / max_signal_age_ms
            #   0.0 = just generated, 1.0 = at expiry boundary, > 1.0 = stale
            "max_signal_age_ratio",

            # ---------------------------------------------------------------
            # [Phase 7] P1: Execution cost ratios — signal quality after cost
            # All fail-open: 0.0 when inputs unavailable.
            # ---------------------------------------------------------------
            # exec_cost_to_tp1_ratio = (half_spread + slippage + fee) / tp1_bps
            #   > 1.0 = trade cannot pay for itself even at TP1
            "exec_cost_to_tp1_ratio",
            # exec_cost_to_sl_ratio = (half_spread + slippage + fee) / sl_bps
            #   high = risk:reward compressed by execution cost
            "exec_cost_to_sl_ratio",
            # exec_cost_to_atr_ratio = (half_spread + slippage + fee) / atr_bps
            #   normalised by volatility: captures regime-adjusted cost burden
            "exec_cost_to_atr_ratio",

            # P1: Signal age — absolute and relative to alpha half-life
            # signal_age_ms: ms since signal was generated (0 = fresh)
            "signal_age_ms",
            # signal_age_to_half_life = signal_age_ms / alpha_half_life_ms
            #   > 1.0 = signal older than its expected useful lifetime
            "signal_age_to_half_life",

            # P1: Volatility dynamics from vol_ratio_fast_slow
            # vol_expansion_score = max(0, vol_ratio_fast_slow - 1)
            #   positive = fast vol accelerating above slow baseline
            "vol_expansion_score",
            # vol_compression_score = max(0, 1 - vol_ratio_fast_slow)
            #   positive = fast vol compressing below slow baseline
            "vol_compression_score",

            # P1: Data quality / freshness signals (continuous, not hard gate)
            # dq_score: 0..1 composite DQ health (1 = pristine, 0 = degraded)
            "dq_score",
            # dq_flag_count: 0-3 severity level of worst active DQ condition
            "dq_flag_count",
            # tick_lag_ms: ms since last valid tick (proxy for data freshness)
            "tick_lag_ms",

            # ---------------------------------------------------------------
            # [Phase 7.2] Extended DQ — book freshness + CVD integrity
            # ---------------------------------------------------------------
            # book_age_ms: ms since last valid order-book snapshot
            #   source: book_staleness_ms / liq_book_stale_ms; 0 = unknown
            "book_age_ms",
            # book_gap_ms: gap between consecutive book timestamps (ms)
            #   source: book_ts_gap_ms; 0 = unknown / first update
            "book_gap_ms",

            # ---------------------------------------------------------------
            # [Phase 4.9] DQ rolling window features — 1-minute sliding window.
            # Computed from _DQ_ROLLING_CACHE (cold start ⇒ 0.0).
            # ---------------------------------------------------------------
            "tick_lag_p95_1m",        # p95 tick-to-ingest lag over last 60s (ms)
            "tick_reorder_rate_1m",   # fraction of out-of-order ticks over 60s
            "tick_dedupe_rate_1m",    # fraction of duplicate-ts ticks over 60s
            "tick_gap_count_1m",      # count of gap events (>500ms) over 60s
            "bad_time_streak",        # consecutive bad-time ticks ending at now
            "book_update_rate_hz",    # EMA of book update rate (Hz); 0 = unknown
            "book_staleness_z",       # robust z-score of book update rate

            # ---------------------------------------------------------------
            # [Phase 7.4] Gate trace — derived diagnostics from rule engine
            # ---------------------------------------------------------------
            # rule_have_need_gap: have - need (negative = below threshold)
            "rule_have_need_gap",
            # missing_legs_count: number of required legs absent at decision
            "missing_legs_count",
            # gate_pressure_score: (1 - have_need_ratio) * missing_legs_count
            #   high value = far from threshold AND many missing legs
            "gate_pressure_score",

            # ---------------------------------------------------------------
            # [Phase 7.6] LOB velocity — slopes over 1s/3s rolling windows.
            # Computed from per-symbol in-process ring buffer (cold start ⇒ 0.0).
            # ---------------------------------------------------------------
            "obi_slope_1s",
            "obi_slope_3s",
            "qimb_slope_1s",
            "qimb_slope_3s",
            "depth_imbalance_5_delta_1s",
            "depth_imbalance_5_delta_3s",
            "spread_widen_velocity_bps_s",  # 1s window, clamped ≥ 0
            "fill_prob_decay_slope",        # 1s window, signed
            # [Phase 4.4] Additional LOB dynamics from ring buffer
            "obi_stability_decay",          # 1/(1+std(obi_3s)); 1.0=stable
            "book_churn_delta_1s",          # |Δobi| + |Δdepth_imb5| per sec
            "book_churn_z",                 # robust z-score of book_churn_delta_1s
            "spread_mean_revert_score",     # (mean_spread-now)/mean ∈ [-1,1]
            # microprice shift velocity/acceleration (via _LOB_MICRO_CACHE)
            "micro_mid_shift_vel_bps_s",    # velocity of microprice shift, bps/s
            "micro_mid_shift_accel_bps_s2", # acceleration of microprice shift, bps/s²

            # ---------------------------------------------------------------
            # [Phase 7.7] Fill-queue (lite) — one-shot from existing depth_*
            # ---------------------------------------------------------------
            # eta_fill_sec_norm: eta_fill_sec / 10.0 clamped [0,1]
            "eta_fill_sec_norm",
            # queue_ahead_qty_l1/l5: maker-side depth on direction-aware level
            "queue_ahead_qty_l1",
            "queue_ahead_qty_l5",
            # depth_to_taker_rate_ratio: depth_top5_sum / (taker_buy+sell rate EMA)
            "depth_to_taker_rate_ratio",
            # maker_fill_vs_taker_cost_edge: fill_prob_proxy * tp1_bps - exec_cost
            "maker_fill_vs_taker_cost_edge",
            # fill_prob_Xs: fill probability at fixed max-wait horizons 1s/3s/5s
            # (same formula as fill_prob_proxy but with max_wait_s pinned)
            "fill_prob_1s",
            "fill_prob_3s",
            "fill_prob_5s",

            # ---------------------------------------------------------------
            # [Phase 7.8] Cross-context hydration — sourced from ADR-0005/06/07
            # services. Lag-guarded: stale entries map to 0.0 / True for `stale`.
            # ---------------------------------------------------------------
            # ADR-0006 anchor returns (BTC/ETH rolling)
            "btc_ret_30s", "btc_ret_1m", "btc_ret_5m",
            "eth_ret_30s", "eth_ret_1m", "eth_ret_5m",
            "rel_ret_1m_vs_btc", "rel_ret_5m_vs_btc",
            # ADR-0006 extended cross-context features
            "leader_confidence",          # BTC+ETH direction sign consistency ∈ [-1,1]
            "market_risk_on_score",       # avg(btc_ret_1m, eth_ret_1m) composite
            "rel_ofi_ml_norm_btc",        # (target_ofi - btc_ofi_1m) / (|btc_ofi_1m| + eps)
            "rel_lob_micro_shift_bps_btc",# target_mps_1m - btc_mps_1m (bps)

            # ADR-0007 PIT priors (extended: +profit_factor, sl_hit_rate, r_std, ev_r_median)
            "prior_winrate_symbol_kind_session",
            "prior_ev_r_symbol_kind_session",
            "prior_ev_r_median",          # median R-multiple (robust central tendency)
            "prior_sample_count_log",     # log1p of sample_count to compress scale
            "prior_age_ms",
            "prior_stale_ms",             # raw staleness ms (numeric; prior_stale bool kept for compat)
            "prior_profit_factor",        # gross_profit / gross_loss; >1 = positive EV
            "prior_sl_hit_rate",          # LOSS / (WIN+LOSS); explicit for training
            "prior_r_std",                # std of R-multiples (consistency metric)

            # ADR-0005 TCA EMA priors
            "tca_eff_spread_bps_ema",
            "tca_realized_spread_1s_bps_ema",
            "tca_realized_spread_5s_bps_ema",
            "tca_perm_impact_1s_bps_ema",
            "tca_perm_impact_5s_bps_ema",
            "tca_is_bps_ema",
            "tca_samples",
            "tca_stale_ms",
            # ADR-0005 p95 percentiles — rolling p95 over last 500 fills per bucket
            "spread_p95_bps_symbol_kind_session",   # p95 of eff_spread_bps
            "slippage_p95_bps_symbol_kind_session",  # p95 of is_bps (impl. shortfall)

            # ---------------------------------------------------------------
            # [Phase 7.9] Derivatives context — funding / OI / liquidations /
            # basis / long-short ratio / market breadth from existing
            # `ctx:deriv:{symbol}` snapshot. Lag-guard: DERIV_CTX_MAX_LAG_MS=60000.
            # ---------------------------------------------------------------
            "funding_rate",
            "funding_rate_z",
            "oi_notional_usd",
            "oi_delta_5m", "oi_delta_1m", "oi_accel",
            "basis_bps",
            "premium_index_bps",
            "basis_pressure_score",
            "liq_long_notional_1m", "liq_short_notional_1m",
            "liq_long_notional_5m", "liq_short_notional_5m",
            "liq_imbalance_1m", "liq_imbalance_5m", "liq_imbalance_z",
            "long_short_ratio", "long_short_ratio_z",
            "leader_btc_eth_confirm",
            "leader_direction_conflict",
            "sector_breadth_ret_24h",
            "sector_breadth_vol_z",
            # fraction of tracked USDT futures with positive 1-min return (~1Hz WS)
            "sector_breadth_1m",

            # ---------------------------------------------------------------
            # [Phase 8.1] Composite derivative scores — derived from same
            # ctx:deriv:{symbol} snapshot, no new infra. See of_confirm_engine.py
            # Phase 7.9b block. Fail-open: 0.0 when _deriv_stale.
            # ---------------------------------------------------------------
            "taker_buy_sell_imbalance",
            "taker_buy_sell_ratio",    # buy/sell volume ratio (>1 = buy dominates)
            "taker_buy_sell_ratio_z",  # robust z-score of ratio over history
            "force_order_imbalance_1m",
            "force_order_long_notional_1m",   # liq buy-side notional USD (alias liq_long_notional_1m)
            "force_order_short_notional_1m",  # liq sell-side notional USD
            "force_order_cluster_score",      # directional liq imbalance × log1p(total/1M)
            "oi_confirmation_score",   # sign(oi_delta_5m)*sign(funding_rate_z)
            "squeeze_risk_score",      # |funding_z|*|ls_z| when both>1.5, cap 25
            "liq_impulse_score",       # |liq_imbalance_z| when >2.0
            "top_trader_long_short_ratio",    # Binance top-trader position L/S
            "futures_crowding_score",         # funding_z × ls_z / 9, clipped ±3

            # ---------------------------------------------------------------
            # [Phase 8.1] Live market breadth — from runtime:breadth HASH
            # (binance_miniticker_breadth_ws, updated ~1Hz). Lag-guard:
            # BREADTH_MAX_LAG_MS default 10000.
            # ---------------------------------------------------------------
            "market_breadth_ret_24h",
            "market_breadth_vol_z",
            "btc_leader_ret_breadth",
            "eth_leader_ret_breadth",
            "breadth_leader_confirm",

            # ---------------------------------------------------------------
            # [Phase 8.1] Deribit vol-regime context — from ctx:deribit:global
            # (deribit_scheduler, ~60s). Lag-guard: DERIBIT_MAX_LAG_MS=120000.
            # ---------------------------------------------------------------
            "deribit_btc_iv_proxy",
            "deribit_eth_iv_proxy",
            "deribit_btc_iv_z",
            "deribit_eth_iv_z",
            "deribit_btc_funding_8h",
            "deribit_eth_funding_8h",
            "deribit_vol_regime_code",  # normal=0, elevated=1, extreme=2

            # ---------------------------------------------------------------
            # [Phase 8.1] Sentiment — from ctx:sentiment:global (Fear&Greed
            # daily). Lag-guard: SENTIMENT_MAX_LAG_MS=7200000 (2h).
            # ---------------------------------------------------------------
            "fear_greed_index",

            # ---------------------------------------------------------------
            # [Phase 8.2] Cyclical time encoding + news gate
            # hour_sin/cos/dow_sin/cos replace one-hot session flags with
            # continuous cyclical encoding (no artificial midnight/day boundary).
            # news_blackout = float(news_gate_veto): 1.0 when news blackout
            # is active, sourced from indicators["news_gate_veto"].
            # ---------------------------------------------------------------
            "hour_sin",
            "hour_cos",
            "dow_sin",
            "dow_cos",
            "news_blackout",

            # ---------------------------------------------------------------
            # [Phase 8.4] Hawkes/VPIN process features — from ctx:hawkes:{symbol}
            # HASH written by of_hawkes_vpin_v1.py. Stale-guard: HAWKES_MAX_LAG_MS=30000.
            # ---------------------------------------------------------------
            "hawkes_dt_s",
            "hawkes_taker_buy_lam",
            "hawkes_taker_sell_lam",
            "hawkes_cancel_bid_lam",
            "hawkes_cancel_ask_lam",
            "hawkes_limit_add_lam",
            "hawkes_taker_lam",
            "hawkes_cancel_lam",
            "hawkes_churn_lam",
            "added_bid_rate_ema",
            "added_ask_rate_ema",
            "added_total_rate_ema",
            "vpin_tox_ema",
            "vpin_tox_z",
            "hawkes_S_taker_buy",    # state 0/1 (Hawkes S-process: buying pressure)
            "hawkes_S_taker_sell",
            "hawkes_S_cancel_bid",
            "hawkes_S_cancel_ask",
            "hawkes_S_limit_add",

            # ---------------------------------------------------------------
            # [Phase 8.4] Hawkes derived composites
            # ---------------------------------------------------------------
            "hawkes_buy_sell_lam_ratio",   # taker_buy_lam / taker_sell_lam (clamped)
            "hawkes_cancel_imbalance",     # (cancel_bid - cancel_ask) / (sum+eps)

            # ---------------------------------------------------------------
            # [Phase 8.4] OI delta and premium z-scores (from v3 deriv snapshot)
            # ---------------------------------------------------------------
            "oi_delta_z",         # robust z-score of oi_delta_1m over history
            "premium_index_z",    # robust z-score of premium_index over history

            # ---------------------------------------------------------------
            # [Phase 8.4] 5-min breadth, news remaining, queue alias
            # ---------------------------------------------------------------
            "sector_breadth_5m",     # 5-min rolling market breadth (runtime:breadth HASH)
            "news_until_ms_norm",    # remaining blackout normalised to [0,1] over 30m

            # queue_ahead_qty_5: alias of queue_ahead_qty_l5 for stable schema key
            "queue_ahead_qty_5",

            # ---------------------------------------------------------------
            # [Phase 8.4] Gate trace — interpretability features
            # ---------------------------------------------------------------
            "of_confirm_scenario",       # scenario int: trend=1,range=2,reversal=3,chop=4,breakout=5
            "of_confirm_reason_group",   # gate result: PASS=1,NEAR_PASS=2,HARD_FAIL=3
            "strong_need",               # bool-as-float: strong_need mode active
            "strong_have",               # have count when strong_need active, else 0

            # ---------------------------------------------------------------
            # [Phase 8.5] Gate trace completeness + ATR age
            # ---------------------------------------------------------------
            "rule_have",        # explicit alias of 'have' for schema stability
            "rule_need",        # explicit alias of 'need' for schema stability
            "have_need_ratio",  # have/need; ∞→0 when need=0
            "atr_age_ms",       # ATR staleness in ms (complements atr_fresh bool)

            # ---------------------------------------------------------------
            # [Phase 4.10] Rolling PIT priors — 7d/30d from pit_priors_rolling_v1.
            # Written by orderflow_services/pit_priors_rolling_v1.py (hourly).
            # Embargo: 1h. Fail-open: 0.0 / neutral defaults when service cold.
            # ---------------------------------------------------------------
            # 7d cross-session (pit_priors:rolling:7d:{sym}:{kind}:all)
            "prior_winrate_symbol_kind_7d",         # winrate last 7d (all sessions)
            "prior_ev_r_symbol_kind_7d",            # EV/R last 7d
            "prior_profit_factor_symbol_kind_7d",   # gross_profit / gross_loss, 7d
            "prior_sl_hit_rate_symbol_kind_7d",     # SL hit fraction, 7d
            "prior_tp1_hit_rate_symbol_kind_7d",    # TP1 hit fraction on winners, 7d
            "prior_samples_symbol_kind_7d",         # log1p(sample_count_7d)
            # 7d session-specific (pit_priors:rolling:7d:{sym}:{kind}:{session})
            "prior_winrate_symbol_kind_session_7d", # winrate in current session, 7d
            # 30d MAE/MFE/giveback (pit_priors:rolling:30d:{sym}:{kind}:all)
            "prior_median_mae_r_winners_30d",       # median MAE/R on winning trades, 30d
            "prior_p90_mae_r_winners_30d",          # p90 MAE/R on winning trades, 30d
            "prior_median_mfe_r_30d",               # median MFE/R all trades, 30d
            "prior_giveback_p75_30d",               # p75 giveback (mfe_r - r) on winners, 30d

            # ---------------------------------------------------------------
            # [Phase 4.5] VPIN rolling windows + Hawkes limit_add bid/ask split
            # Written by of_hawkes_vpin_v1.py into ctx:hawkes:{symbol} HASH.
            # ---------------------------------------------------------------
            "vpin_tox_1m",              # 1-min rolling mean VPIN toxicity
            "vpin_tox_5m",              # 5-min rolling mean VPIN toxicity
            "vpin_tox_slope",           # VPIN slope (tox_1m - tox_5m)
            "hawkes_limit_add_bid_lam", # Hawkes intensity: bid-side limit adds
            "hawkes_limit_add_ask_lam", # Hawkes intensity: ask-side limit adds
            "hawkes_limit_add_imbalance",  # (bid_lam - ask_lam) / (sum + eps)

            # ---------------------------------------------------------------
            # [Phase 4.6] Cross-symbol sector aggregation (in-process cache).
            # Median of oi_delta_z / OBI across all symbols in same worker.
            # ---------------------------------------------------------------
            "sector_delta_z_median",   # median oi_delta_z across active symbols
            "sector_obi_median",       # median OBI across active symbols

            # ---------------------------------------------------------------
            # [Phase 4.7] Liq heatmap aliases — derived from liqmap_5m_* features.
            # Source: liqmap_features_v1 computed from liqmap:snapshot:{symbol}:5m.
            # ---------------------------------------------------------------
            "liq_cluster_dist_above_bps",  # distance to nearest short-liq cluster (bps up)
            "liq_cluster_dist_below_bps",  # distance to nearest long-liq cluster (bps dn)
            "liq_heatmap_density_above",   # log1p(near_short_usd / 1M)
            "liq_heatmap_density_below",   # log1p(near_long_usd / 1M)
        ]

        extra_bool: list[str] = [
            "res_recovered",
            "lob_dw_obi_stable",
            # atr_fresh: True iff atr_age_ms ∈ (0, ATR_FRESH_MS) — model can trust ATR
            "atr_fresh",
            # Phase 7.4: gate trace
            "soft_fail_near_pass",
            # Phase 7.5: session / weekend (UTC-derived from existing hour_utc/dow)
            "session_asia",
            "session_europe",
            "session_us",
            "weekend_flag",
            # Phase 6: EU/US overlap window 13-16 UTC
            "session_overlap_eu_us",
            # Phase 7.8: ADR-0007 PIT prior staleness flag
            "prior_stale",
            # Phase 8.1: Fear&Greed regime flags
            "fear_greed_regime_extreme_fear",   # index < 25
            "fear_greed_regime_extreme_greed",  # index > 75
        ]
        # Note: cvd_quarantine_active is already in v4_of bool_keys — no need to re-add.

        # Append extras without duplicates (stable deterministic order).
        for k in extra_num:
            if k not in self.num_keys:
                self.num_keys.append(k)
        for k in extra_bool:
            if k not in self.bool_keys:
                self.bool_keys.append(k)


def _default_denylist_path() -> str:
    # Keep default local to python-worker/core. Can be overridden via env.
    return os.path.join(os.path.dirname(__file__), "feature_denylist_v1.json")


def _normalize_deny_key(k: str) -> tuple[str, str]:
    """Normalize denylist key.

    Accepts:
      - raw keys: "vol_ratio"
      - prefixed keys: "n:vol_ratio", "b:lob_dw_obi_stable"

    Returns (kind, raw_key) where kind in {"n","b","?"}.
    """
    s = (k or "").strip()
    if not s:
        return "?", ""
    if len(s) > 2 and s[1] == ":" and s[0] in ("n", "b"):
        return s[0], s[2:]
    return "?", s


def _load_denylist(path: str) -> tuple[set[str], set[str], str]:
    """Load denylist json.

    Expected keys:
      - deny_num: [..]
      - deny_bool: [..]
    Also tolerates a single list under "deny" with optional n:/b: prefixes.

    Returns (deny_num, deny_bool, denylist_hash16).
    """
    try:
        with open(path, encoding="utf-8") as f:
            obj = json.load(f)
    except Exception:
        return set(), set(), "na"

    deny_num: set[str] = set()
    deny_bool: set[str] = set()

    if isinstance(obj, dict):
        dn = obj.get("deny_num")
        db = obj.get("deny_bool")
        dany = obj.get("deny")

        if isinstance(dn, list):
            for k in dn:
                _, raw = _normalize_deny_key(str(k))
                if raw:
                    deny_num.add(raw)
        if isinstance(db, list):
            for k in db:
                _, raw = _normalize_deny_key(str(k))
                if raw:
                    deny_bool.add(raw)

        # Optional combined list with prefixes.
        if isinstance(dany, list):
            for k in dany:
                kind, raw = _normalize_deny_key(str(k))
                if not raw:
                    continue
                if kind == "n":
                    deny_num.add(raw)
                elif kind == "b":
                    deny_bool.add(raw)

    # Stable hash binding.
    payload = {
        "deny_num": sorted(deny_num),
        "deny_bool": sorted(deny_bool),
    }
    h = hashlib.sha256(json.dumps(payload, separators=(",", ":"), ensure_ascii=True).encode("utf-8")).hexdigest()
    return deny_num, deny_bool, h[:16]


@dataclass
class MLFeatureSchemaV5OFStable(MLFeatureSchemaV5OF):
    """v5_of_stable = v5_of - denylist.

    Safety:
      - by default we *protect* all v4_of core keys from being denied,
        even if they appear in denylist (misconfig protection)
      - denylist file is optional; missing file => no filtering
    """

    denylist_hash16: str = "na"

    def __post_init__(self) -> None:  # noqa: D401
        super().__post_init__()

        deny_path = (os.getenv("ML_FEATURE_DENYLIST_PATH") or "").strip() or _default_denylist_path()
        deny_num, deny_bool, h16 = _load_denylist(deny_path)
        self.denylist_hash16 = h16

        # Protect v4_of core by default.
        allow_core = int(os.getenv("ML_FEATURE_DENYLIST_ALLOW_CORE", "0") or 0) == 1
        try:
            core = MLFeatureSchemaV4OF()
            core_num = set(core.num_keys)
            core_bool = set(core.bool_keys)
        except Exception:
            core_num, core_bool = set(), set()

        if not allow_core:
            deny_num = {k for k in deny_num if k and k not in core_num}
            deny_bool = {k for k in deny_bool if k and k not in core_bool}

        if deny_num:
            self.num_keys = [k for k in self.num_keys if k not in deny_num]
        if deny_bool:
            self.bool_keys = [k for k in self.bool_keys if k not in deny_bool]
