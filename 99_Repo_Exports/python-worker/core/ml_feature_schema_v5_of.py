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

SCHEMA_HASH = "2db5bda868a6"  # Phase 7.6: +LOB velocity (8 slopes); Phase 7.7: +fill-queue lite (5 features)



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

            # ---------------------------------------------------------------
            # [Phase 7.8] Cross-context hydration — sourced from ADR-0005/06/07
            # services. Lag-guarded: stale entries map to 0.0 / True for `stale`.
            # ---------------------------------------------------------------
            # ADR-0006 anchor returns (BTC/ETH rolling)
            "btc_ret_30s", "btc_ret_1m", "btc_ret_5m",
            "eth_ret_30s", "eth_ret_1m", "eth_ret_5m",
            "rel_ret_1m_vs_btc", "rel_ret_5m_vs_btc",

            # ADR-0007 PIT priors
            "prior_winrate_symbol_kind_session",
            "prior_ev_r_symbol_kind_session",
            "prior_sample_count_log",  # log1p of sample_count to compress scale
            "prior_age_ms",

            # ADR-0005 TCA EMA priors
            "tca_eff_spread_bps_ema",
            "tca_realized_spread_1s_bps_ema",
            "tca_realized_spread_5s_bps_ema",
            "tca_perm_impact_1s_bps_ema",
            "tca_perm_impact_5s_bps_ema",
            "tca_is_bps_ema",
            "tca_samples",
            "tca_stale_ms",
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
            # Phase 7.8: ADR-0007 PIT prior staleness flag
            "prior_stale",
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
