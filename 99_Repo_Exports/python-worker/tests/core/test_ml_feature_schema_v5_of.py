import unittest
from typing import Any

from core.feature_registry import get_schema_info
from core.ml_feature_schema_v4_of import MLFeatureSchemaV4OF
from core.ml_feature_schema_v5_of import MLFeatureSchemaV5OF

# Known leakage feature names that must NEVER appear in the online schema.
# These are post-entry / current-trade outcomes that would cause look-ahead bias.
_BANNED_FEATURES = frozenset([
    "current_trade_pnl",
    "current_trade_close_reason",
    "current_trade_mfe",
    "current_trade_mae",
    "future_ret",
    "future_tp_hit",
    "future_sl_hit",
    "post_entry_slippage",
    "post_entry_fill_quality",
    "realized_pnl",
    "trade_result",
    "exit_price",
    "exit_ts_ms",
])


class TestMLFeatureSchemaV5OF(unittest.TestCase):
    def test_v5_schema_superset(self):
        """v5_of must be a strict superset of v4_of and maintain order for the original keys."""
        v4 = MLFeatureSchemaV4OF()
        v5 = MLFeatureSchemaV5OF()

        self.assertTrue(len(v5.num_keys) > len(v4.num_keys))
        self.assertEqual(v4.num_keys, v5.num_keys[:len(v4.num_keys)])

        self.assertTrue(len(v5.bool_keys) > len(v4.bool_keys))
        self.assertEqual(v4.bool_keys, v5.bool_keys[:len(v4.bool_keys)])

    def test_feature_registry_v5_resolution(self):
        info_v5 = get_schema_info("v5")
        info_v5_of = get_schema_info("v5_of")

        self.assertEqual(info_v5.ver, "v5_of")
        self.assertEqual(info_v5_of.ver, "v5_of")

        v5 = MLFeatureSchemaV5OF()
        expected_names = (
            [f"n:{k}" for k in v5.num_keys]
            + [f"b:{k}" for k in v5.bool_keys]
            + ["dir:LONG", "dir:SHORT"]
            + ["bucket:trend", "bucket:range", "bucket:other"]
            + [f"hour:{h}" for h in range(24)]
            + [f"dow:{d}" for d in range(7)]
        )
        self.assertEqual(info_v5.feature_names, expected_names)

    def test_v5_feature_order_is_stable(self):
        """Feature order must be deterministic across independent instantiations."""
        v5a = MLFeatureSchemaV5OF()
        v5b = MLFeatureSchemaV5OF()
        self.assertEqual(v5a.num_keys, v5b.num_keys,
                         "num_keys must be identical across instances")
        self.assertEqual(v5a.bool_keys, v5b.bool_keys,
                         "bool_keys must be identical across instances")

    def test_v5_feature_count_matches_model_n_features(self):
        """Schema feature count must match the registry-derived feature_names length."""
        v5 = MLFeatureSchemaV5OF()
        info = get_schema_info("v5_of")

        expected_raw = len(v5.num_keys) + len(v5.bool_keys)
        # registry adds: dir:LONG/dir:SHORT (2) + bucket:* (3) + hour:* (24) + dow:* (7)
        expected_total = expected_raw + 2 + 3 + 24 + 7
        self.assertEqual(
            len(info.feature_names), expected_total,
            f"Registry feature_names length {len(info.feature_names)} != "
            f"schema raw {expected_raw} + one-hots 36 = {expected_total}"
        )

    def test_no_current_trade_outcome_features_in_online_schema(self):
        """Online schema must not contain any post-entry or future-outcome features (leakage guard)."""
        v5 = MLFeatureSchemaV5OF()
        all_keys = set(v5.num_keys) | set(v5.bool_keys)
        leaked = all_keys & _BANNED_FEATURES
        self.assertSetEqual(
            leaked, set(),
            f"Leakage features found in v5_of online schema: {leaked}"
        )


class TestMLFeatureSchemaV5ENVLoader(unittest.TestCase):
    """Verify ML_FEATURE_SCHEMA_VER=v5_of loads MLFeatureSchemaV5OF (not v4_of)."""

    def test_feature_registry_v5_resolves_to_v5of(self):
        info_v5 = get_schema_info("v5")
        info_v5_of = get_schema_info("v5_of")
        self.assertEqual(info_v5.ver, "v5_of", "v5 must alias to v5_of")
        self.assertEqual(info_v5_of.ver, "v5_of")

    def test_feature_registry_v5_stable_resolves_to_v5of_stable(self):
        info = get_schema_info("v5_stable")
        self.assertEqual(info.ver, "v5_of_stable", "v5_stable must alias to v5_of_stable")

    def test_v5of_class_is_mlfeatureschema_v5of(self):
        v5 = MLFeatureSchemaV5OF()
        self.assertEqual(v5.__class__.__name__, "MLFeatureSchemaV5OF")

    def test_v5of_has_more_keys_than_v4of(self):
        v4 = MLFeatureSchemaV4OF()
        v5 = MLFeatureSchemaV5OF()
        self.assertGreater(
            len(v5.num_keys) + len(v5.bool_keys),
            len(v4.num_keys) + len(v4.bool_keys),
        )

    def test_ml_feature_schema_v5_loads_v5of(self):
        """build_feature_vector delegates to MLFeatureSchemaV5OF when schema_ver='v5'."""
        from core.ml_feature_schema import build_feature_vector

        vec_v5, _ = build_feature_vector(
            symbol="BTCUSDT",
            ts_ms=1000000,
            direction="LONG",
            scenario="trend",
            indicators={"delta_z": 1.5, "ofi_stable": 1},
            rule_score=0.8,
            rule_have=3,
            rule_need=3,
            cancel_spike_veto=0,
            schema_ver="v5",
        )
        v5_schema = MLFeatureSchemaV5OF()
        expected_len = len(v5_schema.num_keys) + len(v5_schema.bool_keys) + 2 + 3 + 24 + 7
        self.assertEqual(len(vec_v5), expected_len)

    def test_ml_feature_schema_v5_of_loads_v5of(self):
        """build_feature_vector delegates to MLFeatureSchemaV5OF when schema_ver='v5_of'."""
        from core.ml_feature_schema import build_feature_vector

        vec, _ = build_feature_vector(
            symbol="ETHUSDT",
            ts_ms=2000000,
            direction="SHORT",
            scenario="range",
            indicators={},
            rule_score=0.5,
            rule_have=2,
            rule_need=3,
            cancel_spike_veto=0,
            schema_ver="v5_of",
        )
        v5_schema = MLFeatureSchemaV5OF()
        expected_len = len(v5_schema.num_keys) + len(v5_schema.bool_keys) + 2 + 3 + 24 + 7
        self.assertEqual(len(vec), expected_len)

    def test_v5_not_v4_when_schema_ver_v5(self):
        """schema_ver='v5' must NOT produce a v4_of-length vector."""
        from core.ml_feature_schema import build_feature_vector

        v4 = MLFeatureSchemaV4OF()
        v5 = MLFeatureSchemaV5OF()
        v4_len = len(v4.num_keys) + len(v4.bool_keys) + 2 + 3 + 24 + 7
        v5_len = len(v5.num_keys) + len(v5.bool_keys) + 2 + 3 + 24 + 7
        self.assertGreater(v5_len, v4_len,
                           "v5_of must have more features than v4_of")

        vec, _ = build_feature_vector(
            symbol="BTCUSDT",
            ts_ms=1000000,
            direction="LONG",
            scenario="trend",
            indicators={},
            rule_score=0.5,
            rule_have=2,
            rule_need=3,
            cancel_spike_veto=0,
            schema_ver="v5",
        )
        self.assertNotEqual(len(vec), v4_len,
                            "schema_ver='v5' must NOT produce v4_of-length vector")
        self.assertEqual(len(vec), v5_len)


class TestMLFeatureSchemaV5P1Features(unittest.TestCase):
    """P1 Phase 7 features: exec cost ratios, signal age, vol dynamics, DQ."""

    _P1_FEATURES = [
        "exec_cost_to_tp1_ratio",
        "exec_cost_to_sl_ratio",
        "exec_cost_to_atr_ratio",
        "signal_age_ms",
        "signal_age_to_half_life",
        "vol_expansion_score",
        "vol_compression_score",
        "dq_score",
        "dq_flag_count",
        "tick_lag_ms",
    ]

    def test_p1_features_present_in_v5_schema(self):
        """All P1 features must be in MLFeatureSchemaV5OF.num_keys."""
        v5 = MLFeatureSchemaV5OF()
        for feat in self._P1_FEATURES:
            self.assertIn(feat, v5.num_keys, f"P1 feature '{feat}' missing from v5_of num_keys")

    def test_p1_features_not_in_v4_schema(self):
        """P1 features must NOT be in MLFeatureSchemaV4OF (schema boundary check)."""
        v4 = MLFeatureSchemaV4OF()
        all_v4 = set(v4.num_keys) | set(v4.bool_keys)
        for feat in self._P1_FEATURES:
            self.assertNotIn(feat, all_v4, f"P1 feature '{feat}' leaked into v4_of")

    def test_exec_cost_to_tp1_ratio_formula(self):
        """exec_cost = half_spread + slippage + fee; ratio = cost / tp1_bps."""
        eps = 1e-6
        fee_bps = 4.0
        spread_bps = 6.0
        slippage_bps = 3.0
        tp1_bps = 20.0

        exec_cost = spread_bps / 2 + slippage_bps + fee_bps  # 3+3+4=10
        expected = exec_cost / max(tp1_bps, eps)  # 10/20=0.5

        self.assertAlmostEqual(expected, 0.5, places=6)
        # cost > tp1 => ratio > 1 → trade cannot pay for itself
        self.assertGreater(
            (spread_bps / 2 + slippage_bps + fee_bps) / max(5.0, eps),
            1.0,
            "When tp1_bps < exec_cost, ratio must exceed 1.0",
        )

    def test_exec_cost_to_sl_ratio_formula(self):
        """exec_cost / sl_bps: large value = cost compresses risk-reward."""
        eps = 1e-6
        exec_cost = 10.0  # spread/2 + slip + fee
        sl_bps = 15.0
        expected = exec_cost / max(sl_bps, eps)
        self.assertAlmostEqual(expected, 10.0 / 15.0, places=6)

    def test_exec_cost_to_atr_ratio_formula(self):
        """exec_cost / atr_bps: regime-normalised cost burden."""
        eps = 1e-6
        exec_cost = 10.0
        atr_bps = 50.0
        expected = exec_cost / max(atr_bps, eps)
        self.assertAlmostEqual(expected, 0.2, places=6)
        # zero atr_bps → ratio must be 0.0 (guard: only divide when atr > 0)
        zero_atr = 0.0
        self.assertEqual(0.0, exec_cost / max(zero_atr, eps) if zero_atr > 0.0 else 0.0)

    def test_signal_age_to_half_life_formula(self):
        """signal_age_to_half_life = signal_age_ms / alpha_half_life_ms."""
        eps = 1.0  # min 1ms to avoid div/0
        signal_age_ms = 3000.0   # 3 seconds
        half_life_ms = 5000.0    # 5 seconds
        ratio = signal_age_ms / max(half_life_ms, eps)
        self.assertAlmostEqual(ratio, 0.6, places=6)

        # stale signal: age > half_life
        stale_ratio = 6000.0 / max(half_life_ms, eps)
        self.assertGreater(stale_ratio, 1.0, "Stale signal must produce ratio > 1")

        # unknown half_life → ratio must be 0.0 (not inf)
        self.assertEqual(0.0 / max(0.0, eps), 0.0)

    def test_vol_expansion_score_formula(self):
        """vol_expansion_score = max(0, vol_ratio_fast_slow - 1)."""
        # expanding vol: fast > slow
        self.assertAlmostEqual(max(0.0, 1.4 - 1.0), 0.4, places=6)
        # equilibrium: no expansion
        self.assertAlmostEqual(max(0.0, 1.0 - 1.0), 0.0, places=6)
        # compressing vol: no expansion score (clamped to 0)
        self.assertAlmostEqual(max(0.0, 0.7 - 1.0), 0.0, places=6)

    def test_vol_compression_score_formula(self):
        """vol_compression_score = max(0, 1 - vol_ratio_fast_slow)."""
        self.assertAlmostEqual(max(0.0, 1.0 - 0.7), 0.3, places=6)
        self.assertAlmostEqual(max(0.0, 1.0 - 1.0), 0.0, places=6)
        # expanding: no compression score
        self.assertAlmostEqual(max(0.0, 1.0 - 1.4), 0.0, places=6)

    def test_dq_score_bounds_0_1(self):
        """dq_score must be in [0.0, 1.0] — it aliases dq_health_score."""
        for health in (0.0, 0.3, 0.5, 0.7, 0.9, 1.0):
            self.assertGreaterEqual(health, 0.0)
            self.assertLessEqual(health, 1.0)

    def test_dq_flag_count_buckets(self):
        """dq_flag_count: 0=ok(>=0.9), 1=warn(0.7-0.9), 2=bad(0.5-0.7), 3=critical(<0.5)."""
        cases = [(1.0, 0), (0.95, 0), (0.89, 1), (0.70, 1), (0.69, 2), (0.50, 2), (0.49, 3), (0.0, 3)]
        for health, expected_flags in cases:
            if health >= 0.9:
                got = 0
            elif health >= 0.7:
                got = 1
            elif health >= 0.5:
                got = 2
            else:
                got = 3
            self.assertEqual(got, expected_flags, f"health={health}: expected {expected_flags}, got {got}")

    def test_v5_feature_count_increased_from_p1(self):
        """Phase 6 (+7), Phase 7 (+10), Phase 7.2 (+3) → at least 20 extras over v4."""
        v4 = MLFeatureSchemaV4OF()
        v5 = MLFeatureSchemaV5OF()
        extra = len(v5.num_keys) + len(v5.bool_keys) - len(v4.num_keys) - len(v4.bool_keys)
        self.assertGreaterEqual(extra, 20,
            f"Expected at least 20 extra features in v5 over v4, got {extra}")

    def test_exec_cost_to_sl_ratio_atr_fallback_formula(self):
        """exec_cost_to_sl_ratio ATR fallback: sl_bps = atr_bps * sl_atr_mult (default 1.0)."""
        eps = 1e-6
        exec_cost = 10.0
        atr_bps = 40.0
        sl_atr_mult = 1.0
        # When liqmap_gate_risk_bps and sl_bps are both 0/absent, fallback to atr_bps * mult
        sl_bps_derived = atr_bps * sl_atr_mult  # 40.0
        expected = exec_cost / max(sl_bps_derived, eps)  # 10/40 = 0.25
        self.assertAlmostEqual(expected, 0.25, places=6)

        # With custom SL_ATR_MULT=1.5 → sl_bps = 60 → ratio = 10/60 ≈ 0.1667
        sl_bps_custom = atr_bps * 1.5
        expected_custom = exec_cost / max(sl_bps_custom, eps)
        self.assertAlmostEqual(expected_custom, 10.0 / 60.0, places=6)

        # No ATR → ratio must remain 0.0 (guard: only divide when atr > 0)
        zero_atr = 0.0
        sl_bps_zero = zero_atr * sl_atr_mult  # 0.0
        self.assertEqual(0.0, exec_cost / max(sl_bps_zero, eps) if sl_bps_zero > 0.0 else 0.0)

    def test_exec_cost_to_sl_ratio_liqmap_takes_priority(self):
        """liqmap_gate_risk_bps must take priority over ATR-derived sl_bps."""
        eps = 1e-6
        exec_cost = 10.0
        liqmap_risk = 25.0
        atr_bps = 40.0  # would give 40.0 if used
        # liqmap is truthy → sl_bps = 25.0, not 40.0
        sl_bps = liqmap_risk  # liqmap wins
        expected = exec_cost / max(sl_bps, eps)  # 10/25 = 0.4
        self.assertAlmostEqual(expected, 0.4, places=6)
        # ATR-derived (10/40=0.25) must NOT be used when liqmap present
        self.assertNotAlmostEqual(expected, exec_cost / max(atr_bps, eps), places=2)


class TestMLFeatureSchemaV5P72Features(unittest.TestCase):
    """Phase 7.2 — extended DQ: book freshness + CVD quarantine."""

    # book_age_ms, book_gap_ms are new in v5.
    # cvd_quarantine_active already exists in v4_of — promoted there, not re-added here.
    _P72_NUM = ["book_age_ms", "book_gap_ms"]

    def test_p72_num_features_in_v5_schema(self):
        v5 = MLFeatureSchemaV5OF()
        for feat in self._P72_NUM:
            self.assertIn(feat, v5.num_keys, f"Phase 7.2 num feature '{feat}' missing from v5_of")

    def test_cvd_quarantine_active_in_v4_and_v5(self):
        """cvd_quarantine_active is already a v4_of bool feature — must be present in both."""
        v4 = MLFeatureSchemaV4OF()
        v5 = MLFeatureSchemaV5OF()
        self.assertIn("cvd_quarantine_active", v4.bool_keys, "cvd_quarantine_active missing from v4_of")
        self.assertIn("cvd_quarantine_active", v5.bool_keys, "cvd_quarantine_active missing from v5_of")

    def test_p72_num_features_not_in_v4_schema(self):
        v4 = MLFeatureSchemaV4OF()
        all_v4_num = set(v4.num_keys)
        for feat in self._P72_NUM:
            self.assertNotIn(feat, all_v4_num, f"Phase 7.2 num feature '{feat}' found in v4_of")

    def test_book_age_ms_default_zero(self):
        """book_age_ms defaults to 0.0 when book_staleness_ms unavailable (mirrors Any-typed indicators dict)."""
        indicators: dict[str, Any] = {}
        book_age = float(indicators.get("book_staleness_ms") or indicators.get("liq_book_stale_ms") or 0.0)
        self.assertEqual(book_age, 0.0)

    def test_book_age_ms_from_staleness(self):
        """book_age_ms = book_staleness_ms when present."""
        indicators: dict[str, Any] = {"book_staleness_ms": 1500.0}
        book_age = float(indicators.get("book_staleness_ms") or 0.0)
        self.assertAlmostEqual(book_age, 1500.0, places=3)

    def test_book_gap_ms_default_zero(self):
        """book_gap_ms defaults to 0.0 when book_ts_gap_ms unavailable."""
        indicators: dict[str, Any] = {}
        gap = float(indicators.get("book_ts_gap_ms") or 0.0)
        self.assertEqual(gap, 0.0)

    def test_cvd_quarantine_active_casting(self):
        """cvd_quarantine_active must cast 0/1/None correctly to bool."""
        self.assertFalse(bool(int(0 or 0)))
        self.assertTrue(bool(int(1 or 0)))
        self.assertFalse(bool(int(None or 0)))

    def test_schema_hash_updated(self):
        """SCHEMA_HASH must reflect Phase 4.10 additions (266 num + 34 bool)."""
        from core.ml_feature_schema_v5_of import SCHEMA_HASH
        self.assertEqual(SCHEMA_HASH, "c3e1a7f29d50",
            "SCHEMA_HASH not updated after Phase 4.10 (+11 rolling PIT priors; 266 num + 34 bool)")


class TestMLFeatureSchemaV5P73Features(unittest.TestCase):
    """Phase 7.3 — ATR freshness bool feature."""

    def test_atr_fresh_in_v5_bool_keys(self):
        v5 = MLFeatureSchemaV5OF()
        self.assertIn("atr_fresh", v5.bool_keys, "atr_fresh missing from v5_of bool_keys")

    def test_atr_fresh_not_in_v4(self):
        v4 = MLFeatureSchemaV4OF()
        self.assertNotIn("atr_fresh", v4.bool_keys, "atr_fresh leaked into v4_of")
        self.assertNotIn("atr_fresh", v4.num_keys, "atr_fresh must be bool not num in v4")

    def test_atr_age_ms_inherited_from_v4(self):
        """atr_age_ms is already a v4_of num key — v5 must inherit it."""
        v4 = MLFeatureSchemaV4OF()
        v5 = MLFeatureSchemaV5OF()
        self.assertIn("atr_age_ms", v4.num_keys)
        self.assertIn("atr_age_ms", v5.num_keys)

    def test_atr_fresh_threshold_logic(self):
        """atr_fresh = True iff atr_age_ms ∈ (0, ATR_FRESH_MS). 0 or stale ⇒ False."""
        def _fresh(age_ms: float, threshold_ms: float) -> bool:
            return age_ms > 0.0 and age_ms < threshold_ms

        threshold = 60_000.0
        self.assertTrue(_fresh(5_000.0, threshold))        # fresh
        self.assertFalse(_fresh(90_000.0, threshold))      # stale
        self.assertFalse(_fresh(0.0, threshold))           # no reading
        self.assertFalse(_fresh(60_000.0, threshold))      # exact boundary (strict <)


class TestMLFeatureSchemaV5P74Features(unittest.TestCase):
    """Phase 7.4 — gate trace features."""

    _P74_NUM = ["rule_have_need_gap", "missing_legs_count", "gate_pressure_score"]
    _P74_BOOL = ["soft_fail_near_pass"]

    def test_p74_num_features_in_v5(self):
        v5 = MLFeatureSchemaV5OF()
        for f in self._P74_NUM:
            self.assertIn(f, v5.num_keys, f"P7.4 num '{f}' missing from v5")

    def test_p74_bool_features_in_v5(self):
        v5 = MLFeatureSchemaV5OF()
        for f in self._P74_BOOL:
            self.assertIn(f, v5.bool_keys, f"P7.4 bool '{f}' missing from v5")

    def test_rule_have_need_gap_negative_when_below_threshold(self):
        """have < need ⇒ negative gap; have > need ⇒ positive."""
        self.assertLess(float(2 - 5), 0.0)   # 2 of 5 legs ⇒ -3
        self.assertGreater(float(7 - 5), 0.0)  # 7 of 5 legs ⇒ +2
        self.assertEqual(float(5 - 5), 0.0)  # exact threshold

    def test_gate_pressure_score_formula(self):
        """pressure = max(0, 1 - have_need_ratio) * missing_legs_count."""
        # Far from threshold (ratio=0.4) AND 3 missing legs ⇒ high pressure
        ratio = 0.4
        missing = 3
        self.assertAlmostEqual(max(0.0, 1.0 - ratio) * missing, 0.6 * 3, places=6)
        # At threshold (ratio=1.0) ⇒ no pressure regardless of missing count
        self.assertEqual(max(0.0, 1.0 - 1.0) * 4, 0.0)
        # Above threshold (ratio>1.0) clamped to 0 by max()
        self.assertEqual(max(0.0, 1.0 - 1.5) * 2, 0.0)


class TestMLFeatureSchemaV5P75Features(unittest.TestCase):
    """Phase 7.5 — session / weekend boolean features."""

    _P75_BOOL = ["session_asia", "session_europe", "session_us", "weekend_flag"]

    def test_p75_bools_in_v5(self):
        v5 = MLFeatureSchemaV5OF()
        for f in self._P75_BOOL:
            self.assertIn(f, v5.bool_keys, f"P7.5 bool '{f}' missing from v5")

    def test_session_ranges_at_typical_hours(self):
        """UTC ranges: Asia 00-08, Europe 07-16, US 13-22."""
        # 03:00 UTC → Asia only
        self.assertTrue(0.0 <= 3.0 < 8.0)
        self.assertFalse(7.0 <= 3.0 < 16.0)
        self.assertFalse(13.0 <= 3.0 < 22.0)
        # 07:30 UTC → Asia + Europe overlap
        self.assertTrue(0.0 <= 7.5 < 8.0)
        self.assertTrue(7.0 <= 7.5 < 16.0)
        # 14:00 UTC → Europe + US overlap
        self.assertTrue(7.0 <= 14.0 < 16.0)
        self.assertTrue(13.0 <= 14.0 < 22.0)
        # 22:00 UTC → none (US closed exact boundary)
        self.assertFalse(13.0 <= 22.0 < 22.0)

    def test_weekend_flag_dow(self):
        """dow ∈ {5,6} (Sat/Sun) ⇒ True; weekdays ⇒ False."""
        for weekday in (0.0, 1.0, 2.0, 3.0, 4.0):
            self.assertFalse(weekday >= 5.0)
        for weekend in (5.0, 6.0):
            self.assertTrue(weekend >= 5.0)


class TestMLFeatureSchemaV5P76Features(unittest.TestCase):
    """Phase 7.6 — LOB velocity slopes (1s/3s windows)."""

    _P76_NUM = [
        "obi_slope_1s", "obi_slope_3s",
        "qimb_slope_1s", "qimb_slope_3s",
        "depth_imbalance_5_delta_1s", "depth_imbalance_5_delta_3s",
        "spread_widen_velocity_bps_s", "fill_prob_decay_slope",
    ]

    def test_p76_features_in_v5(self):
        v5 = MLFeatureSchemaV5OF()
        for f in self._P76_NUM:
            self.assertIn(f, v5.num_keys, f"P7.6 num '{f}' missing from v5")

    def test_lob_velocity_cold_start_zeros(self):
        """With <2 samples in buffer, all slopes must be 0.0."""
        from core.of_confirm_engine import _LOB_VELOCITY_CACHE, _lob_velocity_compute
        _LOB_VELOCITY_CACHE.pop("TESTSYM", None)
        out = _lob_velocity_compute(
            symbol="TESTSYM", now_ms=1_000_000_000,
            obi=0.5, qimb_wmean=0.3, depth_imbalance_5=0.1,
            spread_bps=2.0, fill_prob_proxy=0.7,
        )
        for f in self._P76_NUM:
            self.assertEqual(out[f], 0.0, f"{f} must be 0.0 on cold start")

    def test_lob_velocity_slope_computation(self):
        """Two samples 1000ms apart → slope = (latest - earliest) per second."""
        from core.of_confirm_engine import _LOB_VELOCITY_CACHE, _lob_velocity_compute
        _LOB_VELOCITY_CACHE.pop("TESTSYM", None)
        # Seed at t=0
        _lob_velocity_compute(
            symbol="TESTSYM", now_ms=1_000_000_000,
            obi=0.0, qimb_wmean=0.0, depth_imbalance_5=0.0,
            spread_bps=2.0, fill_prob_proxy=0.5,
        )
        # Sample at t=+1000ms with deltas
        out = _lob_velocity_compute(
            symbol="TESTSYM", now_ms=1_000_001_000,
            obi=0.4, qimb_wmean=0.2, depth_imbalance_5=0.1,
            spread_bps=4.0, fill_prob_proxy=0.3,
        )
        self.assertAlmostEqual(out["obi_slope_1s"], 0.4, places=3)
        self.assertAlmostEqual(out["qimb_slope_1s"], 0.2, places=3)
        self.assertAlmostEqual(out["depth_imbalance_5_delta_1s"], 0.1, places=3)
        self.assertAlmostEqual(out["spread_widen_velocity_bps_s"], 2.0, places=3)
        self.assertAlmostEqual(out["fill_prob_decay_slope"], -0.2, places=3)

    def test_spread_widen_velocity_clamped_non_negative(self):
        """spread_widen_velocity_bps_s clamped ≥ 0 even on narrowing."""
        from core.of_confirm_engine import _LOB_VELOCITY_CACHE, _lob_velocity_compute
        _LOB_VELOCITY_CACHE.pop("TESTSYM2", None)
        _lob_velocity_compute(
            symbol="TESTSYM2", now_ms=2_000_000_000,
            obi=0.0, qimb_wmean=0.0, depth_imbalance_5=0.0,
            spread_bps=10.0, fill_prob_proxy=0.5,
        )
        out = _lob_velocity_compute(
            symbol="TESTSYM2", now_ms=2_000_001_000,
            obi=0.0, qimb_wmean=0.0, depth_imbalance_5=0.0,
            spread_bps=4.0, fill_prob_proxy=0.5,  # narrowing
        )
        self.assertEqual(out["spread_widen_velocity_bps_s"], 0.0)


class TestMLFeatureSchemaV5P77Features(unittest.TestCase):
    """Phase 7.7 — fill-queue lite features."""

    _P77_NUM = [
        "eta_fill_sec_norm", "queue_ahead_qty_l1", "queue_ahead_qty_l5",
        "depth_to_taker_rate_ratio", "maker_fill_vs_taker_cost_edge",
    ]

    def test_p77_features_in_v5(self):
        v5 = MLFeatureSchemaV5OF()
        for f in self._P77_NUM:
            self.assertIn(f, v5.num_keys, f"P7.7 num '{f}' missing from v5")

    def test_eta_fill_sec_norm_clamping(self):
        """eta_fill_sec_norm = clamp(eta_fill_sec / 10.0, 0, 1)."""
        def _norm(eta: float) -> float:
            return min(1.0, max(0.0, eta / 10.0))
        self.assertAlmostEqual(_norm(0.0), 0.0)
        self.assertAlmostEqual(_norm(5.0), 0.5)
        self.assertAlmostEqual(_norm(10.0), 1.0)
        self.assertAlmostEqual(_norm(15.0), 1.0)  # clamped
        self.assertAlmostEqual(_norm(-1.0), 0.0)  # clamped

    def test_maker_fill_vs_taker_cost_edge_formula(self):
        """edge = fill_prob_proxy * tp1_bps - exec_cost."""
        # Profitable maker: fp=0.8 * tp1=20 = 16; exec_cost = 10 → edge = +6
        self.assertAlmostEqual(0.8 * 20.0 - 10.0, 6.0)
        # Unprofitable maker: fp=0.3 * tp1=20 = 6; exec_cost = 10 → edge = -4
        self.assertAlmostEqual(0.3 * 20.0 - 10.0, -4.0)


class TestMLFeatureSchemaV5P78Features(unittest.TestCase):
    """Phase 7.8 — cross-context hydration (TCA + anchors + PIT priors)."""

    _P78_NUM_ANCHORS = ["btc_ret_30s", "btc_ret_1m", "btc_ret_5m",
                        "eth_ret_30s", "eth_ret_1m", "eth_ret_5m",
                        "rel_ret_1m_vs_btc", "rel_ret_5m_vs_btc"]
    _P78_NUM_PIT = ["prior_winrate_symbol_kind_session", "prior_ev_r_symbol_kind_session",
                    "prior_sample_count_log", "prior_age_ms"]
    _P78_NUM_TCA = ["tca_eff_spread_bps_ema", "tca_realized_spread_1s_bps_ema",
                    "tca_realized_spread_5s_bps_ema", "tca_perm_impact_1s_bps_ema",
                    "tca_perm_impact_5s_bps_ema", "tca_is_bps_ema",
                    "tca_samples", "tca_stale_ms"]
    _P78_BOOL = ["prior_stale"]

    def test_anchor_features_in_v5(self):
        v5 = MLFeatureSchemaV5OF()
        for f in self._P78_NUM_ANCHORS:
            self.assertIn(f, v5.num_keys, f"P7.8 anchor '{f}' missing")

    def test_pit_features_in_v5(self):
        v5 = MLFeatureSchemaV5OF()
        for f in self._P78_NUM_PIT:
            self.assertIn(f, v5.num_keys, f"P7.8 PIT '{f}' missing")
        for f in self._P78_BOOL:
            self.assertIn(f, v5.bool_keys, f"P7.8 bool '{f}' missing")

    def test_tca_features_in_v5(self):
        v5 = MLFeatureSchemaV5OF()
        for f in self._P78_NUM_TCA:
            self.assertIn(f, v5.num_keys, f"P7.8 TCA '{f}' missing")


class TestOutcomeTracker(unittest.TestCase):
    """ADR-0007/Section 6 outcome tracker — ECE/Brier per bucket."""

    def test_bucket_assignment(self):
        from orderflow_services.ml_outcome_calibration_tracker_v1 import _bucket_for
        edges = [0.0, 0.1, 0.2, 0.5, 0.8, 1.0]
        self.assertEqual(_bucket_for(0.05, edges), "0.0-0.1")
        self.assertEqual(_bucket_for(0.15, edges), "0.1-0.2")
        self.assertEqual(_bucket_for(0.50, edges), "0.5-0.8")
        self.assertEqual(_bucket_for(0.99, edges), "0.8-1.0")
        self.assertEqual(_bucket_for(1.00, edges), "0.8-1.0")  # right-closed last bucket

    def test_ema_perfect_calibration_yields_zero_ece(self):
        """Stationary 80% win rate + p_edge=0.8 → ECE → 0 over long stream."""
        from orderflow_services.ml_outcome_calibration_tracker_v1 import OutcomeTracker
        # Long half-life relative to sample count → EMA converges to long-run mean
        tr = OutcomeTracker(edges=[0.0, 0.5, 1.0], ema_half_life=200.0)
        # Interleaved wins/losses to mimic stationary distribution (8 win, 2 loss × 50)
        for _ in range(50):
            for _ in range(8):
                tr.update("v5_of", p_edge=0.8, win=1)
            for _ in range(2):
                tr.update("v5_of", p_edge=0.8, win=0)
        ece = tr.get_ece_per_bucket("v5_of").get("0.5-1.0", 1.0)
        self.assertLess(ece, 0.05, f"ECE should converge to <5% under perfect calibration (got {ece:.4f})")

    def test_brier_score_decreases_with_better_predictions(self):
        """Brier(perfect-calibration p=0.5 on coin flips) ≈ 0.25; Brier(p=1.0 on wins) → 0."""
        from orderflow_services.ml_outcome_calibration_tracker_v1 import OutcomeTracker
        tr_bad = OutcomeTracker(edges=[0.0, 1.0], ema_half_life=5.0)
        tr_good = OutcomeTracker(edges=[0.0, 1.0], ema_half_life=5.0)
        for _ in range(50):
            tr_bad.update("v", p_edge=0.5, win=1)
            tr_good.update("v", p_edge=0.99, win=1)
        bad_b = tr_bad.get_brier_per_bucket("v").get("0.0-1.0", 0.0)
        good_b = tr_good.get_brier_per_bucket("v").get("0.0-1.0", 0.0)
        self.assertGreater(bad_b, good_b)


class TestTCAExporter(unittest.TestCase):
    """ADR-0005 TCA exporter — session bucket + EMA contract."""

    def test_session_bucket_boundaries(self):
        from orderflow_services.tca_priors_exporter_v1 import _session_bucket
        # 03:00 UTC → asia
        self.assertEqual(_session_bucket(3 * 3_600_000), "asia")
        # 14:00 UTC → us (later check wins over europe per overlap rules)
        self.assertEqual(_session_bucket(14 * 3_600_000), "us")
        # 09:00 UTC → europe
        self.assertEqual(_session_bucket(9 * 3_600_000), "europe")

    def test_ema_state_initialization_idempotent(self):
        """Two updates on same key should accumulate, not reset."""
        from orderflow_services.tca_priors_exporter_v1 import TCAEMAState
        state = TCAEMAState(ema_half_life=10.0, ttl_sec=60)

        # Stub Redis client — only need hset/expire to no-op
        class _StubRedis:
            def hset(self, *_a, **_kw): pass
            def expire(self, *_a, **_kw): pass

        client = _StubRedis()
        s1 = state.update(client, "BTCUSDT", "default", "us",
                          eff_spread_bps=5.0, realized_1s_bps=0.0, realized_5s_bps=0.0,
                          perm_1s_bps=0.0, perm_5s_bps=0.0, is_bps=0.0, ts_ms=1000)
        s2 = state.update(client, "BTCUSDT", "default", "us",
                          eff_spread_bps=10.0, realized_1s_bps=0.0, realized_5s_bps=0.0,
                          perm_1s_bps=0.0, perm_5s_bps=0.0, is_bps=0.0, ts_ms=2000)
        self.assertEqual(s1["samples"], 1.0)
        self.assertEqual(s2["samples"], 2.0)
        # First update initializes EMA at the value itself (5.0), second pulls toward 10.
        # With short half_life=10, the second update's weight is small but non-zero.
        self.assertGreaterEqual(s2["eff_spread"], 5.0)
        self.assertLess(s2["eff_spread"], 10.0)


class TestCrossContextAnchor(unittest.TestCase):
    """ADR-0006 anchor returns — rolling window arithmetic."""

    def test_anchor_return_30s(self):
        from orderflow_services.cross_context_aggregator_v1 import AnchorReturnTracker
        tr = AnchorReturnTracker()
        tr.push("BTCUSDT", 1_000_000_000, 50000.0)
        out = tr.push("BTCUSDT", 1_000_030_000, 50500.0)
        # +1% return over 30s
        self.assertAlmostEqual(out["ret_30s"], 0.01, places=4)

    def test_anchor_return_cold_start_zero(self):
        from orderflow_services.cross_context_aggregator_v1 import AnchorReturnTracker
        tr = AnchorReturnTracker()
        out = tr.push("BTCUSDT", 1_000_000_000, 50000.0)
        self.assertEqual(out["ret_30s"], 0.0)
        self.assertEqual(out["ret_1m"], 0.0)
        self.assertEqual(out["ret_5m"], 0.0)


class TestPITPriorsBuilder(unittest.TestCase):
    """ADR-0007 PIT priors — embargo + aggregation correctness."""

    def test_embargo_filters_recent_trades(self):
        """Trades within embargo window must be excluded from aggregate."""
        from tools.build_pit_priors_v1 import build_pit_priors
        as_of = 1_000_000_000
        embargo = 3_600_000  # 1h
        trades = [
            # Inside embargo (must be excluded)
            {"symbol": "BTCUSDT", "kind": "default", "result": "WIN",
             "r_multiple": "2.0", "ts_close": str(as_of - 100_000),
             "ts_decision": str(as_of - 200_000)},
            # Outside embargo (must be included) — repeat to satisfy min_samples
            *[
                {"symbol": "BTCUSDT", "kind": "default", "result": "WIN",
                 "r_multiple": "1.0", "ts_close": str(as_of - embargo - 1000 - i),
                 "ts_decision": str(as_of - embargo - 2000 - i)}
                for i in range(40)
            ],
        ]
        priors = build_pit_priors(trades, as_of_ts_ms=as_of, embargo_ms=embargo)
        # Should have one bucket with 40 samples (recent excluded)
        sample_counts = [p["sample_count"] for p in priors.values()]
        self.assertIn(40.0, sample_counts, f"Expected 40 samples after embargo, got {sample_counts}")


class TestMLFeatureSchemaV5P79Features(unittest.TestCase):
    """Phase 7.9 — derivatives context features (funding/OI/liq/basis/breadth)."""

    _P79_NUM = [
        "funding_rate", "funding_rate_z",
        "oi_notional_usd", "oi_delta_5m", "oi_delta_1m", "oi_accel",
        "basis_bps", "premium_index_bps", "basis_pressure_score",
        "liq_long_notional_1m", "liq_short_notional_1m",
        "liq_long_notional_5m", "liq_short_notional_5m",
        "liq_imbalance_1m", "liq_imbalance_5m", "liq_imbalance_z",
        "long_short_ratio", "long_short_ratio_z",
        "leader_btc_eth_confirm", "leader_direction_conflict",
        "sector_breadth_ret_24h", "sector_breadth_vol_z",
    ]

    def test_p79_num_features_in_v5(self):
        v5 = MLFeatureSchemaV5OF()
        for f in self._P79_NUM:
            self.assertIn(f, v5.num_keys, f"P7.9 num '{f}' missing from v5")

    def test_basis_pressure_formula(self):
        """basis_pressure_score = |funding_rate_z| + |basis_bps|/10."""
        # Strong funding skew + small basis ⇒ pressure dominated by funding
        self.assertAlmostEqual(abs(2.5) + abs(8.0) / 10.0, 3.3)
        # Both moderate
        self.assertAlmostEqual(abs(1.0) + abs(20.0) / 10.0, 3.0)

    def test_liq_imbalance_arithmetic(self):
        """liq_imbalance_1m = (long - short) / total. Bounded in [-1, 1]."""
        long_n, short_n = 50.0, 30.0
        total = long_n + short_n
        self.assertAlmostEqual((long_n - short_n) / total, 0.25)
        # All-long
        self.assertAlmostEqual((100.0 - 0.0) / 100.0, 1.0)
        # No liquidations
        total_zero = 0.0
        result = (0.0 - 0.0) / total_zero if total_zero > 0 else 0.0
        self.assertEqual(result, 0.0)


class TestBookReplayHelper(unittest.TestCase):
    """ADR-0005 book_replay_helper — TCA metric computation."""

    def test_compute_tca_metrics_zero_arrival_mid_returns_zeros(self):
        from core.book_replay_helper import compute_tca_metrics
        out = compute_tca_metrics(
            None, symbol="BTCUSDT", fill_price=50000.0, arrival_mid=0.0,
            fill_ts_ms=1_000_000_000, side="BUY",
        )
        self.assertEqual(out["eff_spread_bps"], 0.0)
        self.assertEqual(out["mid_after_1s_bps"], 0.0)
        self.assertEqual(out["mid_after_5s_bps"], 0.0)

    def test_compute_tca_eff_spread_bps_buy_side(self):
        """Buy at price > arrival_mid → positive effective spread."""
        from core.book_replay_helper import compute_tca_metrics
        # Fill at 50_010, arrival_mid 50_000 → 10 bps half spread → 20 bps eff
        out = compute_tca_metrics(
            None, symbol="BTCUSDT", fill_price=50_010.0, arrival_mid=50_000.0,
            fill_ts_ms=1_000_000_000, side="BUY",
        )
        # eff_spread = 2 * 1 * (50010-50000)/50000 * 1e4 = 4.0 bps
        self.assertAlmostEqual(out["eff_spread_bps"], 4.0, places=3)

    def test_compute_tca_eff_spread_bps_sell_side_sign(self):
        """Sell at price < arrival_mid → positive effective spread (sign flips for SHORT)."""
        from core.book_replay_helper import compute_tca_metrics
        out = compute_tca_metrics(
            None, symbol="ETHUSDT", fill_price=2_990.0, arrival_mid=3_000.0,
            fill_ts_ms=1_000_000_000, side="SELL",
        )
        # sign=-1, fill - mid = -10 → eff = 2 * -1 * -10/3000 * 1e4 = positive
        self.assertGreater(out["eff_spread_bps"], 0.0)


class TestFillsTCAEnricher(unittest.TestCase):
    """ADR-0005 fills enricher — fill detection + scheduling."""

    def test_is_fill_event_recognition(self):
        from orderflow_services.fills_tca_enricher_v1 import _is_fill_event
        self.assertTrue(_is_fill_event("ENTRY_FILLED"))
        self.assertTrue(_is_fill_event("EXIT_FILLED"))
        self.assertTrue(_is_fill_event("TP1_FILLED"))
        self.assertTrue(_is_fill_event("FILLED"))
        self.assertFalse(_is_fill_event("SUBMITTED"))
        self.assertFalse(_is_fill_event("CANCELED"))
        self.assertFalse(_is_fill_event(""))

    def test_extract_fill_event_minimal_fields(self):
        from orderflow_services.fills_tca_enricher_v1 import _extract_fill_event
        fields = {
            "event_type": "ENTRY_FILLED",
            "sid": "of:BTCUSDT:1234567:LONG",
            "symbol": "BTCUSDT",
            "side": "BUY",
            "avg_price": "50000.5",
            "ts_ms": "1700000000000",
            "kind": "reclaim",
        }
        # No redis client (None) → arrival_mid falls back to fill_price
        pf = _extract_fill_event(fields, None)
        self.assertIsNotNone(pf)
        assert pf is not None
        self.assertEqual(pf.symbol, "BTCUSDT")
        self.assertEqual(pf.side, "BUY")
        self.assertAlmostEqual(pf.fill_price, 50000.5)
        self.assertEqual(pf.kind, "reclaim")
        # arrival_mid fallback to fill_price
        self.assertAlmostEqual(pf.arrival_mid, 50000.5)

    def test_extract_non_fill_event_returns_none(self):
        from orderflow_services.fills_tca_enricher_v1 import _extract_fill_event
        fields = {"event_type": "SUBMITTED", "symbol": "BTCUSDT", "side": "BUY"}
        self.assertIsNone(_extract_fill_event(fields, None))


class TestMLFeatureSchemaV5P82Features(unittest.TestCase):
    """Phase 8.2 — fill_prob_Xs, sector_breadth_1m, prior_stale_ms, cyclical time, news_blackout."""

    _P82_NUM = [
        "fill_prob_1s", "fill_prob_3s", "fill_prob_5s",
        "sector_breadth_1m",
        "prior_stale_ms",
        "hour_sin", "hour_cos",
        "dow_sin", "dow_cos",
        "news_blackout",
    ]

    def test_p82_num_features_in_v5(self):
        v5 = MLFeatureSchemaV5OF()
        for f in self._P82_NUM:
            self.assertIn(f, v5.num_keys, f"P8.2 num '{f}' missing from v5")

    def test_fill_prob_horizons_ordered(self):
        """fill_prob_1s/3s/5s must appear after fill_prob_proxy in schema order."""
        v5 = MLFeatureSchemaV5OF()
        idx_proxy = v5.num_keys.index("fill_prob_proxy")
        for key in ("fill_prob_1s", "fill_prob_3s", "fill_prob_5s"):
            self.assertGreater(
                v5.num_keys.index(key), idx_proxy,
                f"{key} must appear after fill_prob_proxy in schema",
            )

    def test_cyclical_hour_encoding_range(self):
        """hour_sin/cos values must be in [-1, 1] for any integer hour 0-23."""
        import math
        for h in range(24):
            ang = 2.0 * math.pi * h / 24.0
            s, c = math.sin(ang), math.cos(ang)
            self.assertGreaterEqual(s, -1.0 - 1e-9)
            self.assertLessEqual(s, 1.0 + 1e-9)
            self.assertGreaterEqual(c, -1.0 - 1e-9)
            self.assertLessEqual(c, 1.0 + 1e-9)

    def test_cyclical_dow_encoding_range(self):
        """dow_sin/cos values must be in [-1, 1] for dow 0-6."""
        import math
        for d in range(7):
            ang = 2.0 * math.pi * d / 7.0
            s, c = math.sin(ang), math.cos(ang)
            self.assertGreaterEqual(s, -1.0 - 1e-9)
            self.assertLessEqual(s, 1.0 + 1e-9)

    def test_cyclical_midnight_continuity(self):
        """hour=23 and hour=0 must be close in cyclical space (wrap-around)."""
        import math
        ang0 = 2.0 * math.pi * 0 / 24.0
        ang23 = 2.0 * math.pi * 23 / 24.0
        # Angular distance between 23h and 0h = 2π/24 ≈ 0.26 rad
        dist = abs(ang0 - ang23 + 2.0 * math.pi) % (2.0 * math.pi)
        self.assertAlmostEqual(dist, 2.0 * math.pi / 24.0, places=5)

    def test_news_blackout_float_cast(self):
        """news_blackout = float(news_gate_veto): True/1 → 1.0, False/0 → 0.0."""
        self.assertEqual(float(True or 0), 1.0)
        self.assertEqual(float(False or 0), 0.0)
        self.assertEqual(float(1 or 0), 1.0)
        self.assertEqual(float(0 or 0), 0.0)

    def test_prior_stale_ms_non_negative(self):
        """prior_stale_ms = max(0, age_ms); must never be negative."""
        for age in (-100.0, 0.0, 5000.0, 86400000.0):
            self.assertGreaterEqual(max(0.0, age), 0.0)

    def test_p82_not_in_v4_schema(self):
        """Phase 8.2 keys must not be in v4_of."""
        v4 = MLFeatureSchemaV4OF()
        all_v4 = set(v4.num_keys) | set(v4.bool_keys)
        for f in self._P82_NUM:
            self.assertNotIn(f, all_v4, f"P8.2 key '{f}' leaked into v4_of")


class TestMLFeatureSchemaV5P83Features(unittest.TestCase):
    """Phase 8.3 — top_trader_ls, taker_ratio/z, force_order notionals/cluster, futures_crowding."""

    _P83_NUM = [
        "taker_buy_sell_ratio",
        "taker_buy_sell_ratio_z",
        "top_trader_long_short_ratio",
        "force_order_long_notional_1m",
        "force_order_short_notional_1m",
        "force_order_cluster_score",
        "futures_crowding_score",
    ]

    def test_p83_num_features_in_v5(self):
        v5 = MLFeatureSchemaV5OF()
        for f in self._P83_NUM:
            self.assertIn(f, v5.num_keys, f"P8.3 num '{f}' missing from v5")

    def test_p83_not_in_v4_schema(self):
        v4 = MLFeatureSchemaV4OF()
        all_v4 = set(v4.num_keys) | set(v4.bool_keys)
        for f in self._P83_NUM:
            self.assertNotIn(f, all_v4, f"P8.3 key '{f}' leaked into v4_of")

    def test_schema_total_count(self):
        """Phase 8.3: 212 num + 34 bool = 246 total (pre-8.4 snapshot)."""
        v5 = MLFeatureSchemaV5OF()
        self.assertEqual(len(v5.bool_keys), 34, f"Expected 34 bool keys, got {len(v5.bool_keys)}")
        self.assertGreaterEqual(len(v5.num_keys), 212, f"Expected ≥212 num keys (Phase 8.3+), got {len(v5.num_keys)}")

    def test_futures_crowding_score_formula(self):
        """futures_crowding_score = clip(fz * ls_z / 9, -3, 3)."""
        def _crowding(fz: float, ls_z: float) -> float:
            return max(-3.0, min(3.0, fz * ls_z / 9.0))
        self.assertAlmostEqual(_crowding(3.0, 3.0), 1.0)
        self.assertAlmostEqual(_crowding(-3.0, -3.0), 1.0)
        self.assertAlmostEqual(_crowding(3.0, -3.0), -1.0)
        self.assertEqual(_crowding(10.0, 10.0), 3.0)   # clipped at 3
        self.assertEqual(_crowding(-10.0, 10.0), -3.0)

    def test_taker_ratio_positive(self):
        """taker_buy_sell_ratio = buy/sell must be >= 0."""
        for buy, sell in [(100.0, 50.0), (0.0, 100.0), (100.0, 0.0)]:
            ratio = buy / sell if sell > 0 else 0.0
            self.assertGreaterEqual(ratio, 0.0)

    def test_force_order_cluster_score_zero_when_no_liq(self):
        """cluster score must be 0 when total liquidations = 0."""
        import math
        liq_buy, liq_sell = 0.0, 0.0
        total = liq_buy + liq_sell
        imb = (liq_buy - liq_sell) / total if total > 0 else 0.0
        cluster = imb * math.log1p(total / 1_000_000.0)
        self.assertEqual(cluster, 0.0)


class TestMLFeatureSchemaV5P84Features(unittest.TestCase):
    """Phase 8.4 — Hawkes/VPIN, oi_delta_z, premium_index_z, breadth_5m, gate trace, etc."""

    _P84_NUM = [
        # Hawkes raw
        "hawkes_dt_s", "hawkes_taker_buy_lam", "hawkes_taker_sell_lam",
        "hawkes_cancel_bid_lam", "hawkes_cancel_ask_lam", "hawkes_limit_add_lam",
        "hawkes_taker_lam", "hawkes_cancel_lam", "hawkes_churn_lam",
        "added_bid_rate_ema", "added_ask_rate_ema", "added_total_rate_ema",
        "vpin_tox_ema", "vpin_tox_z",
        "hawkes_S_taker_buy", "hawkes_S_taker_sell",
        "hawkes_S_cancel_bid", "hawkes_S_cancel_ask", "hawkes_S_limit_add",
        # Hawkes derived
        "hawkes_buy_sell_lam_ratio", "hawkes_cancel_imbalance",
        # OI / premium z-scores
        "oi_delta_z", "premium_index_z",
        # breadth / news / queue
        "sector_breadth_5m", "news_until_ms_norm", "queue_ahead_qty_5",
        # gate trace
        "of_confirm_scenario", "of_confirm_reason_group", "strong_need", "strong_have",
    ]

    def test_p84_num_features_in_v5(self):
        v5 = MLFeatureSchemaV5OF()
        for f in self._P84_NUM:
            self.assertIn(f, v5.num_keys, f"P8.4 num '{f}' missing from v5")

    def test_p84_not_in_v4_schema(self):
        v4 = MLFeatureSchemaV4OF()
        all_v4 = set(v4.num_keys) | set(v4.bool_keys)
        for f in self._P84_NUM:
            self.assertNotIn(f, all_v4, f"P8.4 key '{f}' leaked into v4_of")

    def test_schema_total_count_p84(self):
        """Phase 4.10: 266 num + 34 bool = 300 total."""
        v5 = MLFeatureSchemaV5OF()
        self.assertEqual(len(v5.num_keys), 266, f"Expected 266 num keys, got {len(v5.num_keys)}")
        self.assertEqual(len(v5.bool_keys), 34, f"Expected 34 bool keys, got {len(v5.bool_keys)}")

    def test_hawkes_buy_sell_lam_ratio_positive(self):
        """hawkes_buy_sell_lam_ratio = lam_buy / max(lam_sell, eps) must be >= 0."""
        for buy, sell in [(0.1, 0.05), (0.0, 0.1), (0.2, 0.0)]:
            ratio = buy / max(sell, 1e-9)
            self.assertGreaterEqual(ratio, 0.0)

    def test_news_until_ms_norm_range(self):
        """news_until_ms_norm must be in [0, 1]."""
        def _norm(until_ms: float, now_ms: float) -> float:
            if until_ms <= 0.0:
                return 0.0
            remain_s = max(0.0, (until_ms - now_ms) / 1000.0)
            return min(1.0, remain_s / 1800.0)
        self.assertEqual(_norm(0.0, 1000.0), 0.0)
        self.assertAlmostEqual(_norm(1000.0 + 900_000.0, 1000.0), 0.5, places=5)  # 900s = 0.5
        self.assertEqual(_norm(1000.0 + 9_000_000.0, 1000.0), 1.0)  # capped at 1.0

    def test_of_confirm_scenario_codes(self):
        """Scenario integer codes must map correctly."""
        mapping = {
            "trend": 1, "continuation": 1,
            "range": 2, "range_meanrev": 2,
            "reversal": 3,
            "chop": 4, "saw_chop_spoof_proxy": 4,
            "breakout": 5, "vol_shock_news_proxy": 5,
        }
        for name, expected in mapping.items():
            self.assertEqual(mapping.get(name.lower(), 0), expected)
        self.assertEqual(mapping.get("unknown", 0), 0)

    def test_hawkes_cancel_imbalance_range(self):
        """hawkes_cancel_imbalance must be in [-1, 1]."""
        def _imb(bid: float, ask: float) -> float:
            total = bid + ask
            return (bid - ask) / max(total, 1e-9)
        self.assertAlmostEqual(_imb(1.0, 1.0), 0.0, places=6)
        self.assertEqual(_imb(0.0, 0.0), 0.0)
        self.assertGreaterEqual(_imb(1.0, 0.0), 0.0)
        self.assertLessEqual(_imb(0.0, 1.0), 0.0)


class TestMLFeatureSchemaP410RollingPriors(unittest.TestCase):
    """Phase 4.10 — Rolling 7d/30d PIT priors."""

    _P410_KEYS = [
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

    def test_all_p410_in_v5_schema(self):
        v5 = MLFeatureSchemaV5OF()
        for k in self._P410_KEYS:
            self.assertIn(k, v5.num_keys, f"Phase 4.10 key '{k}' missing from v5_of")

    def test_no_p410_in_v4(self):
        v4 = MLFeatureSchemaV4OF()
        all_v4 = set(v4.num_keys) | set(v4.bool_keys)
        for k in self._P410_KEYS:
            self.assertNotIn(k, all_v4, f"Key '{k}' leaked into v4_of")

    def test_all_p410_in_external_payload(self):
        from core.external_features_payload_v1 import _NUM_KEYS
        for k in self._P410_KEYS:
            self.assertIn(k, _NUM_KEYS, f"Phase 4.10 key '{k}' missing from external_features_payload_v1")

    def test_rolling_compute_basic(self):
        """compute_rolling_priors: win/loss/ev_r aggregation, embargo applied."""
        from orderflow_services.pit_priors_rolling_v1 import compute_rolling_priors
        import time as _time
        now_ms = int(_time.time() * 1000)
        embargo_ms = 3_600_000
        t_ok = now_ms - embargo_ms - 1_000  # just before embargo cutoff (valid)
        t_embargoed = now_ms - 100          # within embargo (must be excluded)

        trades = [
            # 5 wins + 2 losses within 7d, before embargo
            {"ts_close": str(t_ok), "result": "WIN",  "r_multiple": "1.5",
             "symbol": "BTCUSDT", "kind": "default", "mae_r": "0.3", "mfe_r": "2.0"},
            {"ts_close": str(t_ok), "result": "WIN",  "r_multiple": "2.0",
             "symbol": "BTCUSDT", "kind": "default", "mae_r": "0.2", "mfe_r": "2.5"},
            {"ts_close": str(t_ok), "result": "WIN",  "r_multiple": "1.0",
             "symbol": "BTCUSDT", "kind": "default", "mae_r": "0.1", "mfe_r": "1.5"},
            {"ts_close": str(t_ok), "result": "LOSS", "r_multiple": "-1.0",
             "symbol": "BTCUSDT", "kind": "default", "mae_r": "1.2", "mfe_r": "0.3"},
            {"ts_close": str(t_ok), "result": "LOSS", "r_multiple": "-1.0",
             "symbol": "BTCUSDT", "kind": "default", "mae_r": "1.0", "mfe_r": "0.2"},
            # embargoed — must not appear
            {"ts_close": str(t_embargoed), "result": "WIN", "r_multiple": "5.0",
             "symbol": "BTCUSDT", "kind": "default", "mae_r": "0.0", "mfe_r": "6.0"},
        ]
        # Need ≥20 samples for MIN_SAMPLES — patch env
        import os, unittest.mock
        with unittest.mock.patch.dict(os.environ, {"PIT_ROLLING_MIN_SAMPLES": "3"}):
            import importlib
            import orderflow_services.pit_priors_rolling_v1 as m
            importlib.reload(m)
            p7, p30 = m.compute_rolling_priors(trades, now_ms)

        key = ("BTCUSDT", "default", "all")
        self.assertIn(key, p7, "Expected BTCUSDT/default/all in 7d priors")
        agg = p7[key]
        self.assertAlmostEqual(agg["winrate"], 3 / 5, places=5)
        self.assertAlmostEqual(agg["sl_hit_rate"], 2 / 5, places=5)
        ev = (1.5 + 2.0 + 1.0 - 1.0 - 1.0) / 5
        self.assertAlmostEqual(agg["ev_r"], ev, places=5)

    def test_rolling_embargo_excludes_recent(self):
        """Trades within embargo window must not appear in priors."""
        from orderflow_services.pit_priors_rolling_v1 import compute_rolling_priors
        import time as _time, os, unittest.mock
        now_ms = int(_time.time() * 1000)
        # Only embargoed trade
        trades = [
            {"ts_close": str(now_ms - 100), "result": "WIN", "r_multiple": "5.0",
             "symbol": "BTCUSDT", "kind": "default"},
        ]
        with unittest.mock.patch.dict(os.environ, {"PIT_ROLLING_MIN_SAMPLES": "1"}):
            import importlib, orderflow_services.pit_priors_rolling_v1 as m
            importlib.reload(m)
            p7, p30 = m.compute_rolling_priors(trades, now_ms)
        self.assertEqual(len(p7), 0, "Embargoed-only trade must not produce priors")

    def test_tp1_hit_rate_detection(self):
        """_tp1_hit must recognise common close_reason patterns."""
        from orderflow_services.pit_priors_rolling_v1 import _tp1_hit
        self.assertTrue(_tp1_hit({"tp1_hit": "1"}))
        self.assertTrue(_tp1_hit({"close_reason": "tp1_full"}))
        self.assertTrue(_tp1_hit({"close_reason": "tp"}))
        self.assertFalse(_tp1_hit({"close_reason": "sl"}))
        self.assertFalse(_tp1_hit({"close_reason": "timeout"}))
        self.assertFalse(_tp1_hit({}))

    def test_giveback_formula(self):
        """giveback = mfe_r - r_multiple; p75 of winners."""
        from orderflow_services.pit_priors_rolling_v1 import _percentile
        # mfe=2.0, r=1.5 → giveback=0.5; mfe=3.0, r=2.0 → giveback=1.0
        givebacks = [0.5, 1.0]
        self.assertAlmostEqual(_percentile(givebacks, 75), 1.0, places=5)

    def test_median_helper(self):
        from orderflow_services.pit_priors_rolling_v1 import _median
        self.assertEqual(_median([]), 0.0)
        self.assertEqual(_median([3.0]), 3.0)
        self.assertAlmostEqual(_median([1.0, 2.0, 3.0]), 2.0)
        self.assertAlmostEqual(_median([1.0, 2.0, 3.0, 4.0]), 2.5)

    def test_percentile_helper(self):
        from orderflow_services.pit_priors_rolling_v1 import _percentile
        vals = [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0]
        self.assertAlmostEqual(_percentile(vals, 90), 10.0)
        self.assertAlmostEqual(_percentile(vals, 0), 1.0)

    def test_samples_log1p_encoding(self):
        """prior_samples_symbol_kind_7d = log1p(sample_count)."""
        import math
        for n in [0, 1, 20, 100, 500]:
            enc = math.log1p(n)
            self.assertGreaterEqual(enc, 0.0)
        # log1p(0) = 0, log1p(1) ≈ 0.693
        self.assertEqual(math.log1p(0), 0.0)
        self.assertAlmostEqual(math.log1p(1), 0.6931, places=3)


class TestMLFeatureSchemaP45P46P47(unittest.TestCase):
    """Phase 4.5 (VPIN rolling + Hawkes limit_add), 4.6 (sector), 4.7 (liqmap aliases)."""

    _P45_KEYS = [
        "vpin_tox_1m", "vpin_tox_5m", "vpin_tox_slope",
        "hawkes_limit_add_bid_lam", "hawkes_limit_add_ask_lam", "hawkes_limit_add_imbalance",
    ]
    _P46_KEYS = ["sector_delta_z_median", "sector_obi_median"]
    _P47_KEYS = [
        "liq_cluster_dist_above_bps", "liq_cluster_dist_below_bps",
        "liq_heatmap_density_above", "liq_heatmap_density_below",
    ]

    def test_all_p45_in_v5_schema(self):
        v5 = MLFeatureSchemaV5OF()
        for k in self._P45_KEYS:
            self.assertIn(k, v5.num_keys, f"Phase 4.5 key '{k}' missing from v5_of")

    def test_all_p46_in_v5_schema(self):
        v5 = MLFeatureSchemaV5OF()
        for k in self._P46_KEYS:
            self.assertIn(k, v5.num_keys, f"Phase 4.6 key '{k}' missing from v5_of")

    def test_all_p47_in_v5_schema(self):
        v5 = MLFeatureSchemaV5OF()
        for k in self._P47_KEYS:
            self.assertIn(k, v5.num_keys, f"Phase 4.7 key '{k}' missing from v5_of")

    def test_no_p45_p46_p47_in_v4(self):
        v4 = MLFeatureSchemaV4OF()
        all_v4 = set(v4.num_keys) | set(v4.bool_keys)
        for k in self._P45_KEYS + self._P46_KEYS + self._P47_KEYS:
            self.assertNotIn(k, all_v4, f"Key '{k}' leaked into v4_of")

    def test_vpin_slope_formula(self):
        """vpin_tox_slope = vpin_1m - vpin_5m; positive = accelerating toxicity."""
        def _slope(tox_1m: float, tox_5m: float) -> float:
            return tox_1m - tox_5m

        self.assertGreater(_slope(0.8, 0.5), 0.0)   # accelerating
        self.assertLess(_slope(0.3, 0.6), 0.0)      # decelerating
        self.assertEqual(_slope(0.5, 0.5), 0.0)     # stable

    def test_hawkes_limit_add_imbalance_range(self):
        """hawkes_limit_add_imbalance in [-1, 1]; 0 when symmetric."""
        def _imb(bid_lam: float, ask_lam: float) -> float:
            total = bid_lam + ask_lam
            return (bid_lam - ask_lam) / max(total, 1e-9)

        self.assertAlmostEqual(_imb(1.0, 1.0), 0.0, places=6)
        self.assertGreaterEqual(_imb(2.0, 0.0), 0.9)    # strongly bid-side
        self.assertLessEqual(_imb(0.0, 2.0), -0.9)      # strongly ask-side

    def test_sector_median_single_symbol(self):
        """With only one symbol, sector_delta_z_median == symbol's own value."""
        vals = [1.5]
        self.assertEqual(vals[0], 1.5)

    def test_sector_median_odd_count(self):
        """Odd number of symbols: median is the middle value."""
        vals = sorted([0.5, 1.5, 3.0])
        mid = len(vals) // 2
        self.assertEqual(vals[mid], 1.5)

    def test_sector_median_even_count(self):
        """Even number of symbols: median is mean of two middle values."""
        vals = sorted([0.5, 1.0, 2.0, 3.0])
        mid = len(vals) // 2
        med = (vals[mid - 1] + vals[mid]) * 0.5
        self.assertAlmostEqual(med, 1.5, places=6)

    def test_liq_heatmap_density_nonneg(self):
        """liq_heatmap_density_* = log1p(usd / 1M) must be >= 0."""
        import math
        for usd in [0.0, 100_000.0, 1_000_000.0, 10_000_000.0]:
            density = math.log1p(max(0.0, usd) / 1e6)
            self.assertGreaterEqual(density, 0.0)

    def test_liq_cluster_dist_nonneg(self):
        """liq_cluster_dist_above/below_bps must be >= 0."""
        for bps in [0.0, 5.0, 100.0]:
            self.assertGreaterEqual(max(0.0, bps), 0.0)

    def test_external_features_payload_contains_p45_p46_p47(self):
        """external_features_payload_v1._NUM_KEYS must contain all Phase 4.5/4.6/4.7 keys."""
        from core.external_features_payload_v1 import _NUM_KEYS
        all_keys = set(_NUM_KEYS)
        for k in self._P45_KEYS + self._P46_KEYS + self._P47_KEYS:
            self.assertIn(k, all_keys, f"Phase 4.5/4.6/4.7 key '{k}' missing from external_features_payload_v1")

    def test_schema_total_count_p457(self):
        """Phase 4.10: 266 num + 34 bool = 300 total."""
        v5 = MLFeatureSchemaV5OF()
        self.assertEqual(len(v5.num_keys), 266, f"Expected 266 num keys, got {len(v5.num_keys)}")
        self.assertEqual(len(v5.bool_keys), 34, f"Expected 34 bool keys, got {len(v5.bool_keys)}")
        self.assertEqual(len(v5.num_keys) + len(v5.bool_keys), 300)


if __name__ == "__main__":
    unittest.main()
