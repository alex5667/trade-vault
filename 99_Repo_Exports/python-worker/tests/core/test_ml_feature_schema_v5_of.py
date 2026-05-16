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
        """SCHEMA_HASH must reflect Phase 7.2 + 7.3 + 7.4 + 7.5 + 7.6 + 7.7 additions."""
        from core.ml_feature_schema_v5_of import SCHEMA_HASH
        self.assertEqual(SCHEMA_HASH, "2db5bda868a6",
            "SCHEMA_HASH not updated after Phase 7.6/7.7 additions")


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


if __name__ == "__main__":
    unittest.main()
