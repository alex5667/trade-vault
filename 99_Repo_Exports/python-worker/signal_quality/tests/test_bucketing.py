"""
Pytest-based unit tests for signal_quality.bucketing module.

All tests are pure (no DB, no Redis, no I/O).
"""


from signal_quality.bucketing import _bin, make_feature_bucket, get_bucket_quality_description


# ──────────────────────────────────────────────
# _bin helper
# ──────────────────────────────────────────────

class TestBin:
    EDGES = (0.5, 1.0, 2.0)

    def test_none_returns_na(self):
        assert _bin(None, self.EDGES) == "na"

    def test_below_first_edge(self):
        assert _bin(0.3, self.EDGES) == "<0.5"

    def test_exactly_first_edge_goes_to_next_bucket(self):
        # x < e is strict, so x==0.5 falls into next bucket (<1.0)
        assert _bin(0.5, self.EDGES) == "<1.0"

    def test_between_edges(self):
        assert _bin(1.5, self.EDGES) == "<2.0"

    def test_above_last_edge(self):
        assert _bin(3.0, self.EDGES) == ">=2.0"

    def test_exactly_last_edge(self):
        assert _bin(2.0, self.EDGES) == ">=2.0"

    def test_negative_value_below_first_edge(self):
        assert _bin(-99.0, self.EDGES) == "<0.5"


# ──────────────────────────────────────────────
# make_feature_bucket
# ──────────────────────────────────────────────

class TestMakeFeatureBucket:
    def test_normal_values(self):
        bucket = make_feature_bucket(
            delta_spike_z=2.1
            obi=1.5
            weak_progress=0.1
            atr_quantile=0.8
        )
        assert bucket == "dz:<3.0|obi:<2.0|wp:<0.15|atr:<0.9"

    def test_edge_values(self):
        bucket = make_feature_bucket(
            delta_spike_z=-0.1
            obi=0.2
            weak_progress=0.8
            atr_quantile=0.1
        )
        assert bucket == "dz:<0.5|obi:<0.5|wp:>=0.5|atr:<0.3"

    def test_all_none(self):
        bucket = make_feature_bucket(
            delta_spike_z=None
            obi=None
            weak_progress=None
            atr_quantile=None
        )
        assert bucket == "dz:na|obi:na|wp:na|atr:na"

    def test_partial_none(self):
        bucket = make_feature_bucket(
            delta_spike_z=None
            obi=1.0
            weak_progress=None
            atr_quantile=0.5
        )
        assert bucket == "dz:na|obi:<1.5|wp:na|atr:<0.7"

    def test_format_is_pipe_separated(self):
        bucket = make_feature_bucket(
            delta_spike_z=1.0
            obi=1.0
            weak_progress=0.2
            atr_quantile=0.5
        )
        parts = bucket.split("|")
        assert len(parts) == 4
        for part in parts:
            assert ":" in part

    def test_high_values_above_all_edges(self):
        bucket = make_feature_bucket(
            delta_spike_z=10.0
            obi=5.0
            weak_progress=1.0
            atr_quantile=1.0
        )
        assert bucket == "dz:>=3.0|obi:>=2.0|wp:>=0.5|atr:>=0.9"

    def test_returns_string(self):
        result = make_feature_bucket(
            delta_spike_z=1.5, obi=0.8, weak_progress=0.2, atr_quantile=0.6
        )
        assert isinstance(result, str)

    def test_zero_values(self):
        bucket = make_feature_bucket(
            delta_spike_z=0.0
            obi=0.0
            weak_progress=0.0
            atr_quantile=0.0
        )
        # All zeros fall below all edges
        assert bucket == "dz:<0.5|obi:<0.5|wp:<0.15|atr:<0.3"


# ──────────────────────────────────────────────
# get_bucket_quality_description
# ──────────────────────────────────────────────

class TestGetBucketQualityDescription:
    def _make_bucket(self, dz, obi, wp, atr):
        return make_feature_bucket(
            delta_spike_z=dz, obi=obi, weak_progress=wp, atr_quantile=atr
        )

    def test_returns_four_segments(self):
        desc = get_bucket_quality_description(self._make_bucket(1.5, 0.8, 0.2, 0.6))
        assert len(desc.split(" | ")) == 4

    def test_na_fields_labeled_unknown(self):
        desc = get_bucket_quality_description(
            make_feature_bucket(
                delta_spike_z=None, obi=None, weak_progress=None, atr_quantile=None
            )
        )
        assert desc.count("unknown") == 4

    def test_strong_delta_z(self):
        bucket = "dz:>=3.0|obi:<0.5|wp:<0.15|atr:<0.3"
        desc = get_bucket_quality_description(bucket)
        assert "Delta Z: strong" in desc

    def test_volatility_levels(self):
        # low: atr < 0.3
        b_low = make_feature_bucket(delta_spike_z=1.0, obi=1.0, weak_progress=0.2, atr_quantile=0.1)
        assert "Volatility: low" in get_bucket_quality_description(b_low)

        # medium: 0.3 <= atr < 0.7
        b_med = make_feature_bucket(delta_spike_z=1.0, obi=1.0, weak_progress=0.2, atr_quantile=0.5)
        assert "Volatility: medium" in get_bucket_quality_description(b_med)

        # high: 0.7 <= atr < 0.9
        b_high = make_feature_bucket(delta_spike_z=1.0, obi=1.0, weak_progress=0.2, atr_quantile=0.8)
        assert "Volatility: high" in get_bucket_quality_description(b_high)

        # extreme: atr >= 0.9
        b_ext = make_feature_bucket(delta_spike_z=1.0, obi=1.0, weak_progress=0.2, atr_quantile=0.95)
        assert "Volatility: extreme" in get_bucket_quality_description(b_ext)

    def test_progress_levels(self):
        # very weak: wp < 0.15
        b1 = make_feature_bucket(delta_spike_z=1.0, obi=1.0, weak_progress=0.05, atr_quantile=0.5)
        assert "Progress: very weak" in get_bucket_quality_description(b1)

        # weak: 0.15 <= wp < 0.3
        b2 = make_feature_bucket(delta_spike_z=1.0, obi=1.0, weak_progress=0.2, atr_quantile=0.5)
        assert "Progress: weak" in get_bucket_quality_description(b2)

        # strong: wp >= 0.3
        b3 = make_feature_bucket(delta_spike_z=1.0, obi=1.0, weak_progress=0.6, atr_quantile=0.5)
        assert "Progress: strong" in get_bucket_quality_description(b3)
