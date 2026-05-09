from __future__ import annotations

"""
Tests for services/shadow_calib_meta.py — the shared calibration metadata module.

Covers:
  1. extract_calib_fields: correctness, safety with None/missing
  2. merge_calib_fields: priority cascade
  3. stamp_virtual_if_calib: forces is_virtual=1 when calib=1
  4. Integration: signal_pipeline → execution_router → binance_executor passthrough
"""



# [AUTOGRAVITY CLEANUP] sys.path.insert(0, "/home/alex/front/trade/scanner_infra/python-worker")

from services.shadow_calib_meta import (
    CALIB_FIELDS,
    extract_calib_fields,
    merge_calib_fields,
    stamp_virtual_if_calib,
)

# -----------------------------------------------------------------------
# extract_calib_fields
# -----------------------------------------------------------------------


class TestExtractCalibFields:

    def test_full_extraction(self):
        """All known calib fields extracted."""
        src = {
            "paper_only": 1,
            "shadow_only": 0,
            "is_virtual": 1,
            "calib": 1,
            "calib_kind": "cont_ctx_window",
            "calib_run_id": "run_abc123",
            "candidate_window_ms": 180000,
            "baseline_window_ms": 120000,
            "cont_ctx_age_ms": 95000,
            "entry_reason": "cont_ctx_rescued",
            "parent_signal_id": "SIG000",
            # Non-calib noise fields
            "symbol": "BTCUSDT",
            "side": "LONG",
        }
        result = extract_calib_fields(src)
        assert result["calib"] == 1
        assert result["calib_kind"] == "cont_ctx_window"
        assert result["candidate_window_ms"] == 180000
        assert "symbol" not in result
        assert "side" not in result
        assert len(result) == len(CALIB_FIELDS)

    def test_partial_extraction(self):
        """Only present fields returned."""
        src = {"calib": 1, "calib_kind": "adverse_gate"}
        result = extract_calib_fields(src)
        assert result == {"calib": 1, "calib_kind": "adverse_gate"}

    def test_empty_dict(self):
        result = extract_calib_fields({})
        assert result == {}

    def test_none_input(self):
        result = extract_calib_fields(None)
        assert result == {}

    def test_non_dict_input(self):
        result = extract_calib_fields("not a dict")
        assert result == {}

    def test_none_values_skipped(self):
        """Fields with None values are NOT included."""
        src = {"calib": 1, "calib_kind": None, "calib_run_id": "id123"}
        result = extract_calib_fields(src)
        assert "calib_kind" not in result
        assert result == {"calib": 1, "calib_run_id": "id123"}


# -----------------------------------------------------------------------
# merge_calib_fields
# -----------------------------------------------------------------------


class TestMergeCalibFields:

    def test_priority_cascade(self):
        """First non-None source value wins."""
        target: dict = {}
        src1 = {"calib": 1, "calib_kind": "src1"}
        src2 = {"calib": 2, "calib_kind": "src2", "calib_run_id": "from_src2"}
        merge_calib_fields(target, src1, src2)
        assert target["calib"] == 1  # src1 wins
        assert target["calib_kind"] == "src1"  # src1 wins
        assert target["calib_run_id"] == "from_src2"  # only in src2

    def test_no_overwrite_existing(self):
        """Existing target values preserved by default."""
        target = {"calib": 99}
        merge_calib_fields(target, {"calib": 1})
        assert target["calib"] == 99

    def test_overwrite_mode(self):
        """With overwrite=True, later sources can replace."""
        target = {"calib": 99}
        merge_calib_fields(target, {"calib": 1}, overwrite=True)
        assert target["calib"] == 1

    def test_skip_non_dict_sources(self):
        """Non-dict sources are silently ignored."""
        target: dict = {}
        merge_calib_fields(target, None, "bad", {"calib": 1})
        assert target["calib"] == 1

    def test_empty_merge(self):
        target: dict = {}
        merge_calib_fields(target)
        assert target == {}


# -----------------------------------------------------------------------
# stamp_virtual_if_calib
# -----------------------------------------------------------------------


class TestStampVirtualIfCalib:

    def test_forces_is_virtual(self):
        payload = {"calib": 1, "is_virtual": 0}
        stamp_virtual_if_calib(payload)
        assert payload["is_virtual"] == 1

    def test_no_stamp_when_no_calib(self):
        payload = {"is_virtual": 0}
        stamp_virtual_if_calib(payload)
        assert payload["is_virtual"] == 0

    def test_no_stamp_when_calib_zero(self):
        payload = {"calib": 0, "is_virtual": 0}
        stamp_virtual_if_calib(payload)
        assert payload["is_virtual"] == 0

    def test_sets_missing_virtual(self):
        payload = {"calib": 1}
        stamp_virtual_if_calib(payload)
        assert payload["is_virtual"] == 1


# -----------------------------------------------------------------------
# Integration: execution_router passthrough
# -----------------------------------------------------------------------


class TestExecutionRouterCalibPassthrough:

    def test_resize_preserves_calib(self):
        """_build_resize_payload preserves calibration fields from original signal."""
        import services.execution_router as mod

        class FakeRedis:
            def get(self, key):
                return None

        router = mod.ExecutionRouter(FakeRedis())

        original = {
            "sid": "SIG01",
            "symbol": "BTCUSDT",
            "side": "LONG",
            "qty": 0.001,
            "entry": 65000.0,
            "sl": 64000.0,
            "tp_levels": [66000.0, 67000.0],
            # Calibration fields
            "calib": 1,
            "calib_kind": "cont_ctx_window",
            "calib_run_id": "run_xyz",
            "candidate_window_ms": 180000,
        }
        guard = {"sid": "OWNER01"}
        state = {"scale_in_seq": 0, "exec_price": 64500.0, "qty": 0.001, "side": "LONG"}

        resize = router._build_resize_payload(original, guard, state)
        assert resize["calib"] == 1
        assert resize["calib_kind"] == "cont_ctx_window"
        assert resize["calib_run_id"] == "run_xyz"
        assert resize["candidate_window_ms"] == 180000
        assert resize["action"] == "resize"
