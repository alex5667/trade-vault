"""
Regression + new targeted tests for news_patch_v2 optimizations.

Covers:
  - news_gate.py:   _compute_news_impact_bps, _clamp01, decide() full paths
  - analyzer_worker.py: LEASE_SEC constant, _parse_symbols_json, _safe_s
  - calendar_store_worker.py: importance_to_grade_id, map_scopes, hset not raising
  - calendar_store_service.py: _idx_key, _idx_key_currency/region/symbol aliases
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import pytest

# ---------------------------------------------------------------------------
# Shared fakes
# ---------------------------------------------------------------------------


class FakePipeline:
    def __init__(self, r: "FakeRedis") -> None:
        self.r = r
        self.ops: List[Tuple[str, Tuple[Any, ...]]] = []

    def hgetall(self, key: str) -> "FakePipeline":
        self.ops.append(("hgetall", (key,)))
        return self

    def zadd(self, key: str, mapping: dict) -> "FakePipeline":
        self.ops.append(("zadd", (key, mapping)))
        return self

    def expire(self, key: str, ttl: int) -> "FakePipeline":
        self.ops.append(("expire", (key, ttl)))
        return self

    def hset(self, key: str, mapping: dict) -> "FakePipeline":
        self.ops.append(("hset", (key, mapping)))
        return self

    def execute(self) -> List[Any]:
        out: List[Any] = []
        for op, args in self.ops:
            if op == "hgetall":
                (key,) = args
                out.append(dict(self.r.hash_store.get(key, {})))
            else:
                out.append(None)
        self.ops.clear()
        return out


class FakeRedis:
    def __init__(self) -> None:
        self.hash_store: Dict[str, Dict[str, Any]] = {}
        self.str_store: Dict[str, str] = {}
        self._calls: List[Tuple[str, str]] = []

    def get(self, key: str) -> Optional[str]:
        self._calls.append(("get", key))
        return self.str_store.get(key)

    def set(self, key: str, value: str, *, nx: bool = False, ex: int = 0) -> Optional[bool]:
        if nx and key in self.str_store:
            return None
        self.str_store[key] = value
        return True

    def hset(self, key: str, mapping: Dict[str, Any]) -> None:
        self.hash_store.setdefault(key, {}).update(mapping)

    def hgetall(self, key: str) -> Dict[str, Any]:
        self._calls.append(("hgetall", key))
        return dict(self.hash_store.get(key, {}))

    def pipeline(self, transaction: bool = False) -> FakePipeline:
        return FakePipeline(self)


# ---------------------------------------------------------------------------
# news_gate.py — _compute_news_impact_bps
# ---------------------------------------------------------------------------

class TestComputeNewsImpactBps:
    def _fn(self, **kw):
        from news_gate import _compute_news_impact_bps
        return _compute_news_impact_bps(**kw)

    def test_grade3_full_risk_full_confidence(self):
        bps = self._fn(
            news_risk=1.0,
            news_grade_id=3,
            confidence=1.0,
            soft_news_k=0.9,
            soft_news_min_bps=2500,
        )
        # impact = 1.0 * 1.0 * 1.0 = 1.0
        # factor = max(0.25, 1 - 0.9 * 1.0) = max(0.25, 0.1) = 0.25
        # bps = 2500
        assert bps == 2500

    def test_grade0_zero_weight_returns_full_bps(self):
        bps = self._fn(
            news_risk=1.0,
            news_grade_id=0,
            confidence=1.0,
            soft_news_k=0.9,
            soft_news_min_bps=2500,
        )
        # gw=0 => impact=0 => factor=1.0 => 10000
        assert bps == 10000

    def test_grade2_weight_06(self):
        bps = self._fn(
            news_risk=1.0,
            news_grade_id=2,
            confidence=1.0,
            soft_news_k=0.9,
            soft_news_min_bps=2500,
        )
        # impact = 1.0 * 0.6 * 1.0 = 0.6
        # factor = max(0.25, 1 - 0.54) = 0.46
        assert bps == int(10000 * 0.46)

    def test_grade1_weight_03(self):
        bps = self._fn(
            news_risk=1.0,
            news_grade_id=1,
            confidence=1.0,
            soft_news_k=0.9,
            soft_news_min_bps=2500,
        )
        # impact = 0.3 => factor = 1 - 0.27 = 0.73
        assert bps == int(10000 * 0.73)

    def test_min_confidence_clamped_to_02(self):
        bps_zero = self._fn(
            news_risk=1.0,
            news_grade_id=3,
            confidence=0.0,  # clamped to 0.2
            soft_news_k=0.9,
            soft_news_min_bps=2500,
        )
        bps_02 = self._fn(
            news_risk=1.0,
            news_grade_id=3,
            confidence=0.2,
            soft_news_k=0.9,
            soft_news_min_bps=2500,
        )
        assert bps_zero == bps_02

    def test_result_never_below_min_bps(self):
        bps = self._fn(
            news_risk=1.0,
            news_grade_id=5,
            confidence=1.0,
            soft_news_k=1.0,
            soft_news_min_bps=3000,
        )
        assert bps >= 3000


# ---------------------------------------------------------------------------
# news_gate.py — _clamp01
# ---------------------------------------------------------------------------

class TestClamp01:
    def test_below_zero(self):
        from news_gate import _clamp01
        assert _clamp01(-5.0) == 0.0

    def test_above_one(self):
        from news_gate import _clamp01
        assert _clamp01(2.0) == 1.0

    def test_midpoint(self):
        from news_gate import _clamp01
        assert _clamp01(0.5) == 0.5

    def test_boundary_zero(self):
        from news_gate import _clamp01
        assert _clamp01(0.0) == 0.0

    def test_boundary_one(self):
        from news_gate import _clamp01
        assert _clamp01(1.0) == 1.0


# ---------------------------------------------------------------------------
# news_gate.py — GateDecision full paths (regression)
# ---------------------------------------------------------------------------

class TestNewsGateDecide:
    def test_manual_hard_block(self):
        from news_gate import NewsGate

        r = FakeRedis()
        r.str_store["news:hi:active"] = json.dumps(
            {"active": 1, "until_ts_ms": 2_000_000, "reason": "NFP"}
        )
        gate = NewsGate(redis_client=r, asset_class="crypto", window_sec=300, grade_min=4)
        dec = gate.decide(now_ts_ms=1_500_000, symbols=("BTCUSDT",))

        assert dec.hard_block is True
        assert dec.risk_factor_bps == 0
        assert dec.until_ts_ms == 2_000_000

    def test_calendar_hard_block_grade4(self):
        from news_gate import NewsGate

        r = FakeRedis()
        r.hash_store["calendar:agg:fx"] = {
            "event_grade_id": "4",
            "event_ts_ms": "1_000_000",
            "title": "CPI",
        }
        gate = NewsGate(redis_client=r, asset_class="forex", window_sec=300, grade_min=4)
        dec = gate.decide(now_ts_ms=900_000)

        assert dec.hard_block is True
        assert dec.hard_reason == "calendar_hi_impact"
        assert dec.risk_factor_bps == 0

    @pytest.mark.parametrize("grade,max_bps", [(2, 5000), (3, 3500)])
    def test_calendar_soft_gate_by_grade(self, grade, max_bps):
        from news_gate import NewsGate

        r = FakeRedis()
        r.hash_store["calendar:agg:crypto"] = {
            "event_grade_id": str(grade),
            "event_ts_ms": "1000000",
        }
        gate = NewsGate(
            redis_client=r,
            asset_class="crypto",
            window_sec=300,
            grade_min=4,
            soft_enabled=True,
            soft_window_sec=300,
        )
        dec = gate.decide(now_ts_ms=900_000)

        assert dec.hard_block is False
        assert 0 <= dec.risk_factor_bps <= max_bps

    def test_news_soft_gate_clamps_to_min_bps(self):
        from news_gate import NewsGate

        r = FakeRedis()
        gate = NewsGate(redis_client=r, asset_class="crypto", window_sec=300, grade_min=4)
        dec = gate.decide(
            now_ts_ms=1_000_000,
            news_risk=1.0,
            news_grade_id=3,
            confidence=1.0,
            horizon_sec=600,
            asof_ts_ms=999_900,
        )
        assert dec.hard_block is False
        assert dec.risk_factor_bps >= 2500  # default soft_news_min_bps
        assert dec.risk_factor_bps <= 10000

    def test_stale_news_sets_dq_flag(self):
        from news_gate import NewsGate

        r = FakeRedis()
        gate = NewsGate(redis_client=r, asset_class="crypto", window_sec=300, grade_min=4)
        dec = gate.decide(
            now_ts_ms=1_000_000,
            news_risk=0.9,
            news_grade_id=3,
            confidence=0.9,
            horizon_sec=60,        # tiny horizon
            asof_ts_ms=1_000_000 - 120_000,  # 120s stale > 60s horizon
        )
        assert dec.dq_flags.get("news_stale_over_horizon") is True

    def test_no_ts_returns_no_block(self):
        from news_gate import NewsGate

        r = FakeRedis()
        gate = NewsGate(redis_client=r, asset_class="crypto", window_sec=300, grade_min=4)
        dec = gate.decide(now_ts_ms=0)
        assert dec.hard_block is False
        assert dec.risk_factor_bps == 10000


# ---------------------------------------------------------------------------
# analyzer_worker.py — module-level constants and helpers
# ---------------------------------------------------------------------------

class TestAnalyzerWorkerHelpers:
    def test_lease_sec_is_module_level_constant(self):
        import analyzer_worker
        assert hasattr(analyzer_worker, "LEASE_SEC")
        assert isinstance(analyzer_worker.LEASE_SEC, int)
        assert analyzer_worker.LEASE_SEC > 0

    def test_news_analysis_maxlen_is_module_level_constant(self):
        import analyzer_worker
        assert hasattr(analyzer_worker, "NEWS_ANALYSIS_MAXLEN")
        assert isinstance(analyzer_worker.NEWS_ANALYSIS_MAXLEN, int)

    def test_parse_symbols_json_valid(self):
        from analyzer_worker import _parse_symbols_json
        result = _parse_symbols_json('["BTCUSDT", "ETHUSDT"]')
        assert result == ["BTCUSDT", "ETHUSDT"]

    def test_parse_symbols_json_empty_string(self):
        from analyzer_worker import _parse_symbols_json
        assert _parse_symbols_json("") == []

    def test_parse_symbols_json_invalid_json(self):
        from analyzer_worker import _parse_symbols_json
        assert _parse_symbols_json("{not-json}") == []

    def test_parse_symbols_json_filters_blank(self):
        from analyzer_worker import _parse_symbols_json
        result = _parse_symbols_json('["BTC", "", "  "]')
        assert result == ["BTC"]

    def test_safe_s_none(self):
        from analyzer_worker import _safe_s
        assert _safe_s(None) == ""

    def test_safe_s_strips(self):
        from analyzer_worker import _safe_s
        assert _safe_s("  hello  ") == "hello"


# ---------------------------------------------------------------------------
# calendar_store_worker.py — importance_to_grade_id, map_scopes
# ---------------------------------------------------------------------------

class TestImportanceToGradeId:
    def test_zero_returns_zero(self):
        from calendar_store_worker import importance_to_grade_id
        assert importance_to_grade_id(0) == 0

    def test_negative_returns_zero(self):
        from calendar_store_worker import importance_to_grade_id
        assert importance_to_grade_id(-5) == 0

    def test_level1_returns_1(self):
        from calendar_store_worker import importance_to_grade_id
        assert importance_to_grade_id(1) == 1

    def test_level2_returns_2(self):
        from calendar_store_worker import importance_to_grade_id
        assert importance_to_grade_id(2) == 2

    def test_level3_returns_4(self):
        from calendar_store_worker import importance_to_grade_id
        assert importance_to_grade_id(3) == 4

    def test_level_above3_returns_4(self):
        from calendar_store_worker import importance_to_grade_id
        assert importance_to_grade_id(99) == 4


class TestMapScopes:
    def test_usd_high_importance_returns_all_scopes(self):
        from calendar_store_worker import map_scopes, DEFAULT_SCOPES
        scopes = map_scopes(currency="USD", country="US", importance=3)
        assert set(scopes) == set(DEFAULT_SCOPES)

    def test_eur_medium_importance_returns_all_scopes(self):
        from calendar_store_worker import map_scopes, DEFAULT_SCOPES
        scopes = map_scopes(currency="EUR", country="DE", importance=2)
        assert set(scopes) == set(DEFAULT_SCOPES)

    def test_minor_currency_low_importance_returns_only_forex(self):
        from calendar_store_worker import map_scopes
        scopes = map_scopes(currency="TRY", country="TR", importance=1)
        assert scopes == ["forex"]

    def test_minor_currency_medium_importance_includes_metals_crypto(self):
        from calendar_store_worker import map_scopes
        scopes = map_scopes(currency="TRY", country="TR", importance=2)
        assert "forex" in scopes
        assert "metals" in scopes
        assert "crypto" in scopes

    def test_major_currency_low_importance_returns_forex_only(self):
        from calendar_store_worker import map_scopes
        # Major CCY but importance < 2 should NOT trigger all-scopes path
        scopes = map_scopes(currency="USD", country="US", importance=1)
        assert scopes == ["forex"]

    def test_no_duplicates_in_result(self):
        from calendar_store_worker import map_scopes
        scopes = map_scopes(currency="TRY", country="TR", importance=2)
        assert len(scopes) == len(set(scopes))

    def test_module_import_does_not_raise(self):
        """Regression: hset indentation fix — import should succeed."""
        import calendar_store_worker  # noqa: F401


# ---------------------------------------------------------------------------
# calendar_store_service.py — _idx_key, aliases
# ---------------------------------------------------------------------------

class TestIdxKeyHelpers:
    def test_generic_idx_key(self):
        from calendar_store_service import _idx_key
        assert _idx_key("CUR", "USD") == "calendar:idx:CUR:USD"

    def test_currency_alias(self):
        from calendar_store_service import _idx_key_currency, _idx_key
        assert _idx_key_currency("EUR") == _idx_key("CUR", "EUR")

    def test_region_alias(self):
        from calendar_store_service import _idx_key_region, _idx_key
        assert _idx_key_region("US") == _idx_key("REG", "US")

    def test_symbol_alias(self):
        from calendar_store_service import _idx_key_symbol, _idx_key
        assert _idx_key_symbol("BTCUSDT") == _idx_key("SYM", "BTCUSDT")

    def test_agg_key(self):
        from calendar_store_service import _agg_key
        assert _agg_key("crypto") == "calendar:agg:crypto"

    def test_event_key(self):
        from calendar_store_service import _event_key
        assert _event_key("evt-123") == "calendar:event:evt-123"
