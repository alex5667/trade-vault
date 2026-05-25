"""tests/test_news_llm_pipeline.py

Unit tests for the async Playwright enrichment pipeline.
All tests are deterministic — no network, no browser, no Redis required.
"""
from __future__ import annotations

import json
import time
from unittest.mock import MagicMock, patch

import pytest

from news_pipeline.llm_job import (
    ALLOWED_ACTIONS,
    LLMJob,
    LLMResult,
    LLMStatus,
    make_job_id,
    resolve_action,
    validate_llm_result,
)
from news_pipeline.prompt_v2 import PROMPT_VERSION, build_prompt_v2


# ── Fixtures ──────────────────────────────────────────────────────────────────

def _valid_raw() -> dict:
    return {
        "schema_ver": "news_llm_analysis_v2",
        "event_type": "macro_cpi",
        "event_class": "macro",
        "grade_id": 5,
        "risk_score": 0.9,
        "surprise_score": 0.7,
        "confidence": 0.85,
        "sentiment": "risk_off",
        "affected_symbols": ["BTCUSDT", "ETHUSDT"],
        "directional_bias": {"BTCUSDT": "SELL"},
        "recommended_action": "tighten",
        "risk_factor_bps": 5000,
        "reason_code": "macro_high_impact_cpi",
        "time_window_sec": 1800,
        "evidence": ["cpi", "inflation"],
        "dq_flags": [],
        "summary": "Hot CPI surprise triggers risk-off across crypto.",
    }


def _make_job(**kwargs) -> LLMJob:
    defaults = dict(news_uid="uid-abc", title="CPI hotter than expected",
                    url="https://reuters.com/cpi", source="reuters")
    defaults.update(kwargs)
    return LLMJob(**defaults)


# ── validate_llm_result ───────────────────────────────────────────────────────

class TestValidateLLMResult:
    def test_valid_result_ok(self):
        result, errors = validate_llm_result(_valid_raw())
        assert errors == []
        assert result.status == LLMStatus.OK
        assert result.usable
        assert result.event_type == "macro_cpi"
        assert result.recommended_action == "tighten"
        assert result.confidence == 0.85

    def test_invalid_json_returns_schema_error_not_safe(self):
        """Invalid JSON → status=invalid_json, NOT risk=0 / allow silently."""
        raw = _valid_raw()
        raw["event_type"] = "something_made_up"
        result, errors = validate_llm_result(raw)
        assert result.status == LLMStatus.SCHEMA_ERROR
        assert not result.usable
        assert any("invalid_event_type" in e for e in errors)

    def test_missing_reason_code_is_error(self):
        raw = _valid_raw()
        del raw["reason_code"]
        _, errors = validate_llm_result(raw)
        assert "missing_reason_code" in errors

    def test_missing_time_window_is_error(self):
        raw = _valid_raw()
        del raw["time_window_sec"]
        _, errors = validate_llm_result(raw)
        assert "missing_time_window_sec" in errors

    def test_invalid_sentiment_corrected(self):
        raw = _valid_raw()
        raw["sentiment"] = "bearish"  # not in allowed list
        result, errors = validate_llm_result(raw)
        assert result.sentiment == "unknown"
        assert any("invalid_sentiment" in e for e in errors)

    def test_clamp_risk_score(self):
        raw = _valid_raw()
        raw["risk_score"] = 1.5  # above 1.0
        result, _ = validate_llm_result(raw)
        assert result.risk_score == 1.0

    def test_invalid_action_corrected_to_allow(self):
        raw = _valid_raw()
        raw["recommended_action"] = "do_nothing"
        result, errors = validate_llm_result(raw)
        assert result.recommended_action == "allow"
        assert any("invalid_action" in e for e in errors)

    def test_non_list_symbols_tolerated(self):
        raw = _valid_raw()
        raw["affected_symbols"] = "BTCUSDT"  # string, not list
        result, _ = validate_llm_result(raw)
        assert result.affected_symbols == []

    def test_timeout_status_not_safe(self):
        """A timeout result must NOT be treated as confidence=0 safe."""
        result = LLMResult.error(
            job_id="j1", news_uid="n1", provider="playwright_qwen",
            status=LLMStatus.TIMEOUT, dq_flag="llm_timeout",
        )
        assert not result.usable
        assert result.recommended_action == "allow"  # fail-open, not block


# ── resolve_action ────────────────────────────────────────────────────────────

class TestResolveAction:
    def test_block_downgraded_to_tighten_when_hard_block_disabled(self):
        raw = _valid_raw()
        raw["recommended_action"] = "block"
        raw["confidence"] = 0.95
        result, _ = validate_llm_result(raw)
        action, bps, reason = resolve_action(
            rule_action="allow", rule_grade_id=3,
            llm_result=result, hard_block_allow=False,
        )
        assert action == "tighten"
        assert bps == 5000
        assert "downgraded" in reason

    def test_rules_grade5_can_block_without_llm(self):
        action, bps, reason = resolve_action(
            rule_action="block", rule_grade_id=5,
            llm_result=None, hard_block_allow=False,
        )
        assert action == "block"
        assert "rules_grade5" in reason

    def test_block_downgraded_when_low_confidence(self):
        raw = _valid_raw()
        raw["recommended_action"] = "block"
        raw["confidence"] = 0.60  # below 0.80 threshold
        result, _ = validate_llm_result(raw)
        action, bps, _ = resolve_action(
            rule_action="allow", rule_grade_id=3,
            llm_result=result, hard_block_allow=True,
        )
        assert action == "tighten"

    def test_block_allowed_when_hard_block_and_high_confidence(self):
        raw = _valid_raw()
        raw["recommended_action"] = "block"
        raw["confidence"] = 0.90
        result, _ = validate_llm_result(raw)
        result.status = LLMStatus.OK
        action, bps, _ = resolve_action(
            rule_action="allow", rule_grade_id=3,
            llm_result=result, hard_block_allow=True,
        )
        assert action == "block"

    def test_unusable_llm_fallback_to_rules(self):
        result = LLMResult.error(
            job_id="j", news_uid="n", provider="pw",
            status=LLMStatus.TIMEOUT, dq_flag="x",
        )
        action, _, reason = resolve_action(
            rule_action="tighten", rule_grade_id=3,
            llm_result=result, hard_block_allow=False,
        )
        assert action == "tighten"
        assert "rules_only" in reason

    def test_tighten_passes_through(self):
        raw = _valid_raw()
        raw["recommended_action"] = "tighten"
        result, _ = validate_llm_result(raw)
        action, bps, _ = resolve_action(
            rule_action="allow", rule_grade_id=2,
            llm_result=result, hard_block_allow=False,
        )
        assert action == "tighten"


# ── LLMJob ────────────────────────────────────────────────────────────────────

class TestLLMJob:
    def test_job_id_deterministic(self):
        j1 = _make_job(news_uid="x")
        j2 = _make_job(news_uid="x")
        assert j1.job_id == j2.job_id

    def test_job_id_different_per_uid(self):
        assert _make_job(news_uid="a").job_id != _make_job(news_uid="b").job_id

    def test_round_trip_stream_fields(self):
        job = _make_job(rule_grade_id=5, priority="high")
        fields = job.to_stream_fields()
        job2 = LLMJob.from_stream_fields(fields)
        assert job2.news_uid == job.news_uid
        assert job2.rule_grade_id == 5
        assert job2.priority == "high"

    def test_is_expired_past_deadline(self):
        job = _make_job()
        job.deadline_ts_ms = int(time.time() * 1000) - 1000  # 1s ago
        assert job.is_expired()

    def test_not_expired_future_deadline(self):
        job = _make_job()
        job.deadline_ts_ms = int(time.time() * 1000) + 60_000
        assert not job.is_expired()


# ── LLMCache ─────────────────────────────────────────────────────────────────

class TestLLMCache:
    def _fake_redis(self, stored: dict | None = None) -> MagicMock:
        r = MagicMock()
        if stored is not None:
            raw, _ = validate_llm_result(_valid_raw())
            r.get.return_value = json.dumps(raw.to_dict())
        else:
            r.get.return_value = None
        return r

    def test_cache_miss_returns_none(self):
        from news_pipeline.llm_cache import get
        r = self._fake_redis(None)
        assert get("j1", r) is None

    def test_cache_hit_sets_cache_hit_status(self):
        from news_pipeline.llm_cache import get
        result, _ = validate_llm_result(_valid_raw())
        r = MagicMock()
        r.get.return_value = json.dumps(result.to_dict())
        cached = get("j1", r)
        assert cached is not None
        assert cached.status == LLMStatus.CACHE_HIT

    def test_cache_put_skips_error_results(self):
        from news_pipeline.llm_cache import put
        r = MagicMock()
        result = LLMResult.error(job_id="j", news_uid="n", provider="pw",
                                  status=LLMStatus.TIMEOUT, dq_flag="x")
        put("j", result, r)
        r.setex.assert_not_called()

    def test_cache_put_stores_ok_result(self):
        from news_pipeline.llm_cache import put
        r = MagicMock()
        result, _ = validate_llm_result(_valid_raw())
        put("j1", result, r)
        r.setex.assert_called_once()


# ── Circuit breaker ───────────────────────────────────────────────────────────

class TestCircuitBreaker:
    def _redis(self, state: str | None = None, stats: dict | None = None) -> MagicMock:
        r = MagicMock()
        r.get.return_value = state
        r.hgetall.return_value = stats or {}
        pipe = MagicMock()
        pipe.__enter__ = lambda s: s
        pipe.__exit__ = MagicMock(return_value=False)
        r.pipeline.return_value = pipe
        return r

    def test_circuit_closed_by_default(self):
        from news_pipeline.circuit_breaker import is_open
        r = self._redis(None)
        assert not is_open("playwright_qwen", r)

    def test_circuit_open_when_state_open(self):
        from news_pipeline.circuit_breaker import is_open
        r = self._redis("open")
        assert is_open("playwright_qwen", r)

    def test_record_outcome_does_not_raise_on_redis_error(self):
        from news_pipeline.circuit_breaker import record_outcome
        r = MagicMock()
        r.pipeline.side_effect = Exception("redis down")
        record_outcome("playwright_qwen", r, status="timeout", latency_ms=5000)

    def test_maybe_open_fires_on_high_timeout_rate(self):
        from news_pipeline import circuit_breaker as cb
        r = MagicMock()
        pipe = MagicMock()
        r.pipeline.return_value = pipe
        r.hgetall.return_value = {
            "requests": "20", "timeouts": "15",
            "invalid_json": "0", "login_errors": "0", "latency_sum": "10000",
        }
        cb._maybe_open("playwright_qwen", r)
        r.set.assert_called()  # should call set("news:llm:cb:playwright_qwen:state", "open", ...)


# ── Prompt v2 ─────────────────────────────────────────────────────────────────

class TestPromptV2:
    def test_prompt_contains_schema_ver(self):
        p = build_prompt_v2(title="CPI", url="https://x.com", source="reuters")
        assert "news_llm_analysis_v2" in p

    def test_prompt_contains_title(self):
        p = build_prompt_v2(title="Fed cuts rates 50bps", url="https://x.com", source="reuters")
        assert "Fed cuts rates 50bps" in p

    def test_prompt_contains_all_event_types(self):
        p = build_prompt_v2(title="x", url="https://x.com", source="x")
        assert "macro_cpi" in p
        assert "security_hack" in p
        assert "noise" in p

    def test_prompt_contains_symbols(self):
        p = build_prompt_v2(title="x", url="https://x.com", source="x")
        assert "BTCUSDT" in p
        assert "ETHUSDT" in p

    def test_prompt_includes_summary_when_provided(self):
        p = build_prompt_v2(title="x", url="https://x.com", source="x",
                            summary="CPI came in at 3.5%")
        assert "CPI came in at 3.5%" in p

    def test_version_constant(self):
        assert PROMPT_VERSION == "v2.1.0"


# ── analyzer_worker enqueue logic ─────────────────────────────────────────────

class TestAnalyzerWorkerEnqueue:
    """Test _maybe_enqueue_llm_job without starting the full worker."""

    def _worker(self, grade_min: int = 3) -> tuple:
        """Returns (worker, fake_redis). Does not call run_forever."""
        import os
        with patch.dict(os.environ, {
            "NEWS_LLM_PLAYWRIGHT_ENABLE": "1",
            "NEWS_LLM_MIN_RULE_GRADE": str(grade_min),
            "NEWS_LLM_SKIP_NOISE": "1",
        }):
            # Re-import to pick up env changes
            import importlib
            import news_pipeline.analyzer_worker as aw
            importlib.reload(aw)

            r = MagicMock()
            r.get.return_value = None
            r.set.return_value = True
            pipe = MagicMock()
            pipe.__enter__ = lambda s: s
            pipe.__exit__ = MagicMock(return_value=False)
            r.pipeline.return_value = pipe

            # Bypass StreamWorker.__init__ group creation
            with patch.object(aw.StreamWorker, "__init__", lambda s, **kw: None):
                worker = aw.NewsAnalyzerWorker.__new__(aw.NewsAnalyzerWorker)
                worker.r = r
                return worker, r, aw

    def test_low_grade_not_enqueued(self):
        worker, r, aw = self._worker(grade_min=3)
        worker._maybe_enqueue_llm_job(
            uid="u1", title="Stock rally", url="https://x.com",
            source="reuters", summary="", published_ts_ms=0, grade_id=2,
        )
        r.xadd.assert_not_called()

    def test_high_grade_enqueued(self):
        worker, r, aw = self._worker(grade_min=3)
        r.set.return_value = True  # nx=True succeeds → not dedup'd
        worker._maybe_enqueue_llm_job(
            uid="u2", title="CPI hotter than expected", url="https://x.com",
            source="reuters", summary="", published_ts_ms=0, grade_id=5,
        )
        r.xadd.assert_called_once()

    def test_dedup_prevents_double_enqueue(self):
        worker, r, aw = self._worker(grade_min=3)
        r.set.return_value = None  # nx=True fails → already queued
        worker._maybe_enqueue_llm_job(
            uid="u3", title="CPI surprise", url="https://x.com",
            source="reuters", summary="", published_ts_ms=0, grade_id=5,
        )
        r.xadd.assert_not_called()

    def test_noise_title_skipped(self):
        worker, r, aw = self._worker(grade_min=3)
        worker._maybe_enqueue_llm_job(
            uid="u4", title="Analysis: why markets are calm",
            url="https://x.com", source="reuters",
            summary="", published_ts_ms=0, grade_id=5,
        )
        r.xadd.assert_not_called()
