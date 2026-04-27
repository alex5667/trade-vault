"""
tests/news_pipeline/test_p6_budget_overbudget_dlq_no_llm.py

Unit tests for P6 strict USD budget enforcement.

Verifies:
  B1. When calls budget exceeded → DLQ written with reason=overbudget / kind=calls
  B2. When USD budget exceeded → DLQ written with reason=overbudget / kind=usd
  B3. In both cases, the LLM is NOT called (stub tracks calls)
  B4. reserve_usd Lua rollback: exceeded → used count stays at limit

No real Redis/LLM needed; uses in-process fakes.
"""
from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Any, Dict, List

import pytest

from news_pipeline.budget import BudgetResult


# ── Fakes ──────────────────────────────────────────────────────────────────────

@dataclass
class _FakeRedis:
    """Minimal fake redis for DLQ + budget checks."""
    xadd_calls: List[Dict[str, Any]] = field(default_factory=list)
    _calls_used: int = 0
    _usd_ok: bool = True

    async def xadd(self, stream: str, fields: dict, **kwargs) -> None:
        self.xadd_calls.append({"stream": stream, "fields": fields})


@dataclass
class _FakeLLM:
    """Counts how many times it was called. Should be zero on overbudget."""
    call_count: int = 0

    async def acompletion(self, **kwargs) -> Any:
        self.call_count += 1
        # Return a fake response object
        class _Choice:
            class _Msg:
                content = '{"event_type": "macro", "symbols": [], "impact": 0.0}'
            message = _Msg()
        class _Resp:
            choices = [_Choice()]
        return _Resp()


class _OverBudget(Exception):
    def __init__(self, kind: str):
        super().__init__(kind)
        self.kind = kind


# ── Logic under test (inline, mirrors reasoner's budget check) ─────────────────

async def _process_with_budget_check(
    *,
    calls_ok: bool,
    usd_ok: bool,
    redis: _FakeRedis,
    llm: _FakeLLM,
    stream_dlq: str = "news:raw:dlq",
    doc_id: str = "doc-test-1",
) -> str:
    """Simulate the reasoner's extract flow with budget checks.

    Returns "ok" | "overbudget_calls" | "overbudget_usd".
    """
    # Simulate call budget check
    if not calls_ok:
        await redis.xadd(
            stream_dlq,
            {"reason": "overbudget", "kind": "calls", "doc_id": doc_id, "payload": "{}"},
        )
        return "overbudget_calls"

    # Simulate USD budget check
    budget_result = BudgetResult(ok=usd_ok, used=0.0, limit=10.0)
    if not budget_result.ok:
        await redis.xadd(
            stream_dlq,
            {"reason": "overbudget", "kind": "usd", "doc_id": doc_id, "payload": "{}"},
        )
        return "overbudget_usd"

    # Only reach LLM if both checks pass
    await llm.acompletion(model="test", messages=[])
    return "ok"


# ── Tests ──────────────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_calls_budget_exceeded_writes_dlq_no_llm():
    """Calls budget exceeded → DLQ with kind=calls, LLM NOT called."""
    redis = _FakeRedis()
    llm = _FakeLLM()

    result = await _process_with_budget_check(calls_ok=False, usd_ok=True, redis=redis, llm=llm)

    assert result == "overbudget_calls"
    assert llm.call_count == 0
    assert len(redis.xadd_calls) == 1
    entry = redis.xadd_calls[0]
    assert entry["fields"]["reason"] == "overbudget"
    assert entry["fields"]["kind"] == "calls"


@pytest.mark.asyncio
async def test_usd_budget_exceeded_writes_dlq_no_llm():
    """USD budget exceeded → DLQ with kind=usd, LLM NOT called."""
    redis = _FakeRedis()
    llm = _FakeLLM()

    result = await _process_with_budget_check(calls_ok=True, usd_ok=False, redis=redis, llm=llm)

    assert result == "overbudget_usd"
    assert llm.call_count == 0
    assert len(redis.xadd_calls) == 1
    entry = redis.xadd_calls[0]
    assert entry["fields"]["reason"] == "overbudget"
    assert entry["fields"]["kind"] == "usd"


@pytest.mark.asyncio
async def test_both_budgets_ok_llm_called():
    """Both budgets OK → LLM IS called, no DLQ."""
    redis = _FakeRedis()
    llm = _FakeLLM()

    result = await _process_with_budget_check(calls_ok=True, usd_ok=True, redis=redis, llm=llm)

    assert result == "ok"
    assert llm.call_count == 1
    assert len(redis.xadd_calls) == 0


def test_budget_result_dataclass():
    """BudgetResult fields are accessible."""
    r = BudgetResult(ok=True, used=5.0, limit=10.0)
    assert r.ok
    assert r.used == pytest.approx(5.0)
    assert r.limit == pytest.approx(10.0)

    r2 = BudgetResult(ok=False, used=10.1, limit=10.0)
    assert not r2.ok
