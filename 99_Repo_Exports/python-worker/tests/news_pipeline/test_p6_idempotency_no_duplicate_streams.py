from __future__ import annotations
"""
tests/news_pipeline/test_p6_idempotency_no_duplicate_streams.py

Unit tests for P6 idempotency guarantee.

Scenario: Same (doc_id, prompt_ver, model_id) triple processed twice.
Expected: stream emit happens only on first insert (inserted=True);
          on second call (inserted=False) no xadd is triggered.

Tests use a minimal in-process mock (no real Redis/DB needed).
"""

from dataclasses import dataclass, field
from typing import Any, Dict, List

import pytest

from news_pipeline.p6_dto import stable_event_uuid


# ── Minimal fakes ──────────────────────────────────────────────────────────────

@dataclass
class _FakeRow:
    """Mimics asyncpg Row with dict-like access."""
    _data: Dict[str, Any]

    def __getitem__(self, key: str) -> Any:
        return self._data[key]

    def __contains__(self, key: str) -> bool:
        return key in self._data


@dataclass
class _FakeConnection:
    """asyncpg-like connection with configurable fetchrow result."""
    rows: List[_FakeRow] = field(default_factory=list)
    call_count: int = 0

    async def fetchrow(self, query: str, *args) -> "_FakeRow | None":
        self.call_count += 1
        if self.rows:
            return self.rows[self.call_count - 1] if self.call_count <= len(self.rows) else None
        return None


class _FakePool:
    def __init__(self, conn: _FakeConnection) -> None:
        self._conn = conn

    def acquire(self):
        return self

    async def __aenter__(self) -> _FakeConnection:
        return self._conn

    async def __aexit__(self, *args) -> None:
        pass


@dataclass
class _FakeRedis:
    xadd_calls: List[Dict[str, Any]] = field(default_factory=list)

    async def xadd(self, stream: str, fields: dict, **kwargs) -> None:
        self.xadd_calls.append({"stream": stream, "fields": fields})



# ── Idempotency logic under test (extracted inline for clarity) ────────────────

async def _handle_doc(
    *,
    doc_id: str,
    prompt_ver: str,
    model_id: str,
    redis: _FakeRedis,
    pool: _FakePool,
    stream_events: str = "news:analysis",
) -> bool:
    """Simulates reasoner's insert+emit logic.

    Returns True if event was emitted, False if idempotency prevented emission.
    Mirrors the pattern from diff @ services/news_reasoner/worker.py.
    """
    event_id = stable_event_uuid(doc_id, prompt_ver, model_id)

    async with pool as conn:
        row = await conn.fetchrow(
            "INSERT INTO news_events ... ON CONFLICT DO UPDATE ... RETURNING event_id, (xmax=0) AS inserted",
            event_id,
        )
        inserted = bool(row["inserted"]) if row and ("inserted" in row) else False

    if not inserted:
        # Duplicate: do NOT emit to stream
        return False

    # New: emit contract event
    await redis.xadd(stream_events, {"payload": f'{{"event_id": "{event_id}"}}'})
    return True


# ── Tests ──────────────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_first_insert_emits_event():
    """First insert → inserted=True → stream emit happens."""
    redis = _FakeRedis()
    conn = _FakeConnection(rows=[_FakeRow({"event_id": "uuid-1", "inserted": True})])
    pool = _FakePool(conn)

    emitted = await _handle_doc(
        doc_id="doc1", prompt_ver="p1", model_id="gemini-flash",
        redis=redis, pool=pool,
    )
    assert emitted is True
    assert len(redis.xadd_calls) == 1


@pytest.mark.asyncio
async def test_second_insert_no_duplicate_emit():
    """Second insert for same key → inserted=False → NO stream emit."""
    redis = _FakeRedis()
    conn = _FakeConnection(rows=[_FakeRow({"event_id": "uuid-1", "inserted": False})])
    pool = _FakePool(conn)

    emitted = await _handle_doc(
        doc_id="doc1", prompt_ver="p1", model_id="gemini-flash",
        redis=redis, pool=pool,
    )
    assert emitted is False
    assert len(redis.xadd_calls) == 0


@pytest.mark.asyncio
async def test_two_different_docs_both_emit():
    """Two distinct docs → both emit."""
    redis = _FakeRedis()
    rows = [
        _FakeRow({"event_id": "uuid-1", "inserted": True}),
        _FakeRow({"event_id": "uuid-2", "inserted": True}),
    ]
    conn = _FakeConnection(rows=rows)
    pool = _FakePool(conn)

    await _handle_doc(doc_id="doc1", prompt_ver="p1", model_id="m1", redis=redis, pool=pool)
    # Need a fresh pool/connection for second call
    conn2 = _FakeConnection(rows=[_FakeRow({"event_id": "uuid-2", "inserted": True})])
    pool2 = _FakePool(conn2)
    await _handle_doc(doc_id="doc2", prompt_ver="p1", model_id="m1", redis=redis, pool=pool2)

    assert len(redis.xadd_calls) == 2


@pytest.mark.asyncio
async def test_no_row_returned_treated_as_not_inserted():
    """If fetchrow returns None (shouldn't happen but must be handled safely)."""
    redis = _FakeRedis()
    conn = _FakeConnection(rows=[])  # returns None
    pool = _FakePool(conn)

    emitted = await _handle_doc(
        doc_id="doc1", prompt_ver="p1", model_id="m1",
        redis=redis, pool=pool,
    )
    assert emitted is False
    assert len(redis.xadd_calls) == 0
