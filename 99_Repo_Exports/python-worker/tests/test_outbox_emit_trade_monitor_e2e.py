from __future__ import annotations

import json
from types import SimpleNamespace
from typing import Any, Dict, Optional, List, Tuple

import pytest

def _parse_signal(fields: dict) -> dict:
    """Updated _parse_signal with payload wins logic."""
    # 1) legacy "data" JSON
    if "data" in fields:
        try:
            j = json.loads(fields["data"])
            if isinstance(j, dict):
                return j
        except Exception:
            pass

    # 2) outbox "payload_json"
    if "payload_json" in fields:
        base = dict(fields)
        try:
            pj = json.loads(fields["payload_json"])
            if isinstance(pj, dict):
                # payload wins (it is the authoritative signal content)
                base.update(pj)
        except Exception:
            pass
        return base

    # 3) raw fields as-is
    return dict(fields)
from services.trade_monitor import TradeMonitorService
from domain.handlers import create_position, _should_start_trailing_after_tp1


# -----------------------------
# Minimal in-memory Redis stub
# -----------------------------
class FakeRedis:
    """
    Minimal Redis stub for OutboxWriter:
      - set(key, value, nx=..., xx=..., ex=...)
      - delete(key)
      - xadd(stream, fields, *args, **kwargs)
    """
    def __init__(self) -> None:
        self.kv: Dict[str, str] = {}
        self.streams: Dict[str, List[Tuple[str, Dict[str, str]]]] = {}
        self._seq = 0

    def set(self, key: str, value: str, *, nx: bool = False, xx: bool = False, ex: Optional[int] = None) -> bool:
        exists = key in self.kv
        if nx and exists:
            return False
        if xx and (not exists):
            return False
        self.kv[key] = str(value)
        return True

    def delete(self, key: str) -> int:
        if key in self.kv:
            del self.kv[key]
            return 1
        return 0

    def xadd(self, stream: str, fields: Dict[str, Any], *args: Any, **kwargs: Any) -> str:
        self._seq += 1
        entry_id = f"{self._seq}-0"
        d: Dict[str, str] = {}
        for k, v in (fields or {}).items():
            d[str(k)] = v if isinstance(v, str) else str(v)
        self.streams.setdefault(stream, []).append((entry_id, d))
        return entry_id

    def last_stream_fields(self, stream: str) -> Dict[str, str]:
        items = self.streams.get(stream) or []
        assert items, f"Stream {stream} is empty"
        return items[-1][1]


class _SpecStub:
    trailing_profile_default = "rocket_v1"

    def risk_money(self, entry, sl, lot, direction):
        return abs(float(entry) - float(sl)) * float(lot)


def _mk_trade_monitor_like() -> TradeMonitorService:
    """
    We only need _normalize_signal() and _get_spec().
    Avoid full init (repo, locks, threads).
    """
    mon = TradeMonitorService.__new__(TradeMonitorService)
    mon._get_spec = lambda symbol: _SpecStub()
    mon.logger = SimpleNamespace(
        debug=lambda *a, **k: None,
        info=lambda *a, **k: None,
        warning=lambda *a, **k: None,
        exception=lambda *a, **k: None,
    )
    return mon


def _mk_outbox_writer(fake_redis: FakeRedis):
    """
    Build core.OutboxWriter instance without relying on its __init__ signature.
    We only need write(env) behavior.
    """
    from core.outbox_writer import OutboxWriter  # <- real file: python-worker/core/outbox_writer.py

    w = OutboxWriter.__new__(OutboxWriter)
    w.redis = fake_redis
    w.cfg = SimpleNamespace(
        stream_name="stream:signals:outbox",
        placeholder_ttl_s=60,
        dedup_ttl_s=3600,
        max_retries=1,
        retry_backoff_ms=0,
        # add common optional knobs to avoid AttributeError if used:
        stream_maxlen=10000,
        stream_approximate=True,
    )
    w.logger = SimpleNamespace(
        warning=lambda *a, **k: None,
        info=lambda *a, **k: None,
        debug=lambda *a, **k: None,
    )
    w.metrics = None

    # OutboxWriter.write() calls self._redis_set(); provide a compatible wrapper.
    def _redis_set(key: str, value: str, nx: bool = False, xx: bool = False, ex: Optional[int] = None):
        return fake_redis.set(key, value, nx=nx, xx=xx, ex=ex)

    w._redis_set = _redis_set  # type: ignore[attr-defined]
    w._m_inc = lambda *a, **k: None  # type: ignore[attr-defined]
    return w


def _mk_emitter_with_outbox(outbox_writer):
    """
    Build UnifiedSignalEmitter without relying on its __init__.
    Inject outbox writer directly.
    """
    import handlers.emitter.unified_signal_emitter as ue  # <- real file: python-worker/handlers/emitter/unified_signal_emitter.py

    EmitterCls = getattr(ue, "UnifiedSignalEmitter", None)
    assert EmitterCls is not None, "UnifiedSignalEmitter class not found in handlers.emitter.unified_signal_emitter"

    em = EmitterCls.__new__(EmitterCls)
    em._outbox = outbox_writer
    em.metrics = None
    em._m_inc = lambda *a, **k: None
    return em


def test_emit_to_stream_parse_to_trade_monitor_and_position_trailing_flag():
    r = FakeRedis()
    writer = _mk_outbox_writer(r)
    emitter = _mk_emitter_with_outbox(writer)

    # Emit a signal where conditional trailing is DISABLED (trail_after_tp1=0).
    payload = {
        "sid": "sig-123",
        "strategy": "CryptoOrderFlow",
        "source": "CryptoOrderFlow",
        # Use only "timeframe" to verify TradeMonitor fallback timeframe->tf
        "timeframe": "1m",
        "direction": "LONG",
        "entry_price": 100.0,
        "entry_ts_ms": 1700000000000,
        "lot": 1.0,
        "sl": 95.0,
        "tp_levels": [101.0, 102.0, 103.0],
        "trail_profile": "rocket_v1",
        "trail_after_tp1": 0,
        "trail_after_tp1_reason": "NO_MOMO",
    }

    res = emitter.emit(
        signal_id="sig-123",
        kind="touch",
        symbol="BTCUSDT",
        side="LONG",
        raw_score=1.0,
        final_score=1.0,
        confidence_pct=50.0,
        payload=payload,
        labels={"x": 1},
        ts_event_ms=1700000000000,
    )

    assert getattr(res, "ok", False) is True
    # Pull stream fields written by OutboxWriter
    fields = r.last_stream_fields("stream:signals:outbox")
    assert "payload_json" in fields

    # TradeMonitorRunner parsing must merge payload_json -> raw dict
    raw = _parse_signal(fields)
    assert raw.get("trail_after_tp1") in (0, "0")
    assert raw.get("trail_after_tp1_reason") == "NO_MOMO"
    assert raw.get("timeframe") == "1m"

    mon = _mk_trade_monitor_like()
    sig = mon._normalize_signal(raw)
    assert sig is not None

    # create_position must copy trail_after_tp1 to PositionState
    pos = create_position(sig, _SpecStub())
    assert bool(getattr(pos, "trail_after_tp1", True)) is False
    assert str(getattr(pos, "trail_after_tp1_reason", "")) == "NO_MOMO"

    # Policy must respect it (with default TRAIL_COND_ENABLED=1 => uses pos.trail_after_tp1)
    assert _should_start_trailing_after_tp1(pos) is False


def test_outbox_dedup_prevents_double_write_same_signal_id():
    r = FakeRedis()
    writer = _mk_outbox_writer(r)
    emitter = _mk_emitter_with_outbox(writer)

    payload = {"sid": "sig-dup", "strategy": "CryptoOrderFlow", "source": "CryptoOrderFlow", "timeframe": "1m"}

    res1 = emitter.emit(signal_id="sig-dup", kind="touch", symbol="BTCUSDT", payload=payload)
    res2 = emitter.emit(signal_id="sig-dup", kind="touch", symbol="BTCUSDT", payload=payload)

    assert getattr(res1, "ok", False) is True
    assert getattr(res2, "ok", False) is True
    # Second one should be duplicate -> not written
    assert getattr(res2, "duplicate", False) is True
    assert len(r.streams.get("stream:signals:outbox") or []) == 1
