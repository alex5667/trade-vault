from __future__ import annotations

import json
from types import SimpleNamespace
from typing import Any, Dict, Optional, List, Tuple

import pytest

from runners.trade_monitor_runner import _parse_signal
from services.trade_monitor import TradeMonitorService
from domain.handlers import create_position, _should_start_trailing_after_tp1


# --------------------------------------------------------------------
# Fake Redis (supports the subset used by handlers/emitter OutboxWriter)
# --------------------------------------------------------------------
class FakeRedis:
    """
    Minimal Redis for outbox pipeline tests.
    We intentionally support both EX (sec) and PX (ms) TTL-style args because:
      - core/outbox_writer uses EX
      - handlers/emitter/outbox_writer commonly uses ms TTL knobs
    """
    def __init__(self) -> None:
        self.kv: Dict[str, str] = {}
        self.streams: Dict[str, List[Tuple[str, Dict[str, str]]]] = {}
        self._seq = 0

    def set(
        self,
        key: str,
        value: str,
        *,
        nx: bool = False,
        xx: bool = False,
        ex: Optional[int] = None,
        px: Optional[int] = None,
    ) -> bool:
        exists = key in self.kv
        if nx and exists:
            return False
        if xx and (not exists):
            return False
        self.kv[key] = str(value)
        return True

    def get(self, key: str) -> Optional[str]:
        return self.kv.get(key)

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


# --------------------------------------------------------------------
# A "SignalOutboxPublisher-like" adapter (no Lua; behavior-oriented)
# --------------------------------------------------------------------
class FakeSignalOutboxPublisher:
    """
    Stand-in for core.signal_outbox.SignalOutboxPublisher.
    We keep the important external contract:
      - atomic-ish dedup via SETNX (bucketed)
      - XADD to stream with MAXLEN ~
    This allows testing handlers/emitter OutboxWriter + UnifiedSignalEmitter without
    binding to the internal Lua details.
    """
    def __init__(
        self,
        redis: FakeRedis,
        *,
        stream_name: str = "stream:signals:outbox",
        maxlen: int = 20000,
        dedup_ttl_ms: int = 60000,
        dedup_bucket_ms: int = 60000,
    ) -> None:
        self.redis = redis
        self.stream_name = stream_name
        self.maxlen = int(maxlen)
        self.dedup_ttl_ms = int(dedup_ttl_ms)
        self.dedup_bucket_ms = int(dedup_bucket_ms)

    def publish_envelope(self, env: Any) -> Dict[str, Any]:
        """
        Accepts a core.outbox_envelope.OutboxEnvelope-like object.
        Returns a small dict (ok/written/duplicate/entry_id) to be duck-typed by writers.
        """
        # stable bucket (ms) to match OutboxSettings.dedup_bucket_ms behavior
        ts_ms = int(getattr(env, "ts_ms", 0) or 0)
        sid = str(getattr(env, "signal_id", "") or "")
        bucket = (ts_ms // max(self.dedup_bucket_ms, 1)) if ts_ms > 0 else 0
        dkey = f"outbox:dedup:{sid}:{bucket}"

        if not self.redis.set(dkey, "1", nx=True, px=self.dedup_ttl_ms):
            return {"ok": True, "written": False, "duplicate": True, "entry_id": None}

        fields = env.to_stream_fields()
        entry_id = self.redis.xadd(self.stream_name, fields, maxlen=self.maxlen, approximate=True)
        return {"ok": True, "written": True, "duplicate": False, "entry_id": entry_id}


def _mk_trade_monitor_like() -> TradeMonitorService:
    """
    We only need _normalize_signal() and _get_spec().
    Avoid full init (repo, loops).
    """
    class _SpecStub:
        trailing_profile_default = "rocket_v1"

    def risk_money(self, entry, sl, lot, direction):
        return abs(float(entry) - float(sl)) * float(lot)

    mon = TradeMonitorService.__new__(TradeMonitorService)
    mon._get_spec = lambda symbol: _SpecStub()
    mon.logger = SimpleNamespace(
        debug=lambda *a, **k: None,
        info=lambda *a, **k: None,
        warning=lambda *a, **k: None,
        exception=lambda *a, **k: None,
    )
    return mon


def _mk_handlers_outbox_writer(fake_pub: FakeSignalOutboxPublisher):
    """
    Real class: python-worker/handlers/emitter/outbox_writer.py
    We wire it to our FakeSignalOutboxPublisher dependency.
    """
    from handlers.emitter.outbox_writer import OutboxWriter

    logger = SimpleNamespace(
        debug=lambda *a, **k: None,
        info=lambda *a, **k: None,
        warning=lambda *a, **k: None,
        exception=lambda *a, **k: None,
    )

    # NOTE:
    # We set stream_key explicitly to avoid relying on publisher.stream_name attribute name.
    w = OutboxWriter(
        publisher=fake_pub,
        logger=logger,
        retries=0,
        retry_sleep_ms=0,
        dedup_ttl_ms=60000,
        dedup_pending_ttl_ms=10000,
        stream_key=fake_pub.stream_name,
        sem_enabled=False,
        sem_ttl_ms=0,
        sem_pending_ttl_ms=0,
        sem_bucket_ms=0,
        sem_level_decimals=0,
    )
    return w


def _mk_unified_emitter_with_outbox(outbox_writer):
    """
    Real class: python-worker/handlers/emitter/unified_signal_emitter.py
    We inject the outbox writer directly (no constructor coupling).
    """
    import handlers.emitter.unified_signal_emitter as ue

    EmitterCls = getattr(ue, "UnifiedSignalEmitter", None)
    assert EmitterCls is not None, "UnifiedSignalEmitter not found"

    em = EmitterCls.__new__(EmitterCls)
    em._outbox = outbox_writer
    em.metrics = None
    em._m_inc = lambda *a, **k: None
    return em


def _call_outbox_write(outbox_writer: Any, env: Any):
    """
    Make the test resilient if the writer uses a different method name.
    """
    for name in ("write", "emit", "publish"):
        fn = getattr(outbox_writer, name, None)
        if callable(fn):
            return fn(env)
    raise AssertionError("OutboxWriter has no write/emit/publish method")


def test_handlers_outbox_writer_unified_emitter_to_trade_monitor_position_trailing_flag():
    """
    End-to-end (behavior) path:
      UnifiedSignalEmitter.emit()
        -> handlers/emitter OutboxWriter
        -> SignalOutboxPublisher-like dependency (dedup + XADD)
        -> stream fields (payload_json)
        -> TradeMonitorRunner._parse_signal()
        -> TradeMonitorService._normalize_signal()
        -> create_position()
        -> _should_start_trailing_after_tp1()
    """
    r = FakeRedis()
    pub = FakeSignalOutboxPublisher(r)
    outbox = _mk_handlers_outbox_writer(pub)
    emitter = _mk_unified_emitter_with_outbox(outbox)

    payload = {
        "sid": "sig-777",
        "strategy": "CryptoOrderFlow",
        "source": "CryptoOrderFlow",
        "timeframe": "1m",
        "direction": "LONG",
        "entry_price": 100.0,
        "entry_ts_ms": 1700000000000,
        "lot": 1.0,
        "sl": 95.0,
        "tp_levels": [101.0, 102.0, 103.0],
        "trail_profile": "rocket_v1",
        "trail_after_tp1": 0,
        "trail_after_tp1_reason": "LOW_MOMO",
    }

    res = emitter.emit(
        signal_id="sig-777",
        kind="touch",
        symbol="BTCUSDT",
        side="LONG",
        raw_score=1.0,
        final_score=1.0,
        confidence_pct=50.0,
        payload=payload,
        labels=None,
        ts_event_ms=1700000000000,
    )
    assert getattr(res, "ok", True) is True

    fields = r.last_stream_fields("stream:signals:outbox")
    assert "payload_json" in fields

    raw = _parse_signal(fields)
    assert raw.get("trail_after_tp1") in (0, "0")
    assert raw.get("trail_after_tp1_reason") == "LOW_MOMO"

    mon = _mk_trade_monitor_like()
    sig = mon._normalize_signal(raw)
    assert sig is not None

    # Position must carry the policy flag:
    from test_outbox_emit_trade_monitor_e2e import _SpecStub
    pos = create_position(sig, _SpecStub())
    assert bool(getattr(pos, "trail_after_tp1", True)) is False
    assert str(getattr(pos, "trail_after_tp1_reason", "")) == "LOW_MOMO"

    # Policy must respect it:
    assert _should_start_trailing_after_tp1(pos) is False


def test_handlers_outbox_writer_dedup_bucketed():
    r = FakeRedis()
    pub = FakeSignalOutboxPublisher(r, dedup_bucket_ms=60000)
    outbox = _mk_handlers_outbox_writer(pub)
    emitter = _mk_unified_emitter_with_outbox(outbox)

    payload = {"sid": "sig-dup2", "strategy": "CryptoOrderFlow", "source": "CryptoOrderFlow", "timeframe": "1m"}

    res1 = emitter.emit(signal_id="sig-dup2", kind="touch", symbol="BTCUSDT", payload=payload, ts_event_ms=1700000000000)
    res2 = emitter.emit(signal_id="sig-dup2", kind="touch", symbol="BTCUSDT", payload=payload, ts_event_ms=1700000000000)

    assert getattr(res1, "ok", True) is True
    assert getattr(res2, "ok", True) is True
    assert len(r.streams.get("stream:signals:outbox") or []) == 1
