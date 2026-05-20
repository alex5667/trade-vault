from __future__ import annotations

from types import SimpleNamespace
from typing import Any

from domain.handlers import _should_start_trailing_after_tp1, create_position
from runners.trade_monitor_runner import _parse_signal
from services.trade_monitor import TradeMonitorService
from core.redis_keys import RedisStreams as RS


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
        self.kv: dict[str, str] = {}
        self.streams: dict[str, list[tuple[str, dict[str, str]]]] = {}
        self._seq = 0

    def set(
        self,
        key: str,
        value: str,
        *,
        nx: bool = False,
        xx: bool = False,
        ex: int | None = None,
        px: int | None = None,
    ) -> bool:
        exists = key in self.kv
        if nx and exists:
            return False
        if xx and (not exists):
            return False
        self.kv[key] = str(value)
        return True

    def get(self, key: str) -> str | None:
        return self.kv.get(key)

    def delete(self, key: str) -> int:
        if key in self.kv:
            del self.kv[key]
            return 1
        return 0

    def xadd(self, stream: str, fields: dict[str, Any], *args: Any, **kwargs: Any) -> str:
        self._seq += 1
        entry_id = f"{self._seq}-0"
        d: dict[str, str] = {}
        for k, v in (fields or {}).items():
            d[str(k)] = v if isinstance(v, str) else str(v)
        self.streams.setdefault(stream, []).append((entry_id, d))
        return entry_id

    def last_stream_fields(self, stream: str) -> dict[str, str]:
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
        stream_name: str = RS.SIGNAL_OUTBOX,
        maxlen: int = 20000,
        dedup_ttl_ms: int = 60000,
        dedup_bucket_ms: int = 60000,
    ) -> None:
        self.redis = redis
        self.stream_name = stream_name
        self.maxlen = int(maxlen)
        self.dedup_ttl_ms = int(dedup_ttl_ms)
        self.dedup_bucket_ms = int(dedup_bucket_ms)

    def publish_envelope(self, env: Any) -> dict[str, Any]:
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


# --------------------------------------------------------------------
# _FakeWriter — bridges UnifiedSignalEmitter._writer.write() to FakeSignalOutboxPublisher.
#
# OutboxWriter._atomic_xadd uses redis.eval(Lua) which FakeRedis doesn't support.
# This writer bypasses Lua entirely: builds OutboxEnvelope and calls publish_envelope().
# --------------------------------------------------------------------
class _FakeWriter:
    def __init__(self, pub: FakeSignalOutboxPublisher) -> None:
        self._pub = pub

    def write(self, *, payload: dict, signal_id: str, dedup: bool, meta: Any = None) -> bool:
        from core.outbox_envelope import OutboxEnvelope
        ts_ms = int(payload.get("ts_event_ms") or payload.get("entry_ts_ms") or 0)
        env = OutboxEnvelope(
            signal_id=signal_id,
            ts_ms=ts_ms,
            kind=str(payload.get("kind", "touch")),
            symbol=str(payload.get("symbol", "")),
            payload=dict(payload),
        )
        result = self._pub.publish_envelope(env)
        return bool(result.get("written", True))


def _mk_trade_monitor_like() -> TradeMonitorService:
    """
    We only need _normalize_signal() and _get_spec().
    Avoid full init (repo, loops); patch only the attributes accessed by _normalize_signal.
    """
    class _SpecStub:
        trailing_profile_default = "rocket_v1"

    mon = TradeMonitorService.__new__(TradeMonitorService)
    mon._get_spec = lambda symbol: _SpecStub()  # type: ignore[attr-defined]
    mon.logger = SimpleNamespace(  # type: ignore[attr-defined]
        debug=lambda *a, **k: None,
        info=lambda *a, **k: None,
        warning=lambda *a, **k: None,
        exception=lambda *a, **k: None,
    )
    # Attributes accessed by _normalize_signal (missing from __new__):
    mon._max_tick_ts_ms = 0  # type: ignore[attr-defined]
    mon._crypto_suffixes = ("USDT", "USDC", "BTC", "ETH", "BNB")  # type: ignore[attr-defined]
    mon._crypto_exclude_prefixes = ()  # type: ignore[attr-defined]
    mon._margin_fx_symbols: frozenset = frozenset()  # type: ignore[attr-defined]
    mon.default_lot = 1.0  # type: ignore[attr-defined]
    mon.stop_atr_mult = 1.0  # type: ignore[attr-defined]
    mon.rr_levels = [1.0, 2.0, 3.0]  # type: ignore[attr-defined]
    return mon


def _mk_unified_emitter_with_outbox(fake_pub: FakeSignalOutboxPublisher):
    """
    Creates handlers.emitter.UnifiedSignalEmitter wired to FakeSignalOutboxPublisher.

    We bypass __init__ (which creates OutboxWriter internally that uses Lua) and inject
    _FakeWriter directly, which routes to FakeSignalOutboxPublisher.publish_envelope().
    """
    import handlers.emitter.unified_signal_emitter as ue

    EmitterCls = getattr(ue, "UnifiedSignalEmitter", None)
    assert EmitterCls is not None, "UnifiedSignalEmitter not found"

    logger = SimpleNamespace(
        debug=lambda *a, **k: None,
        info=lambda *a, **k: None,
        warning=lambda *a, **k: None,
        exception=lambda *a, **k: None,
    )

    em = EmitterCls.__new__(EmitterCls)
    em._logger = logger
    em._metrics = ue._NoopMetrics()
    em._analytics = SimpleNamespace(record_sem_dedup=lambda **k: None)
    em._retries = 0
    em._retry_sleep_ms = 0
    em._dedup_ttl_ms = 60000
    em._dedup_pending_ttl_ms = 10000
    em._hot_dedup = ue._DedupTTL(ttl_ms=60000, max_items=1000)
    em._sem_cfg = ue._SemDedupCfg(
        enabled=False, bucket_ms=0, ttl_ms=0, level_decimals=0, level_key_kinds=set()
    )
    em._sem_dedup = ue._DedupTTL(ttl_ms=0, max_items=1000)
    em._sem_counts = {}
    writer = _FakeWriter(fake_pub)
    em._writer = writer
    em._writer_labels = writer
    return em


def test_handlers_outbox_writer_unified_emitter_to_trade_monitor_position_trailing_flag():
    """
    End-to-end (behavior) path:
      UnifiedSignalEmitter.emit()
        -> _FakeWriter (no Lua) -> FakeSignalOutboxPublisher (dedup + XADD)
        -> OutboxEnvelope.to_stream_fields() -> payload_json in stream
        -> TradeMonitorRunner._parse_signal()
        -> TradeMonitorService._normalize_signal()
        -> create_position()
        -> _should_start_trailing_after_tp1()
    """
    r = FakeRedis()
    pub = FakeSignalOutboxPublisher(r)
    emitter = _mk_unified_emitter_with_outbox(pub)

    payload = {
        "signal_id": "sig-777",
        "sid": "sig-777",
        "kind": "touch",
        "symbol": "BTCUSDT",
        "side": "LONG",
        "direction": "LONG",
        "ts": 1700000000000,
        "ts_event_ms": 1700000000000,
        "strategy": "CryptoOrderFlow",
        "source": "CryptoOrderFlow",
        "timeframe": "1m",
        "entry": 100.0,        # _normalize_signal reads "entry" or "price", not "entry_price"
        "entry_price": 100.0,
        "entry_ts_ms": 1700000000000,
        "lot": 1.0,
        "sl": 95.0,
        "tp_levels": [101.0, 102.0, 103.0],
        "trail_profile": "rocket_v1",
        "trail_after_tp1": 0,
        "trail_after_tp1_reason": "LOW_MOMO",
    }

    res = emitter.emit(payload)
    assert res is True

    fields = r.last_stream_fields(RS.SIGNAL_OUTBOX)
    assert "payload_json" in fields

    raw = _parse_signal(fields)
    assert raw.get("trail_after_tp1") in (0, "0")
    assert raw.get("trail_after_tp1_reason") == "LOW_MOMO"

    mon = _mk_trade_monitor_like()
    sig = mon._normalize_signal(raw)
    assert sig is not None

    # Position must carry the policy flag:
    class _SpecStub:
        trailing_profile_default = "rocket_v1"

    pos = create_position(sig, _SpecStub())
    assert bool(getattr(pos, "trail_after_tp1", True)) is False
    assert str(getattr(pos, "trail_after_tp1_reason", "")) == "LOW_MOMO"

    # Policy must respect it:
    assert _should_start_trailing_after_tp1(pos) is False


def test_handlers_outbox_writer_dedup_bucketed():
    r = FakeRedis()
    pub = FakeSignalOutboxPublisher(r, dedup_bucket_ms=60000)
    emitter = _mk_unified_emitter_with_outbox(pub)

    payload = {
        "signal_id": "sig-dup2",
        "sid": "sig-dup2",
        "symbol": "BTCUSDT",
        "kind": "touch",
        "ts_event_ms": 1700000000000,
        "strategy": "CryptoOrderFlow",
        "source": "CryptoOrderFlow",
        "timeframe": "1m",
    }

    res1 = emitter.emit(payload)
    res2 = emitter.emit(payload)

    assert res1 is True
    assert res2 is False  # dedup hit (hot_dedup in emitter)
    assert len(r.streams.get(RS.SIGNAL_OUTBOX) or []) == 1
