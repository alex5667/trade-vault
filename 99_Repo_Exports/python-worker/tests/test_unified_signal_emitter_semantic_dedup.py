import types

import pytest

from handlers.emitter.unified_signal_emitter import UnifiedSignalEmitter


class FakeOutbox:
    def __init__(self) -> None:
        self.published = []

    def publish(self, payload):
        self.published.append(payload.copy())


class FakeLogger:
    def __init__(self) -> None:
        self.exceptions = []

    def exception(self, msg: str):
        self.exceptions.append(msg)


class FakeMetrics:
    def __init__(self) -> None:
        self.counters = []  # (name, value, tags)
        self.gauges = []    # (name, value, tags)

    def inc(self, name: str, value: int = 1, tags=None) -> None:
        self.counters.append((name, int(value), dict(tags or {})))

    def gauge(self, name: str, value: float, tags=None) -> None:
        self.gauges.append((name, float(value), dict(tags or {})))


@pytest.fixture()
def clean_env(monkeypatch):
    # Ensure tests don't leak env between each other
    keys = [
        "OUTBOX_SEM_DEDUP",
        "OUTBOX_SEM_DEDUP_BUCKET_MS",
        "OUTBOX_SEM_DEDUP_LEVEL_DECIMALS",
        "OUTBOX_SEM_DEDUP_TTL_MS",
        "OUTBOX_SEM_DEDUP_LEVEL_KEY_KINDS",
        "OUTBOX_SEM_DEDUP_MAX",
        "EMIT_DEDUP_TTL_MS",
        "EMIT_DEDUP_MAX",
        "EMIT_RETRIES",
        "EMIT_RETRY_SLEEP_MS",
    ]
    for k in keys:
        monkeypatch.delenv(k, raising=False)
    yield


def _mk_emitter(monkeypatch, *, enabled=True, level_key_kinds=""):
    monkeypatch.setenv("OUTBOX_SEM_DEDUP", "1" if enabled else "0")
    monkeypatch.setenv("OUTBOX_SEM_DEDUP_BUCKET_MS", "1000")
    monkeypatch.setenv("OUTBOX_SEM_DEDUP_LEVEL_DECIMALS", "2")
    monkeypatch.setenv("OUTBOX_SEM_DEDUP_TTL_MS", "15000")
    if level_key_kinds is not None:
        monkeypatch.setenv("OUTBOX_SEM_DEDUP_LEVEL_KEY_KINDS", level_key_kinds)
    monkeypatch.setenv("EMIT_RETRIES", "0")
    monkeypatch.setenv("EMIT_RETRY_SLEEP_MS", "0")
    monkeypatch.setenv("EMIT_DEDUP_TTL_MS", "60000")
    monkeypatch.setenv("EMIT_DEDUP_MAX", "10000")

    outbox = FakeOutbox()
    logger = FakeLogger()
    metrics = FakeMetrics()
    em = UnifiedSignalEmitter(outbox=outbox, logger=logger, metrics=metrics)
    return em, outbox, logger, metrics


def test_sem_dedup_blocks_same_semantic_event(monkeypatch, clean_env):
    em, outbox, _logger, metrics = _mk_emitter(monkeypatch, enabled=True, level_key_kinds="")

    payload = {
        "symbol": "BTCUSDT",
        "kind": "breakout",
        "ts": 10_000,  # ms
        "side": "buy",
        "venue": "binance",
        "timeframe": "1m",
        "level_price": 100.12349,
        "signal_id": "S1",
    }

    assert em.emit(payload) is True
    assert em.emit(payload) is False  # blocked by semantic key within same bucket

    assert len(outbox.published) == 1

    # metrics: 1 hit, 1 write
    hits = [c for c in metrics.counters if c[0] == "sem_dedup_hits_total"]
    writes = [c for c in metrics.counters if c[0] == "sem_dedup_writes_total"]
    assert sum(v for _n, v, _t in hits) == 1
    assert sum(v for _n, v, _t in writes) == 1


def test_sem_dedup_splits_by_venue_and_timeframe(monkeypatch, clean_env):
    em, outbox, _logger, _metrics = _mk_emitter(monkeypatch, enabled=True, level_key_kinds="")

    base = {
        "symbol": "BTCUSDT",
        "kind": "breakout",
        "ts": 10_500,  # same bucket 10
        "side": "buy",
        "level_price": 100.12,
    }

    p1 = dict(base, venue="binance", timeframe="1m", signal_id="S1")
    p2 = dict(base, venue="okx", timeframe="1m", signal_id="S2")     # different venue => should NOT be blocked
    p3 = dict(base, venue="binance", timeframe="5m", signal_id="S3") # different tf => should NOT be blocked

    assert em.emit(p1) is True
    assert em.emit(p2) is True
    assert em.emit(p3) is True
    assert len(outbox.published) == 3


def test_level_key_is_used_only_for_selected_kinds(monkeypatch, clean_env):
    # level_key included for breakout/absorption only
    em, outbox, _logger, _metrics = _mk_emitter(monkeypatch, enabled=True, level_key_kinds="breakout,absorption")

    # breakout: different level_key => different semantic key => NOT blocked
    b1 = {
        "symbol": "ETHUSDT",
        "kind": "breakout",
        "ts": 21_000,
        "side": "sell",
        "venue": "binance",
        "timeframe": "1m",
        "level_price": 2000.00,
        "level_key": "pdh",
        "signal_id": "B1",
    }
    b2 = dict(b1, level_key="pdl", signal_id="B2")
    assert em.emit(b1) is True
    assert em.emit(b2) is True

    # extreme: level_key ignored => same semantic key => second is blocked
    e1 = {
        "symbol": "ETHUSDT",
        "kind": "extreme",
        "ts": 22_000,
        "side": "sell",
        "venue": "binance",
        "timeframe": "1m",
        "level_price": 2000.00,
        "level_key": "cluster_1",
        "signal_id": "E1",
    }
    e2 = dict(e1, level_key="cluster_2", signal_id="E2")
    assert em.emit(e1) is True
    assert em.emit(e2) is False

    # total publishes: 3 (b1,b2,e1)
    assert len(outbox.published) == 3


def test_sem_dedup_ttl_expiry_allows_reemit(monkeypatch, clean_env):
    # Create emitter with short TTL
    monkeypatch.setenv("OUTBOX_SEM_DEDUP_TTL_MS", "10")
    em, outbox, _logger, _metrics = _mk_emitter(monkeypatch, enabled=True, level_key_kinds="")

    # Override TTL directly for testing (bypass env loading)
    em._sem_dedup.ttl_ms = 10

    # Control time inside emitter (avoid sleeping in tests)
    t = {"now_ms": 1_000_000}
    em._now_ms = types.MethodType(lambda self: t["now_ms"], em)  # noqa: SLF001

    payload = {
        "symbol": "BTCUSDT",
        "kind": "breakout",
        "ts": 10_000,
        "side": "buy",
        "venue": "binance",
        "timeframe": "1m",
        "level_price": 100.12,
        "signal_id": "S1",
    }

    assert em.emit(payload) is True
    assert em.emit(payload, dedup=False) is False  # blocked by semantic dedup (exact dedup bypassed)

    t["now_ms"] += 20  # > TTL
    assert em.emit(payload, dedup=False) is True   # allowed again after TTL expiry

    assert len(outbox.published) == 2
