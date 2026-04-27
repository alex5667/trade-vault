import os
import pytest

from handlers.emitter.unified_signal_emitter import UnifiedSignalEmitter


class FakePublisher:
    def __init__(self) -> None:
        self.published = []
    def publish(self, payload):
        self.published.append(payload.copy())
    def write(self, *, payload, signal_id, dedup):
        # Просто вызываем publish и возвращаем True
        self.publish(payload)
        return True


class FakeLogger:
    def exception(self, _msg: str):
        return


class FakeMetrics:
    def __init__(self) -> None:
        self.counters = []
        self.obs = []
    def inc(self, name: str, value: int = 1, tags=None) -> None:
        self.counters.append((name, int(value), dict(tags or {})))
    def gauge(self, _name: str, _value: float, tags=None) -> None:
        return
    def observe(self, name: str, value: float, tags=None) -> None:
        self.obs.append((name, float(value), dict(tags or {})))


@pytest.fixture()
def clean_env(monkeypatch):
    for k in [
        "OUTBOX_SEM_DEDUP",
        "EMIT_RETRIES",
        "EMIT_RETRY_SLEEP_MS",
        "EMIT_DEDUP_TTL_MS",
        "EMIT_DEDUP_MAX",
    ]:
        monkeypatch.delenv(k, raising=False)
    yield


def test_emitter_exports_signals_sent_and_quality_hists(monkeypatch, clean_env):
    # отключаем семантик-дедуп, чтобы не мешал
    monkeypatch.setenv("OUTBOX_SEM_DEDUP", "0")
    monkeypatch.setenv("EMIT_RETRIES", "0")
    monkeypatch.setenv("EMIT_RETRY_SLEEP_MS", "0")
    monkeypatch.setenv("EMIT_DEDUP_TTL_MS", "60000")
    monkeypatch.setenv("EMIT_DEDUP_MAX", "10000")

    outbox = FakePublisher()
    metrics = FakeMetrics()
    em = UnifiedSignalEmitter(outbox=outbox, logger=FakeLogger(), metrics=metrics)

    payload = {
        "symbol": "BTCUSDT",
        "kind": "breakout",
        "ts": 10000,
        "final_score": 1.25,
        "conf_factor": 0.62,
        "signal_id": "S1",
    }
    assert em.emit(payload, dedup=False) is True

    # conf_factor_hist{kind}, final_score_hist{kind}
    names = [o[0] for o in metrics.obs]
    assert "conf_factor_hist" in names
    assert "final_score_hist" in names
    assert all(o[2].get("kind") == "breakout" for o in metrics.obs)
