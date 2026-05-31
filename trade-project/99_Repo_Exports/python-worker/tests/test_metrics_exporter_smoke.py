
import redis

from services.observability.metrics_exporter import collect_metrics


def test_collect_metrics_smoke(monkeypatch):
    # Requires local redis in CI? Use skip if not available.
    # This is a smoke test only; in your CI replace with fakeredis.
    monkeypatch.setenv("METRICS_MAX_SYMBOLS", "2")
    r = redis.Redis(host="localhost", port=6379, db=15, decode_responses=False)
    try:
        r.flushdb()
    except Exception:
        return
    r.sadd("events:microbar_closed:symbols", "BTCUSDT", "ETHUSDT")
    r.set("cfg:atr_tf:BTCUSDT", "1m")
    r.set("cfg:atr_bad:BTCUSDT", "1")
    out = collect_metrics(r)
    assert "microbar_symbols_active" in out
    assert "atr_bad_active" in out

