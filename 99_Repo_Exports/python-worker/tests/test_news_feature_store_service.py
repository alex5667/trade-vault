from types import SimpleNamespace

from news_pipeline.feature_store_service import NewsFeatureStoreService


class FakePipeline:
    def __init__(self, r):
        self.r = r
        self.ops = []

    def hgetall(self, key):
        self.ops.append(("hgetall", key, None))
        return self

    def hset(self, key, mapping=None, **kwargs):
        self.ops.append(("hset", key, dict(mapping or {})))
        return self

    def expire(self, key, ttl):
        self.ops.append(("expire", key, int(ttl)))
        return self

    def execute(self):
        out = []
        for op, key, val in self.ops:
            if op == "hgetall":
                out.append(dict(self.r.hashes.get(key, {})))
            elif op == "hset":
                cur = self.r.hashes.get(key, {})
                cur.update({k: str(v) for k, v in (val or {}).items()})
                self.r.hashes[key] = cur
                out.append(True)
            elif op == "expire":
                self.r.ttl[key] = int(val)
                out.append(True)
        self.ops = []
        return out


class FakeRedis:
    def __init__(self):
        self.hashes = {}
        self.ttl = {}
        self.stream = []
        self._counters = {}

    def pipeline(self):
        return FakePipeline(self)

    def xadd(self, stream, fields, maxlen=None, approximate=None):
        self.stream.append((stream, dict(fields)))
        return "1-0"

    def incr(self, key):
        self._counters[key] = int(self._counters.get(key, 0)) + 1
        return self._counters[key]

    def expire(self, key, ttl):
        return True


def test_process_compact_computes_grade_and_horizon(monkeypatch):
    r = FakeRedis()
    svc = NewsFeatureStoreService(r)

    import news_pipeline.feature_store_service as mod
    monkeypatch.setattr(mod, "compute_grade_id", lambda **kwargs: 3)
    monkeypatch.setattr(mod, "compute_horizon_sec", lambda **kwargs: 3600)
    monkeypatch.setattr(mod, "compute_horizon_sec_with_grade", lambda **kwargs: 7200)

    a = SimpleNamespace(
        uid="u1",
        news_ref="n1",
        symbols=["BTCUSDT"],
        risk=0.8,
        surprise=-0.2,
        confidence=0.9,
        tags_mask=123,
        primary_tag_id=7,
    )

    svc.process_compact(a, now=1_700_000_000_000)
    g = r.hashes["news:agg:global"]
    s = r.hashes["news:agg:BTCUSDT"]
    assert g["news_grade_id"] == "3"
    assert g["horizon_sec"] == "7200"
    assert s["news_grade_id"] == "3"
    assert s["horizon_sec"] == "7200"


def test_grade_cooldown_freezes_changes(monkeypatch):
    r = FakeRedis()
    svc = NewsFeatureStoreService(r)

    import news_pipeline.feature_store_service as mod
    monkeypatch.setattr(mod, "compute_grade_id", lambda **kwargs: 1)  # wants downgrade
    monkeypatch.setattr(mod, "compute_horizon_sec", lambda **kwargs: 3600)
    monkeypatch.setattr(mod, "compute_horizon_sec_with_grade", lambda **kwargs: 3600)

    r.hashes["news:agg:global"] = {
        "news_grade_id": "4",
        "grade_change_ts_ms": "1700000000000",
        "ts_ms": "1700000000000",
        "risk_ewma": "0.9",
        "surprise_ewma": "-0.9",
    }

    a = SimpleNamespace(
        uid="u1",
        news_ref="n1",
        symbols=[],
        risk=0.1,
        surprise=0.1,
        confidence=0.9,
        tags_mask=0,
        primary_tag_id=0,
    )

    svc.process_compact(a, now=1_700_000_000_100)  # +100ms
    g = r.hashes["news:agg:global"]
    assert g["news_grade_id"] == "4"
    assert g["grade_frozen"] == "1"

