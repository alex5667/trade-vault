from dataclasses import dataclass
from typing import Any

import news_pipeline.feature_store_service as fs
from news_pipeline.feature_store_service import NewsFeatureStoreService


class FakeMetrics:
    def __init__(self) -> None:
        self.inc_calls: list[tuple[str, int, dict[str, Any] | None]] = []
        self.obs_calls: list[tuple[str, float, dict[str, Any] | None]] = []

    def inc(self, name: str, value: int = 1, tags: dict[str, Any] | None = None) -> None:
        self.inc_calls.append((str(name), int(value), tags))

    def observe(self, name: str, value: float, tags: dict[str, Any] | None = None) -> None:
        self.obs_calls.append((str(name), float(value), tags))


class FakePipe:
    def __init__(self, r: "FakeRedis") -> None:
        self.r = r
        self.ops: list[tuple[str, tuple]] = []

    def hgetall(self, key: str):
        self.ops.append(("hgetall", (str(key),)))
        return self

    def hset(self, key: str, mapping: dict[str, Any]):
        self.ops.append(("hset", (str(key), dict(mapping))))
        return self

    def expire(self, key: str, ttl: int):
        self.ops.append(("expire", (str(key), int(ttl))))
        return self

    def execute(self):
        out = []
        for op, args in self.ops:
            if op == "hgetall":
                (key,) = args
                out.append(dict(self.r.hashes.get(key, {})))
            elif op == "hset":
                key, mapping = args
                cur = self.r.hashes.setdefault(key, {})
                for k, v in mapping.items():
                    cur[str(k)] = str(v)
                out.append(1)
            elif op == "expire":
                key, ttl = args
                self.r.ttl[key] = ttl
                out.append(1)
        self.ops.clear()
        return out


class FakeRedis:
    def __init__(self) -> None:
        self.hashes: dict[str, dict[str, str]] = {}
        self.ttl: dict[str, int] = {}
        self.streams: dict[str, list[dict[str, str]]] = {}

    def pipeline(self):
        return FakePipe(self)

    def xadd(self, stream: str, fields: dict[str, Any], maxlen: int = 0, approximate: bool = True):
        st = self.streams.setdefault(str(stream), [])
        st.append({str(k): str(v) for k, v in (fields or {}).items()})
        return f"{len(st)}-0"


class FakePipeFailOnce(FakePipe):
    def __init__(self, r: "FakeRedisFailOnce") -> None:
        super().__init__(r)

    def execute(self):
        self.r.execute_count += 1
        # fail on second execute (write operation)
        if self.r.fail_next_execute and self.r.execute_count == 2:
            self.r.fail_next_execute = False
            raise self.r.fail_exc
        # call parent execute to get the list of results
        return super().execute()


class FakeRedisFailOnce(FakeRedis):
    def __init__(self, fail_exc: Exception, fail_on_write: bool = True) -> None:
        super().__init__()
        self.fail_exc = fail_exc
        self.fail_next_execute = fail_on_write
        self.execute_count = 0

    def pipeline(self):
        return FakePipeFailOnce(self)


@dataclass
class StubAnalysis:
    uid: str
    news_ref: str = "ref"
    symbols: list[str] = None  # type: ignore
    risk: float = 0.0
    surprise: float = 0.0
    confidence: float = 1.0
    tags_mask: int = 0
    primary_tag_id: int = 0


def test_cooldown_keeps_horizon_consistent_with_effective_grade(monkeypatch):
    # Make deterministic grade/horizon mapping:
    # grade candidate always 4, but cooldown blocks change => effective stays prev=1,
    # so horizon must be computed with grade=1 (not 4).
    monkeypatch.setattr(fs, "compute_grade_id", lambda **kw: 4)
    monkeypatch.setattr(fs, "compute_horizon_sec", lambda **kw: 100)
    monkeypatch.setattr(fs, "compute_horizon_sec_with_grade", lambda base_horizon_sec, grade_id: 1000 + int(grade_id))

    # Freeze time close to prev change to trigger cooldown block
    monkeypatch.setattr(fs, "now_ms", lambda: 1_000_000)

    r = FakeRedis()
    m = FakeMetrics()
    svc = NewsFeatureStoreService(r=r, metrics=m, consumer="t", block_ms=1, batch=1)

    # seed previous state for global key
    gk = "news:agg:global"
    r.hashes[gk] = {
        "news_grade_id": "1",
        "grade_change_ts_ms": "999500",  # only 500ms ago -> should block upgrade (cooldown_up_sec default 900s)
        "ts_ms": "999500",
        "risk_ewma": "0.1",
        "surprise_ewma": "0.1",
    }

    a = StubAnalysis(uid="u1", symbols=[], risk=1.0, surprise=1.0, confidence=1.0, tags_mask=0, primary_tag_id=1)
    svc.process_compact(a)  # uses now_ms patched

    # effective grade should remain 1
    assert r.hashes[gk]["news_grade_id"] == "1"
    # horizon must correspond to grade=1 (1000+1), not grade=4 (1000+4)
    assert r.hashes[gk]["horizon_sec"] == "1001"


def test_transient_retry(monkeypatch):
    import redis as redis_pkg

    monkeypatch.setattr(fs, "compute_grade_id", lambda **kw: 2)
    monkeypatch.setattr(fs, "compute_horizon_sec", lambda **kw: 100)
    monkeypatch.setattr(fs, "compute_horizon_sec_with_grade", lambda base_horizon_sec, grade_id: 200)
    monkeypatch.setattr(fs, "now_ms", lambda: 123456)

    r = FakeRedisFailOnce(redis_pkg.exceptions.TimeoutError("timeout"))
    m = FakeMetrics()
    svc = NewsFeatureStoreService(r=r, metrics=m, consumer="t", block_ms=1, batch=1)

    a = StubAnalysis(uid="u2", symbols=["BTCUSDT"], risk=0.8, surprise=-0.6, confidence=0.9, tags_mask=0, primary_tag_id=1)
    svc.process_compact(a)

    # should succeed after retry and write hashes
    assert "news:agg:global" in r.hashes
    assert "news:agg:BTCUSDT" in r.hashes
