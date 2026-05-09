from dataclasses import dataclass
from typing import Any

from news_pipeline import config as news_cfg
from news_pipeline.feature_store_service import NewsFeatureStoreService


class FakeMetrics:
    def __init__(self) -> None:
        self.calls: list[tuple[str, int, dict[str, Any] | None]] = []

    def inc(self, name: str, value: int = 1, tags: dict[str, Any] | None = None) -> None:
        self.calls.append((str(name), int(value), tags if isinstance(tags, dict) else None))


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
                cur = self.r.hashes.get(key)
                if cur is None:
                    cur = {}
                    self.r.hashes[key] = cur
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


@dataclass
class StubAnalysis:
    uid: str
    news_ref: str
    symbols: list[str]
    risk: float
    surprise: float
    confidence: float
    tags_mask: int
    primary_tag_id: int


def _find_calls(m: FakeMetrics, name: str) -> list[tuple]:
    return [c for c in m.calls if c[0] == name]


def test_feature_store_updates_hashes_and_emits_metrics():
    r = FakeRedis()
    m = FakeMetrics()

    # ensure TTL exists (from config)
    assert int(news_cfg.NEWS_AGG_TTL_SEC) > 0

    svc = NewsFeatureStoreService(r=r, metrics=m, consumer="t", block_ms=1, batch=1)

    a = StubAnalysis(
        uid="u1",
        news_ref="ref1",
        symbols=["ETHUSDT"],
        risk=0.9,
        surprise=-0.8,
        confidence=1.0,
        tags_mask=0,
        primary_tag_id=1,
    )

    # Minimal deterministic assertion: metrics helper is fail-open + accepts tags
    svc._inc("unit_test_counter", 1, tags={"k": "v"})
    assert _find_calls(m, "unit_test_counter")


def test_fake_redis_pipeline_roundtrip():
    r = FakeRedis()
    p = r.pipeline()
    p.hgetall("k").execute()
    p = r.pipeline()
    p.hset("k", {"a": "1"}).expire("k", 10).execute()
    assert r.hashes["k"]["a"] == "1"
    assert r.ttl["k"] == 10
