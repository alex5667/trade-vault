from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Tuple

from news_pipeline.feature_store_service import NewsFeatureStoreService
from news_pipeline import config


class _Pipe:
    def __init__(self, r: "_RedisStub") -> None:
        self._r = r
        self._cmds: List[Tuple[str, Tuple[Any, ...], Dict[str, Any]]] = []

    def hgetall(self, key: str):
        self._cmds.append(("hgetall", (key,), {}))
        return self

    def hset(self, key: str, mapping: Dict[str, Any]):
        self._cmds.append(("hset", (key,), {"mapping": mapping}))
        return self

    def expire(self, key: str, ttl: int):
        self._cmds.append(("expire", (key, ttl), {}))
        return self

    def execute(self):
        out = []
        for name, args, kwargs in self._cmds:
            if name == "hgetall":
                key = args[0]
                out.append(dict(self._r._hash.get(key, {})))
            elif name == "hset":
                key = args[0]
                mp = kwargs["mapping"]
                cur = self._r._hash.setdefault(key, {})
                for k, v in mp.items():
                    cur[str(k)] = str(v)
                out.append(True)
            elif name == "expire":
                out.append(True)
        self._cmds.clear()
        return out


class _RedisStub:
    def __init__(self) -> None:
        self._hash: Dict[str, Dict[str, str]] = {}
        self._xadds: List[Tuple[str, Dict[str, str]]] = []

    def pipeline(self):
        return _Pipe(self)

    def xadd(self, stream: str, mapping: Dict[str, Any], maxlen: int = 0, approximate: bool = True):
        m: Dict[str, str] = {str(k): str(v) for k, v in mapping.items()}
        self._xadds.append((str(stream), m))
        return "1-0"


@dataclass
class _A:
    # minimal compatible with NewsAnalysisCompact usage in process_compact()
    uid: str
    news_ref: str
    risk: float
    surprise: float
    confidence: float
    tags_mask: int
    primary_tag_id: int
    symbols: List[str]


def test_feature_store_writes_grade_horizon_and_aliases():
    r = _RedisStub()
    svc = NewsFeatureStoreService(r)  # type: ignore[arg-type]

    a = _A(
        uid="u1",
        news_ref="ref1",
        risk=0.9,
        surprise=+0.3,
        confidence=1.0,
        tags_mask=0,
        primary_tag_id=1,
        symbols=["BTCUSDT"],
    )

    svc.process_compact(a, now=1700000000000)  # type: ignore[arg-type]

    g = r._hash["news:agg:global"]
    s = r._hash["news:agg:BTCUSDT"]

    for h in (g, s):
        assert "news_grade_id" in h
        assert "horizon_sec" in h
        assert "risk_ewma" in h and "risk_ema" in h
        assert "surprise_ewma" in h and "surprise_ema" in h
        assert h["ts_ms"] == "1700000000000"
        assert h["asof_ts_ms"] == "1700000000000"
        assert "grade_change_ts_ms" in h
        assert "grade_frozen" in h


def test_grade_cooldown_freezes_upgrade(monkeypatch):
    # Force long cooldown for up changes
    monkeypatch.setenv("NEWS_GRADE_COOLDOWN_UP_SEC", "600")
    monkeypatch.setenv("NEWS_GRADE_COOLDOWN_DOWN_SEC", "180")

    r = _RedisStub()
    svc = NewsFeatureStoreService(r)  # type: ignore[arg-type]

    # Seed previous state with grade=1 changed very recently
    r._hash["news:agg:global"] = {
        "news_grade_id": "1",
        "grade_change_ts_ms": "1700000000000",
        "ts_ms": "1700000000000",
        "risk_ewma": "0.000000",
        "surprise_ewma": "0.000000",
    }

    a = _A(
        uid="u2",
        news_ref="ref2",
        risk=1.0,          # would push grade up
        surprise=+1.0,
        confidence=1.0,
        tags_mask=0,
        primary_tag_id=1,
        symbols=[],
    )

    # only +1s passed => must freeze at grade=1 (since cooldown is 600s)
    svc.process_compact(a, now=int(1700000000000 + 1 * 1000))  # type: ignore[arg-type]
    g = r._hash["news:agg:global"]
    assert int(g["news_grade_id"]) == 1
    assert g["grade_frozen"] == "1"

