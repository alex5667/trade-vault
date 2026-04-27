from __future__ import annotations

import itertools
from typing import Any, Dict, List, Optional, Tuple


class FakePipeline:
    def __init__(self, redis: "FakeRedis"):
        self._r = redis
        self._ops: List[Tuple[str, Tuple[Any, ...], Dict[str, Any]]] = []

    def hgetall(self, key: str):
        self._ops.append(("hgetall", (key,), {}))
        return self

    def execute(self):
        out = []
        for op, args, kwargs in self._ops:
            if op == "hgetall":
                out.append(self._r.hgetall(*args, **kwargs))
            else:
                out.append(None)
        self._ops.clear()
        return out


class FakeRedis:
    def __init__(self):
        self._hash: Dict[str, Dict[str, str]] = {}
        self._sets: Dict[str, set] = {}
        self._lists: Dict[str, List[str]] = {}
        self._streams: Dict[str, List[Tuple[str, Dict[str, str]]]] = {}
        self._zsets: Dict[str, Dict[str, float]] = {}
        self._id = itertools.count(1)

    def pipeline(self):
        return FakePipeline(self)

    def hset(self, key: str, mapping: Optional[Dict[str, Any]] = None, **kwargs):
        m = dict(mapping or {})
        cur = self._hash.setdefault(key, {})
        for k, v in m.items():
            if v is None:
                continue
            cur[str(k)] = str(v)
        return 1

    def hgetall(self, key: str):
        return dict(self._hash.get(key, {}))

    def sadd(self, key: str, *members):
        s = self._sets.setdefault(key, set())
        for m in members:
            s.add(str(m))
        return 1

    def srem(self, key: str, *members):
        s = self._sets.setdefault(key, set())
        for m in members:
            s.discard(str(m))
        return 1

    def smembers(self, key: str):
        return set(self._sets.get(key, set()))

    def rpush(self, key: str, *vals):
        lst = self._lists.setdefault(key, [])
        for v in vals:
            lst.append(str(v))
        return len(lst)

    def lrange(self, key: str, start: int, end: int):
        lst = self._lists.get(key, []) or []
        if end == -1:
            return lst[start:]
        return lst[start : end + 1]

    def xadd(self, stream: str, fields: Dict[str, str], maxlen: int = 0, approximate: bool = True):
        sid = f"{next(self._id)}-0"
        self._streams.setdefault(stream, []).append((sid, dict(fields)))
        return sid

    def xrevrange(self, stream: str, max: str = "+", min: str = "-", count: int = 10):
        xs = self._streams.get(stream, []) or []
        return list(reversed(xs))[:count]

    def zadd(self, key: str, mapping: Dict[str, float]):
        z = self._zsets.setdefault(key, {})
        for member, score in mapping.items():
            z[str(member)] = float(score)
        return 1

    def zrevrange(self, key: str, start: int, end: int):
        z = self._zsets.get(key, {}) or {}
        items = sorted(z.items(), key=lambda kv: kv[1], reverse=True)
        return [m for (m, _s) in items[start : end + 1]]

    def zrevrangebyscore(self, key: str, max_score: float, min_score: float, start: int = 0, num: int = 100):
        z = self._zsets.get(key, {}) or {}
        items = [(m, s) for (m, s) in z.items() if float(min_score) <= s <= float(max_score)]
        items.sort(key=lambda kv: kv[1], reverse=True)
        items = items[start : start + num]
        return [m for (m, _s) in items]

    def zrangebyscore(self, key: str, min_score: float, max_score: float, start: int = 0, num: int = 100):
        z = self._zsets.get(key, {}) or {}
        items = [(m, s) for (m, s) in z.items() if float(min_score) <= s <= float(max_score)]
        items.sort(key=lambda kv: kv[1])
        items = items[start : start + num]
        return [m for (m, _s) in items]
