from __future__ import annotations

from typing import Any, Dict, List, Tuple, Optional, Iterable


class FakePipeline:
    def __init__(self, r: "FakeRedis"):
        self.r = r
        self.cmds: List[Tuple[str, Tuple[Any, ...], Dict[str, Any]]] = []

    def hgetall(self, key: str):
        self.cmds.append(("hgetall", (key,), {}))
        return self

    def hset(self, key: str, mapping: Dict[str, Any]):
        self.cmds.append(("hset", (key, mapping), {}))
        return self

    def xadd(self, stream: str, fields: Dict[str, str], maxlen: int = 0, approximate: bool = True):
        self.cmds.append(("xadd", (stream, fields), {"maxlen": maxlen, "approximate": approximate}))
        return self

    def rpush(self, key: str, *values):
        self.cmds.append(("rpush", (key,) + values, {}))
        return self

    def srem(self, key: str, *members):
        self.cmds.append(("srem", (key,) + members, {}))
        return self

    def set(self, key: str, value: str, ex: int = None, nx: bool = False):
        self.cmds.append(("set", (key, value), {"ex": ex, "nx": nx}))
        return self

    def delete(self, key: str):
        self.cmds.append(("delete", (key,), {}))
        return self

    def lpush(self, key: str, *values):
        self.cmds.append(("lpush", (key,) + values, {}))
        return self

    def ltrim(self, key: str, start: int, end: int):
        self.cmds.append(("ltrim", (key, start, end), {}))
        return self

    def hincrby(self, key: str, field: str, increment: int):
        self.cmds.append(("hincrby", (key, field, increment), {}))
        return self

    def expire(self, key: str, seconds: int):
        self.cmds.append(("expire", (key, seconds), {}))
        return self


    def execute(self):
        out = []
        for name, args, kwargs in self.cmds:
            method = getattr(self.r, name)
            if kwargs:
                out.append(method(*args, **kwargs))
            else:
                out.append(method(*args))
        self.cmds.clear()
        return out


class FakeRedis:
    """
    Минималистичный fake redis для unit-тестов без внешних зависимостей.
    Реализованы методы, которые нужны тестам и вашим helper'ам:
      - hashes: hset/hgetall
      - sets: sadd/srem/smembers
      - lists: rpush/lrange
      - zsets: zadd/zrevrange/zrange/zrangebyscore/zrevrangebyscore/zremrangebyrank
      - streams: xadd (storage only)
      - keys: get/set/delete
      - pipeline
    """
    def __init__(self):
        self._h: Dict[str, Dict[str, str]] = {}
        self._s: Dict[str, set] = {}
        self._l: Dict[str, List[str]] = {}
        self._z: Dict[str, Dict[str, float]] = {}
        self._streams: Dict[str, List[Dict[str, str]]] = {}
        self._k: Dict[str, str] = {}  # simple key-value storage

    @property
    def streams(self):
        """Compatibility property for accessing _streams"""
        return self._streams

    def pipeline(self, transaction=None):
        return FakePipeline(self)

    # hashes
    def hset(self, key: str, mapping: Dict[str, Any]):
        m = self._h.setdefault(key, {})
        for k, v in (mapping or {}).items():
            if v is None:
                continue
            m[str(k)] = str(v)
        return 1

    def hgetall(self, key: str) -> Dict[str, str]:
        return dict(self._h.get(key, {}))

    def hincrby(self, key: str, field: str, increment: int) -> int:
        """Increment hash field by integer value"""
        m = self._h.setdefault(key, {})
        current = int(float(m.get(field, "0")))
        new_val = current + int(increment)
        m[field] = str(new_val)
        return new_val

    def hincrbyfloat(self, key: str, field: str, increment: float) -> float:
        """Increment hash field by float value"""
        m = self._h.setdefault(key, {})
        current = float(m.get(field, "0.0"))
        new_val = current + float(increment)
        m[field] = str(new_val)
        return new_val

    def expire(self, key: str, seconds: int) -> int:
        """Set TTL on key (no-op in FakeRedis, just return success)"""
        return 1


    # sets
    def sadd(self, key: str, *members):
        s = self._s.setdefault(key, set())
        for m in members:
            s.add(str(m))
        return 1

    def srem(self, key: str, *members):
        s = self._s.setdefault(key, set())
        for m in members:
            s.discard(str(m))
        return 1

    def smembers(self, key: str):
        return set(self._s.get(key, set()))

    # lists
    def lpush(self, key: str, *values):
        """Push values to the left (beginning) of the list"""
        l = self._l.setdefault(key, [])
        for v in reversed(values):  # reversed to maintain order
            l.insert(0, str(v))
        return len(l)

    def rpush(self, key: str, *values):
        l = self._l.setdefault(key, [])
        for v in values:
            l.append(str(v))
        return len(l)

    def ltrim(self, key: str, start: int, end: int):
        """Trim list to specified range"""
        l = self._l.get(key, [])
        if not l:
            return "OK"
        # Redis ltrim is inclusive on both ends
        if end == -1:
            self._l[key] = l[start:]
        else:
            self._l[key] = l[start : end + 1]
        return "OK"

    def lrange(self, key: str, start: int, end: int):
        l = self._l.get(key, [])
        # emulate redis lrange inclusive end
        if end == -1:
            return l[start:]
        return l[start : end + 1]

    # keys (simple key-value)
    def get(self, key: str) -> Optional[str]:
        return self._k.get(key)

    def set(self, key: str, value: str, ex: int = None, nx: bool = False) -> Optional[str]:
        if nx and key in self._k:
            return None  # key exists, nx=True means don't set
        self._k[key] = str(value)
        return "OK"

    def delete(self, key: str) -> int:
        if key in self._k:
            del self._k[key]
            return 1
        return 0

    # streams (minimal)
    def xadd(self, stream: str, fields: Dict[str, str], maxlen: int = 0, approximate: bool = True):
        self._streams.setdefault(stream, []).append({str(k): str(v) for k, v in (fields or {}).items()})
        # return fake id
        return f"{len(self._streams[stream])}-0"

    # zsets
    def zadd(self, key: str, mapping: Dict[str, float]):
        z = self._z.setdefault(key, {})
        for member, score in (mapping or {}).items():
            z[str(member)] = float(score)
        return 1

    def _z_sorted(self, key: str, reverse: bool) -> List[Tuple[str, float]]:
        z = self._z.get(key, {})
        return sorted(z.items(), key=lambda kv: (kv[1], kv[0]), reverse=reverse)

    def zrevrange(self, key: str, start: int, end: int):
        items = self._z_sorted(key, reverse=True)
        if end < 0:
            end = len(items) - 1
        return [m for (m, _) in items[start : end + 1]]

    def zrange(self, key: str, start: int, end: int):
        items = self._z_sorted(key, reverse=False)
        if end < 0:
            end = len(items) - 1
        return [m for (m, _) in items[start : end + 1]]

    def zrangebyscore(self, key: str, min_score: float, max_score: float, start: int = 0, num: int = 1000000):
        items = [(m, s) for (m, s) in self._z_sorted(key, reverse=False) if s >= float(min_score) and s <= float(max_score)]
        return [m for (m, _) in items[start : start + num]]

    def zrevrangebyscore(self, key: str, max_score: float, min_score: float, start: int = 0, num: int = 1000000):
        items = [(m, s) for (m, s) in self._z_sorted(key, reverse=True) if s <= float(max_score) and s >= float(min_score)]
        return [m for (m, _) in items[start : start + num]]

    def zremrangebyrank(self, key: str, start: int, end: int):
        items = self._z_sorted(key, reverse=False)
        if not items:
            return 0
        if end < 0:
            end = len(items) - 1
        to_remove = items[start : end + 1]
        z = self._z.get(key, {})
        for m, _ in to_remove:
            z.pop(m, None)
        return len(to_remove)
