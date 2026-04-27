# python-worker/tests/fake_redis.py
from __future__ import annotations

from typing import Any, Dict, List, Tuple


class FakePipeline:
    def __init__(self, r: "FakeRedis") -> None:
        self.r = r
        self.ops: List[Tuple[str, tuple, dict]] = []

    def hgetall(self, key: str):
        self.ops.append(("hgetall", (key,), {}))
        return self

    def hset(self, key: str, mapping: Dict[str, Any]):
        self.ops.append(("hset", (key,), {"mapping": dict(mapping)}))
        return self

    def expire(self, key: str, ttl: int):
        self.ops.append(("expire", (key, ttl), {}))
        return self

    def set(self, key: str, val: str, ex: int):
        self.ops.append(("set", (key, val, ex), {}))
        return self

    def execute(self):
        out = []
        for op, args, kwargs in self.ops:
            if op == "hgetall":
                out.append(dict(self.r.hashes.get(args[0], {})))
            elif op == "hset":
                self.r.hashes.setdefault(args[0], {}).update(kwargs["mapping"])
                out.append(True)
            elif op == "expire":
                self.r.expires[args[0]] = args[1]
                out.append(True)
            elif op == "set":
                self.r.strings[args[0]] = args[1]
                self.r.expires[args[0]] = args[2]
                out.append(True)
            else:
                out.append(None)
        self.ops = []
        return out


class FakeRedis:
    def __init__(self) -> None:
        self.hashes: Dict[str, Dict[str, Any]] = {}
        self.strings: Dict[str, str] = {}
        self.expires: Dict[str, int] = {}
        self.eval_calls: List[tuple] = []
        self.setnx: Dict[str, str] = {}

    def pipeline(self, transaction: bool = False) -> FakePipeline:
        return FakePipeline(self)

    def hgetall(self, key: str) -> Dict[str, Any]:
        return dict(self.hashes.get(key, {}))

    def hset(self, key: str, mapping: Dict[str, Any]):
        self.hashes.setdefault(key, {}).update(dict(mapping))
        return True

    def expire(self, key: str, ttl: int):
        self.expires[key] = ttl
        return True

    def set(self, key: str, value: str, nx: bool = False, px: int | None = None, ex: int | None = None):
        if nx:
            if key in self.setnx:
                return False
            self.setnx[key] = value
        else:
            self.setnx[key] = value
        if ex is not None:
            self.expires[key] = ex
        if px is not None:
            # store px as ms for inspection
            self.expires[key] = int(px / 1000)
        return True

    def get(self, key: str):
        return self.setnx.get(key)

    def eval(self, script: str, numkeys: int, *args):
        self.eval_calls.append((script, numkeys, args))
        # emulate renew/release semantics
        key = args[0]
        val = args[1] if len(args) > 1 else ""
        if "PEXPIRE" in script:
            return 1 if self.get(key) == val else 0
        if "DEL" in script:
            if self.get(key) == val:
                self.setnx.pop(key, None)
                return 1
            return 0
        return 0

    def setex(self, key: str, ttl: int, value: str):
        self.setnx[key] = value
        self.expires[key] = ttl
        return True
