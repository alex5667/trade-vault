from __future__ import annotations

from typing import Any


class FakePipeline:
    def __init__(self, r: FakeRedis):
        self.r = r
        self.ops: list[tuple[str, tuple[Any, ...]]] = []

    def hgetall(self, key: str) -> FakePipeline:
        self.ops.append(('hgetall', (key,)))
        return self

    def execute(self) -> list[Any]:
        out: list[Any] = []
        for op, args in self.ops:
            if op == 'hgetall':
                (key,) = args
                self.r._calls.append(('hgetall', key))
                out.append(dict(self.r.hash_store.get(key, {})))
            else:
                raise RuntimeError(f'unknown op {op}')
        self.ops.clear()
        return out


class FakeRedis:
    def __init__(self):
        self.hash_store: dict[str, dict[str, Any]] = {}
        self.str_store: dict[str, str] = {}
        self._calls: list[tuple[str, str]] = []

    def get(self, key: str):
        self._calls.append(('get', key))
        return self.str_store.get(key)

    def hgetall(self, key: str):
        self._calls.append(('hgetall', key))
        return dict(self.hash_store.get(key, {}))

    def pipeline(self, transaction: bool = False) -> FakePipeline:
        return FakePipeline(self)
