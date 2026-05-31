from __future__ import annotations

from typing import Any, Dict, List, Tuple


class FakePipeline:
    def __init__(self, r: 'FakeRedis'):
        self.r = r
        self.ops: List[Tuple[str, Tuple[Any, ...]]] = []

    def hgetall(self, key: str) -> 'FakePipeline':
        self.ops.append(('hgetall', (key,)))
        return self

    def execute(self) -> List[Any]:
        out: List[Any] = []
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
        self.hash_store: Dict[str, Dict[str, Any]] = {}
        self.str_store: Dict[str, str] = {}
        self._calls: List[Tuple[str, str]] = []

    def get(self, key: str):
        self._calls.append(('get', key))
        return self.str_store.get(key)

    def hgetall(self, key: str):
        self._calls.append(('hgetall', key))
        return dict(self.hash_store.get(key, {}))

    def pipeline(self, transaction: bool = False) -> FakePipeline:
        return FakePipeline(self)
