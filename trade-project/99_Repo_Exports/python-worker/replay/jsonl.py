from __future__ import annotations

import json
import os
from collections.abc import Iterator
from typing import Any
import contextlib


class JsonlWriter:
    """
    Минимальный JSONL writer:
      - append mode
      - flush/fsync по желанию (для prod-safe записи)
    """

    def __init__(self, path: str, *, flush: bool = True, fsync: bool = False) -> None:
        self.path = path
        self.flush = flush
        self.fsync = fsync
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        self._fh = open(path, "a", encoding="utf-8")

    def write(self, obj: dict[str, Any]) -> None:
        self._fh.write(json.dumps(obj, ensure_ascii=False) + "\n")
        if self.flush:
            self._fh.flush()
            if self.fsync:
                with contextlib.suppress(Exception):
                    os.fsync(self._fh.fileno())

    def close(self) -> None:
        with contextlib.suppress(Exception):
            self._fh.close()


def iter_jsonl(path: str, *, max_lines: int | None = None) -> Iterator[dict[str, Any]]:
    """
    Итератор по JSONL:
      - пропускает битые строки (fail-open)
      - max_lines ограничивает чтение (удобно для быстрой отладки)
    """
    n = 0
    with open(path, encoding="utf-8") as fh:
        for line in fh:
            if max_lines is not None and n >= max_lines:
                break
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
                if isinstance(obj, dict):
                    yield obj
                    n += 1
            except Exception:
                continue
