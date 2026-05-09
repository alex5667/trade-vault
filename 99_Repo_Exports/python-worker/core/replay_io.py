from __future__ import annotations

import hashlib
import json
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from typing import Any

from utils.time_utils import get_ny_time_millis


def now_ms() -> int:
    return get_ny_time_millis()


def stable_json(obj: Any) -> str:
    """Deterministic JSON for hashing/diffing."""
    return json.dumps(obj, sort_keys=True, ensure_ascii=False, separators=(",", ":"))


def fingerprint(obj: Any) -> str:
    return hashlib.sha1(stable_json(obj).encode("utf-8")).hexdigest()


def iter_ndjson(path: str) -> Iterator[dict[str, Any]]:
    with open(path, encoding="utf-8", errors="ignore") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            yield json.loads(line)


def write_ndjson(path: str, rows: Iterable[dict[str, Any]]) -> None:
    with open(path, "w", encoding="utf-8") as f:
        for r in rows:
            f.write(stable_json(r) + "\n")


@dataclass
class DiffItem:
    idx: int
    key: str
    a: Any
    b: Any


def topdiff(baseline: list[dict[str, Any]], current: list[dict[str, Any]], keys: list[str], top_k: int = 20) -> tuple[int, list[DiffItem]]:
    """Return (n_changed, first top_k diffs) comparing by keys."""
    n = min(len(baseline), len(current))
    out: list[DiffItem] = []
    changed = 0
    for i in range(n):
        a = baseline[i]
        b = current[i]
        for k in keys:
            if a.get(k) != b.get(k):
                changed += 1
                out.append(DiffItem(i, k, a.get(k), b.get(k)))
                break
    return changed, out[:top_k]

