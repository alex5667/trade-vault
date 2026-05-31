from __future__ import annotations

import json
import time
from typing import Any, Dict, Iterable, List

def now_ms() -> int:
    return int(time.time() * 1000)

def pctl(xs: List[float], q: float) -> float:
    if not xs:
        return 0.0
    xs = sorted(xs)
    i = int(round((len(xs) - 1) * q))
    i = max(0, min(len(xs) - 1, i))
    return float(xs[i])

def clamp01(x: float) -> float:
    return 0.0 if x < 0.0 else (1.0 if x > 1.0 else float(x))

def safe_float(x: Any, d: float = 0.0) -> float:
    try:
        return float(x)
    except Exception:
        return float(d)

def safe_int(x: Any, d: int = 0) -> int:
    try:
        return int(float(x))
    except Exception:
        return int(d)

def read_ndjson(path: str) -> Iterable[Dict[str, Any]]:
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            yield json.loads(line)

def ece(probs: List[float], y: List[int], n_bins: int = 15) -> float:
    if not probs:
        return 0.0
    bins = [0] * n_bins
    acc = [0.0] * n_bins
    conf = [0.0] * n_bins
    for p, yy in zip(probs, y):
        p = clamp01(p)
        b = min(n_bins - 1, int(p * n_bins))
        bins[b] += 1
        acc[b] += float(yy)
        conf[b] += float(p)
    total = float(len(probs))
    out = 0.0
    for b in range(n_bins):
        if bins[b] == 0:
            continue
        w = bins[b] / total
        a = acc[b] / bins[b]
        c = conf[b] / bins[b]
        out += w * abs(a - c)
    return float(out)

def brier(probs: List[float], y: List[int]) -> float:
    if not probs:
        return 0.0
    s = 0.0
    for p, yy in zip(probs, y):
        p = clamp01(p)
        s += (p - float(yy)) ** 2
    return float(s / len(probs))
