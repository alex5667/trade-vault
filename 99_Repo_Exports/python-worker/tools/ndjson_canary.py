from __future__ import annotations

import hashlib
import json
from collections.abc import Iterable, Iterator
from pathlib import Path


def _stable_hash_u32(s: str) -> int:
    h = hashlib.sha1(s.encode("utf-8")).digest()
    return int.from_bytes(h[:4], byteorder="big", signed=False)


def _pass_share(symbol: str, share: float) -> bool:
    if share <= 0:
        return False
    if share >= 1:
        return True
    v = _stable_hash_u32(symbol.upper()) / float(2**32 - 1)
    return v < share


def iter_ndjson(path: str) -> Iterator[dict]:
    """
    Reads NDJSON (1 JSON per line). Skips empty lines.
    """
    p = Path(path)
    with p.open("r", encoding="utf-8") as f:
        for line in f:
            s = line.strip()
            if not s:
                continue
            yield json.loads(s)


def write_ndjson(path: str, rows: Iterable[dict]) -> int:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    n = 0
    with p.open("w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False, separators=(",", ":")))
            f.write("\n")
            n += 1
    return n


def filter_inputs(
    rows: Iterable[dict],
    *,
    canary_symbols: list[str] | None = None,
    canary_share: float = 0.0,
) -> Iterator[dict]:
    """
    Filters OFInputsV1 rows by either explicit symbols OR deterministic hash-share.
    Priority:
      - if canary_symbols given -> allowlist
      - else if canary_share > 0 -> stable share
      - else -> pass through
    """
    allow = None
    if canary_symbols:
        allow = set([s.strip().upper() for s in canary_symbols if s and s.strip()])

    for r in rows:
        sym = (r.get("symbol") or "").upper()
        if not sym:
            continue

        if allow is not None:
            if sym in allow:
                yield r
            continue

        if canary_share > 0:
            if _pass_share(sym, float(canary_share)):
                yield r
            continue

        yield r


def pick_baseline_for_symbol(baseline_dir: str, symbol: str) -> str | None:
    """
    baseline_dir supports:
      baseline_<SYMBOL>.ndjson
      baseline.ndjson (fallback)
    """
    d = Path(baseline_dir)
    if not d.exists():
        return None
    s = symbol.upper()
    p1 = d / f"baseline_{s}.ndjson"
    if p1.exists():
        return str(p1)
    p0 = d / "baseline.ndjson"
    if p0.exists():
        return str(p0)
    return None


def list_symbols_in_inputs(inputs_path: str, limit: int = 200000) -> list[str]:
    """
    Extract distinct symbols from inputs (bounded).
    """
    seen = set()
    out: list[str] = []
    for i, r in enumerate(iter_ndjson(inputs_path)):
        sym = (r.get("symbol") or "").upper()
        if sym and sym not in seen:
            seen.add(sym)
            out.append(sym)
        if i >= limit:
            break
    return out


def safe_json_get(row: dict[str, Any], path: str, default: Any = None) -> Any:
    """Small helper for nested extraction: 'evidence.scenario_v4'."""
    cur: Any = row
    for part in path.split("."):
        if isinstance(cur, dict) and part in cur:
            cur = cur.get(part)
        else:
            return default
    return cur
