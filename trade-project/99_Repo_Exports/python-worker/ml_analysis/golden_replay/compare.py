from __future__ import annotations

import dataclasses
import hashlib
import json
import math
from collections.abc import Iterable
from typing import Any


def stable_json_dumps(obj: Any) -> str:
    """Deterministic JSON for hashing / manifests."""
    return json.dumps(obj, sort_keys=True, ensure_ascii=False, separators=(",", ":"))


def stable_hash(obj: Any, *, algo: str = "sha256") -> str:
    payload = stable_json_dumps(obj).encode("utf-8")
    h = hashlib.new(algo)
    h.update(payload)
    return h.hexdigest()


def _is_number(x: Any) -> bool:
    return isinstance(x, (int, float)) and not isinstance(x, bool)


def _as_float(x: Any) -> float | None:
    if x is None:
        return None
    if _is_number(x):
        try:
            f = float(x)
            if math.isnan(f) or math.isinf(f):
                return None
            return f
        except Exception:
            return None
    try:
        f = float(x)
        if math.isnan(f) or math.isinf(f):
            return None
        return f
    except Exception:
        return None


def float_close(a: Any, b: Any, *, abs_tol: float = 1e-6, rel_tol: float = 1e-6) -> bool:
    fa = _as_float(a)
    fb = _as_float(b)
    if fa is None or fb is None:
        return a == b
    diff = abs(fa - fb)
    if diff <= abs_tol:
        return True
    denom = max(abs(fa), abs(fb), 1e-12)
    return (diff / denom) <= rel_tol


@dataclasses.dataclass
class DiffItem:
    path: str
    a: Any
    b: Any
    kind: str


def _to_plain(obj: Any) -> Any:
    if dataclasses.is_dataclass(obj):
        return dataclasses.asdict(obj)
    return obj


def diff_objects(
    a: Any,
    b: Any,
    *,
    abs_tol: float = 1e-6,
    rel_tol: float = 1e-6,
    ignore_paths: Iterable[str] | None = None,
    max_diffs: int = 200,
    _path: str = "",
    _out: list[DiffItem] | None = None,
) -> list[DiffItem]:
    """Recursive diff with float tolerance and path-based ignore."""
    if _out is None:
        _out = []
    if len(_out) >= max_diffs:
        return _out

    a = _to_plain(a)
    b = _to_plain(b)

    if ignore_paths:
        for p in ignore_paths:
            if _path == p or (_path.startswith(p) and (len(_path) == len(p) or _path[len(p)] == ".")):
                return _out

    if _is_number(a) or _is_number(b):
        if not float_close(a, b, abs_tol=abs_tol, rel_tol=rel_tol):
            _out.append(DiffItem(path=_path, a=a, b=b, kind="number"))
        return _out

    if isinstance(a, (str, bool)) or a is None:
        if a != b:
            _out.append(DiffItem(path=_path, a=a, b=b, kind="value"))
        return _out

    if isinstance(a, (list, tuple)) and isinstance(b, (list, tuple)):
        if len(a) != len(b):
            _out.append(DiffItem(path=_path + ".__len__", a=len(a), b=len(b), kind="len"))
            return _out
        for i, (ai, bi) in enumerate(zip(a, b)):
            diff_objects(
                ai, bi,
                abs_tol=abs_tol, rel_tol=rel_tol,
                ignore_paths=ignore_paths,
                max_diffs=max_diffs,
                _path=f"{_path}[{i}]" if _path else f"[{i}]",
                _out=_out,
            )
            if len(_out) >= max_diffs:
                break
        return _out

    if isinstance(a, dict) and isinstance(b, dict):
        akeys = set(a.keys())
        bkeys = set(b.keys())
        if akeys != bkeys:
            miss_a = sorted(list(bkeys - akeys))[:50]
            miss_b = sorted(list(akeys - bkeys))[:50]
            _out.append(DiffItem(path=_path + ".__keys__", a=miss_b, b=miss_a, kind="keys"))
        for k in sorted(list(akeys & bkeys)):
            diff_objects(
                a.get(k), b.get(k),
                abs_tol=abs_tol, rel_tol=rel_tol,
                ignore_paths=ignore_paths,
                max_diffs=max_diffs,
                _path=f"{_path}.{k}" if _path else str(k),
                _out=_out,
            )
            if len(_out) >= max_diffs:
                break
        return _out

    if a != b:
        _out.append(DiffItem(path=_path, a=a, b=b, kind="type_or_value"))
    return _out


def extract_policy_keys(rec: dict[str, Any]) -> tuple[str, str]:
    ind = rec.get("indicators") if isinstance(rec.get("indicators"), dict) else rec
    ph = (ind.get("dq_policy_hash") or "")
    mh = str(ind.get("dq_policy_feature_manifest_hash_v1") or ind.get("dq_policy_feature_manifest_hash") or "")
    return ph, mh


def extract_expected_ofc(rec: dict[str, Any]) -> dict[str, Any] | None:
    for k in ("of_confirm", "ofc", "confirm", "of_confirm_v3"):
        v = rec.get(k)
        if isinstance(v, dict):
            return v
    d = rec.get("decision")
    if isinstance(d, dict):
        for k in ("of_confirm", "ofc", "confirm", "of_confirm_v3"):
            v = d.get(k)
            if isinstance(v, dict):
                return v
    return None


def summarize_diffs(diffs: list[DiffItem]) -> dict[str, Any]:
    by_kind: dict[str, int] = {}
    top = []
    for it in diffs[:50]:
        by_kind[it.kind] = by_kind.get(it.kind, 0) + 1
        top.append({"path": it.path, "kind": it.kind, "a": it.a, "b": it.b})
    return {"count": len(diffs), "by_kind": by_kind, "top": top}
