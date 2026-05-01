from __future__ import annotations
"""Feature denylist loader (v1).

Purpose
-------
Provide a single, deterministic way to apply a denylist to ML feature schemas.

This module is intentionally tiny and dependency-free, because it is imported
both by training tooling and runtime services.

Denylist formats
----------------
Path is resolved as:
  - ML_FEATURE_DENYLIST_PATH (if set)
  - otherwise: <this_dir>/feature_denylist_v1.json

Supported file formats:
  1) JSON dict with keys:
        - deny_num:  ["key", ...]   (raw indicator keys)
        - deny_bool: ["key", ...]
        - deny_all:  ["key", ...]   (applied to both num and bool)
        - deny:      ["key", ...]   (alias for deny_all)

  2) JSON list of strings:
        ["key", "key2", ...]   (treated as deny_all)

  3) Plain text (.txt): one key per line (comments with '#', ';').

Notes
-----
- Denylist keys are raw indicator keys (without 'n:'/'b:' prefixes).
- Loader is fail-open: any read/parse error returns an empty denylist.
"""


import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple


_DEFAULT_FILENAME = "feature_denylist_v1.json"
_ENV_PATH = "ML_FEATURE_DENYLIST_PATH"


@dataclass(frozen=True)
class FeatureDenylist:
    deny_num: Set[str]
    deny_bool: Set[str]
    deny_all: Set[str]

    def flat(self) -> Set[str]:
        return set(self.deny_all) | set(self.deny_num) | set(self.deny_bool)


# Cache by absolute path string → parsed denylist
_CACHE: Dict[str, FeatureDenylist] = {}


def denylist_path() -> Path:
    p = (os.environ.get(_ENV_PATH) or "").strip()
    if p:
        return Path(p)
    return Path(__file__).with_name(_DEFAULT_FILENAME)


def _parse_txt(text: str) -> FeatureDenylist:
    keys: List[str] = []
    for raw in (text or "").splitlines():
        line = raw.strip()
        if not line:
            continue
        if line.startswith("#") or line.startswith(";"):
            continue
        # inline comments
        for sep in ("#", ";"):
            if sep in line:
                line = line.split(sep, 1)[0].strip()
        if line:
            keys.append(line)

    s = set(keys)
    return FeatureDenylist(deny_num=set(), deny_bool=set(), deny_all=s)


def _as_str_list(v: Any) -> List[str]:
    if v is None:
        return []
    if isinstance(v, (list, tuple)):
        out: List[str] = []
        for x in v:
            if x is None:
                continue
            try:
                sx = str(x).strip()
            except Exception:
                continue
            if sx:
                out.append(sx)
        return out
    # single scalar
    try:
        sx = str(v).strip()
        return [sx] if sx else []
    except Exception:
        return []


def _parse_json(obj: Any) -> FeatureDenylist:
    # list → deny_all
    if isinstance(obj, list):
        s = set(_as_str_list(obj))
        return FeatureDenylist(deny_num=set(), deny_bool=set(), deny_all=s)

    if not isinstance(obj, dict):
        return FeatureDenylist(deny_num=set(), deny_bool=set(), deny_all=set())

    deny_all = set(_as_str_list(obj.get("deny_all") or obj.get("deny") or []))
    deny_num = set(_as_str_list(obj.get("deny_num") or []))
    deny_bool = set(_as_str_list(obj.get("deny_bool") or []))

    # Some teams keep a single flat list in "deny".
    if deny_all:
        pass

    return FeatureDenylist(deny_num=deny_num, deny_bool=deny_bool, deny_all=deny_all)


def load_feature_denylist(path: Optional[Path] = None) -> FeatureDenylist:
    """Load denylist from path (or env/default). Fail-open."""
    p = Path(path) if path is not None else denylist_path()
    key = str(p.resolve())
    if key in _CACHE:
        return _CACHE[key]

    dl = FeatureDenylist(deny_num=set(), deny_bool=set(), deny_all=set())
    try:
        if not p.exists():
            _CACHE[key] = dl
            return dl
        text = p.read_text(encoding="utf-8")
        if p.suffix.lower() == ".txt":
            dl = _parse_txt(text)
        else:
            dl = _parse_json(json.loads(text))
    except Exception:
        dl = FeatureDenylist(deny_num=set(), deny_bool=set(), deny_all=set())

    _CACHE[key] = dl
    return dl


def denylist_flat(path: Optional[Path] = None) -> Set[str]:
    return load_feature_denylist(path).flat()


def clear_denylist_cache() -> None:
    _CACHE.clear()
