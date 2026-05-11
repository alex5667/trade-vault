from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import Any


def stable_bucket_0_99(key: str) -> int:
    h = hashlib.sha1(key.encode("utf-8", errors="ignore")).digest()
    v = (h[0] << 8) | h[1]
    return int(v % 100)


def choose_arm_abc(*, key: str, split_b: int, split_c: int, salt: str = "") -> str:
    """
    Deterministic routing:
      bucket < split_b              => B
      bucket < split_b + split_c    => C
      else                          => A
    """
    sb = max(0, min(100, int(split_b)))
    sc = max(0, min(100, int(split_c)))
    if sb + sc > 100:
        sc = max(0, 100 - sb)
    b = stable_bucket_0_99(f"{salt}|{key}")
    if b < sb:
        return "B"
    if b < (sb + sc):
        return "C"
    return "A"


def regime_group(regime: str) -> str:
    """Maps fine-grained regime to broad AB-test group.

    Groups:
      thin   — low liquidity / news / illiquid: capital preservation mode
      trend  — directional momentum / expansion: runner profile
      range  — chop / mean-reversion: fade/absorption profile
      mixed  — unclassified / volatile: conservative default
    """
    rg = (regime or "na").strip().lower()
    if rg in ("thin", "news", "illiquid"):
        return "thin"
    if rg in ("trend", "trending", "trending_bull", "trending_bear", "momentum", "expansion"):
        return "trend"
    if rg in ("range", "chop", "meanrev", "sideways"):
        return "range"
    return "mixed"


@dataclass
class ABCConfig:
    enabled: bool = True
    version: int = 1
    salt: str = "smt-entry-v1"
    splits: dict[str, dict[str, int]] = None  # type: ignore
    poll_ms: int = 2000
    overrides: dict[str, str] = None  # type: ignore

    @staticmethod
    def from_dict(d: dict[str, Any]) -> ABCConfig:
        splits = d.get("splits")
        if not isinstance(splits, dict):
            splits = {"default": {"b": 10, "c": 10}, "thin": {"b": 15, "c": 15}}
        overrides = d.get("overrides")
        if not isinstance(overrides, dict):
            overrides = {}
        return ABCConfig(
            enabled=bool(int(d.get("enabled", 1) or 0)),
            version=int(d.get("version", 1) or 1),
            salt=(d.get("salt") or "smt-entry-v1"),
            splits=splits,
            poll_ms=int(d.get("poll_ms", 2000) or 2000),
            overrides=overrides,
        )

    def get_splits(self, group: str) -> tuple[int, int]:
        s = self.splits or {}
        g = group if group in s else "default"
        d = s.get(g) or s.get("default") or {}
        try:
            sb = int(d.get("b", 10))
        except Exception:
            sb = 10
        try:
            sc = int(d.get("c", 10))
        except Exception:
            sc = 10
        sb = max(0, min(100, sb))
        sc = max(0, min(100, sc))
        if sb + sc > 100:
            sc = max(0, 100 - sb)
        return sb, sc
