"""
ensemble_weights_reader.py — Consumer-side cache for ensemble:weights:{symbol}.

Wraps the per-symbol HASH published by ensemble_weights_publisher_v1.
Provides:
  * lookup_source_weight(symbol, source) → float (0..1) or None
  * blend(symbol, source_probs: dict[str, float]) → float
        Logit-space weighted blend ↔ matches the publisher math.

Design:
  * Per-symbol entry in cache; TTL ENSEMBLE_WEIGHTS_READER_TTL_SEC (default 300).
  * Kill-switch ENV ENSEMBLE_WEIGHTS_READ_ENABLED (default 0) → returns
    equal-weight blend (so caller never crashes).
  * Equal-weight fallback when symbol absent or HASH empty.
"""
from __future__ import annotations

import logging
import math
import os
import time
from typing import Any

log = logging.getLogger("ensemble_weights_reader")

_KEY_TPL = "ensemble:weights:{symbol}"
_DEFAULT_TTL_SEC = float(os.getenv("ENSEMBLE_WEIGHTS_READER_TTL_SEC", "300"))
_EPS = 1e-9


def _logit(p: float) -> float:
    p = max(_EPS, min(1.0 - _EPS, float(p)))
    return math.log(p / (1.0 - p))


def _inv_logit(z: float) -> float:
    return 1.0 / (1.0 + math.exp(-z))


class EnsembleWeightsReader:
    def __init__(
        self,
        rc: Any,
        key_tpl: str = _KEY_TPL,
        ttl_sec: float = _DEFAULT_TTL_SEC,
        enabled_env: str = "ENSEMBLE_WEIGHTS_READ_ENABLED",
    ) -> None:
        self._rc = rc
        self._key_tpl = key_tpl
        self._ttl_sec = ttl_sec
        self._enabled_env = enabled_env
        self._cache: dict[str, tuple[float, dict[str, float]]] = {}

    def _enabled(self) -> bool:
        return os.getenv(self._enabled_env, "0").strip() == "1"

    def _load_symbol(self, symbol: str) -> dict[str, float]:
        sym = str(symbol or "").upper()
        if not sym:
            return {}
        now = time.monotonic()
        cached = self._cache.get(sym)
        if cached and (now - cached[0]) < self._ttl_sec:
            return cached[1]
        try:
            raw = self._rc.hgetall(self._key_tpl.format(symbol=sym))
        except Exception as e:
            log.debug("ensemble_weights_reader Redis error: %s", e)
            return cached[1] if cached else {}
        weights: dict[str, float] = {}
        if isinstance(raw, dict):
            for src, w in raw.items():
                try:
                    weights[str(src)] = float(w)
                except Exception:
                    continue
        self._cache[sym] = (now, weights)
        return weights

    def lookup_source_weight(self, symbol: str, source: str) -> float | None:
        if not self._enabled():
            return None
        w = self._load_symbol(symbol).get(str(source))
        return float(w) if w is not None else None

    def blend(
        self,
        symbol: str,
        source_probs: dict[str, float],
    ) -> float:
        """Weighted blend in logit space.

        If gating disabled OR no weights for symbol → equal-weight blend.
        """
        if not source_probs:
            return 0.5
        if not self._enabled():
            return _equal_weight_blend(source_probs)
        w = self._load_symbol(symbol)
        if not w:
            return _equal_weight_blend(source_probs)
        # Use only sources that have BOTH a weight and a probability
        agg_z = 0.0
        agg_w = 0.0
        for src, p in source_probs.items():
            wt = w.get(str(src))
            if wt is None or wt <= 0:
                continue
            agg_z += wt * _logit(p)
            agg_w += wt
        if agg_w <= 0:
            return _equal_weight_blend(source_probs)
        return _inv_logit(agg_z / agg_w)


def _equal_weight_blend(source_probs: dict[str, float]) -> float:
    if not source_probs:
        return 0.5
    z = sum(_logit(p) for p in source_probs.values())
    return _inv_logit(z / len(source_probs))
