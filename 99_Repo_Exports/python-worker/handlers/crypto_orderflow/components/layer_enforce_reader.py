from __future__ import annotations

"""layer_enforce_reader.py

Читает Redis-флаги auto-promotion от of_layers_shadow_calibrator_v1:
  of_gate:layer_{a,b,c}:mode             = off|canary|prod
  of_gate:layer_{a,b,c}:canary_symbols   = csv list
  of_gate:layer_{a,b,c}:bundle           = json
  of_gate:layer_{a,b,c}:bundle_sig       = HMAC-SHA256 hex

Возможности:
  - In-memory cache с TTL (не дёргать Redis на каждый сигнал)
  - HMAC verify (защита от tamper)
  - Fail-safe: при ошибке Redis возвращаем mode='off' (no enforcement)
"""

import hashlib
import hmac
import json
import logging
import os
import threading
import time
from dataclasses import dataclass, field
from typing import Any

logging.getLogger(__name__).addHandler(logging.NullHandler())
log = logging.getLogger(__name__)


@dataclass
class LayerEnforceState:
    mode: str = "off"          # off | canary | prod
    canary_symbols: tuple[str, ...] = field(default_factory=tuple)
    bundle_valid: bool = False
    promoted_ts_ms: int = 0
    raw_bundle: dict[str, Any] = field(default_factory=dict)


@dataclass
class LayerEnforceStates:
    """Snapshot всех 3 enforce-able слоёв (A/B/C; D исключён)."""
    a: LayerEnforceState = field(default_factory=LayerEnforceState)
    b: LayerEnforceState = field(default_factory=LayerEnforceState)
    c: LayerEnforceState = field(default_factory=LayerEnforceState)
    fetched_ts_ms: int = 0


def _verify_hmac(bundle_raw: str, sig_hex: str, secret: str) -> bool:
    if not bundle_raw or not sig_hex or not secret:
        return False
    try:
        bundle = json.loads(bundle_raw)
        canonical = json.dumps(bundle, sort_keys=True, separators=(",", ":")).encode()
        expected = hmac.new(secret.encode(), canonical, hashlib.sha256).hexdigest()
        return hmac.compare_digest(expected, sig_hex)
    except Exception:
        return False


def _csv_set(s: str | None) -> tuple[str, ...]:
    if not s:
        return ()
    return tuple(x.strip().upper() for x in s.split(",") if x.strip())


def _read_one_layer(redis_client: Any, prefix: str, layer: str,
                    secret: str) -> LayerEnforceState:
    base = f"{prefix}:layer_{layer.lower()}"
    try:
        mode    = redis_client.get(f"{base}:mode") or "off"
        symbols = redis_client.get(f"{base}:canary_symbols") or ""
        bundle  = redis_client.get(f"{base}:bundle") or ""
        sig     = redis_client.get(f"{base}:bundle_sig") or ""
        ts_ms   = redis_client.get(f"{base}:promoted_ts_ms") or "0"
    except Exception as ex:
        log.warning(f"redis read failed for layer {layer}: {ex}")
        return LayerEnforceState()

    mode = str(mode).lower().strip()
    if mode not in ("off", "canary", "prod"):
        mode = "off"

    valid = _verify_hmac(str(bundle), str(sig), secret) if bundle else False
    if mode != "off" and not valid:
        # tamper protection: invalid bundle → ignore promotion
        log.warning(f"layer {layer}: HMAC invalid, downgrade to off")
        mode = "off"

    try:
        promoted_ts = int(ts_ms)
    except Exception:
        promoted_ts = 0

    raw_bundle: dict[str, Any] = {}
    if valid:
        try:
            raw_bundle = json.loads(bundle)
        except Exception:
            raw_bundle = {}

    return LayerEnforceState(
        mode=mode,
        canary_symbols=_csv_set(str(symbols)),
        bundle_valid=valid,
        promoted_ts_ms=promoted_ts,
        raw_bundle=raw_bundle,
    )


class LayerEnforceReader:
    """Cached reader для Redis-флагов enforce. Thread-safe."""

    def __init__(
        self,
        redis_client: Any,
        secret: str,
        prefix: str = "of_gate",
        cache_ttl_sec: float = 5.0,
    ) -> None:
        self._redis = redis_client
        self._secret = secret
        self._prefix = prefix
        self._ttl = float(cache_ttl_sec)
        self._lock = threading.Lock()
        self._cached: LayerEnforceStates | None = None
        self._cached_at: float = 0.0

    def _now(self) -> float:
        return time.monotonic()

    def fetch(self, force: bool = False) -> LayerEnforceStates:
        with self._lock:
            now = self._now()
            if not force and self._cached is not None and (now - self._cached_at) < self._ttl:
                return self._cached
            if self._redis is None:
                self._cached = LayerEnforceStates()
                self._cached_at = now
                return self._cached
            states = LayerEnforceStates(
                a=_read_one_layer(self._redis, self._prefix, "a", self._secret),
                b=_read_one_layer(self._redis, self._prefix, "b", self._secret),
                c=_read_one_layer(self._redis, self._prefix, "c", self._secret),
                fetched_ts_ms=int(time.time() * 1000),
            )
            self._cached = states
            self._cached_at = now
            return states

    def get_mode_override(self) -> str | None:
        """Глобальный hot-override OF_LAYER_ENFORCE_MODE: off/shadow/enforce.
        Записывается autotuner'ом в Redis: <prefix>:enforce_mode_override.
        Если установлен — используется вместо ENV (без рестарта worker)."""
        if self._redis is None:
            return None
        try:
            v = self._redis.get(f"{self._prefix}:enforce_mode_override")
            if not v:
                return None
            s = str(v).lower().strip()
            return s if s in ("off", "shadow", "enforce") else None
        except Exception:
            return None

    def get_leg_key_override(self, layer: str, leg_n: int) -> str | None:
        """Hot-override для OF_LAYER_C_ENFORCE_LEG{N}_KEY.
        Записывается autotuner'ом если detected high missing_rate."""
        if self._redis is None:
            return None
        try:
            v = self._redis.get(
                f"{self._prefix}:layer_{layer.lower()}:leg{leg_n}_key_override")
            return str(v).strip() if v else None
        except Exception:
            return None

    def get_for_symbol(self, layer: str, symbol: str) -> LayerEnforceState | None:
        """Возвращает state, если layer активен ДЛЯ ЭТОГО symbol.
        None если layer off или symbol не в allowlist (для canary).
        """
        states = self.fetch()
        layer_state: LayerEnforceState
        if layer.upper() == "A":
            layer_state = states.a
        elif layer.upper() == "B":
            layer_state = states.b
        elif layer.upper() == "C":
            layer_state = states.c
        else:
            return None

        if layer_state.mode == "off":
            return None
        sym = symbol.upper()
        if layer_state.mode == "canary":
            if sym not in layer_state.canary_symbols:
                return None
        # prod → applies to all symbols
        return layer_state


def reader_from_env(redis_client: Any) -> LayerEnforceReader:
    secret = os.environ.get("LAYER_ENFORCE_HMAC_SECRET") \
          or os.environ.get("LAYERS_CAL_HMAC_SECRET") \
          or os.environ.get("RECS_HMAC_SECRET") or ""
    prefix = os.environ.get("LAYER_ENFORCE_KEY_PREFIX", "of_gate")
    ttl    = float(os.environ.get("LAYER_ENFORCE_CACHE_TTL_SEC", "5.0"))
    return LayerEnforceReader(redis_client, secret, prefix=prefix, cache_ttl_sec=ttl)
