from __future__ import annotations

import json
import logging
import os
import time
from dataclasses import asdict, dataclass
from typing import Any

import redis

logger = logging.getLogger(__name__)


def _safe_int(v: Any, default: int = 0) -> int:
    try:
        return int(v)
    except Exception:
        return default


def _ensure_dict(v: Any) -> dict[str, Any]:
    return dict(v) if isinstance(v, dict) else {}


@dataclass(frozen=True)
class ATRPolicyResolution:
    hit: bool
    level: str                # exact | fallback_scenario | fallback_default | miss
    active_key: str
    source: str
    symbol: str
    scenario: str
    regime: str
    risk_horizon_bucket: str
    stop_ttl_mode: str        # live | canary | shadow
    trailing_mode: str        # live | canary | shadow
    reason_code: str
    policy_ver: int
    updated_at_ms: int
    # Phase 3.8 fields
    kill_switch_active: bool
    last_good_used: bool


VALID_MODES = {"shadow", "canary", "live"}
REQUIRED_FIELDS = ["source", "symbol", "scenario", "regime", "risk_horizon_bucket", "stop_ttl_mode", "trailing_mode"]


class ATRPolicyResolver:
    def __init__(self, redis_url: str | None = None):
        self.redis_url = redis_url or os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
        self.cache_ttl_ms = _safe_int(os.getenv("ATR_POLICY_RESOLVER_CACHE_TTL_MS", "5000"), 5000)
        self.enable = os.getenv("ATR_POLICY_RESOLVER_ENABLE", "1") == "1"
        self._r: redis.Redis | None = None
        self._cache: dict[str, tuple[int, dict[str, Any]]] = {}

    def _redis(self) -> redis.Redis | None:
        if not self.enable:
            return None
        if self._r is None:
            try:
                self._r = redis.Redis.from_url(self.redis_url, decode_responses=True)
            except Exception:
                self._r = None
        return self._r

    def _key(self, source: str, symbol: str, scenario: str, regime: str, bucket: str) -> str:
        return f"cfg:atr_policy:active:{source}:{symbol}:{scenario}:{regime}:{bucket}"

    def _last_good_key(self, source: str, symbol: str, scenario: str, regime: str, bucket: str) -> str:
        return f"cfg:atr_policy:last_good:{source}:{symbol}:{scenario}:{regime}:{bucket}"

    def _kill_switch_key(self, source: str, symbol: str, scenario: str, regime: str, bucket: str) -> str:
        return f"cfg:atr_policy:kill_switch:{source}:{symbol}:{scenario}:{regime}:{bucket}"

    def _candidates(self, source: str, symbol: str, scenario: str, regime: str, bucket: str) -> list[tuple[str, str, str]]:
        """Returns (level, active_key, last_good_key) tuples in resolve priority order."""
        return [
            ("exact",
             self._key(source, symbol, scenario, regime, bucket),
             self._last_good_key(source, symbol, scenario, regime, bucket)),
            ("fallback_scenario",
             self._key(source, symbol, scenario, "na", bucket),
             self._last_good_key(source, symbol, scenario, "na", bucket)),
            ("fallback_default",
             self._key(source, symbol, "default", "na", bucket),
             self._last_good_key(source, symbol, "default", "na", bucket)),
        ]

    def _is_kill_switched(self, r: redis.Redis, source: str, symbol: str, scenario: str, regime: str, bucket: str) -> bool:
        """Check if the exact cohort has kill_switch enabled."""
        try:
            raw = r.get(self._kill_switch_key(source, symbol, scenario, regime, bucket))
            if raw:
                obj = json.loads(raw)
                return bool(obj.get("enabled"))
        except Exception:
            pass
        return False

    def _validate_policy_obj(self, obj: Any) -> str | None:
        """
        Validate a deserialized policy dict.
        Returns None if valid, or a reason_code string if invalid.
        """
        if not isinstance(obj, dict):
            return "POLICY_NOT_DICT"
        missing = [k for k in REQUIRED_FIELDS if not obj.get(k)]
        if missing:
            return "ACTIVE_KEY_FIELDS_MISSING"
        if obj.get("stop_ttl_mode") not in VALID_MODES:
            return "ACTIVE_KEY_STOP_MODE_INVALID"
        if obj.get("trailing_mode") not in VALID_MODES:
            return "ACTIVE_KEY_TRAIL_MODE_INVALID"
        return None

    def _build_resolution(
        self, *,
        hit: bool, level: str, active_key: str,
        source: str, symbol: str, scenario: str, regime: str, risk_horizon_bucket: str,
        stop_ttl_mode: str, trailing_mode: str, reason_code: str, policy_ver: int,
        updated_at_ms: int, kill_switch_active: bool = False, last_good_used: bool = False,
    ) -> dict[str, Any]:
        return asdict(ATRPolicyResolution(
            hit=hit, level=level, active_key=active_key,
            source=source, symbol=symbol, scenario=scenario,
            regime=regime, risk_horizon_bucket=risk_horizon_bucket,
            stop_ttl_mode=stop_ttl_mode, trailing_mode=trailing_mode,
            reason_code=reason_code, policy_ver=policy_ver, updated_at_ms=updated_at_ms,
            kill_switch_active=kill_switch_active, last_good_used=last_good_used,
        ))

    def resolve(
        self,
        *,
        source: str,
        symbol: str,
        scenario: str,
        regime: str,
        risk_horizon_bucket: str,
    ) -> dict[str, Any]:
        source = (source or "CryptoOrderFlow")
        symbol = (symbol or "").upper()
        scenario = (scenario or "default").lower()
        regime = (regime or "na").lower()
        risk_horizon_bucket = (risk_horizon_bucket or "unknown").lower()

        cache_key = f"{source}|{symbol}|{scenario}|{regime}|{risk_horizon_bucket}"
        now_ms = int(time.time() * 1000)
        cached = self._cache.get(cache_key)
        if cached and (now_ms - cached[0] <= self.cache_ttl_ms):
            return cached[1]

        r = self._redis()
        if r is None:
            out = self._build_resolution(
                hit=False, level="miss", active_key="",
                source=source, symbol=symbol, scenario=scenario,
                regime=regime, risk_horizon_bucket=risk_horizon_bucket,
                stop_ttl_mode="canary", trailing_mode="canary",
                reason_code="ATR_POLICY_RESOLVER_DISABLED",
                policy_ver=0, updated_at_ms=0,
            )
            self._cache[cache_key] = (now_ms, out)
            return out

        # ── Phase 3.8: kill_switch check (exact cohort only) ──────────────
        if self._is_kill_switched(r, source, symbol, scenario, regime, risk_horizon_bucket):
            out = self._build_resolution(
                hit=False, level="canary_shadow", active_key="",
                source=source, symbol=symbol, scenario=scenario,
                regime=regime, risk_horizon_bucket=risk_horizon_bucket,
                stop_ttl_mode="canary", trailing_mode="canary",
                reason_code="KILL_SWITCH_ACTIVE",
                policy_ver=0, updated_at_ms=0,
                kill_switch_active=True, last_good_used=False,
            )
            self._cache[cache_key] = (now_ms, out)
            logger.debug("resolver: KILL_SWITCH_ACTIVE for %s:%s", source, symbol)
            return out

        # ── Phase 3.8: resolve order: active → last_good → canary/shadow ──
        for level, active_key, lg_key in self._candidates(source, symbol, scenario, regime, risk_horizon_bucket):
            # ── 1. Try active key ─────────────────────────────────────────
            try:
                raw = r.get(active_key)
                if raw:
                    obj = json.loads(raw)
                    invalid = self._validate_policy_obj(obj)
                    if invalid is None:
                        out = self._build_resolution(
                            hit=True, level=level, active_key=active_key,
                            source=source, symbol=symbol, scenario=scenario,
                            regime=regime, risk_horizon_bucket=risk_horizon_bucket,
                            stop_ttl_mode=(obj.get("stop_ttl_mode") or "canary"),
                            trailing_mode=(obj.get("trailing_mode") or "canary"),
                            reason_code=(obj.get("reason_code") or "ATR_POLICY_ACTIVE"),
                            policy_ver=_safe_int(obj.get("policy_ver"), 0),
                            updated_at_ms=_safe_int(obj.get("updated_at_ms"), 0),
                            kill_switch_active=False, last_good_used=False,
                        )
                        self._cache[cache_key] = (now_ms, out)
                        return out
                    else:
                        # Active key is corrupted — try last_good
                        logger.warning(
                            "resolver: active key corrupted (%s) — falling back to last_good: %s",
                            invalid, active_key,
                        )
                        raw_lg = r.get(lg_key)
                        if raw_lg:
                            try:
                                lg_obj = json.loads(raw_lg)
                                lg_invalid = self._validate_policy_obj(lg_obj)
                                if lg_invalid is None:
                                    out = self._build_resolution(
                                        hit=True, level=level + "_last_good", active_key=lg_key,
                                        source=source, symbol=symbol, scenario=scenario,
                                        regime=regime, risk_horizon_bucket=risk_horizon_bucket,
                                        stop_ttl_mode=(lg_obj.get("stop_ttl_mode") or "canary"),
                                        trailing_mode=(lg_obj.get("trailing_mode") or "canary"),
                                        reason_code="ACTIVE_CORRUPTED_FALLBACK_LAST_GOOD",
                                        policy_ver=_safe_int(lg_obj.get("policy_ver"), 0),
                                        updated_at_ms=_safe_int(lg_obj.get("updated_at_ms"), 0),
                                        kill_switch_active=False, last_good_used=True,
                                    )
                                    self._cache[cache_key] = (now_ms, out)
                                    return out
                            except Exception:
                                pass
                        # last_good also missing/corrupted → continue to next candidate
                        continue
            except Exception:
                pass

            # ── 2. Active key doesn't exist — try last_good for this candidate
            try:
                raw_lg = r.get(lg_key)
                if raw_lg:
                    lg_obj = json.loads(raw_lg)
                    lg_invalid = self._validate_policy_obj(lg_obj)
                    if lg_invalid is None:
                        out = self._build_resolution(
                            hit=True, level=level + "_last_good", active_key=lg_key,
                            source=source, symbol=symbol, scenario=scenario,
                            regime=regime, risk_horizon_bucket=risk_horizon_bucket,
                            stop_ttl_mode=(lg_obj.get("stop_ttl_mode") or "canary"),
                            trailing_mode=(lg_obj.get("trailing_mode") or "canary"),
                            reason_code="ACTIVE_MISSING_FALLBACK_LAST_GOOD",
                            policy_ver=_safe_int(lg_obj.get("policy_ver"), 0),
                            updated_at_ms=_safe_int(lg_obj.get("updated_at_ms"), 0),
                            kill_switch_active=False, last_good_used=True,
                        )
                        self._cache[cache_key] = (now_ms, out)
                        return out
            except Exception:
                pass

        # ── 3. Full miss → canary/shadow ──────────────────────────────────
        out = self._build_resolution(
            hit=False, level="miss", active_key="",
            source=source, symbol=symbol, scenario=scenario,
            regime=regime, risk_horizon_bucket=risk_horizon_bucket,
            stop_ttl_mode="canary", trailing_mode="canary",
            reason_code="ATR_POLICY_MISS",
            policy_ver=0, updated_at_ms=0,
            kill_switch_active=False, last_good_used=False,
        )
        self._cache[cache_key] = (now_ms, out)
        return out


_RESOLVER: ATRPolicyResolver | None = None


def get_atr_policy_resolver() -> ATRPolicyResolver:
    global _RESOLVER
    if _RESOLVER is None:
        _RESOLVER = ATRPolicyResolver()
    return _RESOLVER
