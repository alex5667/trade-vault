from __future__ import annotations

"""regime_conditional_execution.py — Task 3.1: Regime-Conditional Execution Engine.

Maps (vol_regime × trend_regime) → ExecutionPolicy that overrides exit knobs
(tp1_target_r / tp_ratios / trail_profile / atr_mult) per institutional best-practice:

  - High-vol trending  → wide TP, tight trailing.
  - Low-vol mean-revert → fast TP1 (0.3R) scale-out, no trailing.
  - Choppy / range / squeeze → skip signal entirely.

Bucket key convention mirrors ``confidence_calibration_v2.json`` hierarchical
fallback: ``{symbol}|{vol}|{trend}`` → ``GLOBAL|{vol}|{trend}`` →
``GLOBAL|any|{trend}`` → ``global``.

Shadow vs. enforce
------------------
Default is SHADOW (``REGIME_EXEC_ENGINE_ENFORCE=0``): writes counterfactual
``regime_exec_*`` indicators only — does not override real execution. Switch
to ``REGIME_EXEC_ENGINE_ENFORCE=1`` after replay-quantify shows positive EV/R.

Runtime overrides
-----------------
Lazy Redis reader on ``autocal:regime_exec:state`` (HMAC-verified, TTL cache).
Each bucket entry may carry ``"enforce": 1`` to selectively activate without
flipping the global flag.

Public API
----------
- ``RegimeConditionalExecutionEngine`` — config + select_policy entrypoint.
- ``ExecutionPolicy`` — dataclass returned per bucket lookup.
- ``get_engine()`` — module-level singleton (lazy).
"""

import hashlib
import hmac
import json
import logging
import os
import threading
import time
from dataclasses import dataclass, field, asdict
from typing import Any

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants / labels
# ---------------------------------------------------------------------------

# Canonical vol regime labels (from VolRegimeTracker.snapshot_typed).
VOL_LABELS = ("shock", "normal", "calm", "na")

# Canonical trend regime labels — derived from existing rg_for_overrides logic
# in signal_pipeline (see signal_pipeline.py:1907-1911).
TREND_LABELS = (
    "trending",
    "trending_bear",
    "range",
    "expansion",
    "squeeze",
    "mixed",
    "na",
)

_AUTOCAL_REDIS_KEY = "autocal:regime_exec:state"
_AUTOCAL_REFRESH_MS = 30_000
_AUTOCAL_STALE_MS = 30 * 60 * 1000


# ---------------------------------------------------------------------------
# Policy dataclass
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class ExecutionPolicy:
    """Exit-knob override returned per (vol×trend) bucket lookup.

    Fields with ``None`` mean "do not override — keep caller's existing value".
    """

    bucket: str = "global"
    skip: bool = False
    tp1_target_r: float | None = None
    tp_ratios: list[float] | None = None
    trail_profile: str | None = None
    trail_atr_mult: float | None = None
    arm_threshold_r: float | None = None
    enforce: bool | None = None  # per-bucket override of global enforce
    fallback_depth: int = 0  # 0 = exact match, higher = more general
    reason: str = ""

    def to_dict(self) -> dict[str, Any]:
        d = asdict(self)
        return d


# ---------------------------------------------------------------------------
# Default buckets — institutional best-practice mapping
# ---------------------------------------------------------------------------


def _default_buckets() -> dict[str, dict[str, Any]]:
    """Return canonical default bucket table.

    Keys follow ``GLOBAL|{vol}|{trend}`` convention. Wildcard ``any`` matches
    any value at that level.
    """
    return {
        # ── High-vol regimes ───────────────────────────────────────────────
        "GLOBAL|shock|trending": {
            "tp1_target_r": 1.5,
            "tp_ratios": [0.40, 0.30, 0.30],
            "trail_profile": "rocket_v1",
            "trail_atr_mult": 1.0,
            "reason": "high_vol_trending: wide TP + tight trail",
        },
        "GLOBAL|shock|trending_bear": {
            "tp1_target_r": 1.5,
            "tp_ratios": [0.40, 0.30, 0.30],
            "trail_profile": "rocket_v1_bear",
            "trail_atr_mult": 1.0,
            "reason": "high_vol_trending_bear: wide TP + tight trail (bear)",
        },
        "GLOBAL|shock|range": {
            "tp1_target_r": 0.5,
            "tp_ratios": [0.60, 0.40],
            "trail_profile": "range_protective",
            "reason": "high_vol_range: low-quality, breakeven only",
        },
        "GLOBAL|shock|expansion": {
            "tp1_target_r": 1.5,
            "tp_ratios": [0.40, 0.30, 0.30],
            "trail_profile": "expansion_v1",
            "trail_atr_mult": 1.5,
            "reason": "high_vol_expansion: wide trail (survives noise)",
        },
        # ── Low-vol regimes ───────────────────────────────────────────────
        "GLOBAL|calm|range": {
            "tp1_target_r": 0.3,
            "tp_ratios": [0.70, 0.30],
            "trail_profile": "range_protective",
            "reason": "low_vol_range: fast TP1 scale-out, no trailing",
        },
        "GLOBAL|calm|trending": {
            "tp1_target_r": 1.0,
            "tp_ratios": [0.50, 0.30, 0.20],
            "trail_profile": "rocket_v1",
            "trail_atr_mult": 1.5,
            "reason": "low_vol_trending: normal TP, wider trail",
        },
        "GLOBAL|calm|trending_bear": {
            "tp1_target_r": 1.0,
            "tp_ratios": [0.50, 0.30, 0.20],
            "trail_profile": "rocket_v1_bear",
            "trail_atr_mult": 1.2,
            "reason": "low_vol_trending_bear: normal TP, moderate trail",
        },
        "GLOBAL|calm|expansion": {
            "tp1_target_r": 1.2,
            "tp_ratios": [0.40, 0.30, 0.30],
            "trail_profile": "expansion_v1",
            "trail_atr_mult": 1.5,
            "reason": "low_vol_expansion: pre-expansion, wider trail",
        },
        # ── Normal-vol regimes ────────────────────────────────────────────
        "GLOBAL|normal|trending": {
            "tp1_target_r": 1.0,
            "tp_ratios": [0.50, 0.30, 0.20],
            "trail_profile": "rocket_v1",
            "trail_atr_mult": 1.2,
            "reason": "normal_vol_trending: baseline trend",
        },
        "GLOBAL|normal|trending_bear": {
            "tp1_target_r": 1.0,
            "tp_ratios": [0.50, 0.30, 0.20],
            "trail_profile": "rocket_v1_bear",
            "trail_atr_mult": 1.0,
            "reason": "normal_vol_trending_bear: baseline bear",
        },
        "GLOBAL|normal|expansion": {
            "tp1_target_r": 1.5,
            "tp_ratios": [0.40, 0.30, 0.30],
            "trail_profile": "expansion_v1",
            "trail_atr_mult": 1.5,
            "reason": "normal_vol_expansion",
        },
        # ── Skip buckets — choppy / unclear ───────────────────────────────
        "GLOBAL|normal|range": {
            "skip": True,
            "reason": "normal_vol_range: choppy — skip",
        },
        "GLOBAL|any|squeeze": {
            "skip": True,
            "reason": "squeeze regime — skip until breakout confirms",
        },
        "GLOBAL|any|mixed": {
            "skip": True,
            "reason": "mixed regime — skip until resolution",
        },
        # ── Wildcard fallbacks for na ─────────────────────────────────────
        "GLOBAL|na|trending": {
            "tp1_target_r": 1.0,
            "tp_ratios": [0.50, 0.30, 0.20],
            "trail_profile": "rocket_v1",
            "trail_atr_mult": 1.2,
            "reason": "vol_na_trending: assume normal trend",
        },
        "GLOBAL|na|trending_bear": {
            "tp1_target_r": 1.0,
            "tp_ratios": [0.50, 0.30, 0.20],
            "trail_profile": "rocket_v1_bear",
            "trail_atr_mult": 1.0,
            "reason": "vol_na_trending_bear",
        },
        # ── Global default — preserves caller behavior (no overrides) ────
        "global": {
            "reason": "global default — passthrough (no overrides)",
        },
    }


# ---------------------------------------------------------------------------
# Env helpers
# ---------------------------------------------------------------------------


def _env(k: str, d: str = "") -> str:
    return os.environ.get(k, d)


def _env_bool(k: str, d: bool) -> bool:
    raw = _env(k, "")
    if not raw:
        return d
    return raw.strip().lower() in ("1", "true", "yes", "on")


def _env_int(k: str, d: int) -> int:
    try:
        return int(_env(k, str(d)))
    except Exception:
        return d


# ---------------------------------------------------------------------------
# Autocal runtime overrides reader (HMAC + TTL cache)
# ---------------------------------------------------------------------------


class _RegimeExecOverridesReader:
    """Reads ``autocal:regime_exec:state`` snapshot with TTL cache.

    Snapshot shape::

        {
          "ts_ms": int,
          "buckets": {"GLOBAL|shock|trending": {"trail_profile": ..., ...}, ...},
          "sig": "<hmac-sha256-hex>"  # optional, verified if secret set
        }
    """

    def __init__(
        self,
        redis_client: Any,
        *,
        redis_key: str = _AUTOCAL_REDIS_KEY,
        refresh_ms: int = _AUTOCAL_REFRESH_MS,
        stale_ms: int = _AUTOCAL_STALE_MS,
        hmac_secret: str = "",
    ) -> None:
        self._redis = redis_client
        self._key = redis_key
        self._refresh_ms = max(1000, refresh_ms)
        self._stale_ms = max(self._refresh_ms, stale_ms)
        self._hmac_secret = hmac_secret
        self._lock = threading.Lock()
        self._snapshot_buckets: dict[str, dict[str, Any]] = {}
        self._snapshot_ts_ms: int = 0
        self._last_refresh_ms: int = 0

    def _maybe_refresh(self) -> None:
        now_ms = int(time.time() * 1000)
        if (now_ms - self._last_refresh_ms) < self._refresh_ms:
            return
        with self._lock:
            if (now_ms - self._last_refresh_ms) < self._refresh_ms:
                return
            self._last_refresh_ms = now_ms
            try:
                raw = self._redis.get(self._key)
                if not raw:
                    return
                data = json.loads(
                    raw if isinstance(raw, (str, bytes, bytearray)) else str(raw)
                )
                if self._hmac_secret and "sig" in data:
                    expected = data.pop("sig")
                    canon = json.dumps(
                        data, sort_keys=True, separators=(",", ":")
                    ).encode()
                    actual = hmac.new(
                        self._hmac_secret.encode(), canon, hashlib.sha256
                    ).hexdigest()
                    if not hmac.compare_digest(actual, str(expected)):
                        logger.warning(
                            "regime_exec overrides: HMAC mismatch — ignoring"
                        )
                        return
                self._snapshot_buckets = data.get("buckets") or {}
                self._snapshot_ts_ms = int(data.get("ts_ms") or 0)
            except Exception as e:
                logger.debug(
                    "regime_exec overrides: refresh fail (fail-open): %s", e
                )

    def get_bucket(self, bucket_key: str) -> dict[str, Any] | None:
        self._maybe_refresh()
        if not self._snapshot_buckets:
            return None
        age_ms = int(time.time() * 1000) - self._snapshot_ts_ms
        if age_ms > self._stale_ms:
            return None
        return self._snapshot_buckets.get(bucket_key)


# ---------------------------------------------------------------------------
# Prometheus metrics (opt-in)
# ---------------------------------------------------------------------------

try:
    from prometheus_client import Counter

    _POLICY_SELECT_TOTAL = Counter(
        "regime_exec_policy_select_total",
        "Regime-conditional execution policy selections",
        ["bucket", "decision"],  # decision = apply|shadow|skip|passthrough
    )
    _VETO_TOTAL = Counter(
        "regime_exec_veto_total",
        "Regime-conditional execution skip vetos (enforce-mode)",
        ["bucket", "symbol", "kind"],
    )
    _SHADOW_DIFF_TOTAL = Counter(
        "regime_exec_shadow_diff_total",
        "Shadow-vs-actual differences in regime exec policy fields",
        ["bucket", "field"],
    )
except Exception:  # pragma: no cover — prometheus optional
    _POLICY_SELECT_TOTAL = None
    _VETO_TOTAL = None
    _SHADOW_DIFF_TOTAL = None


# ---------------------------------------------------------------------------
# Engine
# ---------------------------------------------------------------------------


@dataclass
class RegimeConditionalExecutionEngine:
    """Selects ExecutionPolicy from (vol×trend) regime bucket.

    Configuration sources (in priority order):
      1. Runtime override from Redis ``autocal:regime_exec:state`` (per-bucket).
      2. Static JSON file at ``REGIME_EXEC_BUCKETS_PATH`` (if set + readable).
      3. ``_default_buckets()`` — institutional defaults.
    """

    buckets: dict[str, dict[str, Any]] = field(default_factory=_default_buckets)
    enforce_global: bool = False
    skip_choppy: bool = False  # only veto when this AND policy.skip both True
    enabled: bool = True
    overrides_reader: _RegimeExecOverridesReader | None = None

    # ----- Construction ----------------------------------------------------

    @classmethod
    def from_env(
        cls,
        redis_client: Any | None = None,
    ) -> "RegimeConditionalExecutionEngine":
        enabled = _env_bool("REGIME_EXEC_ENGINE_ENABLED", True)
        enforce = _env_bool("REGIME_EXEC_ENGINE_ENFORCE", False)
        skip_choppy = _env_bool("REGIME_EXEC_SKIP_CHOPPY", False)
        buckets = _default_buckets()

        cfg_path = _env("REGIME_EXEC_BUCKETS_PATH", "")
        if cfg_path:
            try:
                with open(cfg_path, encoding="utf-8") as fh:
                    extra = json.load(fh)
                if isinstance(extra, dict):
                    buckets.update(extra)
                    logger.info(
                        "regime_exec: loaded %d buckets from %s",
                        len(extra),
                        cfg_path,
                    )
            except Exception as e:
                logger.warning(
                    "regime_exec: failed to load buckets from %s: %s",
                    cfg_path,
                    e,
                )

        reader: _RegimeExecOverridesReader | None = None
        if redis_client is not None and _env_bool(
            "AUTOCAL_REGIME_EXEC_READ_ENABLED", False
        ):
            secret = (
                _env("REGIME_EXEC_AUTOCAL_HMAC_SECRET", "")
                or _env("RECS_HMAC_SECRET", "")
                or _env("LAYERS_CAL_HMAC_SECRET", "")
            )
            reader = _RegimeExecOverridesReader(
                redis_client,
                redis_key=_env("AUTOCAL_REGIME_EXEC_KEY", _AUTOCAL_REDIS_KEY),
                refresh_ms=_env_int(
                    "AUTOCAL_REGIME_EXEC_REFRESH_MS", _AUTOCAL_REFRESH_MS
                ),
                stale_ms=_env_int(
                    "AUTOCAL_REGIME_EXEC_STALE_MS", _AUTOCAL_STALE_MS
                ),
                hmac_secret=secret,
            )

        return cls(
            buckets=buckets,
            enforce_global=enforce,
            skip_choppy=skip_choppy,
            enabled=enabled,
            overrides_reader=reader,
        )

    # ----- Public API ------------------------------------------------------

    def is_enforce(self) -> bool:
        return bool(self.enabled and self.enforce_global)

    def select_policy(
        self,
        *,
        vol_regime: str,
        trend_regime: str,
        symbol: str = "",
    ) -> ExecutionPolicy:
        """Resolve (vol×trend) bucket → ExecutionPolicy.

        Lookup follows hierarchical fallback similar to ``confidence_calibration_v2``:
        more specific keys take precedence over wildcards.
        """
        if not self.enabled:
            return ExecutionPolicy(bucket="disabled", reason="engine disabled")

        v = _norm_vol(vol_regime)
        t = _norm_trend(trend_regime)
        sym = (symbol or "").upper() or "GLOBAL"

        candidates = self._candidate_keys(sym=sym, v=v, t=t)

        cfg: dict[str, Any] | None = None
        bkey = "global"
        depth = 0
        for i, k in enumerate(candidates):
            # Runtime override wins over static.
            if self.overrides_reader is not None:
                rt = self.overrides_reader.get_bucket(k)
                if rt:
                    cfg = rt
                    bkey = k
                    depth = i
                    break
            if k in self.buckets:
                cfg = self.buckets[k]
                bkey = k
                depth = i
                break

        if cfg is None:
            cfg = self.buckets.get("global") or {}
            bkey = "global"
            depth = len(candidates) - 1

        policy = ExecutionPolicy(
            bucket=bkey,
            skip=bool(cfg.get("skip", False)),
            tp1_target_r=_opt_float(cfg.get("tp1_target_r")),
            tp_ratios=_opt_list_float(cfg.get("tp_ratios")),
            trail_profile=_opt_str(cfg.get("trail_profile")),
            trail_atr_mult=_opt_float(cfg.get("trail_atr_mult")),
            arm_threshold_r=_opt_float(cfg.get("arm_threshold_r")),
            enforce=(bool(cfg["enforce"]) if "enforce" in cfg else None),
            fallback_depth=depth,
            reason=str(cfg.get("reason", "")),
        )

        # Metric: count selections per (bucket, decision) — decision describes
        # what the engine WOULD do; the *actual* enforcement is decided by
        # ``effective_enforce(policy)`` at call site.
        decision = (
            "skip"
            if policy.skip
            else ("apply" if self._has_overrides(policy) else "passthrough")
        )
        if _POLICY_SELECT_TOTAL is not None:
            try:
                _POLICY_SELECT_TOTAL.labels(bkey, decision).inc()
            except Exception:
                pass

        return policy

    def effective_enforce(self, policy: ExecutionPolicy) -> bool:
        """Whether this policy should override real execution (vs. shadow-only)."""
        if not self.enabled:
            return False
        # Per-bucket enforce overrides global flag in both directions.
        if policy.enforce is True:
            return True
        if policy.enforce is False:
            return False
        return self.enforce_global

    def should_skip(self, policy: ExecutionPolicy) -> bool:
        """Apply skip-veto only when global enforce + skip_choppy both on."""
        return (
            self.effective_enforce(policy)
            and policy.skip
            and self.skip_choppy
        )

    # ----- Internals -------------------------------------------------------

    @staticmethod
    def _has_overrides(policy: ExecutionPolicy) -> bool:
        return any(
            x is not None
            for x in (
                policy.tp1_target_r,
                policy.tp_ratios,
                policy.trail_profile,
                policy.trail_atr_mult,
                policy.arm_threshold_r,
            )
        )

    @staticmethod
    def _candidate_keys(*, sym: str, v: str, t: str) -> list[str]:
        """Hierarchical fallback chain (specific → general)."""
        keys = [
            f"{sym}|{v}|{t}",
            f"{sym}|{v}|any",
            f"{sym}|any|{t}",
            f"{sym}|any|any",
            f"GLOBAL|{v}|{t}",
            f"GLOBAL|{v}|any",
            f"GLOBAL|any|{t}",
            f"GLOBAL|any|any",
            "global",
        ]
        # Deduplicate while preserving order (sym=GLOBAL collapses upper half).
        seen: set[str] = set()
        out: list[str] = []
        for k in keys:
            if k not in seen:
                seen.add(k)
                out.append(k)
        return out


# ---------------------------------------------------------------------------
# Normalizers / coercers
# ---------------------------------------------------------------------------


def _norm_vol(v: str | None) -> str:
    if not v:
        return "na"
    s = str(v).strip().lower()
    if s in ("none", "unknown", "nan", "null", ""):
        return "na"
    if s not in VOL_LABELS:
        # Permissive: anything else collapses to na.
        return "na"
    return s


def _norm_trend(t: str | None) -> str:
    if not t:
        return "na"
    s = str(t).strip().lower()
    # Pipeline emits compound labels like "trending_bear_short_trend_follow";
    # prefer the longest matching canonical prefix.
    if "trending_bear" in s:
        return "trending_bear"
    if "trend" in s:
        return "trending"
    if "range" in s:
        return "range"
    if "expansion" in s:
        return "expansion"
    if "squeeze" in s:
        return "squeeze"
    if "mixed" in s:
        return "mixed"
    return "na"


def _opt_float(v: Any) -> float | None:
    if v is None:
        return None
    try:
        f = float(v)
    except Exception:
        return None
    return f


def _opt_str(v: Any) -> str | None:
    if v is None:
        return None
    s = str(v).strip()
    return s or None


def _opt_list_float(v: Any) -> list[float] | None:
    if v is None:
        return None
    if not isinstance(v, (list, tuple)):
        return None
    out: list[float] = []
    for x in v:
        try:
            out.append(float(x))
        except Exception:
            return None
    return out or None


# ---------------------------------------------------------------------------
# Module singleton
# ---------------------------------------------------------------------------

_ENGINE: RegimeConditionalExecutionEngine | None = None
_ENGINE_LOCK = threading.Lock()


def get_engine(redis_client: Any | None = None) -> RegimeConditionalExecutionEngine | None:
    """Lazy singleton. Returns None when disabled via env."""
    global _ENGINE
    if _ENGINE is not None:
        return _ENGINE if _ENGINE.enabled else None
    with _ENGINE_LOCK:
        if _ENGINE is None:
            try:
                _ENGINE = RegimeConditionalExecutionEngine.from_env(
                    redis_client=redis_client
                )
            except Exception as e:
                logger.debug("regime_exec engine init failed (fail-open): %s", e)
                return None
        return _ENGINE if _ENGINE.enabled else None


def reset_engine_for_tests() -> None:
    """Test helper — clear singleton."""
    global _ENGINE
    with _ENGINE_LOCK:
        _ENGINE = None


# ---------------------------------------------------------------------------
# Shadow-diff helper (for signal_pipeline wiring)
# ---------------------------------------------------------------------------


def record_shadow_diff(
    policy: ExecutionPolicy,
    *,
    actual_trail_profile: str | None,
    actual_tp_ratios: list[float] | None,
    actual_tp1_target_r: float | None,
) -> dict[str, Any]:
    """Compute per-field shadow diff and emit Prometheus counters.

    Returns a dict suitable for stuffing into ``indicators`` as
    ``regime_exec_shadow_diff`` for downstream audit/Replay.
    """
    diff: dict[str, Any] = {}
    if policy.trail_profile and policy.trail_profile != actual_trail_profile:
        diff["trail_profile"] = {
            "proposed": policy.trail_profile,
            "actual": actual_trail_profile,
        }
        if _SHADOW_DIFF_TOTAL is not None:
            try:
                _SHADOW_DIFF_TOTAL.labels(policy.bucket, "trail_profile").inc()
            except Exception:
                pass
    if policy.tp_ratios and list(policy.tp_ratios) != (actual_tp_ratios or []):
        diff["tp_ratios"] = {
            "proposed": list(policy.tp_ratios),
            "actual": actual_tp_ratios,
        }
        if _SHADOW_DIFF_TOTAL is not None:
            try:
                _SHADOW_DIFF_TOTAL.labels(policy.bucket, "tp_ratios").inc()
            except Exception:
                pass
    if (
        policy.tp1_target_r is not None
        and policy.tp1_target_r > 0
        and policy.tp1_target_r != (actual_tp1_target_r or 0.0)
    ):
        diff["tp1_target_r"] = {
            "proposed": policy.tp1_target_r,
            "actual": actual_tp1_target_r,
        }
        if _SHADOW_DIFF_TOTAL is not None:
            try:
                _SHADOW_DIFF_TOTAL.labels(policy.bucket, "tp1_target_r").inc()
            except Exception:
                pass
    return diff


def emit_veto_metric(policy: ExecutionPolicy, *, symbol: str, kind: str) -> None:
    """Emit veto counter when enforce-mode skip occurs."""
    if _VETO_TOTAL is None:
        return
    try:
        _VETO_TOTAL.labels(policy.bucket, symbol or "unknown", kind or "na").inc()
    except Exception:
        pass
