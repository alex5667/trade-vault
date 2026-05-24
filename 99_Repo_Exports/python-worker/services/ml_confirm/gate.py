from __future__ import annotations

import asyncio
import hashlib
import json
import math
import os
import time
from dataclasses import dataclass
from typing import Any

import redis

from core.bucket2_v1 import derive_bucket2_label
from core.champion_cfg_validator import validate_champion_cfg
from core.edge_stack_mh_v1 import EdgeStackMHModelV1
from core.feature_engineering import (
    RobustScalerPack,
    apply_transform,
    bucketize,
    derive_regime_label,
    derive_session_label,
)
from core.meta_model_lr import MetaModelLR
from services.ml_calibration import PlattLogitCalibrator
from utils.time_utils import get_ny_time_millis
import contextlib
from core.redis_keys import RedisStreams as RS

# Prometheus metrics (optional, fail-open if not available)
class _MockMetric:
    def labels(self, **kwargs): return self
    def inc(self, *args, **kwargs): pass
    def set(self, *args, **kwargs): pass
    def observe(self, *args, **kwargs): pass
    def dec(self, *args, **kwargs): pass

try:
    from prometheus_client import Counter, Gauge, Histogram
    PROMETHEUS_AVAILABLE = True
except Exception:
    PROMETHEUS_AVAILABLE = False
    Counter = Gauge = Histogram = lambda *args, **kwargs: _MockMetric()

# Import centralized metrics from registry (fail-open if not available)
try:
    from services.observability.metrics_registry import (
        ml_confirm_cfg_present,
        ml_confirm_cfg_valid,
        ml_confirm_enforce_share,
        ml_confirm_errors_total,
        ml_confirm_events_total,
        ml_confirm_latency_seconds,
        ml_confirm_model_load_seconds,
        ml_confirm_model_loaded,
        ml_missing_critical_total,
    )
    METRICS_REGISTRY_AVAILABLE = True
except Exception:
    METRICS_REGISTRY_AVAILABLE = False
    ml_confirm_events_total = ml_confirm_errors_total = ml_confirm_cfg_present = \
    ml_confirm_cfg_valid = ml_confirm_enforce_share = ml_confirm_model_loaded = \
    ml_confirm_model_load_seconds = ml_confirm_latency_seconds = ml_missing_critical_total = \
    _MockMetric()

try:
    import joblib  # type: ignore
except Exception:  # pragma: no cover
    joblib = None  # type: ignore


# Dual-emit observability: tracks parallel challenger scoring outcomes so
# operators can detect when the challenger SHADOW path drops out silently —
# distinct from champion-side errors.
try:
    if PROMETHEUS_AVAILABLE:
        _ml_dual_emit_total = Counter(
            "ml_confirm_dual_emit_total",
            "Challenger SHADOW scoring outcomes alongside champion",
            ["role", "kind", "outcome"],
        )
    else:
        _ml_dual_emit_total = _MockMetric()
except ValueError:
    from prometheus_client import REGISTRY
    _ml_dual_emit_total = REGISTRY._names_to_collectors.get(  # type: ignore[assignment]
        "ml_confirm_dual_emit_total"
    ) or _MockMetric()



from .decision_policy import MLConfirmDecision, DecisionPolicyMixin
from .config_loader import ConfigLoaderMixin
from .model_loader import ModelLoaderMixin
from .feature_vectorizer import FeatureVectorizerMixin
from .selective_policy import SelectivePolicyMixin
from .replay_capture import ReplayCaptureMixin
from .metrics_writer import MetricsWriterMixin

from .utils import (
    _safe_loads_ex,
    _safe_loads,
    _json_safe,
    _scenario_norm,
    _get_floor,
    _f,
    _bucket_from_scenario,
    _canon_sid,
    _canonical_sid,
    _make_sid,
    _mk_crypto_sid,
    _normalize_crypto_sid,
    _normalize_sid,
    _now_ms,
    _should_sample,
    _stable_hash_u64,
    _stable_sample,
    _stable_u01
)


class MLConfirmGate(ConfigLoaderMixin, ModelLoaderMixin, FeatureVectorizerMixin, DecisionPolicyMixin, SelectivePolicyMixin, ReplayCaptureMixin, MetricsWriterMixin):
    def __init__(
        self,
        *,
        r: redis.Redis,
        mode: str,
        fail_policy: str,
        champion_key: str,
        challenger_key: str,
    ) -> None:
        self.r = r
        self.mode = (mode or "OFF").upper()
        self.fail_policy = (fail_policy or "OPEN").upper()
        self.champion_key = champion_key
        self.challenger_key = challenger_key

        self._cfg_source = "none"  # champion|challenger|hash_fallback|none
        self._cfg_hash_key = os.getenv("ML_CFG_HASH_KEY", "cfg:ml_confirm")

        self._cache_loaded_ms: int = 0
        self._cache_ttl_ms: int = int(os.getenv("ML_MODEL_CACHE_TTL_MS", "60000"))
        self._cfg: dict[str, Any] = {}
        self.ab_variant = ""
        self._model: Any = None
        self._model_load_error: str = ""  # Detailed error reason when model fails to load
        self._last_error_log_ms: int = 0  # Throttle error logging
        self._check_call_count: int = 0  # Throttle DEBUG check log

        # last cfg diagnostics (used when returning ERR_* decisions)
        self._cfg_key_used: str = ""
        self._cfg_source: str = ""
        self._cfg_raw_len: int = 0
        self._cfg_parse_err: str = ""

        # metrics
        self._metrics_stream = os.getenv("ML_CONFIRM_METRICS_STREAM", RS.ML_CONFIRM_METRICS)
        self._metrics_enable = int(os.getenv("ML_CONFIRM_METRICS_ENABLE", "1") or 1) == 1
        # P1 fix: было hardcode 50000 → теперь через ENV (default 200000)
        self._metrics_maxlen = int(os.getenv("ML_CONFIRM_METRICS_MAXLEN", "200000") or 200000)
        try:
            self._metrics_sample = float(os.getenv("ML_CONFIRM_METRICS_SAMPLE", "1.0") or 1.0)
        except Exception:
            self._metrics_sample = 1.0

        # selective prediction defaults (OFF unless enabled)
        try:
            self._abstain_band = float(os.getenv("ML_CONFIRM_ABSTAIN_BAND", "0.0") or 0.0)
        except Exception:
            self._abstain_band = 0.0
        try:
            self._conf_min = float(os.getenv("ML_CONFIRM_CONF_MIN", "0.0") or 0.0)
        except Exception:
            self._conf_min = 0.0
        self._abstain_on_missing = int(os.getenv("ML_CONFIRM_ABSTAIN_ON_MISSING", "0") or 0) == 1
        try:
            self._p_min_hard_floor = float(os.getenv("ML_CONFIRM_P_MIN_HARD_FLOOR", "0.0") or 0.0)
        except Exception:
            self._p_min_hard_floor = 0.0

        # per-symbol mode overrides
        self._mode_by_symbol: dict[str, str] = {}
        self._enforce_share_by_symbol: dict[str, float] = {}

        # golden replay capture
        self._replay_capture = int(os.getenv("ML_REPLAY_CAPTURE_ENABLE", "0") or 0) == 1
        self._replay_stream = os.getenv("ML_REPLAY_INPUTS_STREAM", RS.ML_CONFIRM_INPUTS)
        try:
            self._replay_sample = float(os.getenv("ML_REPLAY_INPUTS_SAMPLE", "0.01") or 0.01)
        except Exception:
            self._replay_sample = 0.01
        self._replay_maxlen = int(os.getenv("ML_REPLAY_INPUTS_MAXLEN", "200000") or 200000)

        # calibration layer (optional)
        self._calibrator: PlattLogitCalibrator | None = None
        self._calibrate_enabled = int(os.getenv("ML_CALIBRATION_ENABLE", "1") or 1) == 1
        self._calib_type = "none"

        # Dual-emit (Patch 3 follow-up): challenger scored alongside champion in
        # SHADOW so ml_outcome_*{kind=<challenger>} becomes observable for the
        # v14_of_auto_promote live PR-AUC/ECE gate. Decoupled cfg/model storage —
        # champion path is untouched; challenger is fire-and-forget.
        self._chal_cfg: dict[str, Any] = {}
        self._chal_model: Any = None
        self._chal_model_load_error: str = ""
        self._chal_cache_loaded_ms: int = 0
        self._chal_cfg_raw_len: int = 0
        self._dual_emit_enabled: bool = (
            int(os.getenv("ML_DUAL_EMIT_CHALLENGER", "0") or 0) == 1
        )
        try:
            self._dual_emit_sample: float = float(
                os.getenv("ML_DUAL_EMIT_CHALLENGER_SAMPLE", "1.0") or 1.0
            )
        except Exception:
            self._dual_emit_sample = 1.0

        # Use centralized metrics from registry (fail-open if not available)
        # Note: metrics_registry defines metrics with same names, so we can use them directly
        # We keep local references for backward compatibility and to handle mock metrics
        if METRICS_REGISTRY_AVAILABLE:
            self._metrics_events_total = ml_confirm_events_total or _MockMetric()
            self._metrics_errors_total = ml_confirm_errors_total or _MockMetric()
            self._metrics_cfg_present = ml_confirm_cfg_present or _MockMetric()
            self._metrics_cfg_valid = ml_confirm_cfg_valid or _MockMetric()
            self._metrics_enforce_share = ml_confirm_enforce_share or _MockMetric()
            self._metrics_model_loaded = ml_confirm_model_loaded or _MockMetric()
            self._metrics_model_load_seconds = ml_confirm_model_load_seconds or _MockMetric()
            self._metrics_latency_seconds = ml_confirm_latency_seconds or _MockMetric()
            # Additional local metric for last successful load timestamp
            self._metrics_last_successful_load_ts: Any = _MockMetric()
            if PROMETHEUS_AVAILABLE:
                try:
                    self._metrics_last_successful_load_ts = Gauge(
                        "ml_confirm_last_successful_load_ts_seconds",
                        "Timestamp of last successful model load",
                        ["kind"]
                    )
                except Exception:
                    # In tests/multiple instances, might already be registered
                    from prometheus_client import REGISTRY
                    collector = REGISTRY._names_to_collectors.get("ml_confirm_last_successful_load_ts_seconds")
                    if collector is not None:
                        self._metrics_last_successful_load_ts = collector
            # cfg_defaulted_total is tracked via ml_missing_critical_total
            self._metrics_cfg_defaulted_total = ml_missing_critical_total
        else:
            # Mock metrics when registry is not available
            self._metrics_events_total = _MockMetric()
            self._metrics_errors_total = _MockMetric()
            self._metrics_cfg_present = _MockMetric()
            self._metrics_cfg_valid = _MockMetric()
            self._metrics_enforce_share = _MockMetric()
            self._metrics_model_loaded = _MockMetric()
            self._metrics_model_load_seconds = _MockMetric()
            self._metrics_last_successful_load_ts = _MockMetric()
            self._metrics_latency_seconds = _MockMetric()
            self._metrics_cfg_defaulted_total = _MockMetric()

    @staticmethod
    def from_env(redis_client: Any | None = None) -> MLConfirmGate:
        if redis_client is not None:
            r = redis_client
        else:
            # Support ML_REDIS_URL for separate config Redis, fallback to REDIS_URL
            redis_url = os.getenv("ML_REDIS_URL") or os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
            r = redis.Redis.from_url(redis_url, decode_responses=True)

        mode = os.getenv("ML_CONFIRM_MODE", "SHADOW")
        fail_policy = os.getenv("ML_CONFIRM_FAIL_POLICY", "OPEN")
        champion_key = os.getenv("ML_CFG_CHAMPION_KEY", "cfg:ml_confirm:champion")
        challenger_key = os.getenv("ML_CFG_CHALLENGER_KEY", "cfg:ml_confirm:challenger")

        return MLConfirmGate(
            r=r,
            mode=mode,
            fail_policy=fail_policy,
            champion_key=champion_key,
            challenger_key=challenger_key,
        )

    def _fail_allow(self) -> bool:
        # FAIL_OPEN => allow, FAIL_CLOSED => block
        return self.fail_policy != "CLOSED"

    def _load_challenger_only_sync(self) -> None:
        """Sync-load `cfg:ml_confirm:challenger` into self._chal_cfg/_chal_model.

        Independent of champion path: champion stays primary for the gate
        decision; challenger is read solely so we can score it in SHADOW and
        emit a parallel `metrics:ml_confirm` row. All failures are silent —
        a broken challenger must never affect champion outcomes.
        """
        if not self._dual_emit_enabled:
            return
        if self._cfg_source == "challenger":
            self._chal_cfg = {}
            self._chal_model = None
            return
        if self.challenger_key == self.champion_key:
            return
        now = _now_ms()
        if self._chal_cache_loaded_ms and (now - self._chal_cache_loaded_ms) < self._cache_ttl_ms:
            return
        import logging
        logger = logging.getLogger("ml_confirm_gate")
        try:
            raw_p = self.r.get(self.challenger_key)
            if not raw_p:
                self._chal_cfg = {}
                self._chal_model = None
                self._chal_model_load_error = "no_cfg"
                self._chal_cache_loaded_ms = now
                return
            saved_key_used = self._cfg_key_used
            self._cfg_key_used = self.challenger_key
            try:
                cfg, model = self._parse_and_load_from_payload(raw_p, id(self.r), logger)
            finally:
                self._cfg_key_used = saved_key_used
            self._chal_cfg = cfg or {}
            self._chal_model = model
            with contextlib.suppress(Exception):
                self._chal_cfg_raw_len = len(raw_p)  # type: ignore[arg-type]
            self._chal_cache_loaded_ms = now
            self._chal_model_load_error = (
                "" if model is not None else (self._model_load_error or "no_model_loaded")
            )
        except Exception as e:
            self._chal_model_load_error = f"load_err:{type(e).__name__}"
            self._chal_cache_loaded_ms = now

    async def _load_challenger_only_async(self, redis_async: Any) -> None:
        """Async sibling of `_load_challenger_only_sync`. Fire-and-forget."""
        if not self._dual_emit_enabled:
            return
        if self._cfg_source == "challenger":
            self._chal_cfg = {}
            self._chal_model = None
            return
        if self.challenger_key == self.champion_key:
            return
        now = _now_ms()
        if self._chal_cache_loaded_ms and (now - self._chal_cache_loaded_ms) < self._cache_ttl_ms:
            return
        import logging
        logger = logging.getLogger("ml_confirm_gate")
        try:
            raw_p = await redis_async.get(self.challenger_key)
            if not raw_p:
                self._chal_cfg = {}
                self._chal_model = None
                self._chal_model_load_error = "no_cfg"
                self._chal_cache_loaded_ms = now
                return
            loop = asyncio.get_running_loop()
            saved_key_used = self._cfg_key_used
            self._cfg_key_used = self.challenger_key
            try:
                cfg, model = await loop.run_in_executor(
                    None,
                    self._parse_and_load_from_payload,
                    raw_p,
                    id(redis_async),
                    logger,
                )
            finally:
                self._cfg_key_used = saved_key_used
            self._chal_cfg = cfg or {}
            self._chal_model = model
            with contextlib.suppress(Exception):
                self._chal_cfg_raw_len = len(raw_p)  # type: ignore[arg-type]
            self._chal_cache_loaded_ms = now
            self._chal_model_load_error = (
                "" if model is not None else (self._model_load_error or "no_model_loaded")
            )
        except Exception as e:
            self._chal_model_load_error = f"load_err:{type(e).__name__}"
            self._chal_cache_loaded_ms = now

    def _score_challenger_shadow(
        self,
        *,
        symbol: str,
        ts_ms: int,
        direction: str,
        scenario: str,
        indicators: dict[str, Any],
        rule_score: float,
        rule_have: int,
        rule_need: int,
        cancel_spike_veto: int,
        ok_rule: int,
        sid: str,
    ) -> None:
        """Score challenger in SHADOW and emit a parallel `metrics:ml_confirm`
        row tagged with the challenger's kind. Champion path is untouched.

        Mechanism: temporarily swap `self._cfg`/`self._model`/cfg-diagnostic
        fields to challenger state, invoke the matching `_decide_*` mixin
        method (always SHADOW — challenger never affects `allow`), emit, then
        restore. No await between swap/restore → race-free in single-task
        Python execution. All exceptions are swallowed; champion is unaffected.
        """
        if not self._dual_emit_enabled:
            return
        if not self._chal_cfg or self._chal_model is None:
            return
        chal_kind = str(self._chal_cfg.get("kind", "") or "").lower()
        if not chal_kind:
            return
        try:
            sr = float(self._dual_emit_sample)
            if sr <= 0.0:
                return
            if sr < 1.0:
                if not _stable_sample(sid or f"{symbol}|{ts_ms}", sr, salt="ml_dual_emit"):
                    return
        except Exception:
            pass

        saved_cfg = self._cfg
        saved_model = self._model
        saved_cfg_source = self._cfg_source
        saved_cfg_key_used = self._cfg_key_used
        saved_cfg_raw_len = self._cfg_raw_len
        saved_model_load_error = self._model_load_error
        outcome_label = "ok"
        try:
            self._cfg = self._chal_cfg
            self._model = self._chal_model
            self._cfg_source = "challenger"
            self._cfg_key_used = self.challenger_key
            self._cfg_raw_len = self._chal_cfg_raw_len
            self._model_load_error = self._chal_model_load_error or ""

            if chal_kind == "edge_stack_v1":
                dec = self._decide_edge_stack_v1(  # type: ignore[attr-defined]
                    symbol=symbol, ts_ms=ts_ms, direction=direction,
                    scenario=scenario, indicators=indicators,
                    effective_mode="SHADOW",
                )
            elif chal_kind.startswith("util_mh"):
                dec = self._decide_util_mh(  # type: ignore[attr-defined]
                    symbol=symbol, ts_ms=ts_ms, direction=direction,
                    scenario=scenario, indicators=indicators,
                    effective_mode="SHADOW",
                )
            elif chal_kind == "meta_lr":
                dec = self._decide_meta_lr(  # type: ignore[attr-defined]
                    symbol=symbol, ts_ms=ts_ms, direction=direction,
                    scenario=scenario, indicators=indicators,
                    effective_mode="SHADOW",
                )
            elif chal_kind == "meta_lr_blend":
                dec = self._decide_meta_lr_blend(  # type: ignore[attr-defined]
                    symbol=symbol, ts_ms=ts_ms, direction=direction,
                    scenario=scenario, indicators=indicators,
                    effective_mode="SHADOW",
                )
            elif chal_kind.startswith("edge_stack_mh"):
                dec = self._decide_edge_stack_mh(  # type: ignore[attr-defined]
                    symbol=symbol, ts_ms=ts_ms, direction=direction,
                    scenario=scenario, indicators=indicators,
                    effective_mode="SHADOW",
                )
            else:
                outcome_label = "skip"
                return

            dec.cfg_key_used = self._cfg_key_used
            dec.cfg_source = "challenger"
            self._emit_metrics(  # type: ignore[attr-defined]
                dec, symbol=symbol, ts_ms=ts_ms, direction=direction,
                scenario=scenario, rule_score=rule_score,
                rule_have=rule_have, rule_need=rule_need,
                cancel_spike_veto=cancel_spike_veto, ok_rule=ok_rule,
                sid=sid, indicators=indicators,
            )
        except Exception:
            outcome_label = "err"
        finally:
            self._cfg = saved_cfg
            self._model = saved_model
            self._cfg_source = saved_cfg_source
            self._cfg_key_used = saved_cfg_key_used
            self._cfg_raw_len = saved_cfg_raw_len
            self._model_load_error = saved_model_load_error
            if METRICS_REGISTRY_AVAILABLE:
                with contextlib.suppress(Exception):
                    _ml_dual_emit_total.labels(  # type: ignore[union-attr]
                        role="challenger",
                        kind=chal_kind,
                        outcome=outcome_label,
                    ).inc()

    def check(
        self,
        *,
        symbol: str,
        ts_ms: int,
        direction: str,
        scenario: str,
        indicators: dict[str, Any],
        rule_score: float,
        rule_have: int,
        rule_need: int,
        cancel_spike_veto: int,
        ok_rule: int,
    ) -> MLConfirmDecision:
        self._check_call_count += 1
        t0_ns = time.perf_counter_ns()
        t0_sec = time.time()
        # Ensure we do NOT load synchronously. refresh_async will load in background on startup.

        kind = str(self._cfg.get("kind", "") or "") if self._cfg else "none"

        # Per-symbol mode resolution
        symbol_up = symbol.upper()
        effective_mode = self.mode  # fallback: global mode from ENV
        _mode_source = "global"

        if _mode_source == "global" and self._cfg:
            try:
                cfg_mode = (self._cfg.get("mode", "")).upper().strip()
                if cfg_mode in ("OFF", "SHADOW", "CANARY", "ENFORCE"):
                    effective_mode = cfg_mode
                    _mode_source = "cfg"
            except Exception:
                pass

        # 2. Check per-symbol ENV fallback
        _env_sym_mode = os.getenv(f"ML_CONFIRM_MODE__{symbol_up}", "").upper().strip()
        if _env_sym_mode in ("OFF", "SHADOW", "CANARY", "ENFORCE"):
            effective_mode = _env_sym_mode
            _mode_source = "env_per_symbol"

        # 1. Check per-symbol config override (highest priority, from champion JSON)
        _cfg_sym_mode = self._mode_by_symbol.get(symbol_up, "")
        if _cfg_sym_mode:
            effective_mode = _cfg_sym_mode
            _mode_source = "cfg_per_symbol"

        # Handle per-symbol OFF: short-circuit before config/model loading
        if effective_mode == "OFF":
            dec = MLConfirmDecision(mode="OFF", kind="none", allow=True,
                                   reason=f"mode_off(source={_mode_source},symbol={symbol_up})")
            dec.status = "OFF"
            dec.effective_mode = "OFF"
            dec.mode_source = _mode_source
            dec.latency_us = int((time.perf_counter_ns() - t0_ns) / 1000)
            latency_sec = time.time() - t0_sec
            if METRICS_REGISTRY_AVAILABLE:
                self._metrics_events_total.labels(ab_variant=(self.ab_variant or ""), kind="none", outcome="OFF").inc()
                self._metrics_latency_seconds.labels(kind="none").observe(latency_sec)
            sid = _canonical_sid(indicators, symbol, ts_ms)
            self._emit_metrics(dec, symbol=symbol, ts_ms=ts_ms, direction=direction, scenario=scenario,
                               rule_score=rule_score, rule_have=rule_have, rule_need=rule_need,
                               cancel_spike_veto=cancel_spike_veto, ok_rule=ok_rule, sid=sid, indicators=indicators)
            return dec

        if not self._cfg:
            allow = self._fail_allow()
            # Distinguish missing key vs bad/empty cfg
            err = self._model_load_error or "no_cfg"
            if err == "parse_error:CfgError":
                err = "bad_cfg"

            rsn = "no_cfg" if err == "no_cfg" else f"bad_cfg({self._cfg_parse_err})"
            dec = MLConfirmDecision(mode="ERR", kind="none", allow=allow, reason=rsn, error=err)
            dec.status = "ERR_NO_CFG" if err == "no_cfg" else "ERR_BAD_CFG"
            dec.cfg_key_used = self._cfg_key_used
            dec.cfg_source = self._cfg_source
            dec.cfg_raw_len = self._cfg_raw_len
            dec.cfg_parse_err = self._cfg_parse_err
            dec.effective_mode = effective_mode
            dec.mode_source = _mode_source
            dec.latency_us = int((time.perf_counter_ns() - t0_ns) / 1000)
            latency_sec = time.time() - t0_sec
            kind_for_metrics = "unknown"
            if METRICS_REGISTRY_AVAILABLE:
                self._metrics_events_total.labels(ab_variant=(self.ab_variant or ""), kind=kind_for_metrics, outcome="ERR").inc()
                self._metrics_errors_total.labels(kind=kind_for_metrics, reason=err).inc()
                self._metrics_latency_seconds.labels(kind=kind_for_metrics).observe(latency_sec)
            # Extract sid from indicators or generate in format crypto-of:{symbol}:{ts_ms}
            sid = _canonical_sid(indicators, symbol, ts_ms)
            self._emit_metrics(dec, symbol=symbol, ts_ms=ts_ms, direction=direction, scenario=scenario,
                               rule_score=rule_score, rule_have=rule_have, rule_need=rule_need,
                               cancel_spike_veto=cancel_spike_veto, ok_rule=ok_rule, sid=sid, indicators=indicators)
            return dec

        # Canary / Rollout logic (effective mode override)
        if effective_mode == "SHADOW":
            # Check for ENFORCE promotion via canary bucket
            try:
                # Priority: 1. Redis Config, 2. Env Var, 3. Default 0.0
                env_share = float(os.getenv("ML_CONFIRM_ENFORCE_SHARE", "0.0") or 0.0)
                # Override via per-symbol config if available
                enforce_share = self._enforce_share_by_symbol.get(symbol_up, self._cfg.get("enforce_share", env_share) or 0.0)

                if enforce_share > 0.0:
                    # CANARY: deterministic routing by sid.
                    # A signal is enforced iff stable_u01 < enforce_share.
                    raw_sid = str(indicators.get("sid") or indicators.get("signal_id") or "") if indicators else ""
                    sid = _canon_sid(symbol, ts_ms, raw_sid=raw_sid)
                    run_id = str(self._cfg.get("run_id", "unknown"))
                    salt = f"{run_id}|{kind}"
                    if _stable_u01(f"canary|{sid}", salt=salt) < enforce_share:
                        effective_mode = "ENFORCE"
            except Exception:
                pass

        if kind.lower().startswith("util_mh"):
            dec = self._decide_util_mh(symbol=symbol, ts_ms=ts_ms, direction=direction, scenario=scenario, indicators=indicators, effective_mode=effective_mode)
            dec.effective_mode = effective_mode
            # apply selective prediction (only matters in ENFORCE + ok_rule)
            self._apply_selective(dec, ok_rule=ok_rule)
            dec.cfg_key_used = self._cfg_key_used
            dec.cfg_source = self._cfg_source
            dec.cfg_raw_len = self._cfg_raw_len
            dec.cfg_parse_err = self._cfg_parse_err
            dec.effective_mode = effective_mode
            dec.mode_source = _mode_source
            dec.latency_us = int((time.perf_counter_ns() - t0_ns) / 1000)
            latency_sec = time.time() - t0_sec

            # Update Prometheus metrics
            if METRICS_REGISTRY_AVAILABLE:
                kind_for_metrics = kind or "unknown"
                # Determine outcome for metrics
                if dec.error:
                    outcome = "ERR"
                    self._metrics_errors_total.labels(kind=kind_for_metrics, reason=dec.error or "unknown").inc()
                elif dec.status == "SHADOW":
                    outcome = "SHADOW"
                elif dec.allow:
                    outcome = "ALLOW"
                else:
                    outcome = "DENY"
                self._metrics_events_total.labels(ab_variant=(self.ab_variant or ""), kind=kind_for_metrics, outcome=outcome).inc()
                self._metrics_latency_seconds.labels(kind=kind_for_metrics).observe(latency_sec)

            # Extract sid from indicators or generate in format crypto-of:{symbol}:{ts_ms}
            sid = _canonical_sid(indicators, symbol, ts_ms)
            self._emit_metrics(dec, symbol=symbol, ts_ms=ts_ms, direction=direction, scenario=scenario,
                               rule_score=rule_score, rule_have=rule_have, rule_need=rule_need,
                               cancel_spike_veto=cancel_spike_veto, ok_rule=ok_rule, sid=sid, indicators=indicators)
            self._cache_ml_decision(dec, sid=sid, symbol=symbol, scenario=scenario, ok_rule=ok_rule)
            self._capture_replay_input(dec, symbol=symbol, ts_ms=ts_ms, direction=direction, scenario=scenario,
                                       indicators=indicators, rule_score=rule_score, rule_have=rule_have,
                                       rule_need=rule_need, cancel_spike_veto=cancel_spike_veto, ok_rule=ok_rule)
            self._score_challenger_shadow(
                symbol=symbol, ts_ms=ts_ms, direction=direction, scenario=scenario,
                indicators=indicators, rule_score=rule_score,
                rule_have=rule_have, rule_need=rule_need,
                cancel_spike_veto=cancel_spike_veto, ok_rule=ok_rule, sid=sid,
            )
            return dec

        if kind.lower() == "edge_stack_v1":
            dec = self._decide_edge_stack_v1(symbol=symbol, ts_ms=ts_ms, direction=direction, scenario=scenario, indicators=indicators, effective_mode=effective_mode)
            dec.effective_mode = effective_mode
            # apply selective prediction (only matters in ENFORCE + ok_rule)
            self._apply_selective(dec, ok_rule=ok_rule)
            dec.cfg_key_used = self._cfg_key_used
            dec.cfg_source = self._cfg_source
            dec.cfg_raw_len = self._cfg_raw_len
            dec.cfg_parse_err = self._cfg_parse_err
            dec.effective_mode = effective_mode
            dec.mode_source = _mode_source
            dec.latency_us = int((time.perf_counter_ns() - t0_ns) / 1000)
            latency_sec = time.time() - t0_sec

            # Update Prometheus metrics
            if METRICS_REGISTRY_AVAILABLE:
                kind_for_metrics = kind or "unknown"
                # Determine outcome for metrics
                if dec.error:
                    outcome = "ERR"
                    self._metrics_errors_total.labels(kind=kind_for_metrics, reason=dec.error or "unknown").inc()
                elif dec.status == "SHADOW":
                    outcome = "SHADOW"
                elif dec.allow:
                    outcome = "ALLOW"
                else:
                    outcome = "DENY"
                self._metrics_events_total.labels(ab_variant=(self.ab_variant or ""), kind=kind_for_metrics, outcome=outcome).inc()
                self._metrics_latency_seconds.labels(kind=kind_for_metrics).observe(latency_sec)

            sid = _canonical_sid(indicators, symbol, ts_ms)
            self._emit_metrics(dec, symbol=symbol, ts_ms=ts_ms, direction=direction, scenario=scenario,
                               rule_score=rule_score, rule_have=rule_have, rule_need=rule_need,
                               cancel_spike_veto=cancel_spike_veto, ok_rule=ok_rule, sid=sid, indicators=indicators)
            self._cache_ml_decision(dec, sid=sid, symbol=symbol, scenario=scenario, ok_rule=ok_rule)
            self._capture_replay_input(dec, symbol=symbol, ts_ms=ts_ms, direction=direction, scenario=scenario,
                                       indicators=indicators, rule_score=rule_score, rule_have=rule_have,
                                       rule_need=rule_need, cancel_spike_veto=cancel_spike_veto, ok_rule=ok_rule)
            self._score_challenger_shadow(
                symbol=symbol, ts_ms=ts_ms, direction=direction, scenario=scenario,
                indicators=indicators, rule_score=rule_score,
                rule_have=rule_have, rule_need=rule_need,
                cancel_spike_veto=cancel_spike_veto, ok_rule=ok_rule, sid=sid,
            )
            return dec

        if kind in ("meta_lr", "meta_lr_blend"):
            if kind == "meta_lr_blend":
                dec = self._decide_meta_lr_blend(symbol=symbol, ts_ms=ts_ms, direction=direction, scenario=scenario, indicators=indicators, effective_mode=effective_mode)
            else:
                dec = self._decide_meta_lr(symbol=symbol, ts_ms=ts_ms, direction=direction, scenario=scenario, indicators=indicators, effective_mode=effective_mode)
            self._apply_selective(dec, ok_rule=ok_rule)
            dec.cfg_key_used = self._cfg_key_used
            dec.cfg_source = self._cfg_source
            dec.cfg_raw_len = self._cfg_raw_len
            dec.cfg_parse_err = self._cfg_parse_err
            dec.effective_mode = effective_mode
            dec.mode_source = _mode_source
            dec.latency_us = int((time.perf_counter_ns() - t0_ns) / 1000)
            latency_sec = time.time() - t0_sec

            if METRICS_REGISTRY_AVAILABLE:
                kind_for_metrics = kind
                if dec.error:
                    outcome = "ERR"
                    self._metrics_errors_total.labels(kind=kind_for_metrics, reason=dec.error or "unknown").inc()
                elif dec.status == "SHADOW":
                    outcome = "SHADOW"
                elif dec.allow:
                    outcome = "ALLOW"
                else:
                    outcome = "DENY"
                self._metrics_events_total.labels(ab_variant=(self.ab_variant or ""), kind=kind_for_metrics, outcome=outcome).inc()
                self._metrics_latency_seconds.labels(kind=kind_for_metrics).observe(latency_sec)

            sid = _canonical_sid(indicators, symbol, ts_ms)
            self._emit_metrics(dec, symbol=symbol, ts_ms=ts_ms, direction=direction, scenario=scenario,
                               rule_score=rule_score, rule_have=rule_have, rule_need=rule_need,
                               cancel_spike_veto=cancel_spike_veto, ok_rule=ok_rule, sid=sid, indicators=indicators)
            self._cache_ml_decision(dec, sid=sid, symbol=symbol, scenario=scenario, ok_rule=ok_rule)
            self._capture_replay_input(dec, symbol=symbol, ts_ms=ts_ms, direction=direction, scenario=scenario,
                                       indicators=indicators, rule_score=rule_score, rule_have=rule_have,
                                       rule_need=rule_need, cancel_spike_veto=cancel_spike_veto, ok_rule=ok_rule)
            self._score_challenger_shadow(
                symbol=symbol, ts_ms=ts_ms, direction=direction, scenario=scenario,
                indicators=indicators, rule_score=rule_score,
                rule_have=rule_have, rule_need=rule_need,
                cancel_spike_veto=cancel_spike_veto, ok_rule=ok_rule, sid=sid,
            )
            return dec

        if kind.lower().startswith("edge_stack_mh"):
            dec = self._decide_edge_stack_mh(symbol=symbol, ts_ms=ts_ms, direction=direction, scenario=scenario, indicators=indicators, effective_mode=effective_mode)
            # apply selective prediction (only matters in ENFORCE + ok_rule)
            self._apply_selective(dec, ok_rule=ok_rule)
            dec.cfg_key_used = self._cfg_key_used
            dec.cfg_source = self._cfg_source
            dec.cfg_raw_len = self._cfg_raw_len
            dec.cfg_parse_err = self._cfg_parse_err
            dec.effective_mode = effective_mode
            dec.mode_source = _mode_source
            dec.latency_us = int((time.perf_counter_ns() - t0_ns) / 1000)
            latency_sec = time.time() - t0_sec

            # Update Prometheus metrics
            if METRICS_REGISTRY_AVAILABLE:
                kind_for_metrics = kind or "unknown"
                # Determine outcome for metrics
                if dec.error:
                    outcome = "ERR"
                    self._metrics_errors_total.labels(kind=kind_for_metrics, reason=dec.error or "unknown").inc()
                elif dec.status == "SHADOW":
                    outcome = "SHADOW"
                elif dec.allow:
                    outcome = "ALLOW"
                else:
                    outcome = "DENY"
                self._metrics_events_total.labels(ab_variant=(self.ab_variant or ""), kind=kind_for_metrics, outcome=outcome).inc()
                self._metrics_latency_seconds.labels(kind=kind_for_metrics).observe(latency_sec)

            # Extract sid from indicators or generate in format crypto-of:{symbol}:{ts_ms}
            sid = _canonical_sid(indicators, symbol, ts_ms)
            self._emit_metrics(dec, symbol=symbol, ts_ms=ts_ms, direction=direction, scenario=scenario,
                               rule_score=rule_score, rule_have=rule_have, rule_need=rule_need,
                               cancel_spike_veto=cancel_spike_veto, ok_rule=ok_rule, sid=sid, indicators=indicators)
            self._cache_ml_decision(dec, sid=sid, symbol=symbol, scenario=scenario, ok_rule=ok_rule)
            self._capture_replay_input(dec, symbol=symbol, ts_ms=ts_ms, direction=direction, scenario=scenario,
                                       indicators=indicators, rule_score=rule_score, rule_have=rule_have,
                                       rule_need=rule_need, cancel_spike_veto=cancel_spike_veto, ok_rule=ok_rule)
            self._score_challenger_shadow(
                symbol=symbol, ts_ms=ts_ms, direction=direction, scenario=scenario,
                indicators=indicators, rule_score=rule_score,
                rule_have=rule_have, rule_need=rule_need,
                cancel_spike_veto=cancel_spike_veto, ok_rule=ok_rule, sid=sid,
            )
            return dec

        # если когда-то будут другие kind — можно расширить, но для v10.4 достаточно util_mh
        allow = self._fail_allow()
        dec = MLConfirmDecision(mode="ERR", kind=kind or "unknown", allow=allow, reason="unsupported_kind", error="unsupported_kind")
        dec.status = "ERR_UNSUPPORTED_KIND"
        dec.cfg_key_used = self._cfg_key_used
        dec.cfg_source = self._cfg_source
        dec.cfg_raw_len = self._cfg_raw_len
        dec.cfg_parse_err = self._cfg_parse_err
        dec.effective_mode = effective_mode
        dec.mode_source = _mode_source
        dec.latency_us = int((time.perf_counter_ns() - t0_ns) / 1000)
        latency_sec = time.time() - t0_sec
        kind_for_metrics = kind or "unknown"
        if METRICS_REGISTRY_AVAILABLE:
            self._metrics_events_total.labels(ab_variant=(self.ab_variant or ""), kind=kind_for_metrics, outcome="ERR").inc()
            self._metrics_errors_total.labels(kind=kind_for_metrics, reason="unsupported_kind").inc()
            self._metrics_latency_seconds.labels(kind=kind_for_metrics).observe(latency_sec)
        # Extract sid from indicators or generate in format crypto-of:{symbol}:{ts_ms}
        sid = _canonical_sid(indicators, symbol, ts_ms)
        self._emit_metrics(dec, symbol=symbol, ts_ms=ts_ms, direction=direction, scenario=scenario,
                           rule_score=rule_score, rule_have=rule_have, rule_need=rule_need,
                           cancel_spike_veto=cancel_spike_veto, ok_rule=ok_rule, sid=sid, indicators=indicators)
        return dec
