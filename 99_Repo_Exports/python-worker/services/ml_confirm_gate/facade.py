import asyncio
import dataclasses
import logging
import os
from typing import Any

import redis

from .config_loader import load_config_from_redis
from .decision_policy import DecisionPolicy
from .dto import MLConfirmDecision, MLConfirmInput
from .metrics_emitter import emit_metrics
from .model_loader import _load_model_cached

logger = logging.getLogger("ml_confirm_gate.facade")

from utils.time_utils import get_ny_time_millis
import time

try:
    from prometheus_client import Histogram, Counter
    _GATE_LATENCY_US = Histogram(
        "ml_confirm_latency_us",  # was "gate_latency_us" — collided with gates.py registry
        "ML confirm gate latency in microseconds",
        ["gate"],
        buckets=[1000, 5000, 10000, 20000, 50000]  # 1ms to 50ms
    )
    _ML_CONFIRM_STATUS_TOTAL = Counter(
        "ml_confirm_status_total",
        "ML confirm decisions",
        ["status", "mode", "enforce", "bucket"]
    )
    _ML_CONFIRM_P_MARGIN = Histogram(
        "ml_confirm_p_margin",
        "ML confirm p margin",
        ["symbol"],
        buckets=[-0.1, -0.05, 0.0, 0.05, 0.1, 0.2, 0.5]
    )
    try:
        from services.orderflow.metrics import ml_feature_schema_version_total as _ML_SCHEMA_VER_TOTAL
    except Exception:
        _ML_SCHEMA_VER_TOTAL = None
except ImportError:
    _GATE_LATENCY_US = None
    _ML_CONFIRM_STATUS_TOTAL = None
    _ML_CONFIRM_P_MARGIN = None
    _ML_SCHEMA_VER_TOTAL = None
def _now_ms() -> int: return get_ny_time_millis()
def _make_sid(symbol: str, ts_ms: int) -> str:
    sym = (symbol or "").upper()
    try: t = ts_ms
    except Exception: t = 0
    return f"crypto-of:{sym}:{t}"

class MLConfirmGate:
    def __init__(
        self,
        *,
        r: redis.Redis | None = None,
        mode: str = "OFF",
        fail_policy: str = "OPEN",
        champion_key: str = "cfg:ml_confirm:champion",
        challenger_key: str = "cfg:ml_confirm:challenger",
        champion_kinds: list[str] | None = None,
        ab_variant: str = "",
    ):
        self.r = r
        self.champion_key = champion_key
        self.challenger_key = challenger_key
        self.ab_variant = (ab_variant or "champion").lower()
        self.mode = mode.upper()
        self.fail_policy = fail_policy.upper()

        self._champion_kinds = champion_kinds or []
        self._cfg_hash_key = "cfg:ml_confirm"
        self._cfg: dict[str, Any] = {}
        self._model: Any | None = None
        self._cfgs: dict[str, dict[str, Any]] = {}
        self._models: dict[str, Any] = {}
        self._cfg_sources: dict[str, str] = {}
        self._cfg_keys_used: dict[str, str] = {}

        self._cache_ttl_ms = 30000
        self._cache_loaded_ms = 0

        self._abstain_band = 0.0
        self._conf_min = 0.0
        self._abstain_on_missing = (os.environ.get("ML_CONFIRM_ABSTAIN_ON_MISSING", "0") or "0").strip().lower() in {"1", "true", "yes", "on"}
        self._p_min_hard_floor = float(os.environ.get("ML_CONFIRM_P_MIN_HARD_FLOOR", "0.52"))

        self._mode_by_symbol: dict[str, str] = {}
        self._enforce_share_by_symbol: dict[str, float] = {}
        self._mode_by_symbol_by_kind: dict[str, dict[str, str]] = {}
        self._enforce_share_by_sym_by_kind: dict[str, dict[str, float]] = {}

        self._calibrator = None
        self._calib_type = "none"

        self.policy = DecisionPolicy(self)

    @classmethod
    def from_env(cls, redis_pool: redis.ConnectionPool | None = None) -> 'MLConfirmGate':
        r_client = None
        if redis_pool is not None:
            r_client = redis.Redis(connection_pool=redis_pool)
        else:
            redis_dsn = os.environ.get("REDIS_DSN") or os.environ.get("REDIS_URL")
            if redis_dsn:
                r_client = redis.Redis.from_url(redis_dsn)
        return cls(
            r=r_client,
            mode=os.getenv("ML_CONFIRM_MODE", "OFF").upper(),
            fail_policy=os.getenv("ML_CONFIRM_FAIL_POLICY", "OPEN").upper()
        )

    def _refresh_cache_if_needed(self) -> None:
        if self.mode == "OFF" or not self.r:
            return
        now = _now_ms()
        if self._cache_loaded_ms and (now - self._cache_loaded_ms) < self._cache_ttl_ms:
            return

        cfg_dict, cfg_source, cfg_key_used, err = load_config_from_redis(
            self.r, self.champion_key, self.challenger_key, self.ab_variant, self._cfg_hash_key
        )
        if cfg_dict:
            self._cfg = cfg_dict
            model_path = cfg_dict.get("model_path")
            kind = cfg_dict.get("kind", "")
            if model_path:
                self._model = _load_model_cached(model_path, kind, logger)
                if self._model is not None and _ML_SCHEMA_VER_TOTAL is not None:
                    try:
                        schema_hash = str(cfg_dict.get("feature_cols_hash") or cfg_dict.get("schema_hash") or cfg_dict.get("model_signature") or "unknown")
                        model_ver = str(cfg_dict.get("run_id") or cfg_dict.get("model_ver") or "unknown")
                        _ML_SCHEMA_VER_TOTAL.labels(schema_hash=schema_hash, model_ver=model_ver).inc()
                    except Exception:
                        pass
            self._cache_loaded_ms = now

    def _get_effective_mode(self, symbol: str, kind: str) -> str:
        sym = symbol.strip().upper()
        overrides = self._mode_by_symbol_by_kind.get(kind, {})
        if sym in overrides:
            return overrides[sym]
        if sym in self._mode_by_symbol:
            return self._mode_by_symbol[sym]

        cfg_mode = self._cfg.get("mode")
        if cfg_mode:
            return str(cfg_mode).upper()

        return self.mode

    def check(
        self,
        *,
        symbol: str,
        ts_ms: int,
        direction: str,
        scenario: str,
        indicators: dict[str, Any],
        rule_score: float = 0.0,
        rule_have: int = 0,
        rule_need: int = 0,
        cancel_spike_veto: int = 0,
        ok_rule: int = 0,
    ) -> MLConfirmDecision:

        self._refresh_cache_if_needed()

        dec = MLConfirmDecision(
            mode=self.mode,
            kind="none",
            allow=True,
            status="MISSING_CFG"
        )
        if self.mode == "OFF":
            dec.status = "OFF"
            return dec

        eff_mode = self._get_effective_mode(symbol, self._cfg.get("kind", "none"))
        dec.effective_mode = eff_mode
        dec.mode_source = "global"

        eff_fail_policy = str(self._cfg.get("fail_policy", self.fail_policy)).upper()

        if not self._cfg or not self._model:
            dec.error = "no_cfg"
            dec.mode = "ERR"
            if eff_fail_policy == "CLOSED":
                dec.allow = False
                dec.status = "ERR_NO_CFG"
            else:
                dec.allow = True
                dec.status = "ERR_NO_CFG"
            return dec

        kind = str(self._cfg.get("kind", "")).lower()
        dec.kind = kind
        dec.model_run_id = str(self._cfg.get("run_id", ""))
        dec.model_path = str(self._cfg.get("model_path", ""))

        sid = _make_sid(symbol, ts_ms)

        input_dto = MLConfirmInput(
            sid=sid,
            symbol=symbol,
            ts_ms=ts_ms,
            direction=direction,
            scenario=scenario,
            indicators=indicators,
            rule_score=rule_score,
            rule_have=rule_have,
            rule_need=rule_need,
            ok_rule=ok_rule,
            cancel_spike_veto=cancel_spike_veto,
        )

        try:
            t_start = time.time()
            if kind == "meta_lr":
                self.policy._decide_meta_lr(dec, model=self._model, cfg=self._cfg, **dataclasses.asdict(input_dto))
            elif kind == "meta_lr_blend":
                self.policy._decide_meta_lr_blend(dec, model=self._model, cfg=self._cfg, **dataclasses.asdict(input_dto))
            elif kind.startswith("util_mh"):
                self.policy._decide_util_mh(dec, model=self._model, cfg=self._cfg, **dataclasses.asdict(input_dto))
            elif kind == "edge_stack_v1":
                self.policy._decide_edge_stack_v1(dec, model=self._model, cfg=self._cfg, **dataclasses.asdict(input_dto))
            elif kind == "edge_stack_mh_v1":
                self.policy._decide_edge_stack_mh(dec, model=self._model, cfg=self._cfg, **dataclasses.asdict(input_dto))
            else:
                dec.error = f"unknown_kind:{kind}"
                dec.status = "ALLOW_UNKNOWN_KIND"
                dec.allow = True
                
            if _GATE_LATENCY_US is not None:
                _GATE_LATENCY_US.labels(gate="ml_confirm").observe((time.time() - t_start) * 1e6)
                
            if _ML_CONFIRM_STATUS_TOTAL is not None:
                # Add status metric. `dec.status`, `dec.mode`, etc.
                mode_str = getattr(dec, "mode", "UNKNOWN")
                enforce_str = str(getattr(dec, "enforce", False))
                # For `bucket`, if the decision doesn't provide one directly, use 'default'
                bucket_str = getattr(dec, "bucket", "default")
                _ML_CONFIRM_STATUS_TOTAL.labels(
                    status=dec.status,
                    mode=mode_str,
                    enforce=enforce_str,
                    bucket=bucket_str
                ).inc()
                
            if _ML_CONFIRM_P_MARGIN is not None and getattr(dec, "p_margin", None) is not None:
                _ML_CONFIRM_P_MARGIN.labels(symbol=symbol).observe(dec.p_margin)

        except Exception as e:
            dec.error = str(e)
            if eff_mode == "ENFORCE" and eff_fail_policy == "CLOSED":
                dec.allow = False
                dec.status = "BLOCK_ERR_CLOSED"
            else:
                dec.allow = True
                dec.status = "ALLOW_ERR_OPEN"

        # Hard guard for P_MIN in ENFORCE mode
        if eff_mode == "ENFORCE" and dec.p_min < self._p_min_hard_floor:
            logger.error(f"ML gate: CRITICAL: p_min < {self._p_min_hard_floor} ({dec.p_min}) in ENFORCE mode. Forcing to {self._p_min_hard_floor} to prevent silent open.")
            dec.p_min = self._p_min_hard_floor
            # re-evaluate allow if needed based on updated p_min
            if dec.p_edge < dec.p_min:
                dec.allow = False
                dec.status = "BLOCK"

        if self.r:
            emit_metrics(
                self.r, dec, symbol=symbol, ts_ms=ts_ms, direction=direction, scenario=scenario,
                rule_score=rule_score, rule_have=rule_have, rule_need=rule_need,
                cancel_spike_veto=cancel_spike_veto, ok_rule=ok_rule, sid=sid,
                indicators=indicators, metrics_stream=self._cfg.get("metrics_stream", "metrics:ml_gate"),
                metrics_enable=True, metrics_sample=1.0
            )

        return dec

    async def check_async(self, **kwargs) -> MLConfirmDecision:
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, lambda: self.check(**kwargs))

