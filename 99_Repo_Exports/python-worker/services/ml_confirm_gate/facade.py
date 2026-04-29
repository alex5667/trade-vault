import logging
import asyncio
import time
import os
import json
from typing import Any, Dict, List, Optional
import redis

from .dto import MLConfirmDecision, MLConfirmInput, MLConfirmOutput
from .config_loader import load_config_from_redis, MLConfirmConfig, _safe_loads
from .model_loader import _load_model_cached
from .feature_builder import build_feature_row
from .decision_cache import cache_ml_decision
from .metrics_emitter import emit_metrics, capture_replay_input
from .decision_policy import DecisionPolicy

logger = logging.getLogger("ml_confirm_gate.facade")

from utils.time_utils import get_ny_time_millis
def _now_ms() -> int: return get_ny_time_millis()
def _make_sid(symbol: str, ts_ms: int) -> str:
    sym = (symbol or "").upper()
    try: t = int(ts_ms)
    except Exception: t = 0
    return f"crypto-of:{sym}:{t}"

class MLConfirmGate:
    def __init__(
        self,
        *,
        r: redis.Redis = None,
        mode: str = "OFF",
        fail_policy: str = "OPEN",
        champion_key: str = "cfg:ml_confirm:champion",
        challenger_key: str = "cfg:ml_confirm:challenger",
        champion_kinds: Optional[List[str]] = None,
        ab_variant: str = "",
    ):
        self.r = r
        self.champion_key = champion_key
        self.challenger_key = challenger_key
        self.ab_variant = str(ab_variant or "champion").lower()
        self.mode = mode.upper()
        self.fail_policy = fail_policy.upper()

        self._champion_kinds = champion_kinds or []
        self._cfg_hash_key = "cfg:ml_confirm"
        self._cfg: Dict[str, Any] = {}
        self._model: Optional[Any] = None
        self._cfgs: Dict[str, Dict[str, Any]] = {}
        self._models: Dict[str, Any] = {}
        self._cfg_sources: Dict[str, str] = {}
        self._cfg_keys_used: Dict[str, str] = {}

        self._cache_ttl_ms = 30000
        self._cache_loaded_ms = 0

        self._abstain_band = 0.0
        self._conf_min = 0.0
        self._abstain_on_missing = False
        self._p_min_hard_floor = float(os.environ.get("ML_CONFIRM_P_MIN_HARD_FLOOR", "0.0"))

        self._mode_by_symbol: Dict[str, str] = {}
        self._enforce_share_by_symbol: Dict[str, float] = {}
        self._mode_by_symbol_by_kind: Dict[str, Dict[str, str]] = {}
        self._enforce_share_by_sym_by_kind: Dict[str, Dict[str, float]] = {}

        self._calibrator = None
        self._calib_type = "none"

        self.policy = DecisionPolicy(self)

    @classmethod
    def from_env(cls, redis_pool: Optional[redis.ConnectionPool] = None) -> 'MLConfirmGate':
        return cls(redis_pool=redis_pool)

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
            self._model = _load_model_cached(model_path, kind, logger)
            self._cache_loaded_ms = now

    def _get_effective_mode(self, symbol: str, kind: str) -> str:
        sym = str(symbol).strip().upper()
        overrides = self._mode_by_symbol_by_kind.get(kind, {})
        if sym in overrides:
            return overrides[sym]
        if sym in self._mode_by_symbol:
            return self._mode_by_symbol[sym]
        return self.mode

    def check(
        self,
        *,
        symbol: str,
        ts_ms: int,
        direction: str,
        scenario: str,
        indicators: Dict[str, Any],
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

        if not self._cfg or not self._model:
            dec.error = "no_cfg"
            if eff_mode == "ENFORCE" and self.fail_policy == "CLOSED":
                dec.allow = False
                dec.status = "BLOCK_NO_CFG_CLOSED"
            else:
                dec.allow = True
                dec.status = "ALLOW_NO_CFG_OPEN"
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
            if kind == "meta_lr":
                self.policy._decide_meta_lr(dec, model=self._model, cfg=self._cfg, **input_dto.__dict__)
            elif kind.startswith("util_mh"):
                self.policy._decide_util_mh(dec, model=self._model, cfg=self._cfg, **input_dto.__dict__)
            elif kind == "edge_stack_v1":
                self.policy._decide_edge_stack_v1(dec, model=self._model, cfg=self._cfg, **input_dto.__dict__)
            else:
                dec.error = f"unknown_kind:{kind}"
                dec.status = "ALLOW_UNKNOWN_KIND"
                dec.allow = True
        except Exception as e:
            dec.error = str(e)
            if eff_mode == "ENFORCE" and self.fail_policy == "CLOSED":
                dec.allow = False
                dec.status = "BLOCK_ERR_CLOSED"
            else:
                dec.allow = True
                dec.status = "ALLOW_ERR_OPEN"

        # Hard guard for P_MIN in ENFORCE mode
        if eff_mode == "ENFORCE" and dec.p_min < 0.5:
            logger.error(f"ML gate: CRITICAL: p_min < 0.5 ({dec.p_min}) in ENFORCE mode. Forcing to 0.5 to prevent silent open.")
            dec.p_min = 0.5
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

