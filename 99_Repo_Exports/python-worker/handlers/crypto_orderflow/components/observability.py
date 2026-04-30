from __future__ import annotations
from utils.time_utils import get_epoch_ms

import os
import time
import json
from typing import Any, Optional

from common.json_fast import dumps1
from common.math_safe import finite_or
from common.decision_trace import trace_enabled, serialize_trace_from_ctx
from common.dq_flags import append_dq_flag
from signal_scoring.reason_registry import normalize_reason

# Global state for sampled debug (moved from handler)
_CDBG_LAST: dict[str, float] = {}

def _c_sampled_debug(logger: Any, key: str, msg: str, *args: Any) -> None:
    try:
        interval = float(os.getenv("SAMPLED_DEBUG_INTERVAL_SEC", "30") or "30")
    except Exception:
        interval = 30.0
    try:
        now = float(time.time())
        last = float(_CDBG_LAST.get(key, 0.0))
        if (now - last) < interval:
            return
        _CDBG_LAST[key] = now
        if logger is not None:
            logger.debug(msg, *args)
    except Exception:
        return

def emit_veto_metric_dual(*, emit_veto_metric, kind: str, ctx: Any, reason_code: str) -> None:
    """
    Fail-open emission for veto metrics with optional legacy dual-emit.
    """
    try:
        emit_veto_metric(kind=kind, ctx=ctx, reason_code=reason_code)
    except Exception as e:
        try:
            append_dq_flag(ctx, "veto_metric_emit_error")
        except Exception:
            _c_sampled_debug(None, "veto_metric_emit_error", "veto metric emit failed err=%r", e)

    if str(os.getenv("EDGE_DUAL_EMIT_LEGACY_THIN_COST", "0") or "0").strip().lower() in {"1", "true", "yes", "on"}:
        try:
            emit_veto_metric(kind=kind, ctx=ctx, reason_code="VETO_EDGE_THIN_COST")
        except Exception as e:
            try:
                append_dq_flag(ctx, "veto_metric_emit_legacy_error")
            except Exception:
                _c_sampled_debug(None, "veto_metric_emit_legacy_error", "legacy veto metric emit failed err=%r", e)


class CryptoObservability:
    """
    Manages logging, metrics, and diagnostic tracing for signals.
    """
    def __init__(self, logger: Any, metrics: Any = None):
        self.logger = logger
        self._metrics = metrics
        self._last_regime_label = ""
        self._candidate_log_sampler = None

    def set_sampler(self, sampler: Any) -> None:
        self._candidate_log_sampler = sampler

    def safe_str(self, v: Any) -> str:
        # Helper for logging
        if v is None: return ""
        try:
            return str(v)
        except Exception:
            return ""

    def maybe_log_candidate(self, *, ctx: Any, cand: Any, parts: dict[str, Any], now_ms: Optional[int] = None) -> None:
        """
        Systematic sampled candidate logging.
        """
        if self._candidate_log_sampler is None:
            return

        try:
            reg = str(getattr(ctx, "regime", "") or "")
            if reg and reg != self._last_regime_label:
                self._last_regime_label = reg
                self._candidate_log_sampler.force()
            
            if not self._candidate_log_sampler.maybe(now_ms):
                return

            obj = {
                "type": "candidate_sample"
                "ts": int(getattr(ctx, "ts", 0) or 0)
                "symbol": getattr(ctx, "symbol", None)
                "kind": self.safe_str(getattr(cand, "kind", "") or "")
                "side": self.safe_str(getattr(cand, "side", "") or "")
                "raw_score": finite_or(getattr(cand, "raw_score", None), 0.0)
                "regime": reg
                "spread_bps": finite_or(getattr(ctx, "spread_bps", None), -1.0)
                "taker_rate": finite_or(getattr(ctx, "taker_rate_ema", None), -1.0)
                "geometry_score": finite_or(getattr(ctx, "geometry_score", None), -1.0)
            }
            self.logger.info(dumps1(obj))
        except Exception:
            pass

    def publish_trace_diag_best_effort(self, ctx: Any, *, reason: str, redis_client: Any) -> None:
        """
        Publish DecisionTrace to diagnostics stream.
        """
        if not trace_enabled():
            return
        try:
            if not os.getenv("DECISION_TRACE_DIAG_STREAM"):
                return
            
            # Redis client passed explicitly (resolving dependency)
            if redis_client is None:
                return

            tr = serialize_trace_from_ctx(ctx)
            if not isinstance(tr, dict):
                return
                
            payload = {
                "type": "diagnostic"
                "tradeable": False
                "reason": str(reason or "")
                "trace_id": str(getattr(ctx, "trace_id", "") or tr.get("trace_id") or "")
                "sid": str(tr.get("sid") or getattr(ctx, "sid", "") or "")
                "symbol": str(tr.get("symbol") or getattr(ctx, "symbol", "") or "")
                "kind": str(tr.get("kind") or "")
                "trace": tr
                "ts_ms": get_epoch_ms()
            }
            stream = str(os.getenv("DECISION_TRACE_DIAG_STREAM") or "stream:signals:diagnostics")
            redis_client.xadd(stream, {"data": json.dumps(payload, ensure_ascii=False)}, maxlen=50000, approximate=True)
        except Exception:
            return

    def emit_veto_metric(self, *, kind: str, ctx: Any, reason_code: str) -> None:
        """
        Minimal metrics: signals_veto_total.
        """
        m = self._metrics
        if not m:
            return
        try:
            # Omit symbol to prevent cardinality explosion (160+ reasons * N symbols)
            rc = normalize_reason(reason_code or "VETO_UNKNOWN")
            m.inc("signals_veto_total", 1, tags={"reason": rc, "gate": str(kind or "")})
        except Exception:
            return

    def emit_level_mode_metric(self, tp_mode: str, ctx: Any) -> None:
        """
        Metrics: signals_levels_tp_mode_total.
        """
        m = self._metrics
        if not m:
            return
        try:
            sym = str(getattr(ctx, "symbol", "") or "")
            m.inc("signals_levels_tp_mode_total", 1, tags={"tp_mode": str(tp_mode).upper(), "symbol": sym})
        except Exception:
            return
