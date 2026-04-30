# signal_processing_service.py
"""
Signal processing functionality extracted from base_orderflow_handler.py
"""

from __future__ import annotations
from utils.time_utils import get_ny_time_millis

from typing import Optional, TYPE_CHECKING, Any, Dict, Iterable

# from common.log import setup_logger
def setup_logger(name):
    import logging
    return logging.getLogger(name)

if TYPE_CHECKING:
    from contexts import PipelineSignalContext, OrderflowSignalContext
    from contexts import BarSample
    from health_metrics import HealthMetrics


class SignalProcessingService:
    """
    Orchestrator for signal processing:
      - unified pipeline first (if configured)
      - fallback to legacy SignalGenerator.generate(ctx)

    Returns PublishResult to keep single contract with outbox/dedup/cooldown.
    """

    def __init__(
        self
        symbol: str
        *
        unified_pipeline: Any = None
        signal_generator: Any = None
        health_metrics: Optional["HealthMetrics"] = None
        outbox: Any = None
    ):
        self.symbol = symbol
        self.unified_pipeline = unified_pipeline
        self.signal_generator = signal_generator
        self.health_metrics = health_metrics
        self.outbox = outbox  # optional, only if unified returns envelopes (dict)
        self.logger = setup_logger(f"SignalProcessingService:{symbol}")

    def _hm_emit(self, signal_type: str, latency_ms: float, confidence: Any):
        if not self.health_metrics:
            return
        try:
            if signal_type == "bucket":
                self.health_metrics.on_signal_bucket_emit(self.symbol, latency_ms=latency_ms, confidence=confidence)
            else:
                self.health_metrics.on_signal_bar_emit(self.symbol, latency_ms=latency_ms, confidence=confidence)
        except Exception:
            pass

    def _hm_fail(self, signal_type: str):
        if not self.health_metrics:
            return
        try:
            if signal_type == "bucket":
                self.health_metrics.on_signal_bucket_failed(self.symbol)
            else:
                self.health_metrics.on_signal_bar_failed(self.symbol)
        except Exception:
            pass

    def _empty_result(self):
        from signals.outbox_utils import PublishResult
        return PublishResult(sent=False, dedup=False, msg_id=None)

    def _as_publish_result(self, x: Any) -> Optional[Any]:
        """
        Best-effort: detect PublishResult-like object without hard dependency on class identity.
        """
        if x is None:
            return None
        if hasattr(x, "sent") and hasattr(x, "dedup") and hasattr(x, "msg_id"):
            return x
        return None

    def _normalize_epoch_ms(self, ts: Any) -> int:
        """
        Нормализация epoch ms для сигнального пайплайна.
        Защита от seconds, minutes-of-day и мусора.
        """
        now = get_ny_time_millis()
        try:
            v = int(ts)
        except Exception:
            return now

        if v <= 0:
            return now
        # epoch seconds -> ms
        if 1_000_000_000 <= v < 100_000_000_000:
            v *= 1000
        
        # окно валидности (2000..now+7d)
        if v < 946_684_800_000 or v > now + 7 * 86_400_000:
            return now
        return v

    def _build_pipeline_ctx(self, of_ctx: "OrderflowSignalContext") -> "PipelineSignalContext":
        """
        Adapter: OrderflowSignalContext -> PipelineSignalContext.
        """
        from contexts import PipelineSignalContext
        ts_ms = self._normalize_epoch_ms(getattr(of_ctx, "ts", 0))
        return PipelineSignalContext(
            symbol=getattr(of_ctx, "symbol", self.symbol)
            ts=ts_ms
            price=float(getattr(of_ctx, "price", 0.0) or 0.0)
            volume=float(getattr(of_ctx, "volume", 0.0) or 0.0)
        )

    def _call_unified(self, of_ctx: "OrderflowSignalContext") -> Any:
        """
        Вызов унифицированного пайплайна с проверкой сигнатуры.
        """
        if self.unified_pipeline is None:
            return None
        pctx = self._build_pipeline_ctx(of_ctx)
        
        fn = getattr(self.unified_pipeline, "process", None)
        if fn is None:
            return None

        import inspect
        try:
            sig = inspect.signature(fn)
            params = sig.parameters
            # Remove 'self' if it's a bound method (it usually is)
            if hasattr(fn, "__self__"):
                pass # signature already handles this for bound methods
            
            non_default_params = [p for p in params.values() if p.default is inspect.Parameter.empty]
            
            # Signature process(pctx)
            if len(non_default_params) == 1:
                 return fn(pctx)
            # Signature process(of_ctx, something_else=...)
            elif len(non_default_params) > 1:
                 # fallback to old behavior but with specific call
                 try:
                     return fn(pctx)
                 except TypeError:
                     return fn(of_ctx)
        except Exception:
            pass

        # absolute fallback if signature fails or it's non-standard
        try:
            return fn(pctx)
        except TypeError:
            return fn(of_ctx)

    def _publish_envelopes(self, envs: Iterable[Dict[str, Any]]):
        """
        If unified returns envelopes (dict), publish them here (optional mode).
        Агрегация результатов:
        - sent = True если хотя бы один env ушел
        - dedup = True только если все env были дедуплицированы (и ни один не sent)
        - msg_id = ID последнего успешно отправленного сигнала
        """
        from signals.outbox_utils import PublishResult
        if not self.outbox:
            return PublishResult(sent=False, dedup=False, msg_id=None)

        sent_any = False
        dedup_any = False
        last_msg_id = None
        last_conf = None

        for env in envs:
            # Тщательная нормализация ts_ms для дедупликатора
            raw_ts = env.get("ts_ms") or 0
            if raw_ts <= 0:
                raw_ts = get_ny_time_millis()
            env["ts_ms"] = self._normalize_epoch_ms(raw_ts)

            try:
                pr_id = self.outbox.publish(
                    source=env.get("source", "unified")
                    strategy=env.get("strategy", "unknown")
                    symbol=env.get("symbol", self.symbol)
                    side=env.get("side", "none")
                    kind=env.get("kind", "unknown")
                    level_key=env.get("level_key", "")
                    ts_ms=env["ts_ms"]
                    envelope=env
                )
                if pr_id:
                    sent_any = True
                    last_msg_id = pr_id
                    c = env.get("confidence")
                    if c is not None:
                        last_conf = c
                else:
                    dedup_any = True
            except Exception as e:
                self.logger.warning("Failed to publish unified envelope: %s", e)
                # Fail-aware return: если часть уже ушла, сохраняем sent=True
                return PublishResult(
                    sent=sent_any
                    dedup=(not sent_any and dedup_any)
                    msg_id=last_msg_id
                    confidence=last_conf
                )

        return PublishResult(
            sent=sent_any
            dedup=(not sent_any and dedup_any)
            msg_id=last_msg_id
            confidence=last_conf
        )

    def process_orderflow_context(self, ctx: "OrderflowSignalContext", signal_type: str = "bar"):
        """
        Main entrypoint for BaseOrderFlowHandler:
          ctx = data_processor.build_signal_ctx(...)
          result = signal_processing.process_orderflow_context(ctx, signal_type="bar")
        """
        import time
        from signals.outbox_utils import PublishResult

        start_time = time.time()

        # 1) Try unified first
        if self.unified_pipeline is not None:
            try:
                ures = self._call_unified(ctx)
                pr = self._as_publish_result(ures)
                if pr is not None:
                    # Track signal success
                    latency_ms = (time.time() - start_time) * 1000
                    self._hm_emit(signal_type, latency_ms, getattr(pr, 'confidence', None))
                    return pr

                # unified may return a single envelope or list of envelopes
                if isinstance(ures, dict):
                    result = self._publish_envelopes([ures])
                    if result.sent:
                        latency_ms = (time.time() - start_time) * 1000
                        self._hm_emit(signal_type, latency_ms, getattr(result, 'confidence', None))
                    return result

                if isinstance(ures, list) and (not ures or isinstance(ures[0], dict)):
                    result = self._publish_envelopes(ures)
                    if result.sent:
                        latency_ms = (time.time() - start_time) * 1000
                        self._hm_emit(signal_type, latency_ms, getattr(result, 'confidence', None))
                    return result

                # unified returned "something else" -> treat as no-signal
            except Exception as e:
                if self.health_metrics:
                    try:
                        self.health_metrics.inc_unified_error(self.symbol)
                        self._hm_fail(signal_type)
                    except Exception:
                        pass
                self.logger.warning("Unified pipeline error, fallback to legacy: %s", e)

            # unified configured but didn't publish; count fallback
            if self.health_metrics:
                try:
                    self.health_metrics.inc_unified_fallback(self.symbol)
                except Exception:
                    pass

        # 2) Legacy fallback (SignalGenerator.generate(ctx))
        if self.signal_generator is None:
            return PublishResult(sent=False, dedup=False, msg_id=None)

        try:
            result = self.signal_generator.generate(ctx, signal_type)
            latency_ms = (time.time() - start_time) * 1000
            if result.sent:
                self._hm_emit(signal_type, latency_ms, getattr(result, 'confidence', None))
            else:
                self._hm_fail(signal_type)
            return result
        except Exception as e:
            self._hm_fail(signal_type)
            self.logger.warning("Legacy signal generation failed: %s", e)
            return PublishResult(sent=False, dedup=False, msg_id=None)

    # Backward-compat shim (if old code still calls bar-based API)
    def process_bar_signals(self, bar: "BarSample"):
        """
        Deprecated: bar-based path. Prefer process_orderflow_context(ctx).
        Kept only to avoid breaking old callers.
        """
        from contexts import OrderflowSignalContext
        ctx = OrderflowSignalContext(
            symbol=self.symbol
            ts=int(getattr(bar, "ts", 0) or 0)
            price=float(getattr(bar, "close", 0.0) or 0.0)
        )
        return self.process_orderflow_context(ctx)
