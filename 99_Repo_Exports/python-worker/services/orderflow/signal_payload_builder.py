import math
import logging
from typing import Any
from utils.time_utils import get_ny_time_millis
from services.orderflow.utils import _cooldown_ms_for
from services.orderflow.metrics import burst_active_gauge
from services.orderflow.burst_flusher import BurstFlusher
from core.burst_gate import BurstCandidate
from utils.task_manager import safe_create_task
from services.signal_preprocess import preprocess_signal_for_publish

logger = logging.getLogger("crypto_signal_payload")

class SignalPayloadBuilder:
    def __init__(self, facade: Any):
        self.facade = facade
        self.redis = facade.redis

    async def emit_payload(self, runtime: Any, payload: dict[str, Any], now_ms: int) -> dict[str, Any] | None:
        """
        Emits signal considering burst mode, cooldowns, and adverse scenarios.
        """
        cfg = runtime.config or {}
        side = payload.get("direction", "UNKNOWN").upper()
        of_score = payload.get("indicators", {}).get("of_confirm_score", 0.0)

        # Adverse Scenario logic
        is_adverse_continue = bool(payload.get("indicators", {}).get("adverse_continuation_used", False))
        burst_disable = int(cfg.get("burst_adverse_continue_disable", 1) or 1)
        force_adverse_bypass = (is_adverse_continue and burst_disable == 1)

        # Cooldown check  # type: ignore
        cd_ms = _cooldown_ms_for(cfg, side, float(getattr(runtime, "last_spread_bps", 0.0) or 0.0), getattr(runtime, "pressure_hi", 0))  # type: ignore
        last_sig = getattr(runtime, "last_signal_ts_ms", 0)

        if not force_adverse_bypass:
             if last_sig > 0 and (now_ms - last_sig) < cd_ms:
                 # Burst Check
                 b_mode = cfg.get("burst_mode", "disabled")
                 if b_mode in ("accumulate", "best"):
                      cand = BurstCandidate(
                           payload=payload,
                           score=of_score,
                           ts_ms=now_ms,  # type: ignore
                           side=side,  # type: ignore
                           delta_z=payload.get("indicators", {}).get("delta_z", 0.0),  # type: ignore
                           pressure_sps=getattr(runtime, "pressure_sps", 0.0) or 0.0,  # type: ignore
                           obi_age_ms=payload.get("indicators", {}).get("obi_age_ms", 0)  # type: ignore
                      )
                      async with runtime.burst_mu:
                           if not runtime.burst.st.active:
                               runtime.burst.st.active = True
                               runtime.burst.st.window_ms = runtime.burst.window_ms
                               runtime.burst.st.max_age_ms = runtime.burst.max_age_ms
                               runtime.burst.st.started_ms = now_ms
                               runtime.burst.st.best_cand = cand
                               runtime.burst.st.cands = [cand]
                               burst_active_gauge.labels(symbol=runtime.symbol).set(1)
                               # Launch flusher  # type: ignore
                               flusher = BurstFlusher(runtime, self.facade.signal_pipeline, self.facade)  # type: ignore
                               safe_create_task(flusher.flush_after_window(now_ms, runtime.burst.st.window_ms))  # type: ignore
                           else:
                               runtime.burst.st.cands.append(cand)
                               if cand.score > runtime.burst.st.best_cand.score:
                                    runtime.burst.st.best_cand = cand

                           # Burst audit
                           indicators = payload.get("indicators", {})
                           safe_create_task(self.facade._burst_audit(
                               runtime=runtime,
                               now_ms=now_ms,
                               event="burst_add",  # type: ignore
                               payload={"cand_side": cand.side, "cand_score": cand.score, "qsize": len(runtime.burst.st.cands)},  # type: ignore
                               indicators=indicators,
                               extra={"window": runtime.burst.st.window_ms}
                           ))
                 return None

        # Either cooled down or force_adverse_bypass
        async with runtime.burst_mu:
             if runtime.burst.st.active:
                  runtime.burst.st.active = False
                  burst_active_gauge.labels(symbol=runtime.symbol).set(0)

        runtime.signal_count += 1
        runtime.last_signal_ts_ms = now_ms
  # type: ignore
        processed_payload = preprocess_signal_for_publish(payload)  # type: ignore
        
        # SRE versioned overrides patch
        o = getattr(runtime, "overrides_obj", None)
        if o is not None and getattr(o, "enabled", 0) == 1:
            try:
                processed_payload["indicators"]["sre_ovr_id"] = str(o.id)
                processed_payload["indicators"]["sre_ovr_sid"] = str(getattr(runtime, "overrides_sid", ""))
            except Exception:
                pass

        await self.facade.publish_signal(runtime, processed_payload)
        await self.facade._publish_orders_queue(runtime, processed_payload)
        
        return processed_payload
