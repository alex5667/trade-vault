import logging
from typing import Any

from core.burst_gate import BurstCandidate
from services.orderflow.metrics import burst_active_gauge
from services.orderflow.utils import _cooldown_ms_for
from services.signal_preprocess import preprocess_signal_for_publish

logger = logging.getLogger("crypto_signal_payload")


class SignalPayloadBuilder:
    """
    Emit-pipeline: cooldown buffering + burst best-of-window aggregation.

    Mirrors services/orderflow/strategy.py:_emit_payload() semantics so the
    `strategy_app` facade behaves like the production path. Flush of the held
    burst best-candidate is owned by BurstFlusher's background loop (started
    by the service); this builder only feeds candidates via `runtime.burst`.
    """

    def __init__(self, facade: Any):
        self.facade = facade
        self.redis = getattr(facade, "redis", None)

    async def emit_payload(
        self,
        runtime: Any,
        payload: dict[str, Any],
        now_ms: int,
    ) -> dict[str, Any] | None:
        import os

        cfg = runtime.config or {}
        indicators = payload.get("indicators") or {}
        of_score = float(indicators.get("of_confirm_score", 0.0) or 0.0)
        confidence = float(payload.get("confidence", 0.0) or 0.0)
        score = of_score if of_score > 0.0 else confidence

        is_adverse_continue = bool(indicators.get("adverse_continuation_used", False))
        burst_disable = int(cfg.get("burst_adverse_continue_disable", 1) or 1)
        force_adverse_bypass = is_adverse_continue and burst_disable == 1

        scenario = str(indicators.get("strong_gate_scn") or "")
        if not scenario:
            scenario = "reversal" if int(indicators.get("sweep", 0) or 0) == 1 else "continuation"
        cd_ms = _cooldown_ms_for(
            runtime,
            scenario=scenario,
            now_ms=now_ms,
            new_dir=str(payload.get("direction") or ""),
        )
        last_sig = int(getattr(runtime, "last_signal_ts_ms", 0) or 0)
        age = (now_ms - last_sig) if last_sig > 0 else 10**9

        if (not force_adverse_bypass) and age < cd_ms:
            # In cooldown: keep best pending candidate.
            prev_score = float(getattr(runtime, "pending_score", 0.0) or 0.0)
            if getattr(runtime, "pending_payload", None) is None or score > prev_score:
                runtime.pending_payload = payload
                runtime.pending_score = score
                runtime.pending_ts_ms = now_ms
                runtime.pending_replaced = int(getattr(runtime, "pending_replaced", 0) or 0) + 1

            # Optional burst aggregation: feed candidate; flush owned by BurstFlusher loop.
            use_burst = bool(int(os.getenv("CRYPTO_BURST_ENABLE", "0"))) or bool(indicators.get("pressure_extreme_flag", 0))
            if use_burst and hasattr(runtime, "burst"):
                try:
                    async with runtime.burst_mu:
                        runtime.burst.consider(
                            ts_ms=now_ms,
                            cand=BurstCandidate(ts_ms=now_ms, score=score, payload=payload),
                        )
                        burst_active_gauge.labels(symbol=runtime.symbol).set(
                            1 if runtime.burst.st.active else 0
                        )
                except Exception:
                    # Fail-open: do nothing, cooldown buffer still applies.
                    pass
            return None

        # Cooldown elapsed (or adverse bypass): promote pending if better.
        pending = getattr(runtime, "pending_payload", None)
        if pending is not None:
            pending_score = float(getattr(runtime, "pending_score", 0.0) or 0.0)
            if pending_score >= score:
                payload = pending
                score = pending_score
            runtime.pending_payload = None
            runtime.pending_score = 0.0

        runtime.signal_count = int(getattr(runtime, "signal_count", 0) or 0) + 1
        runtime.last_signal_ts_ms = now_ms

        processed_payload = preprocess_signal_for_publish(
            payload,
            symbol=runtime.symbol,
            source="CryptoOrderFlow",
            logger=logger,
        )

        # SRE versioned overrides patch
        o = getattr(runtime, "overrides_obj", None)
        if o is not None and getattr(o, "enabled", 0) == 1:
            try:
                processed_payload.setdefault("indicators", {})
                processed_payload["indicators"]["sre_ovr_id"] = str(o.id)
                processed_payload["indicators"]["sre_ovr_sid"] = str(getattr(runtime, "overrides_sid", ""))
            except Exception:
                pass

        publish_fn = getattr(self.facade, "publish_signal", None)
        if publish_fn is not None:
            await publish_fn(runtime, processed_payload)
        orders_fn = getattr(self.facade, "_publish_orders_queue", None)
        if orders_fn is not None:
            await orders_fn(runtime, processed_payload)

        return processed_payload
