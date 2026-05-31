from __future__ import annotations

from typing import Any
import contextlib


class SignalPublisher:
    """
    Publisher builds the transport payload and sends it to outbox/emitter.

    CRITICAL CONTRACT:
      - Publisher MUST NEVER compute or write "confidence".
      - UnifiedSignalPipeline is the ONLY component that writes payload["confidence"].

    Rationale:
      - avoids drift between legacy/unified paths
      - avoids inconsistent 0..1 vs 0..100 scaling
      - makes calibration a single-point change
    """

    def __init__(self, outbox: Any, logger: Any) -> None:
        self._outbox = outbox
        self._logger = logger

    def build_payload(self, *, ctx: Any, result: Any) -> dict[str, Any]:
        """
        Build minimal payload. Keep "final_score" only.
        Do NOT write "confidence" here (pipeline will inject it).
        """
        kind = str(getattr(result, "kind", "") or "")
        side = int(getattr(result, "side", 0) or 0)
        symbol = str(getattr(ctx, "symbol", "") or "")
        ts = getattr(ctx, "ts", None)
        price = getattr(ctx, "price", None)

        payload: dict[str, Any] = {
            "kind": kind,
            "side": side,
            "symbol": symbol,
            "ts": ts,
            "price": price,
            "raw_score": float(getattr(result, "raw_score", 0.0) or 0.0),
            "final_score": float(getattr(result, "final_score", 0.0) or 0.0),
            "signal_id": str(getattr(result, "signal_id", "") or ""),
            "reasons": list(getattr(result, "reasons", None) or []),
            "parts": dict(getattr(result, "parts", None) or {}),
        }

        # Guardrail: explicitly ensure publisher never leaks confidence into wire format.
        if "confidence" in payload:
            del payload["confidence"]

        return payload

    def publish(self, payload: dict[str, Any]) -> bool:
        try:
            self._outbox.publish(payload)
            return True
        except Exception as e:
            with contextlib.suppress(Exception):
                self._logger.exception(f"SignalPublisher.publish failed: {e}")
            return False
