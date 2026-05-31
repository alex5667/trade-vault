from __future__ import annotations

import contextlib
import json
import logging
from typing import Any

from services.orderflow.configuration import _safe_int
from services.orderflow.metrics import ticks_unknown_side_quarantine_published_total
from services.orderflow.side_policy import deterministic_sample
from utils.task_manager import safe_create_task

logger = logging.getLogger("tick_processor")

class TickQuarantineWriter:
    def __init__(
        self,
        main_redis: Any,
        ticks_redis: Any,
        unknown_side_quarantine_stream: str,
        unknown_side_quarantine_sample: float,
        unknown_side_quarantine_maxlen: int,
        quarantine_stream: str,
        side_policy: str,
    ):
        self._main = main_redis
        self._ticks = ticks_redis
        self._side_quarantine_stream = unknown_side_quarantine_stream
        self._side_quarantine_sample = unknown_side_quarantine_sample
        self._side_quarantine_maxlen = unknown_side_quarantine_maxlen
        self._quarantine_stream = quarantine_stream
        self._side_policy = side_policy

    def xadd_dq_quarantine(self, tick: dict, reason: str) -> None:
        q_stream = "stream:tick_dq:quarantine"
        try:
            # FIX P1: Serialize synchronously BEFORE event loop takes over
            tick_payload = json.dumps(tick)
            async def _xadd():
                with contextlib.suppress(Exception):
                    await self._main.xadd(
                        q_stream,
                        {"data": tick_payload, "reason": reason},
                        maxlen=20_000,
                        approximate=True,
                    )
            safe_create_task(_xadd())
        except Exception:
            pass

    async def quarantine_unknown_side(
        self, symbol: str, msg_id: str, tick: dict, raw_fields: dict
    ) -> None:
        try:
            if not self._ticks:
                return
            key_ms = _safe_int(tick.get("event_ts_ms") or tick.get("ts_ms") or 0)
            if not deterministic_sample(int(key_ms), float(self._side_quarantine_sample)):
                return
            payload = {
                "symbol": symbol,
                "reason": "unknown_side",
                "policy": str(self._side_policy),
                "msg_id": str(msg_id),
                "tick_uid": (tick.get("tick_uid") or ""),
                "event_ts_ms": str(_safe_int(tick.get("event_ts_ms") or 0)),
                "ts_source": (tick.get("ts_source") or ""),
                "side": (tick.get("side") or ""),
                "side_conf": (tick.get("side_conf") or ""),
                "side_raw": (tick.get("side_raw") or ""),
                "is_buyer_maker": str(tick.get("is_buyer_maker") if tick.get("is_buyer_maker") is not None else ""),
                "trade_id": (tick.get("trade_id") or ""),
                "price": (tick.get("price") or ""),
                "qty": str(tick.get("qty") or tick.get("volume") or ""),
            }
            with contextlib.suppress(Exception):
                payload["raw_keys"] = ",".join(sorted(list(raw_fields.keys()))[:32])
            await self._ticks.xadd(
                self._side_quarantine_stream,
                payload,
                maxlen=int(self._side_quarantine_maxlen),
                approximate=True,
            )
            with contextlib.suppress(Exception):
                ticks_unknown_side_quarantine_published_total.labels(
                    symbol=symbol, reason="unknown_side"
                ).inc()
        except Exception:
            pass

    async def quarantine_poison(
        self, symbol: str, msg_id: str, fields: Any, exc: Exception
    ) -> bool:
        try:
            await self._ticks.xadd(
                self._quarantine_stream,
                {
                    "symbol": symbol,
                    "msg_id": str(msg_id),
                    "error": str(exc)[:200],
                    "payload": json.dumps(fields, default=str)[:1000],
                },
                maxlen=5000,
            )
            logger.warning("☣️ (%s) Message %s quarantined", symbol, msg_id)
            return True
        except Exception as q_err:
            logger.error("Critical: Failed to quarantine: %s", q_err)
            return False
