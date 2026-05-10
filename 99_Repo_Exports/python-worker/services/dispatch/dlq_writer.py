import json
import contextlib
from typing import Any
from utils.time_utils import get_ny_time_millis
from services.dispatcher.delivery_helpers import DeliveryHelpers


class DlqWriter:
    def __init__(self, config: Any, redis_client: Any, logger: Any):
        self.config = config
        self.redis = redis_client
        self.logger = logger

    def send_target_dlq(self, target: str, sid: str, env: dict[str, Any], *, reason: str, err: str) -> None:
        stream = DeliveryHelpers.get_dlq_stream_for_target(
            target,
            dlq_notify=self.config.dlq_notify,
            dlq_signal_stream=self.config.dlq_signal_stream,
            dlq_audit=self.config.dlq_audit,
            dlq_manual=self.config.dlq_manual,
            dlq_snapshot=self.config.dlq_snapshot,
            dlq_default=self.config.dlq_stream
        )
        DeliveryHelpers.send_to_dlq(
            redis_client=self.redis,
            dlq_stream=stream,
            target=target,
            sid=sid,
            env=env,
            reason=reason,
            error=err,
            logger=self.logger
        )

    def send_dlq_and_ack(self, msg_id: str, fields: dict[str, Any], helper: Any, stream: str, reason: str = "bad_envelope") -> bool:
        """
        Atomic-ish DLQ + ACK. If DLQ succeeds, ACKs.
        """
        try:
            payload = {
                "ts": get_ny_time_millis(),
                "reason": reason,
                "msg_id": msg_id,
                "raw": fields.get("data", fields) if isinstance(fields, dict) else str(fields)[:1000],
            }
            if isinstance(fields, dict) and "sid" in fields:
                payload["sid"] = fields["sid"]
                
            self.redis.xadd(self.config.dlq_stream, {"data": json.dumps(payload, ensure_ascii=False)}, maxlen=self.config.dlq_maxlen, approximate=True)
            
            if helper and stream:
                helper.ack(stream, msg_id)
            return True
        except Exception as e:
            if self.logger:
                self.logger.error(f"Failed to send DLQ and ACK for {msg_id}: {e}", exc_info=True)
            return False
