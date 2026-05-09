from utils.time_utils import get_ny_time_millis

"""
Delivery utilities for SignalDispatcher.

Extracted delivery-related helper methods to reduce SignalDispatcher complexity.
"""

import json
import logging
from typing import Any


class DeliveryHelpers:
    """
    Helper methods for signal delivery.
    
    Extracted from SignalDispatcher to reduce class complexity.
    These are stateless utility methods that can be used by SignalDispatcher.
    """

    @staticmethod
    def marker_key(marker_prefix: str, target: str, sid: str) -> str:
        """
        Generate delivery marker key for deduplication.
        
        Args:
            marker_prefix: Marker key prefix (e.g., "signal:deliver:v2")
            target: Target identifier
            sid: Signal ID
            
        Returns:
            Marker key string
        """
        return f"{marker_prefix}:{target}:{sid}"

    @staticmethod
    def delivery_key(marker_prefix: str, target: str, sid: str) -> str:
        """
        Generate delivery key (alias for marker_key).
        
        Args:
            marker_prefix: Marker key prefix
            target: Target identifier
            sid: Signal ID
            
        Returns:
            Delivery key string
        """
        return DeliveryHelpers.marker_key(marker_prefix, target, sid)

    @staticmethod
    def retry_dedup_key(retry_dedup_prefix: str, target: str, sid: str) -> str:
        """
        Generate retry deduplication key.
        
        Args:
            retry_dedup_prefix: Retry dedup prefix
            target: Target identifier
            sid: Signal ID
            
        Returns:
            Retry dedup key string
        """
        return f"{retry_dedup_prefix}:{target}:{sid}"

    @staticmethod
    def calculate_retry_delay(
        attempt: int,
        base_ms: int = 250,
        max_ms: int = 15000,
        jitter_ms: int = 250
    ) -> int:
        """
        Calculate retry delay with exponential backoff and jitter.
        
        Args:
            attempt: Retry attempt number (0-indexed)
            base_ms: Base delay in milliseconds
            max_ms: Maximum delay in milliseconds
            jitter_ms: Jitter range in milliseconds
            
        Returns:
            Delay in milliseconds
        """
        import random
        delay = min(base_ms * (2 ** attempt), max_ms)
        jitter = random.randint(0, jitter_ms)
        return delay + jitter

    @staticmethod
    def send_to_dlq(
        redis_client: Any,
        dlq_stream: str,
        target: str,
        sid: str,
        env: dict[str, Any],
        reason: str,
        error: str,
        logger: logging.Logger | None = None
    ) -> bool:
        """
        Send failed delivery to DLQ.
        
        Args:
            redis_client: Redis client instance
            dlq_stream: DLQ stream name
            target: Target that failed
            sid: Signal ID
            env: Envelope data
            reason: Failure reason
            error: Error message
            logger: Optional logger
            
        Returns:
            True if successful, False otherwise
        """
        payload = {
            "ts": get_ny_time_millis(),
            "reason": reason,
            "target": target,
            "sid": sid,
            "error": error,
            "env": env,
        }

        try:
            redis_client.xadd(
                dlq_stream,
                {"data": json.dumps(payload, ensure_ascii=False)},
                maxlen=200000,
                approximate=True
            )
            return True
        except Exception as exc:
            if logger:
                logger.error(f"Failed to write target DLQ: {exc}", exc_info=True)
            return False

    @staticmethod
    def get_dlq_stream_for_target(
        target: str,
        dlq_notify: str,
        dlq_signal_stream: str,
        dlq_audit: str,
        dlq_manual: str,
        dlq_snapshot: str,
        dlq_default: str
    ) -> str:
        """
        Get appropriate DLQ stream for target.
        
        Args:
            target: Target identifier
            dlq_notify: Notify DLQ stream
            dlq_signal_stream: Signal stream DLQ
            dlq_audit: Audit DLQ stream
            dlq_manual: Manual DLQ stream
            dlq_snapshot: Snapshot DLQ stream
            dlq_default: Default DLQ stream
            
        Returns:
            DLQ stream name
        """
        dlq_map = {
            "notify": dlq_notify,
            "signal_stream": dlq_signal_stream,
            "audit": dlq_audit,
            "manual": dlq_manual,
            "snapshot": dlq_snapshot,
        }
        return dlq_map.get(target, dlq_default)
