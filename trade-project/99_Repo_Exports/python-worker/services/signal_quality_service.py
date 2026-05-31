
import logging
from typing import Any

from core.redis_client import get_redis
from services.stream_worker import StreamWorker, WorkerPolicy
import contextlib
from core.redis_keys import RedisStreams as RS

logger = logging.getLogger(__name__)

class SignalQualityService:
    """
    Aggregates trade results from `trades:closed` into Real-time KPIs.
    Updates Redis hashes: `signal_quality:{slice}`
    """
    def __init__(self):
        self.redis = get_redis()

        self.policy = WorkerPolicy(
            ack_mode="lossless",
            read_count=50,
            block_ms=2000,
            dlq_stream="dlq:signal_quality"
        )

        self.worker = StreamWorker(
            name="signal-quality",
            client=self.redis,
            group="scanner-signal-quality",
            consumer="worker-1",
            build_streams=lambda: [RS.TRADES_CLOSED],
            process=self.process_closed_trade,
            policy=self.policy,
            logger=logger
        )

    def run(self):
        self.worker.run_loop(lambda: True)

    def process_closed_trade(self, stream: str, msg_id: str, fields: dict[str, Any]) -> bool:
        """
        Process a closed trade and update stats.
        """
        try:
            # Fields are directly in the stream message for trades:closed (flat structure)
            # Check if it is the new format from LabelJoiner

            # LabelJoiner publishes:
            # {
            #    "sid": ..., "symbol": ..., "result": "WIN/LOSS",
            #    "r_multiple": ..., "pnl": ...
            # }

            sid = fields.get("sid")
            symbol = fields.get("symbol")
            result = fields.get("result")
            r_mult = float(fields.get("r_multiple", 0.0))
            pnl = float(fields.get("pnl", 0.0))

            if not sid or not symbol:
                return True # Skip invalid

            # Slices to update
            slices = [
                "global",
                f"symbol:{symbol}"
            ]

            # TODO: Add reason/model slices if available in fields

            pipe = self.redis.pipeline()

            for s in slices:
                key = f"signal_quality:{s}"

                # Base counters
                pipe.hincrby(key, "count", 1)
                pipe.hincrbyfloat(key, "sum_r", r_mult)
                pipe.hincrbyfloat(key, "sum_pnl", pnl)

                if result == "WIN":
                    pipe.hincrby(key, "wins", 1)
                elif result == "LOSS":
                    pipe.hincrby(key, "losses", 1)
                elif result == "BE":
                    pipe.hincrby(key, "be", 1)

            pipe.execute()

            return True

        except Exception as e:
            logger.exception(f"Error processing stats for {msg_id}: {e}")
            return False

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    service = SignalQualityService()
    with contextlib.suppress(KeyboardInterrupt):
        service.run()
