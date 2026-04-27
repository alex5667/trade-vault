import os
import sys
import time
import logging

from prometheus_client import start_http_server, Counter

from core.redis_stream_consumer import SyncRedisStreamHelper
import redis

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger("dlq_consumer")

DLQ_PROCESSED_TOTAL = Counter(
    "dlq_messages_processed_total",
    "Total number of DLQ messages processed and XACKed",
    ["stream"]
)

def main() -> None:
    logger.info("Starting DLQ SLA Consumer...")
    
    # Start metrics server if enabled
    port = int(os.getenv("DLQ_METRICS_PORT", "9850"))
    try:
        start_http_server(port)
        logger.info(f"Prometheus metrics exposed on port {port}")
    except Exception as e:
        logger.warning(f"Could not start metrics server on port {port}: {e}")
    
    redis_url = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
    client = redis.from_url(redis_url, decode_responses=False) # SyncRedisStreamHelper handles bytes/str normalization in read_new
    
    group = "dlq-sla-consumer-group"
    consumer = f"dlq-worker-{os.getpid()}"
    
    streams = [
        "dlq:ticks",
        "dlq:book_deltas",
        "stream:liq_evt_quarantine",
        "stream:signals:dlq"
    ]
    
    helper = SyncRedisStreamHelper(
        client=client,
        group=group,
        consumer=consumer,
        recovery_start_id="$"
    )
    
    # Ensure groups exist
    helper.ensure_groups(streams, recreate=False)
    logger.info(f"Consumer group '{group}' ensured for streams: {streams}")
    
    empty_loops = 0
    while True:
        try:
            msgs = helper.read_new(streams, count=100, block_ms=5000)
            if msgs:
                stream_counts = {}
                for msg in msgs:
                    # Increment SLA metric
                    DLQ_PROCESSED_TOTAL.labels(stream=msg.stream).inc()
                    stream_counts[msg.stream] = stream_counts.get(msg.stream, 0) + 1
                    
                    # Acknowledge processed message from PEL
                    helper.ack(msg.stream, msg.msg_id)
                
                logger.info(f"Processed and XACKed {len(msgs)} messages: {stream_counts}")
                empty_loops = 0
            else:
                empty_loops += 1
                if empty_loops % 12 == 0: # Log heartbeat every ~60s
                    logger.debug("DLQ Consumer heartbeat - no messages in past minute")
                    
        except redis.exceptions.ConnectionError:
            logger.warning("Redis connection error in DLQ consumer, retrying...")
            time.sleep(2)
        except Exception as e:
            logger.error(f"Error in DLQ consumer loop: {e}")
            time.sleep(5)

if __name__ == "__main__":
    main()
