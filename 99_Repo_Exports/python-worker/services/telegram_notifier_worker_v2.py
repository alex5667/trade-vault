from utils.time_utils import get_ny_time_millis
import os
import time
import json
import logging
import urllib.request
import urllib.parse
import urllib.error
import redis
from core.redis_keys import RedisStreams as RS
import socket
import concurrent.futures
from prometheus_client import start_http_server, Counter, Histogram, Gauge

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("TelegramNotifierWorkerV2")

# Configuration
REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")
# Preferred names: TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID
# Backward-compatible fallbacks for older services: BOT_TOKEN / CHAT_ID
TELEGRAM_BOT_TOKEN = (os.getenv("TELEGRAM_BOT_TOKEN") or os.getenv("BOT_TOKEN") or "").strip()
TELEGRAM_CHAT_ID = (os.getenv("TELEGRAM_CHAT_ID") or os.getenv("CHAT_ID") or "").strip()
NOTIFY_TELEGRAM_CHAT_ID_CRIT = (os.getenv("NOTIFY_TELEGRAM_CHAT_ID_CRIT") or "").strip() or TELEGRAM_CHAT_ID
NOTIFY_TELEGRAM_CHAT_ID_PAGE = (os.getenv("NOTIFY_TELEGRAM_CHAT_ID_PAGE") or "").strip() or TELEGRAM_CHAT_ID

# Default: HTML. Set to MarkdownV2 if needed.
TELEGRAM_PARSE_MODE = (os.getenv("TELEGRAM_PARSE_MODE") or "HTML").strip() or "HTML"

NOTIFY_RECEIPT_KEY_PREFIX = os.getenv("NOTIFY_RECEIPT_KEY_PREFIX", "notify:receipt:")
NOTIFY_RECEIPT_TTL_SEC = int(os.getenv("NOTIFY_RECEIPT_TTL_SEC", "3600"))

# Prometheus Config
NOTIFY_METRICS_ENABLE = int(os.getenv("NOTIFY_METRICS_ENABLE", "0"))
NOTIFY_METRICS_PORT = int(os.getenv("NOTIFY_METRICS_PORT", "9125"))
NOTIFY_METRICS_ADDR = os.getenv("NOTIFY_METRICS_ADDR", "0.0.0.0")

# Streams where v2 only writes SLO counters (ok/err), does NOT send Telegram messages.
# Use this for streams already handled by another worker (e.g. scanner-notify-worker).
# Comma-separated list. Default: notify:telegram (handled by legacy notify-worker).
_COUNTER_ONLY_ENV = os.getenv("NOTIFY_COUNTER_ONLY_STREAMS", RS.NOTIFY_TELEGRAM)
COUNTER_ONLY_STREAMS: set[str] = {s.strip() for s in _COUNTER_ONLY_ENV.split(",") if s.strip()}

# Prometheus Metrics
NOTIFY_SEND_TOTAL = Counter("notify_send_total", "Total notifications sent", ["stream", "severity", "status"])
NOTIFY_SEND_LATENCY = Histogram("notify_send_latency_ms", "Time to send notification", ["stream", "severity", "status"], buckets=[100, 500, 1000, 5000, 10000])
NOTIFY_QUEUE_LAG = Gauge("notify_queue_lag_ms", "Time lag of messages in queue", ["stream", "severity"])
NOTIFY_PENDING_N = Gauge("notify_pending_n", "Number of pending messages", ["stream", "severity"])
NOTIFY_RECEIPT_LATENCY = Histogram("notify_receipt_latency_ms", "Time to receive receipt", ["stream", "severity"], buckets=[1000, 5000, 10000, 30000, 60000])
NOTIFY_LAST_OK_TS = Gauge("notify_last_ok_ts_seconds", "Timestamp of last successful send")
NOTIFY_LAST_ERR_TS = Gauge("notify_last_err_ts_seconds", "Timestamp of last failed send")

# Constants
STREAM_KEYS = {
    RS.NOTIFY_TELEGRAM: "notify-group",      # Main info stream 
    RS.NOTIFY_TELEGRAM_CRIT: "notify-crit-group",
    RS.NOTIFY_TELEGRAM_PAGE: "notify-page-group"
}

# Thread pool for offloading slow tasks (LLM analysis)
executor = concurrent.futures.ThreadPoolExecutor(max_workers=3)

CONSUMER_NAME = f"worker-{socket.gethostname()}-{os.getpid()}"
MAX_RETRIES = 3
RETRY_BACKOFF_BASE = 2

def get_redis_client():
    return redis.Redis.from_url(REDIS_URL, decode_responses=True)

def ensure_group(r, stream, group):
    while True:
        try:
            r.xgroup_create(stream, group, id="0", mkstream=True)
            logger.info(f"Created group {group} for {stream}")
            break
        except redis.exceptions.BusyLoadingError:
            logger.warning(f"Redis is busy loading dataset. Retrying group {group} for {stream} in 5s...")
            time.sleep(5)
        except redis.exceptions.ResponseError as e:
            if "BUSYGROUP" in str(e):
                break
            else:
                logger.error(f"Error creating group {group} for {stream}: {e}")
                break
        except Exception as e:
            logger.error(f"Unexpected error creating group {group} for {stream}: {e}")
            break

def send_telegram_message(latched_chat_id, text):
    """
    Sends message to Telegram via urllib (stdlib).
    Returns (success, response_or_error, status_code).
    """
    if not TELEGRAM_BOT_TOKEN or not latched_chat_id:
        return False, "Missing TELEGRAM_BOT_TOKEN (or BOT_TOKEN) or chat_id", 0

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": latched_chat_id,
        "text": text,
        "parse_mode": TELEGRAM_PARSE_MODE,
    }
    
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        url, 
        data=data, 
        headers={"Content-Type": "application/json"}
    )

    try:
        with urllib.request.urlopen(req, timeout=10) as response:
            return True, response.read().decode("utf-8"), 200
    except urllib.error.HTTPError as e:
        err_body = e.read().decode("utf-8") if e.fp else ""
        return False, err_body, e.code
    except Exception as e:
        return False, str(e), 0

def background_process_send_task(r, stream_key, message_id, chat_id, text, payload, severity, start_ts, message_data=None):
    # 1. Analyze text (LLM)
    try:
        from utils.telegram_analyzer import TelegramMessageAnalyzer
        if TelegramMessageAnalyzer.is_enabled():
            full_payload = dict(message_data or {})
            full_payload.update(payload or {})
            text = TelegramMessageAnalyzer.analyze(text=text, payload=full_payload, timeout_sec=900.0)
    except Exception as e:
        logger.warning(f"Background LLM analysis failed: {e}")

    # 2. Send with retries
    sent = False
    last_error = ""
    
    for attempt in range(MAX_RETRIES):
        success, result, status_code = send_telegram_message(chat_id, text)
        if success:
            sent = True
            break
        else:
            last_error = f"HTTP {status_code}: {result}" if status_code > 0 else result
            wait_time = RETRY_BACKOFF_BASE ** attempt

            # Handle 429 Rate Limit
            if status_code == 429:
                try:
                    err_json = json.loads(result)
                    retry_after = err_json.get("parameters", {}).get("retry_after")
                    if retry_after:
                        # Telegram asks to wait X seconds. We wait X+1 to be safe.
                        wait_time = int(retry_after) + 1
                        logger.warning(f"Telegram Rate Limit (429): retry_after={retry_after}s. Sleeping {wait_time}s...")
                except Exception:
                    pass
            
            if status_code == 400:
                logger.error(f"Telegram 400 Bad Request for {stream_key} ID {message_id}. Text: {text!r}")

            logger.warning(f"Attempt {attempt+1}/{MAX_RETRIES} failed for {stream_key} ID {message_id}: {last_error[:200]}. Retrying in {wait_time}s...")
            time.sleep(wait_time)
    
    latency = (time.time() - start_ts) * 1000
    
    # SLO Counters & Metrics
    bucket_1m = int(time.time() / 60)
    bucket_5m = int(time.time() / 300)
    
    if sent:
        if NOTIFY_METRICS_ENABLE:
            NOTIFY_SEND_TOTAL.labels(stream=stream_key, severity=severity, status="ok").inc()
            NOTIFY_SEND_LATENCY.labels(stream=stream_key, severity=severity, status="ok").observe(latency)
            NOTIFY_LAST_OK_TS.set_to_current_time()
        
        # Redis Observable Stats (P6.8)
        r.set("notify:last_ok_ts_ms", get_ny_time_millis())
        r.set(f"notify:last_ok_ts_ms:{severity.upper()}", get_ny_time_millis())

        # Rolling counters (1m) - TTL 3h
        key_1m = f"notify:win1m:{bucket_1m}"
        r.hincrby(key_1m, f"ok:{severity.upper()}", 1)
        r.expire(key_1m, 10800)
        
        # Rolling counters (5m) - TTL 15m (optional, mostly 1m is used for accurate burn)
        key_5m = f"notify:win5m:{bucket_5m}"
        r.hincrby(key_5m, f"ok:{severity.upper()}", 1)
        r.expire(key_5m, 900)

    else:
        logger.error(f"Failed to send {stream_key} ID {message_id} after {MAX_RETRIES} attempts. Error: {last_error}")
        
        if NOTIFY_METRICS_ENABLE:
            NOTIFY_SEND_TOTAL.labels(stream=stream_key, severity=severity, status="err").inc()
            NOTIFY_LAST_ERR_TS.set_to_current_time()
        
        r.set("notify:last_err_ts_ms", get_ny_time_millis())
        r.set(f"notify:last_err_ts_ms:{severity.upper()}", get_ny_time_millis())
        
        key_1m = f"notify:win1m:{bucket_1m}"
        r.hincrby(key_1m, f"err:{severity.upper()}", 1)
        r.expire(key_1m, 10800)
        
        key_5m = f"notify:win5m:{bucket_5m}"
        r.hincrby(key_5m, f"err:{severity.upper()}", 1)
        r.expire(key_5m, 900)

    # Handle Receipt
    receipt_id = payload.get("receipt_id")
    require_receipt = payload.get("require_receipt")
    
    if sent and receipt_id and str(require_receipt) == "1":
        receipt_key = f"{NOTIFY_RECEIPT_KEY_PREFIX}{receipt_id}"
        r.setex(receipt_key, NOTIFY_RECEIPT_TTL_SEC, "1")
        logger.info(f"Set receipt {receipt_key} for message {message_id}")

    logger.info(f"Processed {stream_key} ID {message_id}")

def process_message(r, stream_key, message_id, message_data):
    """
    Process a single message from Redis Stream.
    Returns True if processed (or skipped), False if retry needed.
    
    For streams in COUNTER_ONLY_STREAMS (e.g. notify:telegram handled by legacy worker),
    only SLO counters are written — no Telegram send is performed.
    """
    try:
        payload_str = message_data.get("payload")
        payload = {}
        
        if payload_str:
            try:
                payload = json.loads(payload_str)
            except json.JSONDecodeError:
                payload = {"message": str(payload_str)}
        
        # Determine severity (needed for both send and counter-only paths)
        severity = "info"
        chat_id = TELEGRAM_CHAT_ID
        if stream_key == RS.NOTIFY_TELEGRAM_CRIT:
            chat_id = NOTIFY_TELEGRAM_CHAT_ID_CRIT
            severity = "crit"
        elif stream_key == RS.NOTIFY_TELEGRAM_PAGE:
            chat_id = NOTIFY_TELEGRAM_CHAT_ID_PAGE
            severity = "page"

        # COUNTER_ONLY mode: for streams already delivered by another worker.
        # Just record ok counter and ack — no Telegram send.
        if stream_key in COUNTER_ONLY_STREAMS:
            bucket_1m = int(time.time() / 60)
            key_1m = f"notify:win1m:{bucket_1m}"
            r.hincrby(key_1m, f"ok:{severity.upper()}", 1)
            r.expire(key_1m, 10800)  # TTL 3h
            r.set("notify:last_ok_ts_ms", get_ny_time_millis())
            r.set(f"notify:last_ok_ts_ms:{severity.upper()}", get_ny_time_millis())
            logger.debug(f"[counter-only] {stream_key} ID {message_id} → ok:{severity.upper()}")
            return True

        text = payload.get("message", "")
        if not text:
            # Fallback to direct fields in message_data or other payload fields
            text = (
                message_data.get("message", "") or 
                message_data.get("text", "") or 
                message_data.get("caption", "") or
                payload.get("text", "") or
                payload.get("caption", "")
            )
            
        if not text:
            keys = list(message_data.keys())
            p_keys = list(payload.keys())
            logger.warning(f"No 'message', 'text' or 'caption' field in payload/data {stream_key} ID {message_id}. Keys: msg={keys}, payload={p_keys}")
            return True

        # Deduplication by SID (if present)
        sid = message_data.get("sid") or payload.get("sid")
        if sid:
            dedup_key = f"notify:dedup:{sid}"
            # Try to acquire lock (set if not exists)
            # stored value is timestamp for debug
            if not r.set(dedup_key, int(time.time()), ex=3600, nx=True):
                logger.info(f"Skipping duplicate message {stream_key} ID {message_id} SID {sid}")
                return True

        # Override from payload if present (optional feature)
        if "chat_id" in payload:
            chat_id = payload["chat_id"]

        executor.submit(
            background_process_send_task,
            r, stream_key, message_id, chat_id, text, payload, severity, time.time(), message_data
        )
        return True

    except Exception as e:
        logger.exception(f"Error processing {stream_key} ID {message_id}: {e}")
        return False

def main():
    logger.info(f"Starting TelegramNotifierWorkerV2 consumer={CONSUMER_NAME}")
    
    if NOTIFY_METRICS_ENABLE:
        logger.info(f"Starting Prometheus metrics on {NOTIFY_METRICS_ADDR}:{NOTIFY_METRICS_PORT}")
        start_http_server(NOTIFY_METRICS_PORT, addr=NOTIFY_METRICS_ADDR)

    r = get_redis_client()

    # Ensure groups exist
    for stream, group in STREAM_KEYS.items():
        ensure_group(r, stream, group)

    while True:
        try:
            processed_any = False
            # Read from all streams
            # Using loop to read each stream individually to maintain specific group names
            for stream, group in STREAM_KEYS.items():
                try:
                    items = r.xreadgroup(
                        group,
                        CONSUMER_NAME,
                        {stream: ">"},
                        count=5,
                        block=100
                    )
                except redis.exceptions.ResponseError as e:
                    if "NOGROUP" in str(e):
                        logger.warning(f"Consumer group {group} missing for {stream}, recreating...")
                        ensure_group(r, stream, group)
                        continue
                    else:
                        raise e
                
                if items:
                    processed_any = True
                    for _, messages in items:
                        for message_id, message_data in messages:
                            # Calculate lag
                            try:
                                msg_ts_ms = int(message_id.split("-")[0])
                                lag_ms = get_ny_time_millis() - msg_ts_ms
                                
                                if NOTIFY_METRICS_ENABLE:
                                    NOTIFY_QUEUE_LAG.labels(stream=stream, severity="info").set(lag_ms) # Severity tag is approximation here
                                
                                r.set("notify:last_queue_lag_ms", lag_ms)
                            except Exception:
                                pass

                            if process_message(r, stream, message_id, message_data):
                                r.xack(stream, group, message_id)

            if not processed_any:
                time.sleep(0.1)

        except Exception as e:
            logger.error(f"Main loop error: {e}")
            time.sleep(1)

if __name__ == "__main__":
    main()
