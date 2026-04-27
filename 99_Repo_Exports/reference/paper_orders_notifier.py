from __future__ import annotations

import json
import logging
import os
import signal
import sys
import time
from typing import Any, Dict, Iterable, Optional

import redis
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import RedisError


# Environment defaults
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
ORDERS_STREAM = os.getenv("PAPER_ORDERS_STREAM", "paper:orders")
TELEGRAM_STREAM = os.getenv("NOTIFY_STREAM", "notify:telegram")
CONSUMER_GROUP = os.getenv("PAPER_NOTIFY_GROUP", "paper-notify")
CONSUMER_NAME = os.getenv("PAPER_NOTIFY_CONSUMER", f"paper-notify-{int(time.time())}")
TELEGRAM_MAXLEN = int(os.getenv("PAPER_NOTIFY_TELEGRAM_MAXLEN", "5000"))
READ_COUNT = int(os.getenv("PAPER_NOTIFY_BATCH", "50"))
READ_BLOCK_MS = int(os.getenv("PAPER_NOTIFY_BLOCK_MS", "5000"))


def _configure_logger() -> logging.Logger:
    logger = logging.getLogger("paper_orders_notifier")
    if logger.handlers:
        return logger
    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | paper_orders_notifier | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    return logger


logger = _configure_logger()


def create_redis_connection(max_retries: int = 10, retry_delay: int = 2) -> redis.Redis:
    """Create Redis connection with retry logic."""

    for attempt in range(max_retries):
        try:
            logger.info(
                "Connecting to Redis %s (attempt %d/%d)",
                REDIS_URL,
                attempt + 1,
                max_retries,
            )
            client = redis.Redis.from_url(
                REDIS_URL,
                decode_responses=True,
                socket_connect_timeout=5,
                socket_keepalive=True,
                health_check_interval=30,
                max_connections=20,
            )
            client.ping()
            logger.info("Redis connection established")
            return client
        except (RedisError, RedisConnectionError, OSError) as exc:
            logger.warning("Redis connection failed: %s", exc)
            if attempt < max_retries - 1:
                time.sleep(retry_delay)
            else:
                logger.error("Exceeded Redis connection attempts")
                raise

    raise RedisConnectionError("Failed to connect to Redis")


class PaperOrdersNotifier:
    def __init__(self) -> None:
        self.redis = create_redis_connection()
        self._ensure_group()
        self.stop_requested = False
        signal.signal(signal.SIGINT, self._handle_signal)
        signal.signal(signal.SIGTERM, self._handle_signal)
        logger.info(
            "Initialized PaperOrdersNotifier | stream=%s group=%s consumer=%s",
            ORDERS_STREAM,
            CONSUMER_GROUP,
            CONSUMER_NAME,
        )

    def _handle_signal(self, *_: Any) -> None:
        logger.info("Shutdown requested via signal")
        self.stop_requested = True

    def _ensure_group(self) -> None:
        try:
            self.redis.xgroup_create(
                name=ORDERS_STREAM,
                groupname=CONSUMER_GROUP,
                id="$",
                mkstream=True,
            )
            logger.info(
                "Consumer group %s created for stream %s",
                CONSUMER_GROUP,
                ORDERS_STREAM,
            )
        except RedisError as exc:
            if "BUSYGROUP" in str(exc):
                logger.info(
                    "Consumer group %s already exists for %s",
                    CONSUMER_GROUP,
                    ORDERS_STREAM,
                )
            else:
                logger.warning(
                    "Unable to create consumer group %s for %s: %s",
                    CONSUMER_GROUP,
                    ORDERS_STREAM,
                    exc,
                )

    def run(self) -> None:
        logger.info("Starting notifier loop")
        while not self.stop_requested:
            try:
                messages = self.redis.xreadgroup(
                    groupname=CONSUMER_GROUP,
                    consumername=CONSUMER_NAME,
                    streams={ORDERS_STREAM: ">"},
                    count=READ_COUNT,
                    block=READ_BLOCK_MS,
                )
                if not messages:
                    continue
                for _, items in messages:
                    self._process_entries(items)
            except (RedisError, RedisConnectionError) as exc:
                logger.error("Redis error: %s", exc)
                time.sleep(2)
                try:
                    self.redis = create_redis_connection()
                except Exception as reconnect_exc:  # pylint: disable=broad-except
                    logger.error("Reconnection failed: %s", reconnect_exc)
                    time.sleep(5)
            except Exception as exc:  # pylint: disable=broad-except
                logger.exception("Unexpected error in main loop: %s", exc)
                time.sleep(1)

        logger.info("Notifier loop stopped")

    def _process_entries(self, entries: Iterable[Any]) -> None:
        for msg_id, fields in entries:
            try:
                payload_raw = fields.get("data")
                if not payload_raw:
                    logger.warning("Empty payload in %s", msg_id)
                    self._ack(msg_id)
                    continue
                payload = self._parse_payload(payload_raw)
                if not payload:
                    logger.warning("Unable to parse payload for %s", msg_id)
                    self._ack(msg_id)
                    continue
                formatted = self._format_message(payload)
                self._publish_to_telegram(formatted, payload)
                logger.info(
                    "Forwarded paper order sid=%s action=%s symbol=%s",
                    payload.get("sid"),
                    payload.get("action"),
                    payload.get("symbol"),
                )
            except Exception as exc:  # pylint: disable=broad-except
                logger.exception("Error processing entry %s: %s", msg_id, exc)
            finally:
                self._ack(msg_id)

    def _ack(self, msg_id: str) -> None:
        try:
            self.redis.xack(ORDERS_STREAM, CONSUMER_GROUP, msg_id)
        except RedisError as exc:
            logger.warning("Failed to ACK %s: %s", msg_id, exc)

    @staticmethod
    def _parse_payload(raw: str) -> Optional[Dict[str, Any]]:
        try:
            data = json.loads(raw)
            if isinstance(data, dict):
                return data
            logger.warning("Payload is not JSON object: %s", raw)
        except json.JSONDecodeError as exc:
            logger.warning("JSON decode error: %s", exc)
        return None

    @staticmethod
    def _format_message(payload: Dict[str, Any]) -> str:
        sid = payload.get("sid", "—")
        action = payload.get("action", "—").upper()
        symbol = payload.get("symbol", "—")
        side = payload.get("side", "—").upper()
        lot = payload.get("lot")
        entry = payload.get("entry")
        sl = payload.get("sl")
        tp_levels = payload.get("tp_levels") or []
        metadata = payload.get("metadata")

        lines = [
            "🧾 <b>PAPER ORDER</b>",
            f"SID: <code>{sid}</code>",
            f"Action: <b>{action}</b>",
            f"Symbol: <b>{symbol}</b> {side}",
        ]

        if lot is not None:
            lines.append(f"Lot: {lot}")
        if entry is not None:
            lines.append(f"Entry: {entry}")
        if sl is not None:
            lines.append(f"SL: {sl}")
        if tp_levels:
            levels = ", ".join(str(level) for level in tp_levels)
            lines.append(f"TP: {levels}")
        if metadata:
            meta_repr = PaperOrdersNotifier._format_metadata(metadata)
            if meta_repr:
                lines.append(meta_repr)

        return "\n".join(lines)

    @staticmethod
    def _format_metadata(metadata: Any) -> Optional[str]:
        try:
            if not metadata:
                return None
            if isinstance(metadata, dict):
                parts = []
                for key, value in metadata.items():
                    if isinstance(value, (dict, list)):
                        parts.append(f"{key}: {json.dumps(value, ensure_ascii=False)}")
                    else:
                        parts.append(f"{key}: {value}")
                if parts:
                    return "Metadata: " + "; ".join(parts)
            return f"Metadata: {metadata}"
        except Exception as exc:  # pylint: disable=broad-except
            logger.debug("Metadata formatting failed: %s", exc)
            return None

    def _publish_to_telegram(self, message: str, payload: Dict[str, Any]) -> None:
        data = {
            "text": message,
            "parse_mode": "HTML",
            "data": json.dumps(payload, ensure_ascii=False),
            "source": "paper_orders_notifier",
        }
        self.redis.xadd(
            TELEGRAM_STREAM,
            data,
            maxlen=TELEGRAM_MAXLEN,
        )


def main() -> None:
    notifier = PaperOrdersNotifier()
    notifier.run()


if __name__ == "__main__":
    main()


