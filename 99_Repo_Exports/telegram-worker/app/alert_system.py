"""
Система алертов для telegram-worker.

Назначение:
- Отправка уведомлений о проблемах
- Мониторинг пропущенных сообщений
- Интеграция с Telegram ботом
- Логирование критических событий
"""

import json
import logging
import time
from datetime import datetime
from typing import Dict, List, Optional

import httpx
import redis


class AlertSystem:
    """Система алертов и уведомлений."""

    def __init__(
        self,
        redis_client: redis.Redis,
        logger: logging.Logger,
        bot_token: str = None,
        chat_ids: List[str] = None,
    ):
        self.redis = redis_client
        self.logger = logger
        self.bot_token = bot_token
        self.chat_ids = chat_ids or []
        self.alert_cooldown = 300  # 5 минут между одинаковыми алертами
        self.last_alerts: Dict[str, float] = {}

    async def send_telegram_alert(self, message: str, alert_type: str = "info") -> bool:
        """Отправляет алерт в Telegram (async, non-blocking)."""
        if not self.bot_token or not self.chat_ids:
            self.logger.warning("⚠️ Telegram бот не настроен для алертов")
            return False

        emoji_map = {
            "info": "ℹ️",
            "warning": "⚠️",
            "error": "❌",
            "success": "✅",
            "critical": "🚨",
        }

        emoji = emoji_map.get(alert_type, "ℹ️")
        formatted_message = (
            f"{emoji} <b>ALERT</b>\n\n{message}\n\n"
            f"⏰ {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        )

        success_count = 0
        async with httpx.AsyncClient(timeout=10.0) as client:
            for chat_id in self.chat_ids:
                try:
                    url = f"https://api.telegram.org/bot{self.bot_token}/sendMessage"
                    resp = await client.post(
                        url,
                        json={"chat_id": chat_id, "text": formatted_message, "parse_mode": "HTML"},
                    )
                    if resp.status_code == 200:
                        success_count += 1
                    else:
                        self.logger.error("❌ Ошибка отправки алерта в %s: %s", chat_id, resp.text)

                except Exception as e:
                    self.logger.error("❌ Ошибка отправки Telegram алерта: %s", e)

        if success_count > 0:
            self.logger.info("📤 Отправлено %d алертов в Telegram", success_count)
            return True
        return False

    def send_redis_alert(
        self,
        message: str,
        alert_type: str = "info",
        channel: str = None,
        data: Dict = None,
    ) -> bool:
        """Отправляет алерт в Redis stream."""
        try:
            alert_data = {
                "message": str(message),
                "type": str(alert_type),
                "channel": str(channel or "system"),
                "timestamp": str(int(time.time() * 1000)),
                "data": json.dumps(data or {}, default=str),
            }
            self.redis.xadd("telegram:alerts", alert_data)
            self.logger.info("📊 Алерт сохранен в Redis: %s", message)
            return True

        except Exception as e:
            self.logger.error("❌ Ошибка сохранения алерта в Redis: %s", e)
            return False

    def send_alert(
        self,
        message: str,
        alert_type: str = "info",
        channel: str = None,
        data: Dict = None,
        send_telegram: bool = True,
    ) -> bool:
        """Отправляет алерт через все доступные каналы (Redis sync; Telegram fire-and-forget)."""
        alert_key = f"{alert_type}:{channel}:{message[:50]}"
        current_time = time.time()

        if alert_key in self.last_alerts:
            if current_time - self.last_alerts[alert_key] < self.alert_cooldown:
                self.logger.debug("⏳ Алерт пропущен (cooldown): %s", message[:50])
                return False

        self.last_alerts[alert_key] = current_time

        redis_success = self.send_redis_alert(message, alert_type, channel, data)

        # Telegram alerts are async; log warning if called from sync context
        if send_telegram and alert_type in {"error", "critical", "warning"}:
            self.logger.warning(
                "⚠️ Telegram alert for '%s' should be sent via async send_telegram_alert(); "
                "skipping from sync send_alert().",
                alert_type,
            )

        return redis_success

    def alert_missed_message(self, channel_name: str, expected_time: float = None):
        """Алерт о пропущенном сообщении."""
        message = f"Пропущено сообщение от канала {channel_name}"
        if expected_time:
            expected_dt = datetime.fromtimestamp(expected_time)
            message += f" (ожидалось в {expected_dt.strftime('%H:%M:%S')})"

        self.send_alert(
            message=message,
            alert_type="warning",
            channel=channel_name,
            data={"expected_time": expected_time},
        )

    def alert_channel_inactive(self, channel_name: str, inactive_hours: float):
        """Алерт о неактивном канале (отключён по умолчанию)."""
        pass  # Disabled: not sending inactivity alerts

    def alert_parsing_error(self, channel_name: str, error: str, message_text: str = None):
        """Алерт об ошибке парсинга."""
        message = f"Ошибка парсинга сообщения от {channel_name}: {error}"
        if message_text:
            message += f"\n\nТекст: {message_text[:200]}..."

        self.send_alert(
            message=message,
            alert_type="error",
            channel=channel_name,
            data={"error": error, "message_text": message_text},
        )

    def alert_connection_error(self, error: str):
        """Алерт об ошибке соединения."""
        self.send_alert(
            message=f"Ошибка соединения с Telegram: {error}",
            alert_type="critical",
            channel="system",
            data={"error": error},
        )

    def alert_system_startup(self, channels_count: int):
        """Алерт о запуске системы."""
        self.send_alert(
            message=f"Telegram-worker запущен. Отслеживается {channels_count} каналов",
            alert_type="info",
            channel="system",
            data={"channels_count": channels_count},
        )

    def get_recent_alerts(self, limit: int = 10) -> List[Dict]:
        """Получает последние алерты."""
        try:
            alerts = self.redis.xrevrange("telegram:alerts", count=limit)
            result = []
            for alert_id, fields in alerts:
                alert_data = dict(fields)
                alert_data["id"] = alert_id
                result.append(alert_data)
            return result

        except Exception as e:
            self.logger.error("❌ Ошибка получения алертов: %s", e)
            return []

    def cleanup_old_alerts(self, days: int = 7):
        """Очищает старые алерты."""
        try:
            cutoff_time = int((time.time() - days * 86400) * 1000)
            self.redis.xtrim("telegram:alerts", minid=cutoff_time)
            self.logger.info("🧹 Очищены алерты старше %d дней", days)
        except Exception as e:
            self.logger.error("❌ Ошибка очистки алертов: %s", e)
