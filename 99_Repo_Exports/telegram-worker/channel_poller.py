"""
Channel Poller for Telegram Worker
Active polling approach for channel messages (events don't work for channels!)
"""

import asyncio
import logging
import sys
import time
from typing import Callable, Dict, List, Optional

from telethon import TelegramClient
from telethon.tl.types import Channel

# Global counters for logging reduction
_poll_counter = 0
_empty_poll_counter = 0
POLL_LOG_INTERVAL = 10000  # Log every 10,000th poll


class _MessageEvent:
    """Lightweight event wrapper for compatibility with the message callback signature."""

    __slots__ = ("message", "_chat")

    def __init__(self, message, chat):
        self.message = message
        self._chat = chat

    async def get_chat(self):
        return self._chat


class ChannelPoller:
    """
    Active polling для telegram каналов.

    Telethon НЕ получает push-обновления от каналов через events!
    Нужен active polling через get_messages().
    """

    def __init__(self, client: TelegramClient, logger: logging.Logger):
        self.client = client
        self.logger = logger
        self.last_message_ids: Dict[int, int] = {}  # channel_id -> last_message_id
        self.poll_interval = 15  # seconds between polls
        self.running = False
        self.message_callback: Optional[Callable] = None

    def set_message_callback(self, callback: Callable) -> None:
        """Устанавливает callback для обработки новых сообщений."""
        self.message_callback = callback

    async def poll_channel(self, channel_entity) -> int:
        """
        Проверяет один канал на новые сообщения.

        Returns:
            Количество новых сообщений
        """
        global _poll_counter, _empty_poll_counter

        try:
            channel_id = channel_entity.id
            channel_username = getattr(channel_entity, 'username', f'ID:{channel_id}')

            last_id = self.last_message_ids.get(channel_id, 0)

            _poll_counter += 1
            if _poll_counter % POLL_LOG_INTERVAL == 0:
                self.logger.debug(
                    "Poll #%d: @%s last_id=%d", _poll_counter, channel_username, last_id
                )

            messages = await self.client.get_messages(
                channel_entity,
                limit=10,
                min_id=last_id
            )

            if not messages:
                _empty_poll_counter += 1
                if _empty_poll_counter % POLL_LOG_INTERVAL == 0:
                    self.logger.debug(
                        "Empty polls #%d: @%s (total polls: %d)",
                        _empty_poll_counter, channel_username, _poll_counter
                    )
                return 0

            self.logger.debug(
                "@%s: got %d messages (min_id=%d, range %d-%d)",
                channel_username, len(messages), last_id, messages[-1].id, messages[0].id
            )

            new_count = 0
            for msg in reversed(messages):
                if msg.id > last_id:
                    if self.message_callback:
                        event = _MessageEvent(msg, channel_entity)
                        await self.message_callback(event)
                    self.last_message_ids[channel_id] = msg.id
                    new_count += 1

            if new_count > 0:
                self.logger.info(
                    "📨 @%s: %d new messages", channel_username, new_count
                )

            return new_count

        except Exception as e:
            self.logger.error("❌ Ошибка polling канала: %s", e)
            return 0

    async def poll_all_channels(self, channel_entities: List) -> None:
        """Непрерывный polling всех каналов."""
        self.logger.info("🔄 Запуск polling для %d каналов", len(channel_entities))
        self.running = True
        poll_cycle = 0

        while self.running:
            poll_cycle += 1
            try:
                total_new = 0
                for entity in channel_entities:
                    if not self.running:
                        break
                    try:
                        new_count = await self.poll_channel(entity)
                        total_new += new_count
                        await asyncio.sleep(0.5)
                    except Exception as e:
                        self.logger.error("❌ Ошибка при polling: %s", e)

                if total_new > 0:
                    self.logger.info("📊 Цикл #%d: %d новых сообщений", poll_cycle, total_new)

                await asyncio.sleep(self.poll_interval)

            except Exception as e:
                self.logger.error("❌ Критическая ошибка в poll_all_channels: %s", e)
                await asyncio.sleep(self.poll_interval)

    def stop(self) -> None:
        """Останавливает polling."""
        self.logger.info("🛑 Остановка channel poller")
        self.running = False

    def get_stats(self) -> Dict:
        """Возвращает статистику polling."""
        return {
            'channels_tracked': len(self.last_message_ids),
            'last_message_ids': dict(self.last_message_ids),
            'poll_interval': self.poll_interval,
            'running': self.running
        }
