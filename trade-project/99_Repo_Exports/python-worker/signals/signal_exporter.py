"""
Модуль экспорта всех типов сигналов в Redis на порт 6380.

Поддерживаемые типы сигналов:
- losers: Топ падающих активов
- gainers: Топ растущих активов
- volume: Данные об объемах торгов
- funding: Ставки финансирования
- volatilitybyrange: Волатильность по диапазону
- volatilityspike: Всплески волатильности
"""

import json
import logging
from typing import Any

from common.time_utils import format_timestamp_for_redis, get_current_timestamp_ms
from core.config import STREAM_MAPPING, STREAM_MAX_LENGTH
from core.signals_redis_client import get_signals_redis
from core.redis_keys import RedisStreams as RS

logger = logging.getLogger(__name__)


class SignalExporter:
    """Класс для экспорта всех типов сигналов в Redis на порт 6380."""

    def __init__(self):
        self._redis_client = None
        self.stream_mapping = STREAM_MAPPING

    @property
    def redis_client(self):
        """Lazy Redis connection — not created until first use."""
        if self._redis_client is None:
            self._redis_client = get_signals_redis()
        return self._redis_client

    def export_losers(self, losers_data: list[dict[str, Any]]) -> bool:
        """
        Экспортирует данные о падающих активах в Redis.
        
        Args:
            losers_data: Список данных о падающих активах
            
        Returns:
            bool: True если успешно экспортировано
        """
        try:
            if not losers_data:
                logger.warning("No losers data to export")
                return False

            current_time_ms = get_current_timestamp_ms()
            signal = {
                'type': 'losers',
                'data': losers_data,
                'count': len(losers_data),
                'timestamp': format_timestamp_for_redis(current_time_ms)
            }
            stream_name = self.stream_mapping.get('top:losers', RS.TOP_LOSERS)
            message_id = self._publish_to_stream(stream_name, signal)
            if message_id:
                logger.info("Exported %d losers to Redis (port 6380)", len(losers_data))
                return True
            logger.error("Failed to export losers to Redis")
            return False
        except Exception as e:
            logger.error("Error exporting losers: %s", e)
            return False

    def export_gainers(self, gainers_data: list[dict[str, Any]]) -> bool:
        """
        Экспортирует данные о растущих активах в Redis.
        
        Args:
            gainers_data: Список данных о растущих активах
            
        Returns:
            bool: True если успешно экспортировано
        """
        try:
            if not gainers_data:
                logger.warning("No gainers data to export")
                return False
            current_time_ms = get_current_timestamp_ms()
            signal = {
                'type': 'gainers',
                'data': gainers_data,
                'count': len(gainers_data),
                'timestamp': format_timestamp_for_redis(current_time_ms)
            }
            stream_name = self.stream_mapping.get('top:gainers', RS.TOP_GAINERS)
            message_id = self._publish_to_stream(stream_name, signal)
            if message_id:
                logger.info("Exported %d gainers to Redis (port 6380)", len(gainers_data))
                return True
            logger.error("Failed to export gainers to Redis")
            return False
        except Exception as e:
            logger.error("Error exporting gainers: %s", e)
            return False

    def export_volume(self, volume_data: list[dict[str, Any]]) -> bool:
        """
        Экспортирует данные об объемах торгов в Redis.
        
        Args:
            volume_data: Список данных об объемах
            
        Returns:
            bool: True если успешно экспортировано
        """
        try:
            if not volume_data:
                logger.warning("No volume data to export")
                return False
            current_time_ms = get_current_timestamp_ms()
            signal = {
                'type': 'volume',
                'data': volume_data,
                'count': len(volume_data),
                'timestamp': format_timestamp_for_redis(current_time_ms)
            }
            stream_name = self.stream_mapping.get('signal:volume', 'stream:volume-signals')
            message_id = self._publish_to_stream(stream_name, signal)
            if message_id:
                logger.info("Exported %d volume records to Redis (port 6380)", len(volume_data))
                return True
            logger.error("Failed to export volume to Redis")
            return False
        except Exception as e:
            logger.error("Error exporting volume: %s", e)
            return False

    def export_funding(self, funding_data: list[dict[str, Any]]) -> bool:
        """
        Экспортирует данные о ставках финансирования в Redis.
        
        Args:
            funding_data: Список данных о ставках финансирования
            
        Returns:
            bool: True если успешно экспортировано
        """
        try:
            if not funding_data:
                logger.warning("No funding data to export")
                return False
            current_time_ms = get_current_timestamp_ms()
            signal = {
                'type': 'funding',
                'data': funding_data,
                'count': len(funding_data),
                'timestamp': format_timestamp_for_redis(current_time_ms)
            }
            stream_name = self.stream_mapping.get('signal:funding', 'stream:funding-signals')
            message_id = self._publish_to_stream(stream_name, signal)
            if message_id:
                logger.info("Exported %d funding records to Redis (port 6380)", len(funding_data))
                return True
            logger.error("Failed to export funding to Redis")
            return False
        except Exception as e:
            logger.error("Error exporting funding: %s", e)
            return False

    def export_volatility_by_range(self, volatility_data: dict[str, Any]) -> bool:
        """
        Экспортирует данные о волатильности по диапазону в Redis.
        
        Args:
            volatility_data: Данные о волатильности по диапазону
            
        Returns:
            bool: True если успешно экспортировано
        """
        try:
            if not volatility_data:
                logger.warning("No volatility_by_range data to export")
                return False
            current_time_ms = get_current_timestamp_ms()
            signal = {
                'type': 'volatilitybyrange',
                'data': volatility_data,
                'timestamp': format_timestamp_for_redis(current_time_ms)
            }
            stream_name = self.stream_mapping.get('signal:volatilityRange', RS.VOLATILITY_RANGE)
            message_id = self._publish_to_stream(stream_name, signal)
            if message_id:
                logger.info("Exported volatilitybyrange for %s to Redis (port 6380)",
                            volatility_data.get('symbol', 'unknown'))
                return True
            logger.error("Failed to export volatilitybyrange to Redis")
            return False
        except Exception as e:
            logger.error("Error exporting volatilitybyrange: %s", e)
            return False

    def export_volatility_spike(self, volatility_data: dict[str, Any]) -> bool:
        """
        Экспортирует данные о всплесках волатильности в Redis.
        
        Args:
            volatility_data: Данные о всплеске волатильности
            
        Returns:
            bool: True если успешно экспортировано
        """
        try:
            if not volatility_data:
                logger.warning("No volatility_spike data to export")
                return False
            current_time_ms = get_current_timestamp_ms()
            signal = {
                'type': 'volatilityspike',
                'data': volatility_data,
                'timestamp': format_timestamp_for_redis(current_time_ms)
            }
            stream_name = self.stream_mapping.get('signal:volatility', RS.VOLATILITY)
            message_id = self._publish_to_stream(stream_name, signal)
            if message_id:
                logger.info("Exported volatilityspike for %s to Redis (port 6380)",
                            volatility_data.get('symbol', 'unknown'))
                return True
            logger.error("Failed to export volatilityspike to Redis")
            return False
        except Exception as e:
            logger.error("Error exporting volatilityspike: %s", e)
            return False

    def _publish_to_stream(self, stream_name: str, data: dict[str, Any]) -> str | None:
        """
        Публикует данные в Redis Stream.
        
        Args:
            stream_name: Имя стрима Redis
            data: Данные для публикации
            
        Returns:
            str | None: ID сообщения в стриме или None при ошибке
        """
        try:
            if not self._check_connection():
                return None
            timestamp_value = data.get('timestamp', format_timestamp_for_redis(get_current_timestamp_ms()))
            message_data = {
                'data': json.dumps(data),
                'timestamp': timestamp_value,
                'type': data.get('type', 'unknown'),
                'symbol': data.get('data', {}).get('symbol', 'unknown') if isinstance(data.get('data'), dict) else 'unknown'
            }
            message_id = self.redis_client.xadd(
                stream_name,
                message_data,
                maxlen=STREAM_MAX_LENGTH,
                approximate=True
            )
            return message_id
        except Exception as e:
            logger.error("Error publishing to stream %s: %s", stream_name, e)
            return None

    def _check_connection(self) -> bool:
        """Проверяет доступность Redis."""
        try:
            return bool(self.redis_client.ping())
        except Exception as e:
            logger.warning("Redis connection check failed: %s", e)
            return False


_exporter: SignalExporter | None = None


def _get_exporter() -> SignalExporter:
    """Lazy factory — создаём SignalExporter при первом обращении."""
    global _exporter
    if _exporter is None:
        _exporter = SignalExporter()
    return _exporter


def export_all_signals_to_redis_6380(
    losers: list[dict[str, Any]] | None = None,
    gainers: list[dict[str, Any]] | None = None,
    volume: list[dict[str, Any]] | None = None,
    funding: list[dict[str, Any]] | None = None,
    volatility_by_range: dict[str, Any] | None = None,
    volatility_spike: dict[str, Any] | None = None
) -> dict[str, bool]:
    """
    Экспортирует все переданные сигналы в Redis на порт 6380.
    """
    exporter = _get_exporter()
    results: dict[str, bool] = {}

    if losers is not None:
        results['losers'] = exporter.export_losers(losers)
    if gainers is not None:
        results['gainers'] = exporter.export_gainers(gainers)
    if volume is not None:
        results['volume'] = exporter.export_volume(volume)
    if funding is not None:
        results['funding'] = exporter.export_funding(funding)
    if volatility_by_range is not None:
        results['volatility_by_range'] = exporter.export_volatility_by_range(volatility_by_range)
    if volatility_spike is not None:
        results['volatility_spike'] = exporter.export_volatility_spike(volatility_spike)

    successful = sum(1 for s in results.values() if s)
    logger.info("Signal export done: %d/%d types succeeded", successful, len(results))
    return results
