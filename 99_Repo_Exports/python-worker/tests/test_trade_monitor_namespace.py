"""
Unit-тесты для namespace изоляции в TradeMonitorService.

Проверяют, что разные сервисы с разными TM_NAMESPACE не конфликтуют
при дедупликации SID и event_id в Redis.
"""
import os
import time
import pytest
from unittest.mock import MagicMock, patch
from services.trade_monitor import TradeMonitorService


class TestTradeMonitorNamespace:
    """
    Тесты для проверки namespace изоляции между сервисами.
    
    Цель: предотвратить race condition, когда scanner-trade-monitor и
    scanner-signal-tracker соревнуются за один и тот же SID ключ в Redis.
    """

    @pytest.fixture
    def mock_redis(self):
        """Mock Redis client для изоляции тестов."""
        redis_mock = MagicMock()
        redis_mock.set = MagicMock(return_value=True)
        redis_mock.delete = MagicMock(return_value=1)
        redis_mock.hgetall = MagicMock(return_value={})
        redis_mock.pipeline = MagicMock()
        pipe_mock = MagicMock()
        pipe_mock.execute = MagicMock(return_value=[{}, None, None])
        redis_mock.pipeline.return_value = pipe_mock
        return redis_mock

    def test_default_namespace(self, mock_redis):
        """
        Проверяем, что по умолчанию используется namespace 'default'.
        """
        with patch.dict(os.environ, {}, clear=True):
            monitor = TradeMonitorService(
                redis_client=mock_redis,
                config={},
                regime_guard=None,
                health_metrics=None
            )
            
            assert monitor.namespace == "default"
            assert "default" in monitor._sid_dedup_key("test-signal-123")
            assert "default" in monitor._dedup_key("tp_hit", "event-456")

    def test_custom_namespace_from_env(self, mock_redis):
        """
        Проверяем, что TM_NAMESPACE читается из переменной окружения.
        """
        with patch.dict(os.environ, {"TM_NAMESPACE": "trade-monitor"}, clear=True):
            monitor = TradeMonitorService(
                redis_client=mock_redis,
                config={},
                regime_guard=None,
                health_metrics=None
            )
            
            assert monitor.namespace == "trade-monitor"
            assert "trade-monitor" in monitor._sid_dedup_key("test-signal-789")

    def test_empty_namespace_fallback(self, mock_redis):
        """
        Проверяем, что пустой TM_NAMESPACE падает обратно на 'default'.
        """
        with patch.dict(os.environ, {"TM_NAMESPACE": ""}, clear=True):
            monitor = TradeMonitorService(
                redis_client=mock_redis,
                config={},
                regime_guard=None,
                health_metrics=None
            )
            
            assert monitor.namespace == "default"

    def test_whitespace_namespace_fallback(self, mock_redis):
        """
        Проверяем, что пробельный TM_NAMESPACE падает обратно на 'default'.
        """
        with patch.dict(os.environ, {"TM_NAMESPACE": "   "}, clear=True):
            monitor = TradeMonitorService(
                redis_client=mock_redis,
                config={},
                regime_guard=None,
                health_metrics=None
            )
            
            assert monitor.namespace == "default"

    def test_sid_dedup_key_isolation(self, mock_redis):
        """
        Проверяем, что разные namespace генерируют разные ключи для одного SID.
        
        Критично для предотвращения race condition между сервисами.
        """
        sid = "crypto-signal-btcusdt-1737997029123"
        
        # Сервис 1: trade-monitor
        with patch.dict(os.environ, {"TM_NAMESPACE": "trade-monitor"}, clear=True):
            monitor1 = TradeMonitorService(
                redis_client=mock_redis,
                config={},
                regime_guard=None,
                health_metrics=None
            )
            key1 = monitor1._sid_dedup_key(sid)
        
        # Сервис 2: signal-tracker
        with patch.dict(os.environ, {"TM_NAMESPACE": "signal-tracker"}, clear=True):
            monitor2 = TradeMonitorService(
                redis_client=mock_redis,
                config={},
                regime_guard=None,
                health_metrics=None
            )
            key2 = monitor2._sid_dedup_key(sid)
        
        # Ключи должны быть разными
        assert key1 != key2
        assert "trade-monitor" in key1
        assert "signal-tracker" in key2
        assert sid in key1
        assert sid in key2

    def test_dedup_key_isolation(self, mock_redis):
        """
        Проверяем, что разные namespace генерируют разные ключи для одного event_id.
        """
        kind = "tp_hit"
        event_id = "order-123-tp1"
        
        # Сервис 1: trade-monitor
        with patch.dict(os.environ, {"TM_NAMESPACE": "trade-monitor"}, clear=True):
            monitor1 = TradeMonitorService(
                redis_client=mock_redis,
                config={},
                regime_guard=None,
                health_metrics=None
            )
            key1 = monitor1._dedup_key(kind, event_id)
        
        # Сервис 2: signal-tracker
        with patch.dict(os.environ, {"TM_NAMESPACE": "signal-tracker"}, clear=True):
            monitor2 = TradeMonitorService(
                redis_client=mock_redis,
                config={},
                regime_guard=None,
                health_metrics=None
            )
            key2 = monitor2._dedup_key(kind, event_id)
        
        # Ключи должны быть разными
        assert key1 != key2
        assert "trade-monitor" in key1
        assert "signal-tracker" in key2
        assert kind in key1
        assert kind in key2

    def test_sid_claim_uses_namespace(self, mock_redis):
        """
        Проверяем, что _sid_claim использует namespace в ключе Redis.
        """
        sid = "test-signal-claim"
        
        with patch.dict(os.environ, {"TM_NAMESPACE": "trade-monitor"}, clear=True):
            monitor = TradeMonitorService(
                redis_client=mock_redis,
                config={},
                regime_guard=None,
                health_metrics=None
            )
            
            # Вызываем claim
            result = monitor._sid_claim(sid, ttl_sec=30)
            
            # Проверяем, что Redis.set был вызван с правильным ключом
            assert mock_redis.set.called
            call_args = mock_redis.set.call_args[0]
            key = call_args[0]
            
            assert "trade-monitor" in key
            assert sid in key
            assert result is True

    def test_sid_finalize_uses_namespace(self, mock_redis):
        """
        Проверяем, что _sid_finalize использует namespace в ключе Redis.
        """
        sid = "test-signal-finalize"
        
        with patch.dict(os.environ, {"TM_NAMESPACE": "signal-tracker"}, clear=True):
            monitor = TradeMonitorService(
                redis_client=mock_redis,
                config={},
                regime_guard=None,
                health_metrics=None
            )
            
            # Вызываем finalize
            monitor._sid_finalize(sid, ttl_days=7)
            
            # Проверяем, что Redis.set был вызван с правильным ключом
            assert mock_redis.set.called
            call_args = mock_redis.set.call_args[0]
            key = call_args[0]
            
            assert "signal-tracker" in key
            assert sid in key

    def test_sid_release_uses_namespace(self, mock_redis):
        """
        Проверяем, что _sid_release использует namespace в ключе Redis.
        """
        sid = "test-signal-release"
        
        with patch.dict(os.environ, {"TM_NAMESPACE": "trade-monitor"}, clear=True):
            monitor = TradeMonitorService(
                redis_client=mock_redis,
                config={},
                regime_guard=None,
                health_metrics=None
            )
            
            # Вызываем release
            monitor._sid_release(sid)
            
            # Проверяем, что Redis.delete был вызван с правильным ключом
            assert mock_redis.delete.called
            call_args = mock_redis.delete.call_args[0]
            key = call_args[0]
            
            assert "trade-monitor" in key
            assert sid in key

    def test_dedup_acquire_uses_namespace(self, mock_redis):
        """
        Проверяем, что _dedup_acquire использует namespace.
        """
        kind = "tp_hit"
        event_id = "event-ext-789"
        
        with patch.dict(os.environ, {"TM_NAMESPACE": "signal-tracker"}, clear=True):
            monitor = TradeMonitorService(
                redis_client=mock_redis,
                config={},
                regime_guard=None,
                health_metrics=None
            )
            
            # Вызываем dedup check
            result = monitor._dedup_acquire(kind, event_id)
            
            # Проверяем, что Redis.set был вызван с правильным ключом
            assert mock_redis.set.called
            call_args = mock_redis.set.call_args
            key = call_args[0][0]
            
            assert "signal-tracker" in key
            assert kind in key
            assert event_id in key
            assert result is True

    def test_namespace_isolation_in_redis_keys(self, mock_redis):
        """
        Интеграционный тест: проверяем, что два сервиса с разными namespace
        создают независимые ключи в Redis для одного и того же сигнала.
        
        Это основной сценарий, который решает проблему race condition.
        """
        signal_id = "btcusdt-signal-1737997029-conf-78"
        
        # Симулируем scanner-trade-monitor
        with patch.dict(os.environ, {"TM_NAMESPACE": "trade-monitor"}, clear=True):
            monitor_tm = TradeMonitorService(
                redis_client=mock_redis,
                config={},
                regime_guard=None,
                health_metrics=None
            )
            key_tm = monitor_tm._sid_dedup_key(signal_id)
        
        # Симулируем scanner-signal-tracker
        with patch.dict(os.environ, {"TM_NAMESPACE": "signal-tracker"}, clear=True):
            monitor_st = TradeMonitorService(
                redis_client=mock_redis,
                config={},
                regime_guard=None,
                health_metrics=None
            )
            key_st = monitor_st._sid_dedup_key(signal_id)
        
        # Ключи разные → нет race condition
        assert key_tm != key_st
        
        # Формат ключей корректный
        assert key_tm == f"dedup:trade_monitor:trade-monitor:sid:{signal_id}"
        assert key_st == f"dedup:trade_monitor:signal-tracker:sid:{signal_id}"

    def test_namespace_logging(self, mock_redis, caplog):
        """
        Проверяем, что namespace логируется при инициализации сервиса.
        """
        import logging
        caplog.set_level(logging.INFO)
        
        with patch.dict(os.environ, {"TM_NAMESPACE": "test-namespace-123"}, clear=True):
            monitor = TradeMonitorService(
                redis_client=mock_redis,
                config={},
                regime_guard=None,
                health_metrics=None
            )
            
            # Проверяем, что namespace был залогирован
            assert any("test-namespace-123" in record.message for record in caplog.records)
            assert any("TradeMonitorService namespace" in record.message for record in caplog.records)


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])

