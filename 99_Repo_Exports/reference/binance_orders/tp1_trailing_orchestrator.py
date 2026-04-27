# -*- coding: utf-8 -*-
"""
Оркестратор трейлинга после TP1.

Получает событие TP1_HIT → проверяет исходный сигнал → 
если надо — шлёт команду трейлинга в gateway.

Интегрировано с scanner_infra:
- Redis для сигналов и событий
- Связь с go-gateway через OrderTrailingDispatcher
- Поддержка различных источников сигналов
"""

import json
import math
import os
import time
from dataclasses import dataclass
from typing import Dict, Any, Optional, List, Iterable, Sequence, Tuple
import redis

from services.trailing_profiles import TrailingProfilesRegistry
from services.order_trailing_dispatcher import OrderTrailingDispatcher
from common.log import setup_logger

try:
    from services.trailing_metrics import TrailingMetrics
    HAS_METRICS = True
except ImportError:
    HAS_METRICS = False
    TrailingMetrics = None

try:
    from services.trade_events_logger import TradeEventsLogger
    HAS_EVENTS_LOGGER = True
except ImportError:
    HAS_EVENTS_LOGGER = False
    TradeEventsLogger = None

log = setup_logger("tp1_trailing_orchestrator")


@dataclass
class TrailingResult:
    success: bool
    skipped: bool = False
    new_sl: Optional[float] = None
    profile_name: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
    reason: Optional[str] = None


def _parse_csv_env(name: str, default: Optional[Sequence[str]] = None) -> List[str]:
    """Parse comma-separated env into list of non-empty strings."""
    raw = os.getenv(name)
    if raw is None:
        return list(default) if default else []
    items = [item.strip() for item in raw.split(",") if item.strip()]
    if not items and default:
        return list(default)
    return items


def _normalize_symbol(symbol: Optional[str]) -> Optional[str]:
    if symbol is None:
        return None
    result = symbol.strip().upper()
    return result or None


def _normalize_source(source: Optional[str]) -> Optional[str]:
    if source is None:
        return None
    result = source.strip().lower()
    return result or None


def _normalize_prefixes(prefixes: Iterable[str]) -> List[str]:
    cleaned: List[str] = []
    for item in prefixes:
        if not item:
            continue
        normalized = item.strip()
        if not normalized:
            continue
        if not normalized.endswith(":"):
            normalized = f"{normalized}:"
        cleaned.append(normalized)
    seen = set()
    unique: List[str] = []
    for prefix in cleaned:
        if prefix not in seen:
            unique.append(prefix)
            seen.add(prefix)
    return unique


def _to_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    if isinstance(value, (int, float)):
        return value != 0
    if isinstance(value, str):
        lowered = value.strip().lower()
        return lowered in {"true", "1", "yes", "y", "on"}
    return False


def _to_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value)
        except ValueError:
            return None
    return None


class TP1TrailingOrchestrator:
    """
    Получает событие от исполнителя (TP1_HIT) → 
    смотрит исходный сигнал → 
    если надо — шлёт в gateway команду трейлинга.
    """

    def __init__(
        self, 
        redis_client: Optional[redis.Redis] = None,
        profiles: Optional[TrailingProfilesRegistry] = None,
        gateway_url: Optional[str] = None
    ):
        """
        Args:
            redis_client: Redis клиент (если None, создаётся новый)
            profiles: Реестр профилей трейлинга (если None, создаётся новый)
            gateway_url: URL go-gateway (если None, берётся из env)
        """
        # Redis connection
        if redis_client is None:
            redis_url = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
            self.r = redis.from_url(redis_url, decode_responses=True)
            log.info("✅ Created Redis client: %s", redis_url)
        else:
            self.r = redis_client
            log.debug("Using provided Redis client")
        
        # Profiles registry
        if profiles is None:
            self.profiles = TrailingProfilesRegistry()
        else:
            self.profiles = profiles
        
        # Dispatcher to gateway
        gateway_url = gateway_url or os.getenv("GATEWAY_URL", "http://scanner-go-gateway:8090")
        self.dispatcher = OrderTrailingDispatcher(gateway_url)
        
        # Events logger для trade_back
        if HAS_EVENTS_LOGGER:
            self.events_logger = TradeEventsLogger()
            log.info("✅ Trade events logger initialized")
        else:
            self.events_logger = None
            log.warning("⚠️  Trade events logger not available")
        
        # Конфигурация
        self.signal_key_prefix = os.getenv("SIGNAL_KEY_PREFIX", "signals:")
        self.default_profile = os.getenv("DEFAULT_TRAIL_PROFILE", "rocket_v1")
        trailing_symbols_env = _parse_csv_env("TRAILING_SYMBOLS")
        if trailing_symbols_env and any(sym.strip() == "*" for sym in trailing_symbols_env):
            symbol_list: List[str] = []
            self.symbol_filter_enabled = False
        else:
            if trailing_symbols_env:
                symbol_list = trailing_symbols_env
            else:
                symbol_list = ["XAUUSD", "BTCUSDT", "ETHUSDT"]
            self.symbol_filter_enabled = True
        self.trailing_symbols = {
            _normalize_symbol(sym) for sym in symbol_list if _normalize_symbol(sym)
        }

        trailing_sources_env = _parse_csv_env("TRAILING_SOURCES")
        if trailing_sources_env and any(src.strip() == "*" for src in trailing_sources_env):
            source_list = []
            self.source_filter_enabled = False
        else:
            source_defaults = ["orderflow", "aggregatedhub-v2", "cryptoorderflow"]
            source_list = trailing_sources_env if trailing_sources_env else source_defaults
            self.source_filter_enabled = True
        self.trailing_sources = {
            _normalize_source(src) for src in source_list if _normalize_source(src)
        }

        default_prefixes: List[str] = [
            self.signal_key_prefix,
            "signals:audit:",
            "signals:crypto:",
            "signal:",
            "signal:snap:",
        ]
        custom_prefixes = _parse_csv_env("SIGNAL_KEY_PREFIXES")
        prefixes = custom_prefixes if custom_prefixes else default_prefixes
        self.signal_key_prefixes = _normalize_prefixes(prefixes)
        
        # Статистика
        self.stats = {
            "events_processed": 0,
            "tp1_hits": 0,
            "trailing_started": 0,
            "trailing_failed": 0,
            "signals_not_found": 0,
            "no_trail_flag": 0,
        }
        
        log.info(
            "✅ TP1TrailingOrchestrator initialized | default_profile=%s profiles=%d",
            self.default_profile, len(self.profiles.list_names())
        )
        if self.symbol_filter_enabled:
            log.info("🎯 Trailing symbols: %s", ", ".join(sorted(self.trailing_symbols)))
        else:
            log.info("🎯 Trailing symbols: ALL")
        if self.source_filter_enabled:
            log.info("🎯 Trailing sources: %s", ", ".join(sorted(self.trailing_sources)))
        else:
            log.info("🎯 Trailing sources: ALL")
        log.info("🗝️ Signal key prefixes: %s", ", ".join(self.signal_key_prefixes))

    def handle_event(self, event: Dict[str, Any]) -> bool:
        """
        Обработать событие из Redis stream.
        
        Event format:
        {
            "event_type": "TP1_HIT" | "TP2_HIT" | "SL_HIT" | "POSITION_OPENED",
            "sid": "signal-XAUUSD-1730222790",
            "symbol": "XAUUSD",
            "position_id": "1234567",  # MT5 ticket
            "ticket": "1234567",        # альтернативное поле
            "price": "2769.9",
            "ts": "1730222790",
            "source": "paper_executor" | "mt5" | "backtest"
        }
        
        Args:
            event: Словарь с данными события
            
        Returns:
            True если событие обработано успешно
        """
        result = self._process_tp1_event(event, record_stats=True)
        return result.success or result.skipped

    def start_trailing(
        self,
        sid: str,
        symbol: str,
        price: float,
        position_id: Optional[str] = None,
        source: str = "signal_performance_tracker",
        event_ts: Optional[Any] = None,
        signal_payload: Optional[Dict[str, Any]] = None,
        signal_key: Optional[str] = None,
    ) -> TrailingResult:
        """
        Прямой запуск расчёта трейлинга из приложений (без внешнего события).
        """
        event: Dict[str, Any] = {
            "event_type": "TP1_HIT",
            "sid": sid,
            "symbol": symbol,
            "price": price,
            "position_id": position_id,
            "source": source,
            "ts": event_ts,
        }
        if signal_payload:
            event["_signal_payload"] = signal_payload
        if signal_key:
            event["_signal_key"] = signal_key
        return self._process_tp1_event(event, record_stats=True)

    def _process_tp1_event(self, event: Dict[str, Any], record_stats: bool) -> TrailingResult:
        # Recommendation D: deduplication to prevent double processing of TP1_HIT
        sid = event.get("sid")
        if sid:
            dedup_key = f"dedup:tp1_trailing:{sid}"
            # TTL: 3 days to match recommendation idea or enough for typical trade duration
            if not self.r.set(dedup_key, "1", nx=True, ex=86400*3):
                return TrailingResult(success=True, skipped=True, reason="dedup_hit")

        self.stats["events_processed"] += 1
        
        event_type = event.get("event_type")
        symbol_raw = event.get("symbol", "UNKNOWN")
        symbol = _normalize_symbol(symbol_raw) or "UNKNOWN"
        
        if HAS_METRICS:
            TrailingMetrics.record_event(event_type, symbol)
        
        if event_type != "TP1_HIT":
            return TrailingResult(success=False, skipped=True, error="unsupported_event")
        
        if self.symbol_filter_enabled and symbol not in self.trailing_symbols:
            log.debug("Skipping event for symbol %s (not in trailing list)", symbol)
            return TrailingResult(success=False, skipped=True, error="symbol_filtered")
        
        if record_stats:
            self.stats["tp1_hits"] += 1
        
        sid = event.get("sid")
        symbol = symbol or "XAUUSD"
        position_id = event.get("position_id") or event.get("ticket")
        price = float(event.get("price", 0.0))
        source = event.get("source", "unknown")

        if not sid:
            log.warning("⚠️  Event missing sid, skip: %s", event)
            return TrailingResult(success=False, skipped=False, error="sid_missing")
        
        log.info(
            "🎯 TP1_HIT event: sid=%s symbol=%s price=%.2f position=%s source=%s",
            sid, symbol, price, position_id, source
        )

        signal_key = event.get("_signal_key")
        signal_payload = event.get("_signal_payload")
        if signal_payload:
            signal = dict(signal_payload)
        else:
            signal_data = self._get_signal(sid)
            if not signal_data:
                if record_stats:
                    self.stats["signals_not_found"] += 1
                if HAS_METRICS:
                    TrailingMetrics.record_signal_not_found(symbol)
                log.warning("⚠️  Signal not found in Redis: %s", sid)
                return TrailingResult(success=False, skipped=False, error="signal_not_found")
            signal, signal_key = signal_data
        
        signal_source = _normalize_source(signal.get("source"))
        if self.source_filter_enabled and signal_source not in self.trailing_sources:
            log.debug(
                "Signal %s source %s is not in trailing sources list, skip",
                sid,
                signal_source,
            )
            return TrailingResult(success=False, skipped=True, error="source_filtered")
        
        if not _to_bool(signal.get("trail_after_tp1")):
            if record_stats:
                self.stats["no_trail_flag"] += 1
            if HAS_METRICS:
                TrailingMetrics.record_signal_without_flag(symbol)
            log.debug(
                "Signal %s does not have trail_after_tp1 flag, skip trailing",
                sid
            )
            return TrailingResult(success=False, skipped=True, error="trail_flag_disabled")
        
        profile_name = signal.get("trail_profile", self.default_profile)
        profile = self.profiles.get(profile_name)
        
        if not profile:
            log.warning(
                "⚠️  Trailing profile not found: %s (sid=%s), using default: %s",
                profile_name, sid, self.default_profile
            )
            profile = self.profiles.get(self.default_profile)
            
            if not profile:
                log.error(
                    "❌ Default profile not found: %s, cannot start trailing",
                    self.default_profile
                )
                if record_stats:
                    self.stats["trailing_failed"] += 1
                return TrailingResult(success=False, skipped=False, error="profile_not_found")
        
        side = str(signal.get("side", "LONG")).upper()
        if side not in ("LONG", "SHORT"):
            side = "LONG"

        tp_levels = self._normalize_tp_levels(signal.get("tp_levels"))

        original_sl = signal.get("sl")
        original_sl_value = _to_float(original_sl)

        base_metadata = {
            "triggered_by": "TP1_HIT",
            "tp1_price": price,
            "source": source,
            "timestamp": event.get("ts"),
            "profile_name": profile.name,
            "profile_mode": profile.mode,
        }
        if original_sl_value is not None:
            base_metadata["previous_sl"] = original_sl_value

        atr_value = _to_float(signal.get("atr"))

        point_size = self.dispatcher.get_symbol_point(symbol)

        trail_distance_price = None
        if atr_value and atr_value > 0:
            trail_distance_price = atr_value * profile.atr_mult
        elif profile.mode == "POINTS" and profile.points:
            trail_distance_price = profile.points * point_size

        if not trail_distance_price or trail_distance_price <= 0:
            log.warning(
                "⚠️  Cannot compute trailing distance for sid=%s (ATR missing and profile mode %s)",
                sid, profile.mode
            )
            if record_stats:
                self.stats["trailing_failed"] += 1
            if HAS_METRICS:
                TrailingMetrics.record_trailing_failed(symbol, "distance_not_computed")
            return TrailingResult(success=False, skipped=False, error="distance_not_computed")

        new_sl = self._compute_trailing_sl(
            side=side,
            tp1_price=price,
            trail_distance=trail_distance_price,
            original_sl=original_sl_value,
            point=point_size
        )

        if not new_sl:
            log.info(
                "ℹ️ Trailing SL unchanged for sid=%s (computed distance %.5f insufficient)",
                sid, trail_distance_price
            )
            return TrailingResult(success=False, skipped=True, error="distance_insufficient")

        trail_points = trail_distance_price / point_size if point_size > 0 else None
        metadata = dict(base_metadata)
        metadata.update({
            "trail_distance_price": trail_distance_price,
            "point_size": point_size,
            "trail_mode": "continuous",
        })
        if atr_value and atr_value > 0:
            metadata.update({
                "atr_value": atr_value,
                "atr_mult": profile.atr_mult,
                "calculated_from_signal_atr": True,
            })
        if trail_points:
            metadata["trail_points"] = trail_points

        command_metadata = dict(metadata)
        command_metadata["stage"] = "start_trailing"

        if atr_value and atr_value > 0:
            trailing_sent = self.dispatcher.send_trailing_command_from_atr(
                sid=sid,
                symbol=symbol,
                position_id=position_id,
                atr_value=atr_value,
                atr_mult=profile.atr_mult,
                point=point_size,
                metadata=command_metadata,
            )
        else:
            trailing_sent = self.dispatcher.send_trailing_command(
                sid=sid,
                symbol=symbol,
                position_id=position_id,
                profile=profile,
                metadata=command_metadata,
            )

        if not trailing_sent:
            log.error(
                "❌ Failed to start trailing for sid=%s symbol=%s profile=%s",
                sid,
                symbol,
                profile.name,
            )
            if record_stats:
                self.stats["trailing_failed"] += 1
            if HAS_METRICS:
                TrailingMetrics.record_trailing_failed(symbol, "trailing_command_failed")
            return TrailingResult(success=False, skipped=False, error="trailing_command_failed")

        # Ограничиваем очистку TP2/TP3 только для профиля rocket_v1
        is_rocket = (profile.name == "rocket_v1")
        
        modify_metadata = dict(metadata)
        modify_metadata["stage"] = "set_initial_sl"
        modify_metadata["tp_levels_before"] = list(tp_levels) if tp_levels else []
        modify_metadata["tp_levels_cleared"] = is_rocket
        modify_metadata["clear_tp_levels"] = is_rocket

        dispatch_success = self.dispatcher.send_trailing_modify(
            sid=sid,
            symbol=symbol,
            side=side,
            position_id=position_id,
            new_sl=new_sl,
            tp_levels=[] if is_rocket else list(tp_levels),
            metadata=modify_metadata,
            clear_tp_levels=is_rocket,
        )
        if not dispatch_success:
            log.error(
                "❌ Failed to send trailing modify to gateway: sid=%s side=%s profile=%s",
                sid,
                side,
                profile.name,
            )
            if record_stats:
                self.stats["trailing_failed"] += 1
            if HAS_METRICS:
                TrailingMetrics.record_trailing_failed(symbol, "gateway_error")
            return TrailingResult(success=False, skipped=False, error="gateway_error")

        initial_sl_for_log = signal.get("sl")
        self._persist_signal_sl_update(signal_key, signal, new_sl, clear_tp_levels=is_rocket)

        if record_stats:
            self.stats["trailing_started"] += 1
        if HAS_METRICS:
            TrailingMetrics.record_trailing_started(symbol, profile.name)
        
        log.info(
            "✅ Trailing modify sent: sid=%s side=%s new_sl=%.5f (profile=%s)",
            sid, side, new_sl if new_sl is not None else float('nan'), profile.name
        )

        self._write_trailing_event(
            sid=sid,
            symbol=symbol,
            profile_name=profile.name,
            event_type="TRAILING_STARTED",
            metadata={
                "tp1_price": price,
                "position_id": position_id,
                "source": source,
                "new_sl": f"{new_sl:.10f}" if new_sl is not None else "",
                "tp_levels_cleared": is_rocket,
                "clear_tp_levels": is_rocket,
            }
        )
        
        if self.events_logger:
            self.events_logger.log_trailing_started(
                sid=sid,
                symbol=symbol,
                profile=profile.name,
                initial_sl=initial_sl_for_log,
                tp1_price=price,
                position_id=position_id,
                source="tp1_trailing_orchestrator"
            )

        metadata["tp_levels_cleared"] = is_rocket
        metadata["tp_levels_before"] = list(tp_levels) if tp_levels else []
        metadata["clear_tp_levels"] = is_rocket

        return TrailingResult(
            success=True,
            skipped=False,
            new_sl=new_sl,
            profile_name=profile.name,
            metadata=metadata
        )
    
    def _persist_signal_sl_update(
        self,
        key: Optional[str],
        signal: Dict[str, Any],
        new_sl: float,
        clear_tp_levels: bool = False
    ) -> None:
        if not key:
            return
        try:
            previous_sl = signal.get("sl")
            signal["sl"] = new_sl
            signal.setdefault("trailing_history", []).append({
                "ts": int(time.time() * 1000),
                "new_sl": new_sl,
                "reason": "tp1_trailing_orchestrator",
                "tp_levels_cleared": clear_tp_levels,
            })
            if clear_tp_levels:
                signal["tp_levels"] = []
                signal.pop("tp2", None)
                signal.pop("tp3", None)
                signal.pop("tp_rest", None)
            self.r.set(key, json.dumps(signal))
            log.debug(
                "🔄 Signal SL updated in Redis: key=%s old=%s new=%.5f",
                key,
                previous_sl,
                new_sl
            )
        except Exception as exc:
            log.warning("⚠️ Failed to persist updated SL for %s: %s", key, exc)

    def _get_signal(self, sid: str) -> Optional[Tuple[Dict[str, Any], str]]:
        """
        Получить сигнал из Redis.
        
        Проверяет несколько ключей:
        - signals:{sid}
        - signals:audit:{sid}
        - signal:{sid}
        
        Args:
            sid: ID сигнала
            
        Returns:
            Словарь с данными сигнала или None
        """
        # Возможные ключи для поиска сигнала
        for key in (f"{prefix}{sid}" for prefix in self.signal_key_prefixes):
            try:
                data = self.r.get(key)
                if data:
                    signal = json.loads(data)
                    log.debug("Signal found in Redis: %s", key)
                    return signal, key
            except json.JSONDecodeError as e:
                log.warning("Failed to parse signal JSON from %s: %s", key, e)
            except Exception as e:
                log.debug("Error reading signal from %s: %s", key, e)
        
        return None
    
    def _write_trailing_event(
        self,
        sid: str,
        symbol: str,
        profile_name: str,
        event_type: str = "TRAILING_STARTED",
        metadata: Optional[Dict] = None
    ):
        """
        Записать событие трейлинга в Redis.
        
        Пишет в:
        - stream: events:trades
        - hash: trade:events:{sid}
        
        Args:
            sid: ID сигнала
            symbol: Символ
            profile_name: Имя профиля трейлинга
            event_type: Тип события
            metadata: Дополнительные метаданные
        """
        event = {
            "event_type": event_type,
            "sid": sid,
            "symbol": symbol,
            "profile": profile_name,
            "ts": int(time.time() * 1000),
            "source": "tp1_trailing_orchestrator"
        }
        
        if metadata:
            event.update(metadata)
            # Дублируем clear_tp_levels для слушателей, конвертируем в int
            clear_value = event.get("clear_tp_levels") or event.get("tp_levels_cleared")
            event.setdefault("clear_tp_levels", int(bool(clear_value)) if clear_value is not None else 0)

        # Конвертируем все значения в сериализуемые типы для Redis
        def _make_serializable(obj):
            if isinstance(obj, bool):
                return int(obj)
            elif isinstance(obj, (int, float, str)):
                return obj
            elif obj is None:
                return ""
            else:
                return str(obj)

        serializable_event = {k: _make_serializable(v) for k, v in event.items()}

        try:
            # Пишем в stream
            stream_name = "events:trades"
            self.r.xadd(stream_name, serializable_event, maxlen=10000, approximate=True)
            
            # Пишем в hash для истории
            hash_key = f"trade:events:{sid}"
            self.r.rpush(hash_key, json.dumps(event))
            self.r.expire(hash_key, 86400 * 7)  # TTL 7 дней
            
            log.debug("Trailing event written: %s", event_type)
            
        except Exception as e:
            log.warning("Failed to write trailing event: %s", e)
    
    @staticmethod
    def _normalize_tp_levels(raw_levels: Optional[Any]) -> List[float]:
        levels: List[float] = []
        if not raw_levels:
            return levels
        if isinstance(raw_levels, str):
            raw_levels = [part.strip() for part in raw_levels.split(",") if part.strip()]
        elif not isinstance(raw_levels, list):
            raw_levels = [raw_levels]
        for item in raw_levels:
            value = _to_float(item)
            if value is not None:
                levels.append(value)
        return levels

    @staticmethod
    def _round_to_point(side: str, value: float, point: float) -> float:
        if point <= 0:
            return value
        if side == "SHORT":
            return math.ceil(value / point) * point
        return math.floor(value / point) * point

    def _compute_trailing_sl(
        self,
        side: str,
        tp1_price: float,
        trail_distance: float,
        original_sl: Optional[float],
        point: float
    ) -> Optional[float]:
        if trail_distance <= 0 or tp1_price <= 0:
            return None

        if point <= 0:
            point = 0.0001

        side = side.upper()

        if side == "SHORT":
            candidate = tp1_price + trail_distance
            if original_sl is not None:
                candidate = min(candidate, original_sl)
            candidate = max(candidate, tp1_price + point)
            candidate = self._round_to_point(side, candidate, point)
            if candidate <= tp1_price:
                candidate = tp1_price + point
        else:
            candidate = tp1_price - trail_distance
            if original_sl is not None:
                candidate = max(candidate, original_sl)
            candidate = min(candidate, tp1_price - point)
            candidate = self._round_to_point("LONG", candidate, point)
            if candidate >= tp1_price:
                candidate = tp1_price - point

        if candidate <= 0:
            return None

        if original_sl is not None and abs(candidate - original_sl) < (point / 2):
            return None

        return candidate

    def get_stats(self) -> Dict[str, int]:
        """Получить статистику работы оркестратора."""
        return self.stats.copy()
    
    def log_stats(self):
        """Вывести статистику в лог."""
        log.info(
            "📊 TP1 Trailing Stats: processed=%d tp1_hits=%d started=%d failed=%d not_found=%d no_flag=%d",
            self.stats["events_processed"],
            self.stats["tp1_hits"],
            self.stats["trailing_started"],
            self.stats["trailing_failed"],
            self.stats["signals_not_found"],
            self.stats["no_trail_flag"]
        )


if __name__ == "__main__":
    # Тестирование
    orchestrator = TP1TrailingOrchestrator()
    
    # Тестовое событие
    test_event = {
        "event_type": "TP1_HIT",
        "sid": "signal-XAUUSD-1730222790",
        "symbol": "XAUUSD",
        "position_id": "1234567",
        "price": "2769.9",
        "ts": "1730222790",
        "source": "test"
    }
    
    print("\n=== Testing TP1TrailingOrchestrator ===")
    print(f"Test event: {test_event}")
    
    # Обработка события
    success = orchestrator.handle_event(test_event)
    print(f"\n{'✅' if success else '❌'} Event handled: {success}")
    
    # Статистика
    orchestrator.log_stats()

