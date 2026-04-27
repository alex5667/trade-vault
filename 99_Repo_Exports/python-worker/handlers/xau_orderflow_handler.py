from utils.time_utils import get_ny_time_millis
"""
XAU Order Flow Handler - анализ ордер-флоу по тиковым данным XAUUSD.

ФУНКЦИОНАЛ:
- Чтение тиков из stream:tick_XAUUSD через consumer group
- Расчет Delta, Z-score, Weak Progress, OBI (Order Book Imbalance)
- Детекция Iceberg orders (эвристика)
- Фильтрация сигналов по дневным уровням Pivot/Camarilla
- Публикация сигналов в notify:telegram

ТИПЫ СИГНАЛОВ:
1. Absorption - агрессивная покупка/продажа у уровня без прогресса цены
2. Breakout - delta spike с пробоем уровня и удержанием
3. Continuation - устойчивый OBI в одном направлении
4. Iceberg - обнаружение скрытых крупных ордеров

ИНТЕГРАЦИЯ:
- Использует DualRedisClient для устойчивости
- Соблюдает паттерны consumer groups (XREADGROUP/XACK)
- Публикует в notify:telegram (читается notify-worker)
"""

import os
import json
import time
import sys
import threading
from collections import deque
from statistics import mean, pstdev
from dataclasses import dataclass
from typing import Optional, Dict

# from core.redis_client import ...
# from core.dual_redis_client import ...
# from core.config import ...
from core.xauusd_signal_formatter import XAUUSDSignalFormatter, XAUUSDSignal
from signals.pivots import compute_daily_pivots, check_pivot_proximity, PivotProximityCfg
from signals.atr import ATR
from signals.position_sizing import suggest_lot
from signals.detectors import obi_from_book, weak_progress as check_weak_progress
from signals.orderbook_metrics import BestLevelTracker
from signals.risk_levels import compute_levels  # v5.1: SL/TP calculation
from .regime_gate import RegimeGateCfg, regime_allows


# Конфигурация из переменных окружения
REDIS_URL = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
TICK_STREAM = XAU_TICK_STREAM
BOOK_STREAM = XAU_BOOK_STREAM
GROUP = os.getenv("XAU_GROUP", "xauusd-signal-group")  # v3: unified group для tick+book
CONSUMER_NAME_PREFIX = os.getenv("XAU_CONSUMER", "xau-handler")
NOTIFY_STREAM = os.getenv("NOTIFY_STREAM", "notify:telegram")
USE_TG_BTNS = os.getenv("USE_TELEGRAM_BUTTONS", "0") == "1"  # v4: optional Telegram inline buttons
ATR_SOURCE = os.getenv("ATR_SOURCE", "redis")  # v5: ticks | redis
ATR_TF = os.getenv("ATR_TF", "1m")  # v5: timeframe for Redis ATR
SYMBOL = os.getenv("XAU_SYMBOL", "XAUUSD")  # v5: symbol name
ORDERFLOW_SIGNAL_STREAM = os.getenv("ORDERFLOW_SIGNAL_STREAM", f"signals:orderflow:{SYMBOL}")  # v4.1: для aggregated-hub
SNAP_PREFIX = os.getenv("SNAP_PREFIX", "signal:snap:")  # v6: snapshot storage prefix
SNAP_TTL = int(os.getenv("SNAP_TTL", "21600"))  # v6: snapshot TTL (6 hours default)
AUDIT_SIGNAL_STREAM = os.getenv("SIGNAL_AUDIT_STREAM", f"signals:audit:{SYMBOL}")  # v7: полный аудит сигналов

# Параметры обработчика
CFG = {
    "delta_window_ticks": int(os.getenv("XAU_DELTA_WINDOW", "120")),
    "delta_z_threshold": float(os.getenv("XAU_DELTA_Z_THRESHOLD", "3.0")),
    "weak_progress_atr": float(os.getenv("XAU_WEAK_PROGRESS_ATR", "0.10")),
    "obi_threshold": float(os.getenv("XAU_OBI_THRESHOLD", "0.5")),
    "obi_min_duration": float(os.getenv("XAU_OBI_MIN_DURATION", "2.0")),
    "iceberg_refresh_count": int(os.getenv("XAU_ICEBERG_REFRESH", "2")),
    "iceberg_min_duration": float(os.getenv("XAU_ICEBERG_DURATION", "1.5")),
    "iceberg_refresh_min_abs": float(os.getenv("XAU_ICEBERG_REFRESH_MIN_ABS", "1.0")),
    "dist_atr_threshold": float(os.getenv("XAU_DIST_ATR_THRESHOLD", "0.5")),
    "dist_bp_threshold": float(val) if (val := os.getenv("XAU_DIST_BP_THRESHOLD")) else None, 
    "dist_mode": os.getenv("XAU_DIST_MODE", "or"),
    "min_signal_interval_sec": int(os.getenv("XAU_MIN_SIGNAL_INTERVAL", "60")),  # 1 минута между сигналами
    "read_count": int(os.getenv("XAU_READ_COUNT", "100")),
    "read_block_ms": int(os.getenv("XAU_READ_BLOCK_MS", "1000")),
    # v5.1: SL/TP configuration
    "stop_mode": os.getenv("STOP_MODE", "ATR"),
    "stop_atr_mult": float(os.getenv("STOP_ATR_MULT", "1.0")),  # was 0.6
    "stop_pct": float(os.getenv("STOP_PCT", "0.2")),
    "stop_points": float(os.getenv("STOP_POINTS", "1.0")),
    "tp_mode": os.getenv("TP_MODE", "RR"),
    "tp_rr": os.getenv("TP_RR", "1,2,3"),
    "tp_atr_mults": os.getenv("TP_ATR_MULTS", "0.6,1.0,1.5"),
}


@dataclass
class Tick:
    """Структура данных тика."""
    ts: int          # timestamp в мс
    bid: float       # bid цена
    ask: float       # ask цена
    last: float      # last цена сделки
    volume: float    # объем
    flags: int       # флаги (направление сделки)


class XAUOrderFlowHandler:
    """
    Обработчик ордер-флоу для XAUUSD на основе тиковых данных.
    """
    
    def __init__(self):
        """Инициализация обработчика."""
        # Redis клиенты
        self.redis_client = get_redis()
        self.dual_redis = lambda: None()
        
        # Состояние работы
        self.is_running = False
        
        # Буферы для анализа
        self.delta_window = deque(maxlen=CFG["delta_window_ticks"])
        self.last_signal_ts = 0
        self.processed_ticks = 0  # DEBUG counter для отслеживания обработанных тиков
        self.z_delta_trigger_count = 0  # Counter для Z-DELTA TRIGGER messages
        self.signal_count_long = 0  # Счетчик LONG сигналов
        self.signal_count_short = 0  # Счетчик SHORT сигналов
        self.current_z_delta = 0.0  # Текущий z_delta для audit trail
        self.atr_fallback_count = 0  # Counter для ATR fallback warnings
        
        # Состояние для детекции iceberg
        self.best_level_state = {
            "price": None,
            "since": None,
            "refresh": 0,
            "side": None
        }
        
        # OBI tracking
        self.obi_state = deque()
        
        # ATR расчет
        self.atr_calculator = ATR(period=14)
        
        # Дневные уровни (пересчитываются на новом дне)
        self.daily_pivots = None
        self.last_pivot_date = None
        
        # Отслеживание диапазона текущего бара для weak progress
        self.bar_high = -1e9
        self.bar_low = 1e9
        self.bar_start_ts = 0
        
        # v3: Best Level Tracker для реальной iceberg детекции
        self.best_level_tracker = BestLevelTracker(
            min_duration_ms=int(CFG["iceberg_min_duration"] * 1000),
            refresh_min_abs=CFG["iceberg_refresh_min_abs"],
            refresh_count_target=CFG["iceberg_refresh_count"]
        )

        # Regime gate configuration (single source for XAU)
        self.regime_gate = RegimeGateCfg(
            breakout_min_score=float(getattr(self, "regime_breakout_min_score", 0.0)),
            extreme_min_score=float(getattr(self, "regime_extreme_min_score", 0.0)),
            obi_spike_min_score=float(getattr(self, "regime_obi_spike_min_score", 0.0)),
            absorption_max_score=float(getattr(self, "regime_absorption_max_score", 0.0)),
            allow_sweep_any=bool(getattr(self, "regime_allow_sweep_any", True)),
        )
        
        print("✅ XAUOrderFlowHandler v3 инициализирован")
        print(f"   Tick Stream: {TICK_STREAM}")
        print(f"   Book Stream: {BOOK_STREAM}")
        print(f"   Group: {GROUP} (unified consumer)")
        print(f"   Delta Z threshold: {CFG['delta_z_threshold']}")
        print(f"   Iceberg: duration={CFG['iceberg_min_duration']}s, refresh={CFG['iceberg_refresh_count']}")
        sys.stdout.flush()
    
    def start(self) -> None:
        """Запускает обработчик в отдельном потоке."""
        if self.is_running:
            print("⚠️ XAUOrderFlowHandler уже запущен")
            return
        
        self.is_running = True
        thread = threading.Thread(target=self._run_loop, daemon=True)
        thread.start()
        print("🚀 XAUOrderFlowHandler запущен")
        sys.stdout.flush()
    
    def stop(self) -> None:
        """Останавливает обработчик."""
        self.is_running = False
        print("⛔ XAUOrderFlowHandler остановлен")
        sys.stdout.flush()
    
    def _run_loop(self) -> None:
        """Основной цикл обработки тиков и order book (unified consumer)."""
        # Вспомогательная функция для создания consumer groups
        def ensure_consumer_group(stream_name: str) -> bool:
            """Создаёт consumer group для стрима, возвращает True если успешно."""
            try:
                self.redis_client.xgroup_create(
                    stream_name,
                    GROUP,
                    id='$',
                    mkstream=True
                )
                print(f"✅ Consumer group {GROUP} создана для {stream_name}")
                return True
            except Exception as e:
                error_str = str(e).upper()
                if "BUSYGROUP" in error_str:
                    print(f"ℹ️ Consumer group {GROUP} уже существует для {stream_name}")
                    return True
                else:
                    print(f"❌ Ошибка создания consumer group для {stream_name}: {e}")
                    return False
        
        try:
            # Создаем consumer groups для обоих стримов (v3: unified)
            for stream_name in [TICK_STREAM, BOOK_STREAM]:
                ensure_consumer_group(stream_name)
            
            # Уникальное имя консьюмера
            consumer_name = f"{CONSUMER_NAME_PREFIX}-{os.getpid()}-{int(time.time())}"
            
            print(f"🔄 Запуск цикла обработки тиков (consumer: {consumer_name})...")
            sys.stdout.flush()
            
            tick_count = 0
            signal_count = 0
            start_time = time.time()
            
            # Основной цикл (v3: unified consumer для tick+book)
            while self.is_running:
                try:
                    # ВОССТАНОВЛЕНО: Читаем оба стрима, но Order Book обработка временно отключена
                    messages = self.redis_client.xreadgroup(
                        GROUP,
                        consumer_name,
                        {TICK_STREAM: '>', BOOK_STREAM: '>'},  # Оба стрима как изначально
                        count=CFG["read_count"],
                        block=CFG["read_block_ms"]
                    )
                    
                    if not messages:
                        continue
                    
                    for stream, items in messages:
                        for msg_id, fields in items:
                            try:
                                # Определяем тип сообщения по stream
                                if stream == TICK_STREAM:
                                    # Обработка тика (данные напрямую в fields, не в JSON)
                                    tick_data = {
                                        'ts': int(fields.get('ts', 0)),
                                        'bid': float(fields.get('bid', 0)),
                                        'ask': float(fields.get('ask', 0)),
                                        'last': float(fields.get('last', 0)),
                                        'volume': float(fields.get('volume', 0)),
                                        'flags': int(fields.get('flags', 0)),
                                    }
                                    tick = Tick(**tick_data)
                                    self._process_tick(tick)
                                    tick_count += 1
                                    
                                elif stream == BOOK_STREAM:
                                    # ВРЕМЕННО ОТКЛЮЧЕНО: Order Book обработка
                                    # TODO: Включить когда BookBridge заработает в MT5
                                    # book_data = json.loads(fields.get("data", "{}"))
                                    # self._process_book(book_data)
                                    pass  # Временно пропускаем
                                
                            except Exception as e:
                                print(f"❌ Ошибка обработки {stream} {msg_id}: {e}")
                                sys.stdout.flush()
                            finally:
                                # ACK сообщения (once-only delivery)
                                try:
                                    self.redis_client.xack(stream, GROUP, msg_id)
                                except Exception as e:
                                    print(f"❌ Ошибка ACK {msg_id}: {e}")
                                    sys.stdout.flush()
                    
                    # Статистика каждые 60 секунд
                    if time.time() - start_time >= 60:
                        print(f"📊 XAU OrderFlow: {tick_count} тиков, {signal_count} сигналов за 60с")
                        sys.stdout.flush()
                        tick_count = 0
                        signal_count = 0
                        start_time = time.time()
                        
                except Exception as e:
                    error_str = str(e).upper()
                    # Обработка NOGROUP ошибки - пересоздаём consumer groups
                    if "NOGROUP" in error_str:
                        print(f"⚠️ Обнаружен NOGROUP для стримов, пересоздаём consumer groups...")
                        sys.stdout.flush()
                        # Пересоздаём consumer groups для всех стримов
                        for stream_name in [TICK_STREAM, BOOK_STREAM]:
                            ensure_consumer_group(stream_name)
                        sys.stdout.flush()
                        time.sleep(2)  # Даём время на создание групп
                    else:
                        print(f"❌ Ошибка в цикле обработки: {e}")
                        sys.stdout.flush()
                        time.sleep(1)
                    
        except Exception as e:
            print(f"❌ Критическая ошибка XAUOrderFlowHandler: {e}")
            sys.stdout.flush()
    
    def _process_tick(self, tick: Tick) -> None:
        """
        Обработка одного тика.
        
        Args:
            tick: Данные тика
        """
        # DEBUG: Логируем первые 20 тиков для отладки pivot инициализации
        self.processed_ticks += 1
        if self.processed_ticks <= 20 or self.processed_ticks % 100 == 0:
            print(f"🔧 DEBUG: Обработано {self.processed_ticks} тиков, delta_z_threshold={CFG.get('delta_z_threshold', 'NOT_SET')}")
            print(f"🔧 DEBUG: daily_pivots exists: {self.daily_pivots is not None}")
            print(f"🔧 DEBUG: last_pivot_date: {self.last_pivot_date}")
            if self.daily_pivots:
                print(f"🔧 DEBUG: pivot keys: {list(self.daily_pivots.keys())}")
            sys.stdout.flush()
        
        # Вычисляем mid price
        mid = (tick.bid + tick.ask) / 2 if (tick.bid and tick.ask) else (tick.last or 0.0)
        
        if mid <= 0:
            return
        
        # 1. Обновляем ATR (v5: поддержка Redis ATR)
        atr_val = self._get_atr(mid, tick.ts)
        
        # 2. Обновляем/пересчитываем дневные pivots
        self._update_pivots(tick.ts)
        
        # DEBUG: Force update pivots on first 10 ticks if they don't exist
        if self.processed_ticks <= 10 and self.daily_pivots is None:
            print(f"🔧 FORCE DEBUG: Принудительно инициализируем pivots на тике #{self.processed_ticks}")
            self.last_pivot_date = None  # Force re-initialization
            self._update_pivots(tick.ts)
            sys.stdout.flush()
        
        # 3. Классифицируем Delta
        delta = self._classify_delta(tick)
        self.delta_window.append(delta)
        
        # 4. Вычисляем Z-score Delta
        z_delta = self._zscore(self.delta_window)
        
        # 5. Weak Progress (диапазон бара / ATR)
        self._update_bar_range(mid, tick.ts)
        bar_range = abs(self.bar_high - self.bar_low)
        weak_progress = check_weak_progress(bar_range, atr_val, CFG["weak_progress_atr"])
        
        # DEBUG: Логируем каждые 50 тиков в _process_tick
        if self.processed_ticks % 50 == 0:
            recent_deltas = list(self.delta_window)[-5:] if len(self.delta_window) >= 5 else list(self.delta_window)
            print(f"🔍 PROCESS_TICK DEBUG: tick #{self.processed_ticks}, z_delta={z_delta:.3f}, atr={atr_val:.4f}, delta_window_len={len(self.delta_window)}, recent_deltas={recent_deltas}")
            sys.stdout.flush()
        
        # 6. OBI (Order Book Imbalance) - реальный из DOM или суррогат
        obi = self._calc_real_obi(tick.ts, mid)
        self._track_obi(tick.ts, obi)
        
        # 7. Iceberg эвристика
        # ВРЕМЕННО ОТКЛЮЧЕНО: Требует Order Book данных
        # TODO: Включить когда BookBridge заработает в MT5
        # self._track_iceberg(tick, mid)
        
        # 8. Генерация сигналов (v5: передаем ts для sid)
        self._generate_signals(tick.ts, mid, z_delta, weak_progress, obi, atr_val)
    
    def _classify_delta(self, tick: Tick) -> float:
        """
        Классифицирует направление сделки и возвращает Delta.
        
        Args:
            tick: Данные тика
            
        Returns:
            Delta (+volume для покупок, -volume для продаж)
        """
        # ВРЕМЕННО ОТКЛЮЧЕНО: Оригинальная логика требует реальные объёмы
        # TODO: Раскомментировать когда BookBridge заработает и будут реальные объёмы
        
        # # Примитивная классификация на основе last vs bid/ask
        # if tick.last and tick.ask and tick.last >= tick.ask:
        #     return +tick.volume  # агрессивная покупка
        # if tick.last and tick.bid and tick.last <= tick.bid:
        #     return -tick.volume  # агрессивная продажа
        # 
        # # Fallback: по движению mid
        # return +tick.volume if tick.ask > tick.bid else -tick.volume
        
        # ВРЕМЕННАЯ ЗАГЛУШКА: Возвращаем ±1.0 для работы без реальных объёмов
        # Основано на flags (TICK_FLAG_BID=2, TICK_FLAG_ASK=4)
        if tick.flags:
            if tick.flags & 2:  # Изменение bid
                return +1.0
            if tick.flags & 4:  # Изменение ask  
                return -1.0
        
        # Fallback: анализ last price vs bid/ask
        if tick.last and tick.ask and tick.last >= tick.ask:
            return +1.0  # агрессивная покупка
        if tick.last and tick.bid and tick.last <= tick.bid:
            return -1.0  # агрессивная продажа
        
        # Простейший fallback на основе спреда
        if tick.bid and tick.ask:
            return +1.0 if tick.ask > tick.bid else -1.0
        
        return 0.0  # нейтральный тик
    
    def _zscore(self, window: deque) -> float:
        """
        Вычисляет Z-score для последнего значения в окне.
        
        Args:
            window: Окно значений Delta
            
        Returns:
            Z-score последнего значения
        """
        if len(window) < max(30, window.maxlen // 4):
            return 0.0
        
        m = mean(window)
        s = pstdev(window)
        
        if s == 0:
            return 0.0
        
        return (window[-1] - m) / s
    
    def _update_bar_range(self, price: float, ts: int) -> None:
        """
        Обновляет диапазон текущего минутного бара.
        
        Args:
            price: Текущая цена
            ts: Timestamp в мс
        """
        # Сброс каждые 60 секунд
        if self.bar_start_ts == 0:
            self.bar_start_ts = ts
        
        if ts - self.bar_start_ts >= 60_000:
            self.bar_start_ts = ts
            self.bar_high = price
            self.bar_low = price
        
        self.bar_high = max(self.bar_high, price)
        self.bar_low = min(self.bar_low, price)
    
    def _get_atr(self, price: float, ts: int) -> float:
        """
        Получает ATR из Redis кэша или вычисляет локально (паттерн из candle_of_worker.py).
        
        Args:
            price: Текущая цена
            ts: Timestamp (не используется, но принимается для совместимости)
            
        Returns:
            Текущее значение ATR или расчётное значение на основе цены
        """
        # 1) Если ATR_SOURCE = "redis", пытаемся взять из кэша
        if ATR_SOURCE == "redis":
            try:
                # Попытка 1: основной ключ от go-gateway (JSON формат)
                key_gw = f"ta:last:atr:{SYMBOL}"
                cached = self.redis_client.get(key_gw)
                if cached:
                    try:
                        # Парсим JSON: {"atr": 3.5, "period": 14, "method": "wilder", "tf": "M1", "source": "gw", "ts": 1234567890}
                        import json
                        atr_data = json.loads(cached)
                        val = float(atr_data.get("atr", 0))
                        if val > 0:
                            return val
                    except (json.JSONDecodeError, KeyError, ValueError):
                        pass
                
                # Попытка 2: новый формат ключа (atr:val:SYMBOL:TF)
                key_val = f"atr:val:{SYMBOL}:{ATR_TF}"
                cached = self.redis_client.get(key_val)
                if cached:
                    val = float(cached)
                    if val > 0:
                        return val
                
                # Попытка 3: старый формат ключа (atr:SYMBOL:TF) - для совместимости
                key_old = f"atr:{SYMBOL}:{ATR_TF}"
                cached = self.redis_client.get(key_old)
                if cached:
                    val = float(cached)
                    if val > 0:
                        return val
            except Exception as e:
                print(f"⚠️ Не удалось получить ATR из Redis: {e}")
        
        # 2) Fallback: локальный ATR калькулятор (на основе тиков)
        self.atr_calculator.feed_tick(price, ts)
        atr_val = self.atr_calculator.value()
        
        if atr_val and atr_val > 0:
            return atr_val
        
        # 3) Последний fallback: фиксированное значение для XAUUSD 1m
        # Для XAUUSD типичный ATR(14) на 1m составляет ~0.5-2.0 пункта
        # Используем среднее значение 1.2 как консервативную оценку
        if ATR_TF == "1m":
            estimated_atr = 1.2  # Типичный ATR для 1m XAUUSD
        elif ATR_TF == "5m":
            estimated_atr = 3.5  # Типичный ATR для 5m XAUUSD
        elif ATR_TF == "15m":
            estimated_atr = 6.5  # Типичный ATR для 15m XAUUSD
        else:
            estimated_atr = price * 0.0003  # 0.03% от цены для других TF
        
        # Выводим предупреждение только каждое 10000-е сообщение, чтобы не засорять логи
        self.atr_fallback_count += 1
        if self.atr_fallback_count % 10000 == 0:
            print(f"⚠️ ATR недоступен (событие #{self.atr_fallback_count}), используем типичное значение для {ATR_TF}: {estimated_atr:.2f}")
        
        return estimated_atr
    
    def _calc_real_obi(self, ts: int, price: float) -> float:
        """
        Вычисляет реальный OBI из Order Book или fallback на суррогат.
        
        Args:
            ts: Timestamp
            price: Текущая цена (для логирования)
            
        Returns:
            OBI в диапазоне [-1, 1]
        """
        # Пытаемся получить реальный Order Book из кеша
        try:
            symbol = "XAUUSD"  # TODO: Сделать конфигурируемым если нужна мультисимвольность
            cache_key = f"book:latest:{symbol}"
            book_json = self.redis_client.get(cache_key)
            
            if book_json:
                book = json.loads(book_json)
                real_obi = obi_from_book(book, depth=5)
                
                if real_obi is not None:
                    # Используем реальный OBI из DOM
                    return real_obi
        except Exception:
            # Если ошибка получения DOM - используем fallback
            pass
        
        # Fallback: суррогат на основе Delta window
        return self._calc_obi_surrogate()
    
    def _calc_obi_surrogate(self) -> float:
        """
        Вычисляет суррогат OBI (Order Book Imbalance) на основе Delta.
        
        Returns:
            OBI в диапазоне [-1, 1]
        """
        if not self.delta_window:
            return 0.0
        
        buys = sum(1 for v in self.delta_window if v > 0)
        sells = sum(1 for v in self.delta_window if v < 0)
        total = buys + sells
        
        if total == 0:
            return 0.0
        
        return (buys - sells) / total
    
    def _track_obi(self, ts: int, obi: float) -> None:
        """
        Отслеживает OBI во времени для детекции устойчивости.
        
        Args:
            ts: Timestamp в мс
            obi: Значение OBI
        """
        self.obi_state.append((ts, obi))
        
        # Удаляем старые значения (старше min_duration)
        duration_ms = CFG["obi_min_duration"] * 1000
        while self.obi_state and ts - self.obi_state[0][0] > duration_ms:
            self.obi_state.popleft()
    
    def _obi_is_sustained(self) -> bool:
        """
        Проверяет устойчивость OBI.
        
        Returns:
            True, если OBI устойчиво в одном направлении
        """
        if not self.obi_state:
            return False
        
        avg_obi = sum(obi for _, obi in self.obi_state) / len(self.obi_state)
        return abs(avg_obi) >= CFG["obi_threshold"]
    
    def _track_iceberg(self, tick: Tick, mid: float) -> None:
        """
        Отслеживает потенциальные iceberg orders.
        
        Args:
            tick: Данные тика
            mid: Mid price
        """
        price = round(mid, 2)
        st = self.best_level_state
        now = tick.ts
        
        # Если цена изменилась
        if st["price"] != price:
            st.update({
                "price": price,
                "since": now,
                "refresh": 0
            })
            return
        
        # Цена "залипла" на уровне
        duration_ms = CFG["iceberg_min_duration"] * 1000
        if now - st["since"] >= duration_ms:
            # Проверяем, что есть объем
            if abs(self._recent_executed_volume()) > 0:
                st["refresh"] += 1
                st["since"] = now
                
                # Если достаточно refresh-ей, генерируем сигнал
                if st["refresh"] >= CFG["iceberg_refresh_count"]:
                    side = "SHORT" if self._recent_buying_dominant() else "LONG"
                    self._publish_signal(side, mid, "Iceberg absorption", "🧊", now)
                    st["refresh"] = 0  # сброс после сигнала
    
    def _recent_executed_volume(self) -> float:
        """Возвращает суммарный объем в delta_window."""
        return sum(abs(v) for v in self.delta_window)
    
    def _recent_buying_dominant(self) -> bool:
        """Проверяет преобладание покупок в delta_window."""
        buys = sum(1 for v in self.delta_window if v > 0)
        sells = sum(1 for v in self.delta_window if v < 0)
        return buys >= sells
    
    def _process_book(self, book_data: dict) -> None:
        """
        Обработка Order Book snapshot (v3).
        
        Args:
            book_data: DOM данные с bids/asks
        """
        ts = int(book_data.get("ts", 0))
        
        # Обновляем Best Level Tracker для iceberg детекции
        self.best_level_tracker.feed_book(book_data, ts)
        
        # Обновляем OBI из реального DOM
        real_obi = obi_from_book(book_data, depth=5)
        if real_obi is not None:
            self._track_obi(ts, real_obi)
    
    def _update_pivots(self, ts: int) -> None:
        """
        Обновляет дневные уровни Pivot при необходимости (v3).
        
        Args:
            ts: Timestamp в мс
        """
        # Определяем текущую дату (UTC)
        from datetime import datetime
        current_date = datetime.utcfromtimestamp(ts / 1000).date()
        
        # Если новый день или pivots не инициализированы
        if self.last_pivot_date != current_date:
            self.last_pivot_date = current_date
            # v3: Загружаем из Redis (публикуется ohlc_aggregator)
            hlc = self._load_yesterday_hlc()
            if hlc:
                self.daily_pivots = compute_daily_pivots(hlc)
                print(f"📊 Обновлены Pivot уровни для {current_date}")
                print(f"   H:{hlc['H']:.2f}, L:{hlc['L']:.2f}, C:{hlc['C']:.2f}")
                sys.stdout.flush()
    
    def _load_yesterday_hlc(self) -> Optional[Dict[str, float]]:
        """
        Загружает H/L/C предыдущего дня из Redis (v3).
        
        Данные публикуются сервисом ohlc_aggregator в key pivots:latest.
        
        Returns:
            Словарь с H, L, C или None
        """
        try:
            # v3: Пытаемся загрузить из Redis
            hlc_json = self.redis_client.get("pivots:latest")
            if hlc_json:
                hlc = json.loads(hlc_json)
                return hlc
        except Exception as e:
            print(f"⚠️ Не удалось загрузить pivots из Redis: {e}")
        
        # Fallback: рассчитываем H/L/C из доступных тиков (последние 24 часа)
        print("⚠️ pivots:latest не найден, рассчитываем H/L/C из тиков (запустите ohlc_aggregator)")
        return self._calculate_hlc_from_ticks()
    
    def _calculate_hlc_from_ticks(self) -> Dict[str, float]:
        """
        Рассчитывает H/L/C из доступных тиков за последние 24 часа.
        Fallback логика для случая когда ohlc_aggregator не работает.
        
        Returns:
            Словарь с H, L, C или дефолтные значения
        """
        try:
            # Получаем тики за последние 24 часа (примерно 1440 минут)
            current_time_ms = get_ny_time_millis()
            start_time_ms = current_time_ms - (24 * 60 * 60 * 1000)  # 24 часа назад
            
            # Читаем тики из Redis stream (последние ~2000 тиков)
            ticks = self.redis_client.xrevrange(
                TICK_STREAM,
                max="+",  # Самые новые
                min="-",  # До самых старых
                count=2000  # Достаточно для 24 часов при ~1-2 тика/сек
            )
            
            if not ticks:
                print("⚠️ Нет тиков для расчета H/L/C, пробуем получить последний тик")
                # Пытаемся получить хотя бы последний тик
                last_tick = self.redis_client.xrevrange(TICK_STREAM, count=1)
                if last_tick:
                    fields = last_tick[0][1]
                    bid = float(fields.get("bid", 0))
                    ask = float(fields.get("ask", 0))
                    current_price = (bid + ask) / 2 if (bid and ask) else 3956.0
                else:
                    current_price = 3956.0  # Примерная текущая цена XAUUSD
                
                return {
                    "H": current_price + 30,  # +30 пипсов
                    "L": current_price - 30,  # -30 пипсов
                    "C": current_price
                }
            
            # Извлекаем цены из тиков и находим H/L/C
            prices = []
            last_price = None
            
            for tick_id, fields in ticks:
                try:
                    bid = float(fields.get("bid", 0))
                    ask = float(fields.get("ask", 0))
                    last = float(fields.get("last", 0))
                    
                    # Используем mid price
                    mid_price = (bid + ask) / 2 if (bid and ask) else (last or 0.0)
                    
                    if mid_price > 0:
                        prices.append(mid_price)
                        if last_price is None:  # Первый (самый свежий) тик
                            last_price = mid_price
                            
                except (ValueError, TypeError):
                    continue
            
            if not prices:
                print("⚠️ Не удалось извлечь цены из тиков")
                return {
                    "H": 3980.0,
                    "L": 3930.0, 
                    "C": 3955.0
                }
            
            # Рассчитываем H/L/C
            high = max(prices)
            low = min(prices)
            close = last_price or prices[0]  # Самая свежая цена
            
            print(f"📊 H/L/C из {len(prices)} тиков: H={high:.2f}, L={low:.2f}, C={close:.2f}")
            
            return {
                "H": high,
                "L": low,
                "C": close
            }
            
        except Exception as e:
            print(f"❌ Ошибка расчета H/L/C из тиков: {e}")
            # Последний fallback - актуальные значения для XAUUSD
            return {
                "H": 3980.0,
                "L": 3930.0,
                "C": 3955.0
            }
    
    def _generate_signals(self, ts: int, price: float, z_delta: float, 
                         weak_progress: bool, obi: float, atr: float) -> None:
        """
        Генерирует торговые сигналы на основе анализа.
        
        Args:
            ts: Timestamp в мс
            price: Текущая цена
            z_delta: Z-score Delta
            weak_progress: Флаг слабого прогресса цены
            obi: Order Book Imbalance
            atr: Значение ATR
        """
        # Сохраняем z_delta для использования в _publish_signal (audit trail)
        self.current_z_delta = z_delta
        
        # Антиспам - минимальный интервал между сигналами
        if ts - self.last_signal_ts < CFG["min_signal_interval_sec"] * 1000:
            return
        
        if not self.daily_pivots:
            return
        
        # Сигнал 1: ABSORPTION
        # Условие: weak progress + delta spike у уровня
        
        if (weak_progress and
            abs(z_delta) >= CFG["delta_z_threshold"]):
            
            # v3.1: Enhanced pivot proximity check (ATR + bps)
            # Check only if other conditions met (optimization)
            piv_cfg = PivotProximityCfg(
                dist_atr_threshold=CFG["dist_atr_threshold"],
                dist_bp_threshold=CFG.get("dist_bp_threshold"),
                mode=CFG.get("dist_mode", "or")
            )
            is_near, piv_details = check_pivot_proximity(price, self.daily_pivots, atr, piv_cfg, return_details=True)
            
            if is_near:
                # Regime gate (XAU: use 0.0 for now, will integrate full regime later)
                rscore = 0.0  # mixed regime as default for XAU
                if not regime_allows("absorption", rscore, self.regime_gate):
                    return

                # Если покупатели давят, но прогресса нет → SHORT от сопротивления
                # Если продавцы давят, но прогресса нет → LONG от поддержки
                side = "SHORT" if z_delta > 0 else "LONG"
                
                # Enrich note with proximity info
                prox_info = f"(near {piv_details.get('closest_key')} {piv_details.get('dist_bps'):.1f}bps)"
                self._publish_signal(side, price, f"Absorption {prox_info}", "🛡️", ts, obi, weak_progress, pivot_details=piv_details)
                self.last_signal_ts = ts
                return
        
        # Сигнал 2: BREAKOUT
        # Условие: delta spike + пробой уровня
        # DEBUG: Логируем ключевые метрики каждые 50 тиков
        if self.processed_ticks % 50 == 0:
            print(f"🔍 DEBUG METRICS: z_delta={z_delta:.3f} (threshold={CFG['delta_z_threshold']:.1f}), ATR={atr:.4f}")
            sys.stdout.flush()
        
        if abs(z_delta) >= CFG["delta_z_threshold"]:
            self.z_delta_trigger_count += 1
            # Логируем каждое 10-е сообщение Z-DELTA TRIGGER для мониторинга LONG/SHORT баланса
            if self.z_delta_trigger_count % 10 == 0:
                direction = "BUYING" if z_delta > 0 else "SELLING"
                print(f"🚨 Z-DELTA TRIGGER #{self.z_delta_trigger_count}: {direction} pressure Z={z_delta:.3f} (threshold={CFG['delta_z_threshold']:.1f})")
                sys.stdout.flush()
            
            dir_up = z_delta > 0
            side = "LONG" if dir_up else "SHORT"
            
            # Проверяем пробой уровня
            if self._is_breakout(price, dir_up):
                # Regime gate (XAU: use 0.0 for now, will integrate full regime later)
                rscore = 0.0  # mixed regime as default for XAU
                if not regime_allows("breakout", rscore, self.regime_gate):
                    return
                self._publish_signal(side, price, "Breakout (delta spike)", "🚀", ts, obi, weak_progress)
                self.last_signal_ts = ts
                return

            # НОВОЕ: Альтернативный сигнал при экстремальном Z-score (без пробоя)
            elif abs(z_delta) >= CFG["delta_z_threshold"] * 1.5:  # 3.0 * 1.5 = 4.5
                # Regime gate (XAU: use 0.0 for now, will integrate full regime later)
                rscore = 0.0  # mixed regime as default for XAU
                if not regime_allows("extreme", rscore, self.regime_gate):
                    return
                self._publish_signal(side, price, f"Extreme delta activity (Z={z_delta:.1f})", "💥", ts, obi, weak_progress)
                self.last_signal_ts = ts
                return
        
        # Сигнал 3: ICEBERG (v3: реальная детекция из DOM)
        # ВРЕМЕННО ОТКЛЮЧЕНО: Требует Order Book данные из BookBridge.mq5
        # TODO: Включить когда BookBridge заработает в MT5
        """
        # Условие: best level держится долго + refresh-и + агрессия в противоположную сторону
        best_metrics = self.best_level_tracker.metrics(ts)
        
        if self.best_level_tracker.is_iceberg("bid", ts) and z_delta < -CFG["delta_z_threshold"] / 2:
            # Iceberg на bid (скрытый покупатель) + агрессивная продажа → LONG
            duration = best_metrics["bid"]["duration"]
            refresh = best_metrics["bid"]["refresh"]
            self._publish_signal(
                "LONG", 
                price, 
                f"Iceberg @ bid (hold {duration:.1f}s, refresh={refresh})",
                "🧊"
            )
            self.last_signal_ts = ts
            # Сброс трекера после сигнала
            self.best_level_tracker.bid = type(self.best_level_tracker.bid)()
            return
        
        if self.best_level_tracker.is_iceberg("ask", ts) and z_delta > CFG["delta_z_threshold"] / 2:
            # Iceberg на ask (скрытый продавец) + агрессивная покупка → SHORT
            duration = best_metrics["ask"]["duration"]
            refresh = best_metrics["ask"]["refresh"]
            self._publish_signal(
                "SHORT",
                price,
                f"Iceberg @ ask (hold {duration:.1f}s, refresh={refresh})",
                "🧊"
            )
            self.last_signal_ts = ts
            # Сброс трекера после сигнала
            self.best_level_tracker.ask = type(self.best_level_tracker.ask)()
            return
        
        # Сигнал 4: CONTINUATION
        # Условие: устойчивый OBI в одном направлении
        if self._obi_is_sustained():
            side = "LONG" if obi > 0 else "SHORT"
            self._publish_signal(side, price, "Continuation (sustained OBI)", "➡️", ts)
            self.last_signal_ts = ts
            return
        """
    
    def _is_breakout(self, price: float, up: bool) -> bool:
        """
        Проверяет, является ли движение пробоем уровня.
        
        Args:
            price: Текущая цена
            up: True для пробоя вверх, False для пробоя вниз
            
        Returns:
            True, если пробой подтвержден
        """
        if not self.daily_pivots:
            return False
        
        # Проверяем пробой R1/R2/R3 или S1/S2/S3
        keys = ["R3", "R2", "R1"] if up else ["S3", "S2", "S1"]
        
        for key in keys:
            if key not in self.daily_pivots:
                continue
            
            level = self.daily_pivots[key]
            if (up and price > level) or (not up and price < level):
                return True
        
        return False
    
    def _publish_signal(self, side: str, price: float, note: str, emoji: str = "🚨", ts: int = 0, 
                       obi: float = 0.0, weak_progress: bool = False, pivot_details: dict = None) -> None:
        """
        Публикует торговый сигнал в notify:telegram используя единый форматировщик.
        
        Args:
            side: Направление сделки (LONG/SHORT)
            price: Цена входа
            note: Описание сигнала
            emoji: Эмодзи для сообщения
            ts: Timestamp для генерации sid (v5)
            obi: Order Book Imbalance (optional, for audit)
            weak_progress: Weak progress flag (optional, for audit)
            pivot_details: Pivot proximity details (optional, for telemetry)
        """
        # Расчет рекомендуемого лота - используем метод _get_atr для получения корректного значения
        atr = self._get_atr(price, ts or get_ny_time_millis())
        lot = suggest_lot(price=price, atr=atr)
        
        # Используем текущее время если ts не передан
        if not ts:
            ts = get_ny_time_millis()
        
        # v5.1: Calculate SL/TP levels
        # Для rocket_v1: TP1 = 0.78 ATR, остальные через RR
        trail_profile = "rocket_v1"  # Дефолт для XAUUSD
        
        levels = compute_levels(price, atr, side, {
            "STOP_MODE": CFG["stop_mode"],
            "STOP_ATR_MULT": CFG["stop_atr_mult"],
            "STOP_PCT": CFG["stop_pct"],
            "STOP_POINTS": CFG["stop_points"],
            "TP_MODE": "ATR",  # Для rocket_v1 используем ATR режим
            "TP_RR": CFG["tp_rr"],
            "TP_ATR_MULTS": "0.78",  # TP1 = 0.78 ATR для rocket_v1
            "trail_profile": trail_profile,  # Передаем профиль для правильного расчета
        })
        
        # ✅ ИСПОЛЬЗУЕМ ЕДИНЫЙ ФОРМАТИРОВЩИК XAUUSD
        xauusd_signal = XAUUSDSignal(
            sid=XAUUSDSignalFormatter.create_signal_id(side, price, ts),
            symbol=SYMBOL,
            side=side,
            entry=price,
            sl=levels['sl'],
            tp_levels=levels['tp_levels'],
            lot=lot,
            source="OrderFlow",
            reason=note,
            confidence=85.0,  # OrderFlow signals have high confidence
            atr=atr,
            ts=ts,
            indicators={
                "z_delta": self.current_z_delta or 0.0,
                "obi": obi,
                "weak_progress": weak_progress,
                "atr": round(atr, 4),
                "delta_window_len": len(self.delta_window),
                "pivot_proximity": pivot_details
            },
            trail_after_tp1=True,  # Включаем трейлинг по умолчанию для XAUUSD
            trail_profile="rocket_v1"  # Дефолт rocket_v1 для XAUUSD
        )
        
        # Получаем payload в едином формате
        redis_payload = XAUUSDSignalFormatter.format_redis_payload(xauusd_signal)
        
        # v4: Add Telegram inline buttons if enabled
        if USE_TG_BTNS:
            redis_payload["buttons"] = json.dumps([
                [
                    {"text": "Открыть", "callback": f"open:{side}:{lot:.2f}:{xauusd_signal.sid}"},
                    {"text": "SL/TP", "callback": f"sltp:set:{xauusd_signal.sid}"},
                    {"text": "Отменить", "callback": f"cancel::{xauusd_signal.sid}"}
                ],
                [
                    {"text": "x0.5", "callback": f"size:0.5:{xauusd_signal.sid}"},
                    {"text": "x1", "callback": f"size:1:{xauusd_signal.sid}"},
                    {"text": "x2", "callback": f"size:2:{xauusd_signal.sid}"}
                ]
            ])
        
        try:
            # Конвертируем для Redis (все значения должны быть строками)
            redis_data = {}
            for key, value in redis_payload.items():
                if isinstance(value, (dict, list)):
                    redis_data[key] = json.dumps(value)
                else:
                    redis_data[key] = str(value)
            
            # Публикуем в notify:telegram через dual redis для надежности
            self.dual_redis.xadd(
                NOTIFY_STREAM,
                redis_data,
                maxlen=500,
                approximate=True
            )
            
            # v4.1: Также публикуем в signals:orderflow:XAUUSD для aggregated-hub
            signal_payload = XAUUSDSignalFormatter.format_audit_payload(
                xauusd_signal,
                extra_context={
                    "obi": obi,
                    "weak_progress": weak_progress
                }
            )
            
            # Используем простой Redis для нового stream (не dual)
            try:
                simple_redis = get_redis()
                # from core.redis_client import ...
                simple_redis.xadd(
                    ORDERFLOW_SIGNAL_STREAM,
                    {"data": to_json(signal_payload)},
                    maxlen=1000,
                    approximate=True
                )
            except Exception as e:
                print(f"⚠️ Failed to publish to {ORDERFLOW_SIGNAL_STREAM}: {e}")
            
            # v6: Store signal snapshot for orders router
            snap_key = SNAP_PREFIX + xauusd_signal.sid
            self.redis_client.setex(
                snap_key,
                SNAP_TTL,
                json.dumps(redis_data)
            )

            # v7: Audit stream — сохраняем полный контекст сигнала для обучения
            try:
                audit_env = {
                    "ATR_SOURCE": os.getenv("ATR_SOURCE", ""),
                    "ATR_TF": os.getenv("ATR_TF", ""),
                    "USE_TELEGRAM_BUTTONS": os.getenv("USE_TELEGRAM_BUTTONS", ""),
                    "ACCOUNT_DEPOSIT_USD": os.getenv("ACCOUNT_DEPOSIT_USD", ""),
                    "ACCOUNT_LEVERAGE": os.getenv("ACCOUNT_LEVERAGE", ""),
                    "RISK_PERCENT": os.getenv("RISK_PERCENT", ""),
                    "XAU_CONTRACT_SIZE": os.getenv("XAU_CONTRACT_SIZE", ""),
                    "XAU_LOT_STEP": os.getenv("XAU_LOT_STEP", ""),
                    "STOP_MODE": os.getenv("STOP_MODE", ""),
                    "STOP_ATR_MULT": os.getenv("STOP_ATR_MULT", ""),
                    "STOP_PCT": os.getenv("STOP_PCT", ""),
                    "STOP_POINTS": os.getenv("STOP_POINTS", ""),
                    "TP_MODE": os.getenv("TP_MODE", ""),
                    "TP_RR": os.getenv("TP_RR", ""),
                    "TP_ATR_MULTS": os.getenv("TP_ATR_MULTS", ""),
                }
                
                # Используем единый формат для audit
                audit_payload = XAUUSDSignalFormatter.format_audit_payload(
                    xauusd_signal,
                    extra_context={
                        "obi": obi,
                        "weak_progress": weak_progress,
                        "env": audit_env
                    }
                )
                
                self.redis_client.xadd(
                    AUDIT_SIGNAL_STREAM,
                    {"data": json.dumps(audit_payload)},
                    maxlen=200000,
                    approximate=True,
                )
            except Exception as _:
                pass
            
            # Обновляем счетчики сигналов
            if side == "LONG":
                self.signal_count_long += 1
            else:
                self.signal_count_short += 1
            
            total_signals = self.signal_count_long + self.signal_count_short
            print(f"📤 Сигнал опубликован: {xauusd_signal.sid} | {side} @ {price:.2f}")
            print(f"📸 Snapshot saved: {snap_key} (TTL={SNAP_TTL}s)")
            print(f"📊 Статистика сигналов: LONG={self.signal_count_long}, SHORT={self.signal_count_short} (всего={total_signals})")
            sys.stdout.flush()
            
        except Exception as e:
            print(f"❌ Ошибка публикации сигнала: {e}")
            sys.stdout.flush()

