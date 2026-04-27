# -*- coding: utf-8 -*-
from core.confidence_utils import normalize_confidence_pct, confidence_pct_to_ratio
"""
AggregatedSignalHub V2 — улучшенная версия с поддержкой:
- True bid/ask-дельта по принтам (MicrostructureSpikeDetectorPro)
- Инъекция cluster-score из DOM (SmartClusterAnalyzer)
- Расширенное логирование меток в Parquet
- Офлайн-реплей принтов для тестирования
- Weighted confidence blending из нескольких источников
- Отдельный Redis для тиков (redis-ticks) для изоляции нагрузки

АРХИТЕКТУРА REDIS:
- redis-ticks (scanner-redis-ticks) → чтение тиков и принтов (высокочастотные данные)
- scanner-redis → запись сигналов, чтение ATR, DOM, pivots (общие данные)

CONSUMER GROUPS:
- Используют префикс "ticks-" для redis-ticks streams
- Например: "ticks-hub-v2-XAUUSD", "ticks-orderflow-group"

СОВМЕСТИМ С:
  - python-worker/filtered_signal_writer.py
  - python-worker/order_push_dispatcher.py
  - python-worker/smart_cluster_analyzer.py
  - python-worker/microstructure_spike_detector.py
  - core/microstructure_spike_detector_pro.py
  - core/ticks_redis_client.py (новый)
"""

from dataclasses import dataclass, field
from typing import Any, Dict, Optional, List, Tuple
from collections import defaultdict
import json
import os
import time
import logging
from datetime import datetime
from pathlib import Path

try:
    import redis
except ImportError:
    redis = None

# === Импорт существующих модулей проекта ===
from common.log import setup_logger

# Redis клиенты для тиков (отдельный instance)
try:
    from core.ticks_redis_client import get_ticks_redis, TicksRedisClient
    HAS_TICKS_CLIENT = True
except ImportError as e:
    HAS_TICKS_CLIENT = False
    get_ticks_redis = None
    TicksRedisClient = None

# Основные детекторы
import sys
from pathlib import Path

# Добавляем путь к python-worker в PYTHONPATH для корректных импортов
_worker_path = Path(__file__).parent
if str(_worker_path) not in sys.path:
    sys.path.insert(0, str(_worker_path))

_import_warnings = []

try:
    # Импортируем Pro детектор из python-worker/core (с методом update)
    from core.microstructure_spike_detector_pro import (
        ProConfig,
        MicrostructureSpikeDetectorPro
    )
    HAS_PRO_DETECTOR = True
except ImportError as e:
    HAS_PRO_DETECTOR = False
    ProConfig = None
    MicrostructureSpikeDetectorPro = None
    _import_warnings.append(f"Pro detector import failed: {e}")

try:
    # Импортируем Legacy детектор из python-worker/core (с методом update)
    from core.microstructure_spike_detector import MicrostructureSpikeDetector, SpikeConfig
    
    # Проверяем, что это правильная версия с методом update
    if not hasattr(MicrostructureSpikeDetector, 'update'):
        _import_warnings.append("Wrong MicrostructureSpikeDetector version imported (no 'update' method)")
        HAS_LEGACY_DETECTOR = False
        MicrostructureSpikeDetector = None
        SpikeConfig = None
    else:
        HAS_LEGACY_DETECTOR = True
except ImportError as e:
    HAS_LEGACY_DETECTOR = False
    MicrostructureSpikeDetector = None
    SpikeConfig = None
    _import_warnings.append(f"Legacy detector import failed: {e}")

# Кластерный анализ DOM
try:
    from smart_cluster_analyzer import SmartClusterAnalyzer
    HAS_CLUSTER = True
except ImportError:
    HAS_CLUSTER = False
    SmartClusterAnalyzer = None

# Writer и dispatcher - ОБНОВЛЕНО: используем версию с XAUUSDSignalFormatter
try:
    from core.filtered_signal_writer import FilteredSignalWriter
    from dispatch.order_push_dispatcher import OrderPushDispatcher
    from infra.config import Config
    HAS_WRITER = True
    HAS_CONFIG = True
except ImportError as e:
    _import_warnings.append(f"Writer/Config import failed: {e}")
    HAS_WRITER = False
    FilteredSignalWriter = None
    OrderPushDispatcher = None
    Config = None
    HAS_CONFIG = False

# Parquet sink (если есть)
try:
    from persistence.label_sink import ParquetLabelSink
    HAS_PARQUET = True
except ImportError:
    HAS_PARQUET = False
    ParquetLabelSink = None


log = setup_logger("agg_hub_v2")

# Log any import warnings collected during module initialization
for warning in _import_warnings:
    log.warning(f"⚠️  {warning}")

CRYPTO_ORDERFLOW_STREAM = os.getenv("CRYPTO_ORDERFLOW_STREAM", "stream:manual-signals")
CRYPTO_ORDERFLOW_GROUP = os.getenv("CRYPTO_ORDERFLOW_GROUP", "hub-v2-crypto")


# ========================== CONFIG ==========================
@dataclass
class HubConfig:
    """Конфигурация aggregated signal hub v2."""
    symbol: str = os.getenv("SYMBOL", "XAUUSD")
    
    # Redis URLs - РАЗДЕЛЕНО: тики и сигналы
    redis_url: str = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")  # Для сигналов, ATR, DOM
    redis_ticks_url: str = os.getenv("REDIS_TICKS_URL", "redis://redis-ticks:6379/0")  # Для тиков и принтов

    # Redis streams/keys
    tick_stream: Optional[str] = field(default_factory=lambda: os.getenv("TICK_STREAM"))
    prints_stream: Optional[str] = field(default_factory=lambda: os.getenv("PRINTS_STREAM"))
    book_levels_key: str = "book:levels:{SYMBOL}"
    pivots_key: str = "pivots:latest"

    # Thresholds
    # Снижен порог с 0.62 до 0.25 для более реалистичной генерации сигналов
    confidence_threshold: float = float(os.getenv("HUB_CONFIDENCE_THR", "0.25"))
    min_signal_interval_sec: int = int(os.getenv("HUB_MIN_SIG_INT_SEC", "60"))  # Антиспам: 1 минута

    # Weights для confidence blending
    w_delta_pro: float = float(os.getenv("W_DELTA_PRO", "0.50"))    # true bid/ask delta
    w_speed: float = float(os.getenv("W_SPEED", "0.15"))             # tick speed/range
    w_cluster: float = float(os.getenv("W_CLUSTER", "0.25"))         # DOM stacked/absorption
    w_legacy: float = float(os.getenv("W_LEGACY", "0.10"))           # legacy detector

    # Anti-dither
    side_lock_sec: int = int(os.getenv("HUB_SIDE_LOCK_SEC", "20"))

    # Writer config passthrough
    min_conf_writer: float = float(os.getenv("MIN_CONF", "60.0"))
    cooldown_writer: int = int(os.getenv("HUB_COOLDOWN", "300"))
    risk_pct: float = float(os.getenv("RISK_PCT", "1.0"))
    sl_mult: float = float(os.getenv("SL_MULT", "1.5"))
    tp_mults_str: str = os.getenv("TP_MULTS", "2.0,3.0,4.0")

    # Parquet sink
    parquet_dir: Optional[str] = os.getenv("PARQUET_LABELS_DIR")

    def get_tp_mults(self) -> List[float]:
        """Parse TP multipliers from env."""
        return [float(x.strip()) for x in self.tp_mults_str.split(",") if x.strip()]


# ================ MAIN HUB =================
class AggregatedSignalHubV2:
    """
    Улучшенный aggregated signal hub с поддержкой:
    - Pro detector (true bid/ask delta по принтам)
    - Cluster analyzer (DOM stacked/absorption)
    - Weighted confidence blending
    - Parquet label sink для оффлайн-валидации
    - Офлайн-реплей принтов
    """

    def __init__(self, cfg: Optional[HubConfig] = None):
        self.cfg = cfg or HubConfig()

        # Проверка зависимостей
        if not HAS_WRITER:
            raise RuntimeError("FilteredSignalWriter / order_push_dispatcher не найдены")

        if not redis:
            raise RuntimeError("redis-py не установлен")

        # Redis клиенты - РАЗДЕЛЕНО на два instance
        # 1. redis-ticks: для чтения тиков и принтов (высокочастотные данные)
        if HAS_TICKS_CLIENT:
            try:
                self.r_ticks = get_ticks_redis(ticks_url=self.cfg.redis_ticks_url)
                log.info("✅ Connected to redis-ticks: %s", self.cfg.redis_ticks_url)
            except Exception as e:
                log.warning("⚠️  Failed to connect to redis-ticks, falling back to main Redis: %s", e)
                self.r_ticks = redis.Redis.from_url(self.cfg.redis_url, decode_responses=True)
        else:
            log.warning("⚠️  TicksRedisClient not available, using main Redis for ticks")
            self.r_ticks = redis.Redis.from_url(self.cfg.redis_url, decode_responses=True)
        
        # 2. scanner-redis: для записи сигналов, чтения ATR, DOM, pivots
        self.r = redis.Redis.from_url(self.cfg.redis_url, decode_responses=True)
        self.crypto_stream = CRYPTO_ORDERFLOW_STREAM if CRYPTO_ORDERFLOW_STREAM else None
        self.crypto_group = CRYPTO_ORDERFLOW_GROUP
        self.crypto_consumer = f"{self.crypto_group}-{int(time.time())}"
        self.external_streams = {}
        if self.crypto_stream:
            self.external_streams[self.crypto_stream] = "crypto-orderflow"

        self._load_symbol_config_overrides()
        
        # ATR fallback значения для XAUUSD
        self.atr_fallback_values = {
            "1m": 1.2,    # Типичный ATR для 1m XAUUSD
            "5m": 3.5,    # Типичный ATR для 5m XAUUSD
            "15m": 6.5,   # Типичный ATR для 15m XAUUSD
            "default": 2.0  # Консервативное значение
        }
        self.atr_cache_ttl = 60  # TTL кэша ATR из Redis (секунды)
        self.last_atr_fetch = 0
        self.cached_atr = 0.0

        # Детекторы
        self.det_pro = None
        self.det_legacy = None

        if HAS_PRO_DETECTOR and ProConfig and MicrostructureSpikeDetectorPro:
            self.det_pro = MicrostructureSpikeDetectorPro(ProConfig())
            log.info("✅ Pro detector (true delta) enabled")
        else:
            log.warning("⚠️  Pro detector not available")

        if HAS_LEGACY_DETECTOR and MicrostructureSpikeDetector and SpikeConfig:
            try:
                legacy_cfg = SpikeConfig(
                    z_delta_thr=float(os.getenv("Z_DELTA_THR", "3.0")),
                    z_extreme_thr=float(os.getenv("Z_EXTREME_THR", "4.5")),
                    speed_z_thr=float(os.getenv("SPEED_Z_THR", "3.0")),
                    win_ticks=int(os.getenv("WIN_TICKS", "300")),
                    win_speed_sec=int(os.getenv("WIN_SPEED_SEC", "30"))
                )
                self.det_legacy = MicrostructureSpikeDetector(legacy_cfg)
                
                # Verify it has the update method
                if not hasattr(self.det_legacy, 'update'):
                    log.error("❌ Legacy detector doesn't have 'update' method - disabling")
                    self.det_legacy = None
                else:
                    log.info("✅ Legacy detector enabled with 'update' method")
            except Exception as e:
                log.error(f"❌ Failed to initialize legacy detector: {e}")
                self.det_legacy = None

        # Cluster analyzer
        self.cluster = SmartClusterAnalyzer() if HAS_CLUSTER else None
        if self.cluster:
            log.info("✅ Cluster analyzer (DOM) enabled")
        else:
            log.warning("⚠️  Cluster analyzer not available")

        # Writer (risk/sizing/push) - ОБНОВЛЕНО: используем новую версию с XAUUSDSignalFormatter
        if not HAS_CONFIG:
            raise RuntimeError("Config не найден - невозможно создать FilteredSignalWriter")
        
        # Создаём Config для нового FilteredSignalWriter
        writer_cfg = Config()
        writer_cfg.symbol = self.cfg.symbol
        writer_cfg.redis_url = self.cfg.redis_url
        writer_cfg.cooldown_sec = self.cfg.cooldown_writer
        writer_cfg.risk_pct = self.cfg.risk_pct
        writer_cfg.atr_sl_mult = self.cfg.sl_mult
        writer_cfg.atr_tp_mults = self.cfg.get_tp_mults()
        writer_cfg.notify_stream = os.getenv("NOTIFY_STREAM", "notify:telegram")
        writer_cfg.notify_signal_counter_key = os.getenv(
            "NOTIFY_SIGNAL_COUNTER_KEY",
            "notify:telegram:signal_counter"
        )
        try:
            writer_cfg.notify_signal_every_n = max(
                1,
                int(os.getenv("NOTIFY_SIGNAL_EVERY_N", "1"))
            )
        except ValueError:
            writer_cfg.notify_signal_every_n = 1
        writer_cfg.gateway_url = os.getenv("GATEWAY_URL", "http://scanner-go-gateway:8090")
        # Добавляем недостающие поля для совместимости
        if not hasattr(writer_cfg, 'orders_push_path'):
            writer_cfg.orders_push_path = os.getenv("GATEWAY_PUSH_PATH", "/orders/push")
        writer_cfg.balance_path = os.getenv("BALANCE_PATH", "/account/balance")
        
        # Создаём dispatcher
        dispatcher = OrderPushDispatcher(self.r, writer_cfg, log)
        
        # Создаём writer с новым форматированием
        self.writer = FilteredSignalWriter(
            r=self.r,
            cfg=writer_cfg,
            logger=log,
            dispatcher=dispatcher
        )

        # Parquet sink
        self.label_sink = None
        if HAS_PARQUET and self.cfg.parquet_dir:
            try:
                self.label_sink = ParquetLabelSink(root_dir=self.cfg.parquet_dir)
                log.info("✅ Parquet label sink enabled: %s", self.cfg.parquet_dir)
            except Exception as e:
                log.warning("⚠️  Parquet sink init failed: %s", e)

        # State
        self.last_signal_ts: float = 0.0
        self.last_side: Optional[str] = None
        self.signal_count = 0
        self.crypto_signal_count = 0
        
        # Счётчик для отфильтрованных сигналов (выводим каждое 10000-е)
        self.filtered_signal_count = 0
        
        # Счётчик для всех сообщений (выводим каждое 10000-е, кроме ошибок)
        self.message_count = 0
        self.stream_read_error_counter = defaultdict(int)

        log.info("AggregatedSignalHubV2 initialized: symbol=%s", self.cfg.symbol)

    # ========== PUBLIC API ==========
    
    def _get_atr_with_fallback(self, snap_atr: float, timeframe: str = "1m") -> float:
        """
        Получить ATR с fallback механизмами (по примеру xau_orderflow_handler.py).
        
        Приоритет:
        1. ATR из snapshot (если > 0)
        2. Кэшированный ATR из Redis (atr:val:SYMBOL:TF)
        3. ATR из go-gateway API (/runtime/atr)
        4. Fallback значение для timeframe
        
        Args:
            snap_atr: ATR из snapshot
            timeframe: Таймфрейм для fallback значения
            
        Returns:
            Валидное значение ATR > 0
        """
        # 1. ATR из snapshot
        if snap_atr > 0:
            self.cached_atr = snap_atr
            self.last_atr_fetch = time.time()
            return snap_atr
        
        # 2. Используем кэшированное значение если не истекло
        now = time.time()
        if self.cached_atr > 0 and (now - self.last_atr_fetch) < self.atr_cache_ttl:
            return self.cached_atr
        
        # 3. Попытка получить ATR из Redis
        try:
            # Сначала пробуем основной ключ от go-gateway (JSON формат)
            key = f"ta:last:atr:{self.cfg.symbol}"
            try:
                val = self.r.get(key)
                if val:
                    # Парсим JSON от go-gateway: {"atr": 3.5, "period": 14, "method": "wilder", "tf": "M1", "source": "gw", "ts": 1234567890}
                    import json
                    atr_data = json.loads(val)
                    atr = float(atr_data.get("atr", 0))
                    if atr > 0:
                        self.cached_atr = atr
                        self.last_atr_fetch = now
                        self.message_count += 1
                        if self.message_count % 10000 == 0:
                            log.info("✅ ATR from go-gateway Redis: %.4f (source=%s, period=%d) [msg #%d]", 
                                    atr, atr_data.get("source", "?"), atr_data.get("period", 14), self.message_count)
                        return atr
            except (json.JSONDecodeError, KeyError, ValueError) as e:
                log.debug("Failed to parse ATR from key %s: %s", key, e)
            
            # Фолбэк на старые форматы (простые числа)
            redis_keys = [
                f"atr:val:{self.cfg.symbol}:{timeframe}",
                f"atr:{self.cfg.symbol}:{timeframe}",
            ]
            
            for key in redis_keys:
                try:
                    val = self.r.get(key)
                    if val:
                        atr = float(val)
                        if atr > 0:
                            self.cached_atr = atr
                            self.last_atr_fetch = now
                            self.message_count += 1
                            if self.message_count % 10000 == 0:
                                log.debug("ATR from Redis key=%s: %.4f [msg #%d]", key, atr, self.message_count)
                            return atr
                except Exception:
                    continue
                    
        except Exception as e:
            # Debug ошибки не выводим часто (не критично)
            pass
        
        # 4. Попытка получить ATR из go-gateway API
        try:
            import requests
            gateway_url = os.getenv("GATEWAY_URL", "http://scanner-go-gateway:8090")
            atr_path = os.getenv("RUNTIME_ATR_PATH", "/runtime/atr")
            url = f"{gateway_url}{atr_path}"
            
            resp = requests.get(url, timeout=1.0)
            if resp.ok:
                # Ответ от go-gateway: {"atr": 3.5, "period": 14, "method": "wilder", "tf": "M1", "source": "gw", "ts": 1234567890}
                data = resp.json()
                atr = float(data.get("atr", 0))
                if atr > 0:
                    self.cached_atr = atr
                    self.last_atr_fetch = now
                    log.info("✅ ATR from go-gateway API: %.4f (source=%s, period=%d)", 
                            atr, data.get("source", "?"), data.get("period", 14))
                    return atr
        except Exception as e:
            # Debug ошибки не выводим часто (не критично)
            pass
        
        # 5. Fallback значение для timeframe
        fallback = self.atr_fallback_values.get(timeframe, self.atr_fallback_values["default"])
        log.warning(
            "⚠️  Using fallback ATR=%.2f for %s:%s (no data from snapshot/Redis/gateway)",
            fallback, self.cfg.symbol, timeframe
        )
        self.cached_atr = fallback
        self.last_atr_fetch = now
        return fallback

    def step(self, snap: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Основной шаг обработки снапшота.

        Args:
            snap: {
                "ts": int (ms),
                "bid": float,
                "ask": float,
                "atr": float,
                "mid": float (optional),
            }

        Returns:
            Dict с результатом или None если сигнал отфильтрован
        """
        ts_ms = int(snap["ts"])
        bid = float(snap["bid"])
        ask = float(snap["ask"])
        snap_atr = float(snap.get("atr", 0.0))
        mid = float(snap.get("mid", (bid + ask) / 2.0 if bid and ask else 0.0))
        
        # Получаем ATR с fallback механизмами
        atr = self._get_atr_with_fallback(snap_atr, timeframe="1m")

        # 1. Update detectors
        metrics_pro = {}
        if self.det_pro:
            self.det_pro.update_tick(bid=bid, ask=ask, ts_ms=ts_ms)
            metrics_pro = self.det_pro.metrics()

        metrics_legacy = {}
        if self.det_legacy:
            try:
                # Verify the method exists before calling
                if hasattr(self.det_legacy, 'update'):
                    # update() returns metrics directly
                    metrics_legacy = self.det_legacy.update(bid=bid, ask=ask, volume=1.0, ts_ms=ts_ms)
                else:
                    # Disable the detector if it doesn't have the update method
                    log.error("❌ Legacy detector missing 'update' method - disabling it")
                    self.det_legacy = None
            except AttributeError as e:
                log.error(f"❌ Legacy detector AttributeError: {e} - disabling it")
                self.det_legacy = None
            except Exception as e:
                # Не спамим логами о legacy detector ошибках
                pass

        # 2. Cluster score from DOM
        cl_score = self._get_cluster_score()

        # 3. Compose weighted confidence
        conf, side_hint, conf_parts = self._compose_confidence(
            metrics_pro, metrics_legacy, cl_score
        )

        # 4. Filters
        if not self._is_time_ok(ts_ms):
            self.message_count += 1
            if self.message_count % 10000 == 0:
                log.debug("Signal filtered: min_interval not passed (%.1fs since last) [msg #%d]",
                         (ts_ms / 1000.0) - self.last_signal_ts, self.message_count)
            return None

        if conf < self.cfg.confidence_threshold:
            # Выводим только каждое 10000-е сообщение о фильтрации для диагностики
            self.filtered_signal_count += 1
            self.message_count += 1
            if self.message_count % 10000 == 0:
                log.debug(
                    "Signal filtered: confidence %.2f < %.2f | parts: %s | z_delta=%.2f pro=%s legacy=%s | filtered=%d msg=%d",
                    conf, 
                    self.cfg.confidence_threshold,
                    {k: f"{v:.3f}" for k, v in conf_parts.items()},
                    metrics_pro.get("z_delta", 0.0),
                    bool(self.det_pro),
                    bool(self.det_legacy),
                    self.filtered_signal_count,
                    self.message_count
                )
            return None

        # 5. Determine side
        side = side_hint or self._side_from_pro(metrics_pro) or self.last_side or "LONG"

        # Anti-dither: keep last side if flip happens too fast
        if self.last_side and self.last_side != side:
            if (ts_ms / 1000.0) - self.last_signal_ts < self.cfg.side_lock_sec:
                log.debug("Side flip too fast, keeping last side: %s", self.last_side)
                side = self.last_side

        # 6. Build reason string
        reason = self._build_reason(metrics_pro, cl_score, conf_parts)

        # Валидация данных перед отправкой
        if mid <= 0:
            log.warning(
                "⚠️  Invalid entry price: mid=%.4f (bid=%.4f, ask=%.4f) - signal skipped",
                mid, bid, ask
            )
            return None
        
        if atr <= 0:
            log.error(
                "❌ ATR is still <= 0 after fallback (atr=%.4f, snap_atr=%.4f) - this should not happen!",
                atr, snap_atr
            )
            return None
        
        # 7. Write and push via FilteredSignalWriter (ОБНОВЛЕНО: используем write_and_push)
        self.message_count += 1
        if self.message_count % 10000 == 0:
            log.debug(
                "Calling write_and_push: side=%s entry=%.2f atr=%.4f conf=%.2f [msg #%d]",
                side, mid, atr, conf, self.message_count
            )
        
        # 🎯 Умный выбор профиля трейлинга на основе метрик
        trail_after_tp1 = False
        trail_profile = "rocket_v1"
        
        # Включаем трейлинг для качественных сигналов (conf > 0.60)
        if conf >= 0.60:
            trail_after_tp1 = True
            
            # Выбор профиля на основе силы сигнала
            z_delta = abs(metrics_pro.get("z_delta", 0.0))
            
            # Экстремальные сигналы -> агрессивный профиль
            if conf >= 0.85 and z_delta >= 6.0:
                trail_profile = "rocket_v1"  # ATR × 0.6
            # Очень сильные сигналы
            elif conf >= 0.75 and z_delta >= 4.5:
                trail_profile = "rocket_v1"  # ATR × 0.6
            # Сильные сигналы
            elif conf >= 0.65:
                trail_profile = "lock_and_trail"  # ATR × 0.8
            # Средние сигналы -> консервативный подход
            else:
                trail_profile = "wide_swing"  # ATR × 1.2
        
        result = self.writer.write_and_push(
            symbol=self.cfg.symbol,
            side=side,
            entry=mid,
            atr=atr,
            confidence=conf,  # передаем как 0..1, внутри преобразуется в 0..100
            reason=reason,
            source="AggregatedHub-V2",
            trail_after_tp1=trail_after_tp1,
            trail_profile=trail_profile
        )

        if result:
            # Update state
            self.last_signal_ts = ts_ms / 1000.0
            self.last_side = side
            self.signal_count += 1

            log.info(
                "✅ Signal #%d: %s conf=%.1f%% entry=%.2f reason=%s",
                self.signal_count, side, conf * 100, result.price, reason[:80]
            )

            # 8. Write label to Parquet
            if self.label_sink:
                # Создаем словарь payload для совместимости с _write_label
                writer_result = {
                    "ok": True,
                    "payload": {
                        "sid": result.sid,
                        "sl": result.sl,
                        "tp_levels": result.tp_levels,
                        "lot": result.lot,
                    }
                }
                self._write_label(ts_ms, side, mid, atr, conf, reason, metrics_pro, cl_score, writer_result)

            return {
                "side": side,
                "entry": result.price,
                "confidence": conf,
                "reason": reason,
                "final_signal": {
                    "sid": result.sid,
                    "sl": result.sl,
                    "tp_levels": result.tp_levels,
                    "lot": result.lot,
                },
            }

        return None

    def on_trade(self, price: float, qty: float, side: str, ts_ms: Optional[int] = None) -> None:
        """
        Feed реальные принты/сделки в pro detector.

        Args:
            price: Цена сделки
            qty: Объём
            side: 'buy' | 'sell' (агрессор)
            ts_ms: Timestamp в мс (optional)
        """
        if self.det_pro:
            self.det_pro.on_trade(price=price, qty=qty, side=side, ts_ms=ts_ms)

    def run(self) -> None:
        """
        Основной цикл обработки в реальном времени.
        Читает тики и принты из Redis streams, обрабатывает через детекторы.
        """
        log.info(
            "AggregatedSignalHubV2 started | symbol=%s tick_stream=%s prints_stream=%s",
            self.cfg.symbol,
            self.cfg.tick_stream,
            self.cfg.prints_stream
        )

        # Определяем streams для чтения
        streams_to_read = []
        if self.cfg.tick_stream:
            streams_to_read.append(("tick", self.cfg.tick_stream))
        if self.cfg.prints_stream:
            streams_to_read.append(("print", self.cfg.prints_stream))

        if not streams_to_read:
            log.warning("No streams configured. Set TICK_STREAM and/or PRINTS_STREAM env vars.")
            log.info("Use --replay-csv for offline mode or call hub.step(snap) manually.")
            return

        # Consumer group setup - с префиксом "ticks-" согласно архитектуре redis-ticks
        group_name = f"ticks-hub-v2-{self.cfg.symbol}"
        consumer_name = f"hub-v2-consumer-{int(time.time())}"

        for stream_type, stream_name in streams_to_read:
            try:
                # Используем r_ticks для создания consumer groups на redis-ticks
                self.r_ticks.xgroup_create(stream_name, group_name, id="$", mkstream=True)
                log.info("✅ Consumer group created/exists: %s (group=%s)", stream_name, group_name)
            except Exception as e:
                log.debug("Consumer group already exists or error: %s", e)

        if self.external_streams:
            for stream_name in self.external_streams:
                try:
                    self.r.xgroup_create(stream_name, self.crypto_group, id="$", mkstream=True)
                    log.info("✅ External signal group ready: stream=%s group=%s", stream_name, self.crypto_group)
                except Exception as e:
                    log.debug("External group create skipped: %s", e)

        # Счетчики для логирования
        tick_count = 0
        print_count = 0
        last_log_time = time.time()
        log_interval = 60  # Логируем статистику каждую минуту

        # Последние известные bid/ask/atr для построения snapshot
        last_bid = 0.0
        last_ask = 0.0
        last_atr = 0.0

        log.info("🚀 Entering main processing loop...")

        while True:
            try:
                # Читаем тики из redis-ticks
                if self.cfg.tick_stream:
                    tick_msgs = self._read_stream(
                        self.cfg.tick_stream,
                        group_name,
                        consumer_name,
                        count=50,
                        block_ms=100,
                        client=self.r_ticks  # Используем redis-ticks
                    )

                    for msg_id, fields in tick_msgs:
                        try:
                            # Парсим тик
                            data = self._parse_message(fields)
                            if not data:
                                self.r_ticks.xack(self.cfg.tick_stream, group_name, msg_id)
                                continue

                            ts_ms = int(data.get("ts", time.time() * 1000))
                            bid = float(data.get("bid", last_bid))
                            ask = float(data.get("ask", last_ask))
                            atr = float(data.get("atr", last_atr))

                            # Обновляем последние значения
                            if bid > 0:
                                last_bid = bid
                            if ask > 0:
                                last_ask = ask
                            if atr > 0:
                                last_atr = atr

                            # Формируем snapshot
                            snap = {
                                "ts": ts_ms,
                                "bid": last_bid,
                                "ask": last_ask,
                                "atr": last_atr,
                                "mid": (last_bid + last_ask) / 2.0 if last_bid and last_ask else 0.0
                            }

                            # Обрабатываем
                            result = self.step(snap)
                            tick_count += 1

                            # Сигналы всегда логируем (это важные события)
                            if result:
                                log.info("✅ Signal from tick: %s", result)

                            # Подтверждаем обработку
                            self.r_ticks.xack(self.cfg.tick_stream, group_name, msg_id)

                        except Exception as e:
                            log.error("Error processing tick %s: %s", msg_id, e)
                            self.r_ticks.xack(self.cfg.tick_stream, group_name, msg_id)

                # Читаем принты (trades) из redis-ticks
                if self.cfg.prints_stream:
                    print_msgs = self._read_stream(
                        self.cfg.prints_stream,
                        group_name,
                        consumer_name,
                        count=100,
                        block_ms=100,
                        client=self.r_ticks  # Используем redis-ticks
                    )

                    for msg_id, fields in print_msgs:
                        try:
                            # Парсим принт
                            data = self._parse_message(fields)
                            if not data:
                                self.r_ticks.xack(self.cfg.prints_stream, group_name, msg_id)
                                continue

                            price = float(data.get("price", 0.0))
                            qty = float(data.get("qty", 0.0) or data.get("volume", 0.0))
                            side = str(data.get("side", "")).lower()
                            ts_ms = int(data.get("ts", time.time() * 1000))

                            if price > 0 and qty > 0 and side in ["buy", "sell"]:
                                # Feed в pro detector
                                self.on_trade(price=price, qty=qty, side=side, ts_ms=ts_ms)
                                print_count += 1

                            # Подтверждаем обработку
                            self.r_ticks.xack(self.cfg.prints_stream, group_name, msg_id)

                        except Exception as e:
                            log.error("Error processing print %s: %s", msg_id, e)
                            self.r_ticks.xack(self.cfg.prints_stream, group_name, msg_id)

                # Периодическое логирование статистики
                now = time.time()
                if now - last_log_time >= log_interval:
                    log.info(
                        "📊 Stats: ticks=%d prints=%d signals=%d crypto_signals=%d messages=%d (last %ds)",
                        tick_count,
                        print_count,
                        self.signal_count,
                        self.crypto_signal_count,
                        self.message_count,
                        log_interval
                    )
                    last_log_time = now

                if self.external_streams:
                    self._poll_external_signals()

            except KeyboardInterrupt:
                log.info("⛔ Stopped by user")
                break
            except Exception as e:
                log.error("Loop error: %s", e, exc_info=True)
                time.sleep(1.0)

        log.info("Shutdown complete. Total signals: %d", self.signal_count)

    def replay_trades_csv(
        self,
        csv_path: str,
        realtime_speed: float = 0.0,
        max_rows: Optional[int] = None
    ) -> None:
        """
        Офлайн-реплей принтов из CSV.

        CSV columns: ts,price,qty,side[,bid,ask,atr]
        - bid/ask/atr опциональны
        - realtime_speed=0 → без задержек; 1.0 → реал-тайм скорость

        Args:
            csv_path: Путь к CSV файлу
            realtime_speed: Множитель скорости (0=максимально быстро, 1=реальное время)
            max_rows: Максимум строк для обработки (для тестов)
        """
        import pandas as pd

        log.info("Starting trades replay: %s (speed=%.2f)", csv_path, realtime_speed)

        df = pd.read_csv(csv_path)
        df = df.sort_values("ts")

        if max_rows:
            df = df.head(max_rows)

        prev_ts = None
        processed = 0

        for idx, row in df.iterrows():
            ts = int(row["ts"])
            price = float(row["price"])
            qty = float(row["qty"])
            side = str(row["side"]).lower()

            bid = float(row["bid"]) if "bid" in row and not pd.isna(row["bid"]) else price
            ask = float(row["ask"]) if "ask" in row and not pd.isna(row["ask"]) else price
            atr = float(row["atr"]) if "atr" in row and not pd.isna(row["atr"]) else 0.0

            # Feed trade
            self.on_trade(price=price, qty=qty, side=side, ts_ms=ts)

            # Process step
            snap = {"ts": ts, "bid": bid, "ask": ask, "atr": atr, "mid": (bid + ask) / 2.0}
            result = self.step(snap)

            processed += 1

            # Сигналы всегда логируем (это важные события)
            if result:
                log.info("  [%d] Signal: %s", processed, result)

            # Realtime delay
            if realtime_speed > 0 and prev_ts is not None:
                dt_ms = max(0, ts - prev_ts)
                time.sleep(realtime_speed * (dt_ms / 1000.0))

            prev_ts = ts

        log.info("Replay complete: processed=%d signals=%d", processed, self.signal_count)

    # ========== INTERNALS ==========

    def _read_stream(
        self,
        stream: str,
        group: str,
        consumer: str,
        count: int = 20,
        block_ms: int = 1000,
        client: Optional[Any] = None
    ) -> List[Tuple[str, Dict[str, str]]]:
        """
        Читает сообщения из Redis stream через consumer group.

        Args:
            stream: Имя stream
            group: Имя consumer group
            consumer: Имя consumer
            count: Количество сообщений для чтения
            block_ms: Таймаут блокировки (мс)
            client: Redis клиент (по умолчанию self.r_ticks для тиков)

        Returns:
            List of (msg_id, fields_dict) tuples
        """
        # Используем переданный клиент или fallback на r_ticks
        redis_client = client if client is not None else self.r_ticks
        
        try:
            msgs = redis_client.xreadgroup(
                group,
                consumer,
                {stream: ">"},
                count=count,
                block=block_ms
            )
            if not msgs:
                return []

            # msgs = [(stream, [(id, {k:v,...}), ...])]
            items = msgs[0][1] if msgs and msgs[0][0] == stream else []
            return items

        except Exception as e:
            self.stream_read_error_counter[stream] += 1
            if self.stream_read_error_counter[stream] % 10000 != 0:
                return []
            log.debug("Stream read error (%s) [occurrence #%d]: %s", stream, self.stream_read_error_counter[stream], e)
            return []

    def _parse_message(self, fields: Dict[str, str]) -> Optional[Dict[str, Any]]:
        """
        Парсит сообщение из Redis stream.

        Supports:
        - JSON in 'data' field
        - Flat key-value fields
        """
        if not fields:
            return None

        # Если есть поле 'data' с JSON
        if "data" in fields:
            try:
                return json.loads(fields["data"])
            except Exception as e:
                # Не спамим логами о parse ошибках
                return None

        # Иначе возвращаем поля как есть, пытаясь распарсить JSON-значения
        result = {}
        for k, v in fields.items():
            if v and isinstance(v, str) and (v.startswith("{") or v.startswith("[")):
                try:
                    result[k] = json.loads(v)
                except Exception:
                    result[k] = v
            else:
                result[k] = v

        return result

    def _is_time_ok(self, ts_ms: int) -> bool:
        """Check if enough time passed since last signal."""
        if self.last_signal_ts <= 0:
            return True
        now_s = ts_ms / 1000.0
        return (now_s - self.last_signal_ts) >= self.cfg.min_signal_interval_sec

    def _get_cluster_score(self) -> Any:
        """Read DOM from Redis and analyze cluster."""
        if not self.cluster:
            return None

        try:
            key = self.cfg.book_levels_key.replace("{SYMBOL}", self.cfg.symbol)
            raw = self.r.get(key)
            if not raw:
                return self.cluster.analyze_empty() if hasattr(self.cluster, 'analyze_empty') else None

            levels = json.loads(raw)
            return self.cluster.analyze_from_dom(levels)
        except Exception as e:
            # Не спамим логами о cluster ошибках
            return None

    def _side_from_pro(self, m_pro: Dict[str, Any]) -> Optional[str]:
        """Extract side hint from pro metrics."""
        dir_up = m_pro.get("dir_up")
        if dir_up is True:
            return "LONG"
        if dir_up is False:
            return "SHORT"
        return None

    def _compose_confidence(
        self,
        m_pro: Dict[str, Any],
        m_legacy: Dict[str, Any],
        cl: Any
    ) -> Tuple[float, Optional[str], Dict[str, float]]:
        """
        Weighted confidence blending from multiple sources.

        Returns:
            (confidence: 0..1, side_hint, parts: dict)
        """
        # Pro detector: z_delta, z_speed
        z_delta = float(m_pro.get("z_delta", 0.0))
        z_speed = float(m_pro.get("z_speed", 0.0))

        # Cluster
        cl_conf = 0.0
        cl_dir = None
        if cl:
            # cl is a dict from SmartClusterAnalyzer.analyze_from_dom()
            if isinstance(cl, dict):
                cl_conf = float(cl.get("cluster_score", 0.0)) / 100.0  # convert 0..100 to 0..1
                cl_dir = cl.get("direction")
            else:
                # Canonical: store/emit percent 0..100. Ratio only derived locally.
                # cluster_score historically could be 0..100; do NOT blindly /100 here.
                cl_conf_pct = normalize_confidence_pct(
                    getattr(cl, "confidence_pct", None)
                    if hasattr(cl, "confidence_pct")
                    else (getattr(cl, "confidence", None) if getattr(cl, "confidence", None) is not None else getattr(cl, "cluster_score", 0.0))
                )
                cl_conf = float(cl_conf_pct)
                cl_conf01 = confidence_pct_to_ratio(cl_conf_pct)
                out["confidence_pct"] = cl_conf
                out["confidence"] = cl_conf  # legacy percent
                out["confidence_ratio"] = float(cl_conf01)
        cl_dir = getattr(cl, "direction", None)

        # Normalize to [0..1] via sigmoid
        p_delta = self._sigmoid_abs(z_delta, k=1.05)
        p_speed = self._sigmoid_abs(z_speed, k=0.9)

        # Legacy detector
        p_legacy = 0.0
        if m_legacy:
            z_leg = float(m_legacy.get("z", 0.0) or m_legacy.get("z_delta", 0.0))
            p_legacy = self._sigmoid_abs(z_leg, k=0.8)

        # Weighted blending
        w = self.cfg
        parts = {
            "p_delta": p_delta * w.w_delta_pro,
            "p_speed": p_speed * w.w_speed,
            "p_cluster": cl_conf * w.w_cluster,
            "p_legacy": p_legacy * w.w_legacy,
        }

        conf = max(0.0, min(1.0, sum(parts.values())))

        # Side hint from cluster
        side_hint = None
        if cl and isinstance(cl, dict):
            # Determine direction from cluster imbalance
            imb_up = cl.get("imb_up", 0.0)
            imb_dn = cl.get("imb_dn", 0.0)
            if cl.get("stacked_sell"):
                side_hint = "SHORT"  # Heavy sell wall above -> expect resistance
            elif cl.get("stacked_buy"):
                side_hint = "LONG"   # Heavy buy wall below -> expect support
            elif imb_up > imb_dn * 1.5:
                side_hint = "SHORT"  # Sellers dominate
            elif imb_dn > imb_up * 1.5:
                side_hint = "LONG"   # Buyers dominate
        
        # Fallback to delta
        if not side_hint:
            side_hint = "LONG" if z_delta > 0 else ("SHORT" if z_delta < 0 else None)

        return conf, side_hint, parts

    def _sigmoid_abs(self, x: float, k: float = 1.0) -> float:
        """Sigmoid normalization of |x| to [0..1]."""
        from math import exp
        ax = abs(x)
        return 1.0 / (1.0 + exp(-k * (ax - 1.0)))

    def _build_reason(
        self,
        m_pro: Dict[str, Any],
        cl: Any,
        parts: Dict[str, float]
    ) -> str:
        """Build human-readable reason string."""
        reasons = []

        z = m_pro.get("z_delta", 0.0)
        if abs(z) >= 4.5:
            reasons.append(f"EXTREME Δ (z={z:.1f})")
        elif abs(z) >= 3.0:
            reasons.append(f"Δ spike (z={z:.1f})")

        if cl:
            cl_reason = getattr(cl, "reason", "")
            if cl_reason:
                reasons.append(f"cluster:{cl_reason}")

        # Compact parts
        parts_str = ",".join(f"{k}={v:.2f}" for k, v in parts.items())
        reasons.append(f"mix:{parts_str}")

        return "; ".join(reasons)

    def _safe_cluster_dict(self, cl: Any) -> Dict[str, Any]:
        """Convert cluster score to safe dict."""
        if not cl:
            return {}

        if isinstance(cl, dict):
            # cl is already a dict from SmartClusterAnalyzer
            return {
                "confidence": cl.get("cluster_score", 0.0),
                "available": cl.get("available", False),
                "stacked_sell": cl.get("stacked_sell", False),
                "stacked_buy": cl.get("stacked_buy", False),
                "imb_up": cl.get("imb_up", 0.0),
                "imb_dn": cl.get("imb_dn", 0.0),
            }
        
        return {
            "confidence": getattr(cl, "confidence", 0.0) or getattr(cl, "cluster_score", 0.0),
            "direction": getattr(cl, "direction", None),
            "reason": getattr(cl, "reason", ""),
            "stacked_sell": getattr(cl, "stacked_sell", False),
            "stacked_buy": getattr(cl, "stacked_buy", False),
            "imb_up": getattr(cl, "imb_up", 0.0),
            "imb_dn": getattr(cl, "imb_dn", 0.0),
        }

    def _load_symbol_config_overrides(self) -> None:
        """Загружает overrides конфигурации символа из Redis (config:orderflow:{symbol})."""
        cfg_key = f"config:orderflow:{self.cfg.symbol}"
        raw_cfg = None

        for client_name, client in (("redis-ticks", getattr(self, "r_ticks", None)), ("redis-main", getattr(self, "r", None))):
            if client is None:
                continue
            try:
                raw = client.get(cfg_key)
            except Exception as exc:
                log.debug("Skip config fetch (%s) %s: %s", client_name, cfg_key, exc)
                continue
            if raw:
                raw_cfg = raw
                break

        if not raw_cfg:
            log.debug("No redis config overrides for %s (key=%s)", self.cfg.symbol, cfg_key)
            return

        if isinstance(raw_cfg, bytes):
            try:
                raw_cfg = raw_cfg.decode("utf-8")
            except Exception:
                log.warning("Failed to decode redis config for %s", self.cfg.symbol)
                return

        try:
            cfg_data = json.loads(raw_cfg) if isinstance(raw_cfg, str) else raw_cfg
        except (TypeError, ValueError) as exc:
            log.warning("Invalid JSON in %s: %s", cfg_key, exc)
            return

        if not isinstance(cfg_data, dict):
            log.debug("Config for %s is not a dict: %s", self.cfg.symbol, type(cfg_data))
            return

        hub_candidates = [
            cfg_data.get("hub_v2"),
            cfg_data.get("hub"),
            cfg_data.get("aggregated_hub"),
            cfg_data.get("aggregatedHub"),
            cfg_data.get("aggregated"),
            cfg_data,
        ]

        applied_any = False
        for candidate in hub_candidates:
            if isinstance(candidate, dict):
                applied_any = self._apply_hub_overrides(candidate) or applied_any
                break

        writer_candidates = [
            cfg_data.get("writer"),
            cfg_data.get("writer_cfg"),
            cfg_data.get("writerConfig"),
        ]
        for candidate in writer_candidates:
            if isinstance(candidate, dict):
                applied_any = self._apply_writer_overrides(candidate) or applied_any
                break

        if applied_any:
            log.info("✅ Redis config overrides applied for %s", self.cfg.symbol)
        else:
            log.debug("No applicable overrides found in %s for %s", cfg_key, self.cfg.symbol)

    def _apply_hub_overrides(self, overrides: Dict[str, Any]) -> bool:
        """Применяет hub-override значения к конфигурации."""
        updated = False

        def apply_attr(attr: str, key: str, cast):
            nonlocal updated
            if key in overrides:
                try:
                    value = cast(overrides[key])
                except (TypeError, ValueError):
                    log.warning("Invalid value for %s in hub overrides: %s", key, overrides[key])
                    return
                setattr(self.cfg, attr, value)
                log.debug("Config override: %s=%s (key=%s)", attr, value, key)
                updated = True

        apply_attr("confidence_threshold", "confidence_threshold", float)
        apply_attr("min_signal_interval_sec", "min_signal_interval_sec", int)
        apply_attr("side_lock_sec", "side_lock_sec", int)
        apply_attr("atr_cache_ttl", "atr_cache_ttl", int)
        apply_attr("w_delta_pro", "w_delta_pro", float)
        apply_attr("w_speed", "w_speed", float)
        apply_attr("w_cluster", "w_cluster", float)
        apply_attr("w_legacy", "w_legacy", float)

        weights = overrides.get("weights")
        if isinstance(weights, dict):
            if "delta" in weights:
                try:
                    self.cfg.w_delta_pro = float(weights["delta"])
                    log.debug("Config override: w_delta_pro=%s (weights.delta)", self.cfg.w_delta_pro)
                    updated = True
                except (TypeError, ValueError):
                    log.warning("Invalid weights.delta value: %s", weights["delta"])
            if "delta_pro" in weights:
                try:
                    self.cfg.w_delta_pro = float(weights["delta_pro"])
                    log.debug("Config override: w_delta_pro=%s (weights.delta_pro)", self.cfg.w_delta_pro)
                    updated = True
                except (TypeError, ValueError):
                    log.warning("Invalid weights.delta_pro value: %s", weights["delta_pro"])
            if "speed" in weights:
                try:
                    self.cfg.w_speed = float(weights["speed"])
                    log.debug("Config override: w_speed=%s (weights.speed)", self.cfg.w_speed)
                    updated = True
                except (TypeError, ValueError):
                    log.warning("Invalid weights.speed value: %s", weights["speed"])
            if "cluster" in weights:
                try:
                    self.cfg.w_cluster = float(weights["cluster"])
                    log.debug("Config override: w_cluster=%s (weights.cluster)", self.cfg.w_cluster)
                    updated = True
                except (TypeError, ValueError):
                    log.warning("Invalid weights.cluster value: %s", weights["cluster"])
            if "legacy" in weights:
                try:
                    self.cfg.w_legacy = float(weights["legacy"])
                    log.debug("Config override: w_legacy=%s (weights.legacy)", self.cfg.w_legacy)
                    updated = True
                except (TypeError, ValueError):
                    log.warning("Invalid weights.legacy value: %s", weights["legacy"])

        thresholds = overrides.get("thresholds")
        if isinstance(thresholds, dict):
            if "confidence" in thresholds:
                try:
                    self.cfg.confidence_threshold = float(thresholds["confidence"])
                    log.debug("Config override: confidence_threshold=%s (thresholds.confidence)", self.cfg.confidence_threshold)
                    updated = True
                except (TypeError, ValueError):
                    log.warning("Invalid thresholds.confidence value: %s", thresholds["confidence"])
            if "min_signal_interval_sec" in thresholds:
                try:
                    self.cfg.min_signal_interval_sec = int(thresholds["min_signal_interval_sec"])
                    log.debug(
                        "Config override: min_signal_interval_sec=%s (thresholds.min_signal_interval_sec)",
                        self.cfg.min_signal_interval_sec
                    )
                    updated = True
                except (TypeError, ValueError):
                    log.warning(
                        "Invalid thresholds.min_signal_interval_sec value: %s",
                        thresholds["min_signal_interval_sec"]
                    )

        return updated

    def _apply_writer_overrides(self, overrides: Dict[str, Any]) -> bool:
        """Применяет writer-override значения к конфигурации."""
        updated = False

        def apply_attr(attr: str, key: str, cast):
            nonlocal updated
            if key in overrides:
                try:
                    value = cast(overrides[key])
                except (TypeError, ValueError):
                    log.warning("Invalid writer override for %s: %s", key, overrides[key])
                    return
                setattr(self.cfg, attr, value)
                log.debug("Writer override: %s=%s (key=%s)", attr, value, key)
                updated = True

        apply_attr("min_conf_writer", "min_confidence", float)
        apply_attr("cooldown_writer", "cooldown_sec", int)
        apply_attr("risk_pct", "risk_pct", float)
        apply_attr("sl_mult", "sl_mult", float)

        if "tp_mults" in overrides:
            tp_mults_val = overrides["tp_mults"]
            if isinstance(tp_mults_val, (list, tuple)):
                tp_mults = ",".join(str(v) for v in tp_mults_val)
            else:
                tp_mults = str(tp_mults_val)
            self.cfg.tp_mults_str = tp_mults
            log.debug("Writer override: tp_mults_str=%s", tp_mults)
            updated = True

        return updated

    def _write_label(
        self,
        ts_ms: int,
        side: str,
        price: float,
        atr: float,
        conf: float,
        reason: str,
        m_pro: Dict,
        cl: Any,
        writer_result: Dict
    ) -> None:
        """Write label to Parquet for offline validation."""
        if not self.label_sink:
            return

        try:
            payload = writer_result.get("payload", {})
            label = {
                "ts": ts_ms,
                "symbol": self.cfg.symbol,
                "source": "hub_v2",
                "side": side,
                "price": price,
                "sl": payload.get("sl", 0.0),
                "tp_levels": payload.get("tp_levels", []),
                "lot": payload.get("lot", 0.0),
                "confidence": conf,
                "atr": atr,
                "reason": reason,
                "metrics": {
                    "z_delta": m_pro.get("z_delta", 0.0),
                    "z_speed": m_pro.get("z_speed", 0.0),
                    "z_range": m_pro.get("z_range", 0.0),
                    "svbp_imbalance": m_pro.get("svbp_imbalance", 0.0),
                    "cluster_conf": getattr(cl, "confidence", 0.0) if cl else 0.0,
                    "cluster_dir": getattr(cl, "direction", None) if cl else None,
                },
                "emitted": True,
            }
            self.label_sink.write(label)
        except Exception as e:
            log.exception("Parquet write failed: %s", e)

    def _poll_external_signals(self) -> None:
        """Читает внешние сигналы из зарегистрированных стримов и записывает их без доп. фильтрации."""
        if not self.external_streams:
            return

        try:
            msgs = self.r.xreadgroup(
                self.crypto_group,
                self.crypto_consumer,
                {stream: ">" for stream in self.external_streams},
                count=100,
                block=0
            )
        except Exception as exc:
            msg = str(exc)
            if "NOGROUP" in msg.upper():
                for stream_name in self.external_streams:
                    try:
                        self.r.xgroup_create(
                            stream_name,
                            self.crypto_group,
                            id="$",
                            mkstream=True,
                        )
                        log.info(
                            "✅ External consumer group recreated: stream=%s group=%s",
                            stream_name,
                            self.crypto_group,
                        )
                    except Exception as recreate_err:
                        if "BUSYGROUP" in str(recreate_err):
                            log.debug(
                                "External consumer group already exists: stream=%s group=%s",
                                stream_name,
                                self.crypto_group,
                            )
                        else:
                            log.warning(
                                "Failed to recreate external consumer group: stream=%s err=%s",
                                stream_name,
                                recreate_err,
                            )
            log.debug("External signals read error: %s", exc)
            return

        if not msgs:
            return

        for stream_name, payloads in msgs:
            if not payloads:
                continue

            default_source = self.external_streams.get(stream_name)
            for msg_id, fields in payloads:
                try:
                    signal = self._parse_message(fields)
                    if not signal:
                        continue

                    signal_source = signal.get("source") or default_source
                    if signal_source == "crypto-orderflow":
                        enriched = self._enrich_crypto_orderflow_signal(signal)
                        if enriched:
                            self._emit_crypto_signal(enriched)
                    else:
                        log.debug("⚠️  Unsupported external source=%s stream=%s", signal_source, stream_name)
                except Exception as exc:
                    log.error("Failed to process external signal from %s: %s", stream_name, exc, exc_info=True)
                finally:
                    try:
                        self.r.xack(stream_name, self.crypto_group, msg_id)
                    except Exception:
                        pass

    def _enrich_crypto_orderflow_signal(self, signal: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Добавляет trailing-настройки и обязательные поля для downstream сервисов."""
        sid = signal.get("sid")
        symbol = signal.get("symbol")
        if not sid or not symbol:
            log.warning("⚠️  Crypto signal missing sid/symbol: %s", signal)
            return None

        enriched = dict(signal)
        enriched.setdefault("trail_after_tp1", True)
        enriched.setdefault("trail_profile", "lock_and_trail")
        enriched.setdefault("source", "crypto-orderflow")
        enriched.setdefault("ts", int(time.time() * 1000))

        indicators = enriched.get("indicators") or {}
        if not isinstance(indicators, dict):
            indicators = {}
        indicators.setdefault("ingested_by", "AggregatedHub-V2")
        enriched["indicators"] = indicators
        return enriched

    def _emit_crypto_signal(self, signal: Dict[str, Any]) -> None:
        """Записывает обогащённый crypto-orderflow сигнал в Redis."""
        symbol = signal["symbol"]
        sid = signal["sid"]
        payload_json = json.dumps(signal)

        try:
            stream_name = f"signals:aggregated:{symbol}"
            self.r.xadd(stream_name, {"data": payload_json}, maxlen=2000, approximate=True)
        except Exception as exc:
            log.error("Failed to publish crypto signal to %s: %s", stream_name, exc)

        try:
            self.r.set(f"signals:{sid}", payload_json, ex=86400)
        except Exception as exc:
            log.warning("Failed to store crypto signal key signals:%s: %s", sid, exc)

        self.crypto_signal_count += 1
        log.info("✅ Crypto orderflow signal ingested: %s %s", symbol, sid)


# ========== CLI ENTRY POINT ==========
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Aggregated Signal Hub V2")
    parser.add_argument("--symbol", default=None, help="Trading symbol (overrides SYMBOL env var)")
    parser.add_argument("--mode", choices=["live", "replay"], default="live",
                       help="Mode: 'live' for real-time streams, 'replay' for CSV")
    parser.add_argument("--replay-csv", help="CSV file for offline replay (requires --mode=replay)")
    parser.add_argument("--replay-speed", type=float, default=0.0, help="Replay speed multiplier")
    parser.add_argument("--max-rows", type=int, help="Max rows to process in replay")
    args = parser.parse_args()

    # Создаём конфигурацию
    cfg = HubConfig()
    if args.symbol:
        cfg.symbol = args.symbol

    log.info("=" * 80)
    log.info("AggregatedSignalHubV2 starting")
    log.info("Symbol: %s", cfg.symbol)
    log.info("Mode: %s", args.mode)
    log.info("=" * 80)
    log.info("Redis Configuration:")
    log.info("  Signals/ATR/DOM: %s", cfg.redis_url)
    log.info("  Ticks/Prints:    %s", cfg.redis_ticks_url)
    log.info("=" * 80)
    log.info("Streams:")
    log.info("  Tick stream:   %s", cfg.tick_stream or "Not configured")
    log.info("  Prints stream: %s", cfg.prints_stream or "Not configured")
    log.info("=" * 80)
    log.info("Thresholds:")
    log.info("  Confidence threshold: %.2f", cfg.confidence_threshold)
    log.info("  Min signal interval:  %ds", cfg.min_signal_interval_sec)
    log.info("=" * 80)

    hub = AggregatedSignalHubV2(cfg)

    if args.mode == "replay":
        if not args.replay_csv:
            log.error("--replay-csv required for replay mode")
            exit(1)
        hub.replay_trades_csv(
            args.replay_csv,
            realtime_speed=args.replay_speed,
            max_rows=args.max_rows
        )
    else:
        # Live mode - запускаем основной цикл
        hub.run()

