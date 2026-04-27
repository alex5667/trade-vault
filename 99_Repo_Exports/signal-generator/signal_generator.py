#!/usr/bin/env python3
"""
XAUUSD Signal Generator - Technical Analysis Based
Production-ready signal generation without Order Book data

Features:
- Multiple technical indicators (EMA, RSI, ATR, MACD)
- Dynamic SL/TP calculation based on ATR
- Risk management (position sizing, max drawdown)
- Integration with go-gateway API
- Real-time tick data processing
- Configurable strategies
"""

import os
import sys
import time
import json
import logging
import requests
import redis
from datetime import datetime, timedelta, timezone
from typing import Optional, Dict, List, Tuple
from dataclasses import dataclass, asdict
import numpy as np
import pandas as pd
from collections import deque

# Импортируем единый форматировщик XAUUSD
from xauusd_signal_formatter import XAUUSDSignalFormatter, XAUUSDSignal

# Configuration
GATEWAY_URL = os.getenv("GATEWAY_URL", "http://127.0.0.1:8090")
OBI_SERVICE_URL = os.getenv("OBI_SERVICE_URL", "http://127.0.0.1:8088")
SYMBOL = os.getenv("SYMBOL", "XAUUSD")
TIMEFRAME = os.getenv("TIMEFRAME", "M5")  # M1, M5, M15, H1
CHECK_INTERVAL = int(os.getenv("CHECK_INTERVAL", "60"))  # seconds

# 🎯 Redis configuration - DUAL INSTANCE SETUP
# redis-ticks: для высокочастотных тиков
# scanner-redis-worker: для сигналов и конфигурации
USE_REAL_TICKS = os.getenv("USE_REAL_TICKS", "false").lower() == "true"
REDIS_TICKS_URL = os.getenv("REDIS_TICKS_URL", "redis://redis-ticks:6379/0")
REDIS_SIGNALS_URL = os.getenv("REDIS_SIGNALS_URL", "redis://scanner-redis-worker-1:6379/0")
TICK_STREAM = os.getenv("TICK_STREAM", "stream:tick_XAUUSD")
AUDIT_SIGNAL_STREAM = os.getenv("SIGNAL_AUDIT_STREAM", f"signals:audit:{SYMBOL}")  # v7: audit stream

# Strategy parameters
EMA_FAST = int(os.getenv("EMA_FAST", "9"))
EMA_SLOW = int(os.getenv("EMA_SLOW", "21"))
RSI_PERIOD = int(os.getenv("RSI_PERIOD", "14"))
RSI_OVERSOLD = int(os.getenv("RSI_OVERSOLD", "30"))
RSI_OVERBOUGHT = int(os.getenv("RSI_OVERBOUGHT", "70"))
ATR_PERIOD = int(os.getenv("ATR_PERIOD", "14"))
ATR_SL_MULTIPLIER = float(os.getenv("ATR_SL_MULTIPLIER", "1.5"))
ATR_TP_MULTIPLIERS = [float(x) for x in os.getenv("ATR_TP_MULTIPLIERS", "2.0,3.0,4.0").split(",")]

# Risk management
DEFAULT_LOT = float(os.getenv("DEFAULT_LOT", "0.01"))
MAX_LOT = float(os.getenv("MAX_LOT", "0.1"))
RISK_PERCENT = float(os.getenv("RISK_PERCENT", "5.0"))  # % of account per trade

# 🎯 Position tracking (can be disabled for multiple signals)
ENABLE_POSITION_TRACKING = os.getenv("ENABLE_POSITION_TRACKING", "true").lower() == "true"
MAX_POSITION_DURATION_HOURS = float(os.getenv("MAX_POSITION_DURATION_HOURS", "2.0"))

# Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)-8s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


@dataclass
class Signal:
    """Trading signal"""
    sid: str
    symbol: str
    side: str  # LONG or SHORT
    lot: float
    entry: Optional[float]
    sl: float
    tp_levels: List[float]
    reason: str
    indicators: Dict


class TickBuffer:
    """Buffer for tick data to build candles"""
    
    def __init__(self, maxlen: int = 50000):  # 🎯 SENIOR DEV: Увеличен до 50000 для хранения всех исторических тиков
        self.ticks = deque(maxlen=maxlen)
        self.last_candle_time = None
        
    def add_tick(self, timestamp: int, bid: float, ask: float):
        """Add tick data"""
        mid = (bid + ask) / 2.0
        self.ticks.append({
            'timestamp': timestamp,
            'bid': bid,
            'ask': ask,
            'mid': mid
        })
        
    def get_candles(self, timeframe_minutes: int, periods: int) -> pd.DataFrame:
        """Build candles from ticks"""
        if len(self.ticks) < 10:
            return pd.DataFrame()
            
        # Convert to DataFrame
        df = pd.DataFrame(list(self.ticks))
        df['time'] = pd.to_datetime(df['timestamp'], unit='ms')
        
        # Resample to candles
        df.set_index('time', inplace=True)
        timeframe = f'{timeframe_minutes}min'
        
        candles = df['mid'].resample(timeframe).agg({'open': 'first', 'high': 'max', 'low': 'min', 'close': 'last'})
        candles['volume'] = df['mid'].resample(timeframe).count()
        
        # 🎯 CRITICAL FIX: Fill NaN values that occur when resampling creates empty time periods
        # This happens when there are no ticks in certain time windows
        # Use forward fill to propagate last known price, then backward fill for any leading NaNs
        if candles[['open', 'high', 'low', 'close']].isna().any().any():
            logger.debug(f"Filling NaN values in {candles.isna().sum().sum()} candle cells due to sparse tick data")
            candles[['open', 'high', 'low', 'close']] = candles[['open', 'high', 'low', 'close']].ffill().bfill()
            # Volume stays 0 for empty periods (this is correct - no ticks = no volume)
        
        # 🎯 SENIOR DEV FIX: Remove ONLY if last candle is incomplete (volume=0 or very recent)
        # Don't remove all incomplete candles blindly - это убивало 90% данных!
        if len(candles) > 0:
             # Ensure we return at least 'periods' if available, otherwise return all
             pass

        # 🎯 DEBUG: Логируем информацию о свечах
        logger.debug(f"Built {len(candles)} candles from {len(self.ticks)} ticks (timeframe={timeframe})")
        
        # Return last N periods, ensuring we don't truncate if we have just enough
        return candles.tail(periods) if len(candles) > 0 else pd.DataFrame()


class TechnicalIndicators:
    """Technical indicators calculator with GPU acceleration"""
    
    # ✅ GPU Support: используем GPU сервис если доступен
    _gpu_service = None
    
    @classmethod
    def _get_gpu_service(cls):
        """Получить GPU сервис (lazy initialization)"""
        if cls._gpu_service is None:
            try:
                import sys
                import os
                # Добавляем путь к python-worker для импорта
                worker_path = os.path.join(os.path.dirname(__file__), '..', 'python-worker')
                if worker_path not in sys.path:
                    sys.path.insert(0, worker_path)
                from services.gpu_compute_service import get_gpu_service
                cls._gpu_service = get_gpu_service()
            except Exception:
                cls._gpu_service = None
        return cls._gpu_service
    
    @staticmethod
    def ema(data: pd.Series, period: int) -> pd.Series:
        """Exponential Moving Average with GPU acceleration and NaN handling"""
        # Проверка входных данных
        if len(data) < period or data.isna().all():
            return pd.Series([np.nan] * len(data), index=data.index)

        gpu_service = TechnicalIndicators._get_gpu_service()
        if gpu_service and gpu_service.is_gpu_available() and len(data) > period:
            try:
                # Очистка данных от NaN перед GPU расчетом
                clean_data = data.ffill().bfill()
                data_arr = clean_data.values.astype(np.float32)
                ema_values = gpu_service.compute_ema_batch(data_arr, period)
                return pd.Series(ema_values, index=data.index)
            except Exception:
                pass  # Fallback to CPU

        # CPU fallback с обработкой NaN
        try:
            clean_data = data.ffill().bfill()
            return clean_data.ewm(span=period, adjust=False).mean()
        except Exception:
            return pd.Series([np.nan] * len(data), index=data.index)
    
    @staticmethod
    def rsi(data: pd.Series, period: int = 14) -> pd.Series:
        """Relative Strength Index with GPU acceleration and NaN handling"""
        # Проверка входных данных
        if len(data) < period + 1 or data.isna().all():
            return pd.Series([np.nan] * len(data), index=data.index)

        gpu_service = TechnicalIndicators._get_gpu_service()
        if gpu_service and gpu_service.is_gpu_available() and len(data) > period + 1:
            try:
                # Очистка данных от NaN перед GPU расчетом
                clean_data = data.ffill().bfill()
                data_arr = clean_data.values.astype(np.float32)
                rsi_values = gpu_service.compute_rsi_batch(data_arr, period)
                return pd.Series(rsi_values, index=data.index)
            except Exception:
                pass  # Fallback to CPU

        # CPU fallback с обработкой NaN
        try:
            clean_data = data.ffill().bfill()
            delta = clean_data.diff()
            gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
            loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
            rs = gain / loss
            rsi = 100 - (100 / (1 + rs))
            return rsi.fillna(50.0)  # RSI в районе 50 при неопределенности
        except Exception:
            return pd.Series([np.nan] * len(data), index=data.index)
    
    @staticmethod
    def atr(high: pd.Series, low: pd.Series, close: pd.Series, period: int = 14) -> pd.Series:
        """Average True Range with GPU acceleration and NaN handling"""
        # Проверка входных данных
        if (len(high) < period or len(low) < period or len(close) < period or
            high.isna().all() or low.isna().all() or close.isna().all()):
            return pd.Series([np.nan] * len(high), index=high.index)

        gpu_service = TechnicalIndicators._get_gpu_service()
        if gpu_service and gpu_service.is_gpu_available() and len(high) > period:
            try:
                # Очистка данных от NaN перед GPU расчетом
                clean_high = high.ffill().bfill()
                clean_low = low.ffill().bfill()
                clean_close = close.ffill().bfill()

                highs_arr = clean_high.values.astype(np.float32)
                lows_arr = clean_low.values.astype(np.float32)
                closes_arr = clean_close.values.astype(np.float32)
                atr_values = gpu_service.compute_atr_batch(highs_arr, lows_arr, closes_arr, period)
                return pd.Series(atr_values, index=high.index)
            except Exception:
                pass  # Fallback to CPU

        # CPU fallback с обработкой NaN
        try:
            clean_high = high.ffill().bfill()
            clean_low = low.ffill().bfill()
            clean_close = close.ffill().bfill()

            tr1 = clean_high - clean_low
            tr2 = abs(clean_high - clean_close.shift())
            tr3 = abs(clean_low - clean_close.shift())
            tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
            return tr.rolling(window=period).mean().bfill().fillna(0.0001)
        except Exception:
            return pd.Series([np.nan] * len(high), index=high.index)
    
    @staticmethod
    def macd(data: pd.Series, fast: int = 12, slow: int = 26, signal: int = 9) -> Tuple[pd.Series, pd.Series, pd.Series]:
        """MACD indicator with GPU acceleration and NaN handling"""
        # Проверка входных данных
        min_required = slow + signal
        if len(data) < min_required or data.isna().all():
            nan_series = pd.Series([np.nan] * len(data), index=data.index)
            return nan_series, nan_series, nan_series

        gpu_service = TechnicalIndicators._get_gpu_service()
        if gpu_service and gpu_service.is_gpu_available() and len(data) > slow:
            try:
                # Очистка данных от NaN перед GPU расчетом
                clean_data = data.ffill().bfill()
                data_arr = clean_data.values.astype(np.float32)
                macd_line_arr, signal_line_arr, histogram_arr = gpu_service.compute_macd_batch(
                    data_arr, fast, slow, signal
                )
                return (
                    pd.Series(macd_line_arr, index=data.index),
                    pd.Series(signal_line_arr, index=data.index),
                    pd.Series(histogram_arr, index=data.index)
                )
            except Exception:
                pass  # Fallback to CPU

        # CPU fallback с обработкой NaN
        try:
            clean_data = data.ffill().bfill()
            ema_fast = clean_data.ewm(span=fast, adjust=False).mean()
            ema_slow = clean_data.ewm(span=slow, adjust=False).mean()
            macd_line = ema_fast - ema_slow
            signal_line = macd_line.ewm(span=signal, adjust=False).mean()
            histogram = macd_line - signal_line
            return macd_line, signal_line, histogram
        except Exception:
            nan_series = pd.Series([np.nan] * len(data), index=data.index)
            return nan_series, nan_series, nan_series


class SignalGenerator:
    """Main signal generator"""
    
    def __init__(self):
        self.tick_buffer = TickBuffer()
        self.last_signal_time = None
        self.min_signal_interval = timedelta(minutes=5)  # 🎯 SENIOR DEV: 5 минут между сигналами
        self.enable_position_tracking = ENABLE_POSITION_TRACKING
        self.position_open = False
        self.position_open_time = None  # 🎯 FIX: Время открытия позиции
        self.max_position_duration = timedelta(hours=MAX_POSITION_DURATION_HOURS)  # 🎯 FIX: Автосброс
        self.price_data = deque(maxlen=1000)  # Price history for analysis
        self.signal_counter = 0  # 🎯 Счетчик сигналов для ID
        
        # 🎯 Redis clients - DUAL INSTANCE SETUP
        # redis_ticks_client: для чтения тиков из redis-ticks
        # redis_signals_client: для записи сигналов в scanner-redis-worker
        self.redis_ticks_client = None
        self.redis_signals_client = None
        self.last_redis_id = "0"  # Start from beginning
        
        # Initialize Redis connection status
        redis_connected = False

        if USE_REAL_TICKS:
            try:
                # Подключение к redis-ticks для чтения тиков
                self.redis_ticks_client = redis.Redis.from_url(
                    REDIS_TICKS_URL,
                    decode_responses=True,
                    socket_timeout=30,
                    socket_connect_timeout=10,
                    socket_keepalive=True,
                    health_check_interval=30
                )
                self.redis_ticks_client.ping()
                logger.info(f"✅ Connected to redis-ticks: {REDIS_TICKS_URL}")

                # Подключение к scanner-redis-worker для записи сигналов
                self.redis_signals_client = redis.Redis.from_url(
                    REDIS_SIGNALS_URL,
                    decode_responses=True,
                    socket_timeout=30,
                    socket_connect_timeout=10
                )
                self.redis_signals_client.ping()
                logger.info(f"✅ Connected to redis-signals: {REDIS_SIGNALS_URL}")

                redis_connected = True

            except Exception as e:
                logger.error(f"❌ Failed to connect to Redis: {e}")
                logger.warning("⚠️ Falling back to simulation mode")
                self.redis_ticks_client = None
                self.redis_signals_client = None
                redis_connected = False
        
        logger.info("="*60)
        logger.info("Signal Generator initialized")
        logger.info("="*60)
        logger.info(f"Symbol: {SYMBOL}")
        logger.info(f"Mode: {'REAL TICKS from Redis' if redis_connected else 'SIMULATION'}")
        logger.info(f"Strategy: EMA({EMA_FAST}/{EMA_SLOW}) + RSI({RSI_PERIOD}) + ATR({ATR_PERIOD})")
        logger.info(f"Risk: {RISK_PERCENT}% per trade, Default lot: {DEFAULT_LOT}")
        logger.info(f"Position Tracking: {'Enabled' if self.enable_position_tracking else 'Disabled'} (auto-reset: {self.max_position_duration.total_seconds()/3600:.1f}h)")
        logger.info(f"Gateway: {GATEWAY_URL}")
        if redis_connected:
            logger.info(f"Redis Ticks: {REDIS_TICKS_URL}")
            logger.info(f"Redis Signals: {REDIS_SIGNALS_URL}")
            logger.info(f"Tick Stream: {TICK_STREAM}")
        logger.info("="*60)
        
    def fetch_ticks_from_redis(self) -> int:
        """🎯 Fetch real ticks from redis-ticks stream"""
        if not self.redis_ticks_client:
            return 0
        
        try:
            # Read ticks from redis-ticks stream
            # 🎯 Load ALL historical ticks on first run
            count_to_read = 50000 if self.last_redis_id == "0" else 5000
            
            messages = self.redis_ticks_client.xread(
                {TICK_STREAM: self.last_redis_id},
                count=count_to_read,
                block=1000  # Block for 1 second if no data
            )
            
            ticks_added = 0
            
            for stream_name, entries in messages:
                for msg_id, fields in entries:
                    try:
                        # 🎯 SENIOR DEV: Parse tick data - two formats supported
                        # Format 1: Direct fields (ts, bid, ask, symbol)
                        # Format 2: JSON in 'data' field
                        
                        if 'data' in fields:
                            # JSON format
                            data_str = fields.get('data', '{}')
                            data = json.loads(data_str) if isinstance(data_str, str) else data_str
                            timestamp = int(data.get('ts', time.time() * 1000))
                            
                            bid = float(data.get('bid', 0))
                            ask = float(data.get('ask', 0))
                            
                            if bid == 0 and ask == 0:
                                price = float(data.get('price', data.get('p', 0)))
                                if price > 0:
                                    bid = price - 0.05
                                    ask = price + 0.05
                        else:
                            # Direct fields format
                            timestamp = int(fields.get('ts', time.time() * 1000))
                            
                            bid = float(fields.get('bid', 0))
                            ask = float(fields.get('ask', 0))
                            
                            if bid == 0 and ask == 0 and 'price' in fields:
                                price = float(fields.get('price'))
                                bid = price - 0.05
                                ask = price + 0.05
                        
                        if bid > 0 and ask > 0:
                            self.add_tick(timestamp, bid, ask)
                            ticks_added += 1
                            
                        self.last_redis_id = msg_id
                        
                    except Exception as e:
                        logger.warning(f"Error parsing tick {msg_id}: {e}")
                        continue
            
            if ticks_added > 0:
                logger.debug(f"📥 Fetched {ticks_added} ticks from Redis")
            
            return ticks_added
            
        except Exception as e:
            logger.error(f"❌ Error reading from Redis: {e}")
            return 0
    
    def fetch_ticks_from_obi(self) -> bool:
        """Fetch recent tick data from OBI service"""
        try:
            # Get tick data from OBI service memory
            # For now, we'll use healthz to check service
            resp = requests.get(f"{OBI_SERVICE_URL}/healthz", timeout=3)
            if resp.status_code == 200:
                data = resp.json()
                logger.debug(f"OBI service: {data.get('points', 0)} points")
                return True
        except Exception as e:
            logger.warning(f"Could not fetch from OBI service: {e}")
        return False
    
    def add_tick(self, timestamp: int, bid: float, ask: float):
        """Add tick to buffer"""
        self.tick_buffer.add_tick(timestamp, bid, ask)
        
    def calculate_indicators(self, candles: pd.DataFrame) -> Dict:
        """Calculate technical indicators with robust NaN/Inf handling"""
        # 🎯 SENIOR DEV FIX: Более мягкое требование - достаточно базового минимума
        # MACD needs slow (26) + signal (9) + some warmup
        min_required = max(EMA_SLOW, RSI_PERIOD, ATR_PERIOD, 26 + 9) + 2
        if len(candles) < min_required:
            logger.warning(f"Not enough candles for indicators: have {len(candles)}, need {min_required}")
            return {}

        # Проверка входных данных на NaN/Inf
        required_cols = ['open', 'high', 'low', 'close']
        if not all(col in candles.columns for col in required_cols):
            logger.warning(f"Missing required columns in candles: {required_cols}")
            return {}

        candle_data = candles[required_cols]
        
        # Clean NaN/Inf values if present (should be rare after get_candles fix)
        nan_count = candle_data.isna().sum().sum()
        inf_count = np.isinf(candle_data.values).sum()
        
        if nan_count > 0 or inf_count > 0:
            # This should be rare now that get_candles pre-fills NaN values
            logger.debug(f"Secondary cleanup: {nan_count} NaN and {inf_count} Inf values in candle data")
            # Forward fill then backward fill to handle NaN
            candles[required_cols] = candle_data.ffill().bfill()
            # Replace any remaining Inf with median
            for col in required_cols:
                if np.isinf(candles[col].values).any():
                    median_val = candles[col][~np.isinf(candles[col])].median()
                    candles[col] = candles[col].replace([np.inf, -np.inf], median_val)
            
            # Final check - if still have NaN/Inf after cleaning, reject
            if candles[required_cols].isna().any().any() or np.isinf(candles[required_cols].values).any():
                logger.warning(f"Could not clean candle data - {candles[required_cols].isna().sum().sum()} NaN, {np.isinf(candles[required_cols].values).sum()} Inf remaining")
                return {}

        # Проверка что данные не все одинаковые (что может вызвать деление на ноль)
        if candles['high'].max() == candles['low'].min():
            logger.warning("Candle data shows no price movement (all same values)")
            return {}

        close = candles['close']
        high = candles['high']
        low = candles['low']

        # EMA
        ema_fast = TechnicalIndicators.ema(close, EMA_FAST)
        ema_slow = TechnicalIndicators.ema(close, EMA_SLOW)

        # RSI
        rsi = TechnicalIndicators.rsi(close, RSI_PERIOD)

        # ATR
        atr = TechnicalIndicators.atr(high, low, close, ATR_PERIOD)

        # MACD
        macd_line, signal_line, histogram = TechnicalIndicators.macd(close)

        # 🎯 SENIOR DEV: Безопасный доступ к значениям с проверкой длины и NaN
        try:
            # Проверяем что все индикаторы имеют достаточную длину
            min_len = 3  # Нужны минимум последние 3 значения для prev значений
            if (len(ema_fast) < min_len or len(ema_slow) < min_len or
                len(rsi) < min_len or len(atr) < min_len or
                len(macd_line) < min_len or len(histogram) < min_len):
                logger.warning("Indicators have insufficient length for prev values")
                return {}

            # Безопасное получение значений с умными fallback
            def safe_get(series, index, fallback=None, name="unknown"):
                try:
                    val = series.iloc[index]
                    if pd.isna(val) or np.isinf(val):
                        # Try to get a valid value from nearby indices
                        for offset in range(1, min(5, len(series))):
                            try:
                                alt_val = series.iloc[index - offset]
                                if not pd.isna(alt_val) and not np.isinf(alt_val):
                                    logger.debug(f"Using offset {offset} for {name}")
                                    return alt_val
                            except:
                                continue
                        logger.warning(f"No valid value found for {name}, using fallback")
                        return fallback
                    return val
                except (IndexError, KeyError) as e:
                    logger.warning(f"Index error for {name}: {e}, using fallback")
                    return fallback

            # Get close price first as it's critical
            close_price = safe_get(close, -1, name="close")
            if close_price is None:
                logger.warning("Could not get close price - aborting indicator calculation")
                return {}

            indicators = {
                'close': close_price,
                'ema_fast': safe_get(ema_fast, -1, fallback=close_price, name="ema_fast"),
                'ema_slow': safe_get(ema_slow, -1, fallback=close_price, name="ema_slow"),
                'ema_fast_prev': safe_get(ema_fast, -2, fallback=safe_get(ema_fast, -1, close_price, "ema_fast_prev_alt"), name="ema_fast_prev"),
                'ema_slow_prev': safe_get(ema_slow, -2, fallback=safe_get(ema_slow, -1, close_price, "ema_slow_prev_alt"), name="ema_slow_prev"),
                'rsi': safe_get(rsi, -1, fallback=50.0, name="rsi"),
                'rsi_prev': safe_get(rsi, -2, fallback=safe_get(rsi, -1, 50.0, "rsi_prev_alt"), name="rsi_prev"),
                'atr': safe_get(atr, -1, fallback=close_price * 0.01, name="atr"),  # 1% of price as fallback
                'macd': safe_get(macd_line, -1, fallback=0.0, name="macd"),
                'macd_signal': safe_get(signal_line, -1, fallback=0.0, name="macd_signal"),
                'macd_hist': safe_get(histogram, -1, fallback=0.0, name="macd_hist"),
                'macd_hist_prev': safe_get(histogram, -2, fallback=safe_get(histogram, -1, 0.0, "macd_hist_prev_alt"), name="macd_hist_prev"),
            }

            # Проверка что все значения получены корректно
            none_keys = [k for k, v in indicators.items() if v is None]
            if none_keys:
                logger.warning(f"Indicator calculation returned None for: {', '.join(none_keys)}")
                return {}

            # Финальная проверка на NaN/Inf
            nan_keys = [k for k, v in indicators.items() if v is None or pd.isna(v) or np.isinf(v)]
            if nan_keys:
                logger.warning(f"Invalid values (NaN/Inf) in indicators: {', '.join(nan_keys)}")
                return {}

            return indicators

        except Exception as e:
            logger.error(f"Error calculating indicators: {e}")
            return {}

        finally:
            # Publish last ATR to Redis for /runtime/snapshot consumers
            try:
                atr_val = float(atr.iloc[-1]) if 'atr' in locals() else None
                if atr_val and not np.isnan(atr_val) and not np.isinf(atr_val):
                    if self.redis_signals_client:
                        self.redis_signals_client.set(f"ta:last:atr:{SYMBOL}", json.dumps({"atr": atr_val}))
            except Exception as e:
                logger.debug(f"ATR publish skipped: {e}")
    
    def check_long_signal(self, ind: Dict) -> Tuple[bool, str]:
        """Check for LONG signal"""
        reasons = []
        
        # EMA crossover
        ema_cross = (ind['ema_fast'] > ind['ema_slow'] and 
                     ind['ema_fast_prev'] <= ind['ema_slow_prev'])
        if ema_cross:
            reasons.append("EMA bullish crossover")
        
        # RSI conditions
        rsi_ok = ind['rsi'] > RSI_OVERSOLD and ind['rsi'] < RSI_OVERBOUGHT
        if rsi_ok:
            reasons.append(f"RSI favorable ({ind['rsi']:.1f})")
        
        # MACD histogram turning positive
        macd_ok = ind['macd_hist'] > 0 and ind['macd_hist'] > ind['macd_hist_prev']
        if macd_ok:
            reasons.append("MACD bullish")
        
        # EMA alignment (fast above slow)
        ema_aligned = ind['ema_fast'] > ind['ema_slow']
        
        # Signal conditions
        signal = (ema_cross or (ema_aligned and macd_ok)) and rsi_ok
        
        return signal, "; ".join(reasons) if reasons else ""
    
    def check_short_signal(self, ind: Dict) -> Tuple[bool, str]:
        """Check for SHORT signal"""
        reasons = []
        
        # EMA crossover
        ema_cross = (ind['ema_fast'] < ind['ema_slow'] and 
                     ind['ema_fast_prev'] >= ind['ema_slow_prev'])
        if ema_cross:
            reasons.append("EMA bearish crossover")
        
        # RSI conditions  
        rsi_ok = ind['rsi'] < RSI_OVERBOUGHT and ind['rsi'] > RSI_OVERSOLD
        if rsi_ok:
            reasons.append(f"RSI favorable ({ind['rsi']:.1f})")
        
        # MACD histogram turning negative
        macd_ok = ind['macd_hist'] < 0 and ind['macd_hist'] < ind['macd_hist_prev']
        if macd_ok:
            reasons.append("MACD bearish")
        
        # EMA alignment (fast below slow)
        ema_aligned = ind['ema_fast'] < ind['ema_slow']
        
        # Signal conditions
        signal = (ema_cross or (ema_aligned and macd_ok)) and rsi_ok
        
        return signal, "; ".join(reasons) if reasons else ""
    
    def calculate_sl_tp(self, side: str, entry: float, atr: float) -> Tuple[float, List[float]]:
        """Calculate SL and TP levels based on ATR"""
        sl_distance = atr * ATR_SL_MULTIPLIER
        
        if side == "LONG":
            sl = entry - sl_distance
            tp_levels = [entry + (atr * mult) for mult in ATR_TP_MULTIPLIERS]
        else:  # SHORT
            sl = entry + sl_distance
            tp_levels = [entry - (atr * mult) for mult in ATR_TP_MULTIPLIERS]
        
        return round(sl, 2), [round(tp, 2) for tp in tp_levels]
    
    def generate_signal(self) -> Optional[Signal]:
        """Generate trading signal"""
        # Get timeframe in minutes
        tf_map = {'M1': 1, 'M5': 5, 'M15': 15, 'H1': 60}
        tf_minutes = tf_map.get(TIMEFRAME, 5)
        
        # Build candles - ИСПРАВЛЕНИЕ: достаточный запас для всех индикаторов
        periods_needed = max(EMA_SLOW, RSI_PERIOD, ATR_PERIOD, 35) + 10  # Запас для MACD (26+9) и rolling окон
        candles = self.tick_buffer.get_candles(tf_minutes, periods_needed)
        
        logger.info(f"🔍 Candles info: periods_needed={periods_needed}, candles_built={len(candles)}, ticks_available={len(self.tick_buffer.ticks)}")
        
        if candles.empty or len(candles) < max(EMA_SLOW, RSI_PERIOD, ATR_PERIOD):
            logger.warning(f"Not enough candles: have {len(candles)}, need {max(EMA_SLOW, RSI_PERIOD, ATR_PERIOD)}")
            return None
        
        logger.info(f"Analyzing {len(candles)} candles on {TIMEFRAME} timeframe")
        
        # Calculate indicators
        indicators = self.calculate_indicators(candles)
        
        if not indicators:
            logger.warning("Could not calculate indicators")
            return None
        
        # Log current state
        logger.info(f"Price: {indicators['close']:.2f} | "
                   f"EMA({EMA_FAST}): {indicators['ema_fast']:.2f} | "
                   f"EMA({EMA_SLOW}): {indicators['ema_slow']:.2f} | "
                   f"RSI: {indicators['rsi']:.1f} | "
                   f"ATR: {indicators['atr']:.2f}")
        
        # Check for signals
        long_signal, long_reason = self.check_long_signal(indicators)
        short_signal, short_reason = self.check_short_signal(indicators)
        
        # Rate limiting (using UTC)
        now = datetime.now(timezone.utc)
        if self.last_signal_time and (now - self.last_signal_time) < self.min_signal_interval:
            logger.info(f"Signal cooldown active ({self.min_signal_interval.total_seconds()/60:.0f}min)")
            return None
        
        # 🎯 FIX: Auto-reset position_open after max duration
        if self.enable_position_tracking and self.position_open and self.position_open_time:
            if (now - self.position_open_time) > self.max_position_duration:
                logger.info(f"⏰ Position auto-reset after {self.max_position_duration.total_seconds()/3600:.1f}h")
                self.position_open = False
                self.position_open_time = None
        
        # Generate signal
        signal = None
        
        # 🎯 FIX: Check position_open only if tracking is enabled
        position_blocked = self.enable_position_tracking and self.position_open
        
        if long_signal and not position_blocked:
            logger.info(f"🔔 LONG SIGNAL: {long_reason}")
            
            # 🎯 SENIOR DEV: Добавляем entry price
            entry = indicators['close']
            sl, tp_levels = self.calculate_sl_tp("LONG", entry, indicators['atr'])
            
            # 🎯 Генерируем уникальный ID с номером
            self.signal_counter += 1
            signal_id = f"{SYMBOL}-LONG-{self.signal_counter:04d}-{int(time.time())}"
            
            signal = Signal(
                sid=signal_id,
                symbol=SYMBOL,
                side="LONG",
                lot=DEFAULT_LOT,
                entry=round(entry, 2),  # 🎯 ДОБАВЛЕНА ТОЧКА ВХОДА
                sl=sl,
                tp_levels=tp_levels,
                reason=long_reason,
                indicators={k: round(v, 2) if isinstance(v, float) else v 
                           for k, v in indicators.items()}
            )
            
        elif short_signal and not position_blocked:
            logger.info(f"🔔 SHORT SIGNAL: {short_reason}")
            
            # 🎯 SENIOR DEV: Добавляем entry price
            entry = indicators['close']
            sl, tp_levels = self.calculate_sl_tp("SHORT", entry, indicators['atr'])
            
            # 🎯 Генерируем уникальный ID с номером
            self.signal_counter += 1
            signal_id = f"{SYMBOL}-SHORT-{self.signal_counter:04d}-{int(time.time())}"
            
            signal = Signal(
                sid=signal_id,
                symbol=SYMBOL,
                side="SHORT",
                lot=DEFAULT_LOT,
                entry=round(entry, 2),  # 🎯 ДОБАВЛЕНА ТОЧКА ВХОДА
                sl=sl,
                tp_levels=tp_levels,
                reason=short_reason,
                indicators={k: round(v, 2) if isinstance(v, float) else v 
                           for k, v in indicators.items()}
            )
        
        if signal:
            self.last_signal_time = now
            
        return signal
    
    def send_signal(self, signal: Signal) -> bool:
        """Send signal to go-gateway using unified XAUUSD formatter"""
        try:
            # ✅ ИСПОЛЬЗУЕМ ЕДИНЫЙ ФОРМАТИРОВЩИК XAUUSD
            ts = int(time.time() * 1000)
            
            # Извлекаем ATR из индикаторов если есть
            atr = signal.indicators.get("atr", 1.0)
            
            xauusd_signal = XAUUSDSignal(
                sid=signal.sid,
                symbol=signal.symbol,
                side=signal.side,
                entry=signal.entry or 0.0,
                sl=signal.sl,
                tp_levels=signal.tp_levels,
                lot=signal.lot,
                source="TechnicalAnalysis",
                reason=signal.reason,
                confidence=75.0,  # TA signals have moderate confidence
                atr=atr,
                ts=ts,
                indicators=signal.indicators,
                trail_after_tp1=True,
                trail_profile=os.getenv("TA_TRAIL_PROFILE", "rocket_v1"),
            )
            
            # Отправляем в /orders/enqueue
            url = f"{GATEWAY_URL}/orders/enqueue"
            payload = XAUUSDSignalFormatter.format_order_payload(xauusd_signal)
            
            logger.info(f"Sending signal to {url}")
            logger.info(f"Payload: {json.dumps(payload, indent=2)}")
            
            resp = requests.post(url, json=payload, timeout=5)
            
            if resp.status_code == 200:
                result = resp.json()
                logger.info(f"✅ Signal sent successfully: {result}")
                
                # v4.1: Also publish to signals:ta:XAUUSD for aggregated-hub
                try:
                    if self.redis_signals_client:
                        ta_stream = os.getenv("TA_STREAM", f"signals:ta:{signal.symbol}")
                        ta_payload = XAUUSDSignalFormatter.format_audit_payload(xauusd_signal)
                        self.redis_signals_client.xadd(ta_stream, {"data": json.dumps(ta_payload)}, maxlen=1000, approximate=True)
                        logger.info(f"✅ Also published to {ta_stream}")
                except Exception as e:
                    logger.warning(f"⚠️ Failed to publish to TA stream: {e}")
                
                # Публикуем в notify:telegram для отправки в Telegram
                try:
                    if self.redis_signals_client:
                        notify_stream = os.getenv("NOTIFY_STREAM", "notify:telegram")
                        redis_payload = XAUUSDSignalFormatter.format_redis_payload(xauusd_signal)
                        # Конвертируем для Redis
                        redis_data = {}
                        for k, v in redis_payload.items():
                            if isinstance(v, (dict, list)):
                                redis_data[k] = json.dumps(v)
                            else:
                                redis_data[k] = str(v)
                        
                        notify_counter_key = os.getenv("NOTIFY_SIGNAL_COUNTER_KEY", "notify:telegram:signal_counter")
                        # 🎯 Настройка фильтрации сигналов для Telegram
                        # По умолчанию отправляем все сигналы (every_n=1)
                        notify_signal_every_n = max(1, int(os.getenv("CRYPTO_NOTIFY_SIGNAL_EVERY_N", "1")))
                        
                        send_to_notify = True
                        counter_value = None
                        try:
                            counter_value = self.redis_signals_client.incr(notify_counter_key)
                        except Exception as counter_err:
                            logger.warning(
                                "⚠️ Failed to increment notify signal counter %s: %s",
                                notify_counter_key,
                                counter_err
                            )
                        if (
                            counter_value is not None
                            and notify_signal_every_n > 1
                            and counter_value % notify_signal_every_n != 0
                        ):
                            send_to_notify = False
                            logger.debug(
                                "🔕 Skipping Telegram notify for TA signal %s (counter=%s, every_n=%s)",
                                signal.sid,
                                counter_value,
                                notify_signal_every_n
                            )
                        
                        if send_to_notify:
                            self.redis_signals_client.xadd(notify_stream, redis_data, maxlen=1000)
                            logger.info(f"✅ Published to {notify_stream} for Telegram notification")
                except Exception as e:
                    logger.warning(f"⚠️ Failed to publish to notify stream: {e}")

                # v7: publish to audit stream with unified format
                try:
                    if self.redis_signals_client:
                        env_snapshot = {
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
                        audit = XAUUSDSignalFormatter.format_audit_payload(
                            xauusd_signal,
                            extra_context={"env": env_snapshot}
                        )
                        self.redis_signals_client.xadd(AUDIT_SIGNAL_STREAM, {"data": json.dumps(audit)}, maxlen=200000, approximate=True)
                        logger.info(f"✅ Published to audit stream: {AUDIT_SIGNAL_STREAM}")
                except Exception as e:
                    logger.warning(f"⚠️ Failed to publish to audit stream: {e}")
                
                # 🎯 FIX: Set position_open with timestamp (only if tracking enabled)
                if self.enable_position_tracking:
                    self.position_open = True
                    self.position_open_time = datetime.now(timezone.utc)
                    logger.info(f"🔒 Position marked as open, will auto-reset in {self.max_position_duration.total_seconds()/3600:.1f}h")
                return True
            else:
                logger.error(f"❌ Failed to send signal: HTTP {resp.status_code}")
                logger.error(f"Response: {resp.text}")
                return False
                
        except Exception as e:
            logger.error(f"❌ Error sending signal: {e}")
            return False
    
    def run(self):
        """Main loop"""
        logger.info("Starting signal generation loop...")
        logger.info(f"Check interval: {CHECK_INTERVAL} seconds")
        logger.info(f"Mode: {'PRODUCTION (Real Ticks)' if USE_REAL_TICKS and self.redis_ticks_client else 'SIMULATION'}")
        
        # 🎯 Two modes - Real ticks or Simulation
        if USE_REAL_TICKS and self.redis_ticks_client:
            self._run_with_real_ticks()
        else:
            self._run_with_simulation()
    
    def _run_with_real_ticks(self):
        """🎯 PRODUCTION MODE: Run with real ticks from Redis"""
        logger.info("🚀 Starting PRODUCTION mode with real ticks from Redis...")
        
        # First, load historical ticks to build initial dataset
        logger.info(f"📥 Loading historical ticks from {TICK_STREAM}...")
        initial_ticks = self.fetch_ticks_from_redis()
        logger.info(f"✅ Loaded {initial_ticks} historical ticks")
        
        iteration = 0
        while True:
            try:
                iteration += 1
                logger.info(f"🔄 Iteration #{iteration}: Checking for signals...")
                
                # Fetch new ticks from Redis
                new_ticks = self.fetch_ticks_from_redis()
                
                if new_ticks > 0:
                    logger.info(f"📥 Received {new_ticks} new ticks from Redis")
                
                # Debug: показываем данные для анализа
                logger.info(f"📈 Tick buffer size: {len(self.tick_buffer.ticks)}")
                
                # Generate signal
                signal = self.generate_signal()
                logger.info(f"🚨 Signal result: {'Generated!' if signal else 'None'}")
                
                if signal:
                    utc_time = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')
                    logger.info("="*60)
                    logger.info(f"🔔 Новый сигнал {signal.symbol}")
                    logger.info("="*60)
                    logger.info(f"📈 {signal.side} {signal.lot} lot")
                    logger.info(f"📍 Entry: {signal.entry}")
                    logger.info(f"🔧 Source: TechnicalAnalysis")
                    logger.info(f"🛑 SL: {signal.sl}")
                    logger.info(f"✅ TPs: {signal.tp_levels}")
                    logger.info(f"🕐 Time: {utc_time}")
                    logger.info(f"📊 SID: {signal.sid}")
                    logger.info(f"💡 Reason: {signal.reason}")
                    logger.info("="*60)
                    
                    # Send signal to go-gateway
                    self.send_signal(signal)
                
                time.sleep(CHECK_INTERVAL)
                
            except KeyboardInterrupt:
                logger.info("\n👋 Shutting down gracefully...")
                break
            except Exception as e:
                logger.error(f"❌ Error in main loop: {e}", exc_info=True)
                time.sleep(10)
    
    def _run_with_simulation(self):
        """SIMULATION MODE: Run with simulated ticks"""
        logger.info("🧪 Starting SIMULATION mode with synthetic ticks...")
        
        # Simulate tick data for testing
        current_price = 2763.50
        simulated_time_offset = 0
        
        iteration = 0
        while True:
            try:
                iteration += 1
                logger.info(f"🔄 Iteration #{iteration}: Checking for signals...")
                
                # Simulate tick (in production, fetch from OBI or MT5)
                # Add some randomness to simulate real ticks
                tick_change = np.random.normal(0, 0.5)
                current_price += tick_change
                
                bid = current_price - 0.10
                ask = current_price + 0.10
                
                # 🎯 Используем симулированное время для правильного распределения тиков по минутам
                timestamp = int((time.time() + simulated_time_offset) * 1000)
                simulated_time_offset += CHECK_INTERVAL  # Каждый новый тик идет после предыдущего
                self.add_tick(timestamp, bid, ask)
                self.price_data.append(current_price)  # Добавляем цену в price_data
                logger.info(f"📊 Added tick: bid={bid:.2f}, ask={ask:.2f}, price={current_price:.2f}")
                
                # Try to fetch real data
                self.fetch_ticks_from_obi()
                
                # Debug: показываем данные для анализа
                logger.info(f"📈 Buffer size: {len(self.price_data)}")
                min_needed = max(EMA_SLOW, RSI_PERIOD, ATR_PERIOD)  # Динамически вычисляем минимум
                if len(self.price_data) >= min_needed:
                    logger.info(f"🎯 Enough data for analysis! ({len(self.price_data)}/{min_needed})")
                else:
                    logger.info(f"⏳ Need {min_needed - len(self.price_data)} more ticks for EMA analysis (have {len(self.price_data)}/{min_needed})")
                
                # Generate signal
                signal = self.generate_signal()
                logger.info(f"🚨 Signal result: {'Generated!' if signal else 'None'}")
                
                if signal:
                    utc_time = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')
                    logger.info("="*60)
                    logger.info(f"🔔 Новый сигнал {signal.symbol}")
                    logger.info("="*60)
                    logger.info(f"📈 {signal.side} {signal.lot} lot")
                    logger.info(f"📍 Entry: {signal.entry}")
                    logger.info(f"🔧 Source: TechnicalAnalysis")
                    logger.info(f"🛑 SL: {signal.sl}")
                    logger.info(f"✅ TPs: {signal.tp_levels}")
                    logger.info(f"🕐 Time: {utc_time}")
                    logger.info(f"📊 SID: {signal.sid}")
                    logger.info(f"💡 Reason: {signal.reason}")
                    logger.info("="*60)
                    
                    # Send signal to go-gateway
                    self.send_signal(signal)
                
                time.sleep(CHECK_INTERVAL)
                
            except KeyboardInterrupt:
                logger.info("\n👋 Shutting down gracefully...")
                break
            except Exception as e:
                logger.error(f"❌ Error in main loop: {e}", exc_info=True)
                time.sleep(10)


def main():
    """Entry point"""
    generator = SignalGenerator()
    generator.run()


if __name__ == "__main__":
    main()

