from utils.time_utils import get_ny_time_millis

"""
from core.confidence_utils import normalize_confidence_pct, confidence_pct_to_ratio
Unified Signal Generator - Универсальный генератор сигналов для любого символа

Интегрируется в BaseOrderFlowHandler для генерации сигналов на основе:
- OrderFlow анализа (Delta, OBI, Weak Progress, Iceberg)
- Technical Analysis (EMA, RSI, ATR, MACD)
- Price Action (Breakouts, Pivots)

Публикует сигналы в: signals:audit:{SYMBOL}
"""

import json
import logging
import time
from collections import deque
from dataclasses import asdict, dataclass

import numpy as np

from core.instrument_config import OrderFlowConfig, SymbolSpecs
from core.redis_keys import RedisStreams as RS
from core.retention import MAXLEN_GLOBAL, MAXLEN_PER_SYMBOL


@dataclass
class UnifiedSignal:
    """Универсальный формат сигнала для любого символа"""
    sid: str                    # Signal ID
    symbol: str                 #  BTCUSD, ETHUSD, etc
    side: str                   # LONG or SHORT
    lot: float                  # Position size
    entry: float | None      # Entry price
    sl: float                   # Stop Loss
    tp_levels: list[float]      # Take Profit levels
    reason: str                 # Signal reason
    confidence: float           # 0.0 - 1.0
    indicators: dict            # Technical indicators state
    orderflow: dict             # OrderFlow metrics
    timestamp: int              # Unix timestamp (ms)

    # Metadata
    timeframe: str = "M5"
    source: str = "unified_orderflow"
    version: str = "2.0"


class UnifiedSignalGenerator:
    """
    Универсальный генератор сигналов для любого торгового инструмента.
    
    Используется внутри BaseOrderFlowHandler для генерации сигналов
    на основе OrderFlow + Technical Analysis.
    """

    def __init__(
        self,
        symbol: str,
        symbol_specs: SymbolSpecs,
        config: OrderFlowConfig,
        redis_client,
        logger: logging.Logger | None = None
    ):
        """
        Args:
            symbol: Торговый символ ( BTCUSD, etc)
            symbol_specs: Спецификации инструмента
            config: Конфигурация OrderFlow
            redis_client: Redis client для публикации
            logger: Logger instance
        """
        self.symbol = symbol
        self.specs = symbol_specs
        self.config = config
        self.redis = redis_client
        self.logger = logger or logging.getLogger(__name__)

        # Streams для публикации
        self.audit_stream = RS.SIGNAL_AUDIT_TPL.format(symbol=symbol)
        self.unified_stream = RS.SIGNALS_UNIFIED

        # Технические индикаторы
        self.price_buffer = deque(maxlen=100)  # Последние 100 цен
        self.ema_fast = deque(maxlen=20)
        self.ema_slow = deque(maxlen=50)
        self.rsi_values = deque(maxlen=14)

        # Статистика
        self.signals_generated = 0
        self.last_signal_time = 0

        self.logger.info(f"✅ UnifiedSignalGenerator initialized for {symbol}")

    def should_generate_signal(
        self,
        current_price: float,
        delta: float,
        obi: float,
        weak_progress: bool,
        iceberg_detected: bool,
        atr: float,
        pivots: dict
    ) -> UnifiedSignal | None:
        """
        Определяет, нужно ли генерировать сигнал на основе всех метрик.
        
        Args:
            current_price: Текущая цена
            delta: Delta (Buy Volume - Sell Volume)
            obi: Order Book Imbalance
            weak_progress: Weak Progress detected
            iceberg_detected: Iceberg order detected
            atr: Average True Range
            pivots: Pivot Points (R1, R2, R3, S1, S2, S3, PP)
        
        Returns:
            UnifiedSignal если сигнал сгенерирован, None иначе
        """
        # Обновляем price buffer
        self.price_buffer.append(current_price)

        if len(self.price_buffer) < 20:
            return None  # Недостаточно данных

        # Вычисляем технические индикаторы
        indicators = self._calculate_indicators(current_price, atr)

        # OrderFlow метрики
        orderflow = {
            'delta': delta,
            'obi': obi,
            'weak_progress': weak_progress,
            'iceberg_detected': iceberg_detected,
            'atr': atr,
        }

        # Логика генерации сигнала
        signal_type, confidence, reason = self._evaluate_signal(
            indicators, orderflow, pivots
        )

        if signal_type is None:
            return None

        # Минимальная пауза между сигналами (5 минут)
        current_time = get_ny_time_millis()
        if current_time - self.last_signal_time < 300000:
            return None

        # Генерируем сигнал
        signal = self._create_signal(
            signal_type,
            current_price,
            atr,
            confidence,
            reason,
            indicators,
            orderflow
        )

        self.last_signal_time = current_time
        self.signals_generated += 1

        return signal

    def _calculate_indicators(self, price: float, atr: float) -> dict:
        """Вычисляет технические индикаторы"""
        prices = list(self.price_buffer)

        # EMA
        ema_fast = self._calculate_ema(prices, period=9)
        ema_slow = self._calculate_ema(prices, period=21)

        # RSI
        rsi = self._calculate_rsi(prices, period=14)

        # MACD
        macd_line, signal_line, histogram = self._calculate_macd(prices)

        return {  # type: ignore
            'ema_fast': ema_fast,
            'ema_slow': ema_slow,
            'rsi': rsi,
            'macd': macd_line,
            'macd_signal': signal_line,
            'macd_histogram': histogram,
            'atr': atr,
            'atr_pct': (atr / price * 100) if price > 0 else 0
        },

    def _evaluate_signal(
        self,
        indicators: dict,
        orderflow: dict,
        pivots: dict
    ) -> tuple[str | None, float, str]:
        """
        Оценивает условия для генерации сигнала.
        
        Returns:
            (signal_type, confidence, reason)
            signal_type: "LONG", "SHORT", или None
            confidence: 0.0 - 1.0
            reason: Описание причины
        """
        confidence = 0.0
        reasons = []

        # === BULLISH условия ===
        bullish_score = 0

        # 1. EMA Crossover
        if indicators['ema_fast'] > indicators['ema_slow']:
            bullish_score += 20
            reasons.append("EMA_BULL_CROSS")

        # 2. RSI Oversold
        if indicators['rsi'] < 30:
            bullish_score += 15
            reasons.append("RSI_OVERSOLD")

        # 3. MACD Histogram positive
        if indicators['macd_histogram'] > 0:
            bullish_score += 10
            reasons.append("MACD_BULL")

        # 4. Delta positive (buying pressure)
        if orderflow['delta'] > self.config.delta_threshold_extreme:  # type: ignore
            bullish_score += 25
            reasons.append("DELTA_EXTREME_BUY")
        elif orderflow['delta'] > self.config.delta_threshold_moderate:  # type: ignore
            bullish_score += 15
            reasons.append("DELTA_MODERATE_BUY")

        # 5. OBI positive (order book imbalance to buy side)
        if orderflow['obi'] > 0.3:
            bullish_score += 20
            reasons.append("OBI_BUY_SIDE")

        # 6. Iceberg (hidden large buy orders)
        if orderflow['iceberg_detected']:
            bullish_score += 15
            reasons.append("ICEBERG_BUY")

        # 7. Weak Progress (absorption on sell side)
        if orderflow['weak_progress']:
            bullish_score += 10
            reasons.append("WEAK_PROGRESS_SELL")

        # === BEARISH условия ===
        bearish_score = 0

        # 1. EMA Crossover
        if indicators['ema_fast'] < indicators['ema_slow']:
            bearish_score += 20
            reasons.append("EMA_BEAR_CROSS")

        # 2. RSI Overbought
        if indicators['rsi'] > 70:
            bearish_score += 15
            reasons.append("RSI_OVERBOUGHT")

        # 3. MACD Histogram negative
        if indicators['macd_histogram'] < 0:
            bearish_score += 10
            reasons.append("MACD_BEAR")

        # 4. Delta negative (selling pressure)
        if orderflow['delta'] < -self.config.delta_threshold_extreme:  # type: ignore
            bearish_score += 25
            reasons.append("DELTA_EXTREME_SELL")
        elif orderflow['delta'] < -self.config.delta_threshold_moderate:  # type: ignore
            bearish_score += 15
            reasons.append("DELTA_MODERATE_SELL")

        # 5. OBI negative
        if orderflow['obi'] < -0.3:
            bearish_score += 20
            reasons.append("OBI_SELL_SIDE")

        # Определяем направление
        if bullish_score > 60 and bullish_score > bearish_score:
            # bullish_score is percent by convention; normalize handles legacy ratio too.
            bullish_pct = normalize_confidence_pct(bullish_score)  # type: ignore
            confidence = min(confidence_pct_to_ratio(bullish_pct), 0.95)  # type: ignore
            return ("LONG", confidence, " + ".join(reasons[:5]))

        elif bearish_score > 60 and bearish_score > bullish_score:
            bearish_pct = normalize_confidence_pct(bearish_score)  # type: ignore
            confidence = min(confidence_pct_to_ratio(bearish_pct), 0.95)  # type: ignore
            return ("SHORT", confidence, " + ".join(reasons[:5]))

        return (None, 0.0, "")

    def _create_signal(
        self,
        signal_type: str,
        entry_price: float,
        atr: float,
        confidence: float,
        reason: str,
        indicators: dict,
        orderflow: dict
    ) -> UnifiedSignal:
        """Создает объект сигнала с SL/TP"""

        # Вычисляем SL/TP на основе ATR
        sl_distance = atr * 1.5
        tp_distances = [atr * 2.0, atr * 3.0, atr * 4.0]

        if signal_type == "LONG":
            sl = entry_price - sl_distance
            tp_levels = [entry_price + tp for tp in tp_distances]
        else:  # SHORT
            sl = entry_price + sl_distance
            tp_levels = [entry_price - tp for tp in tp_distances]

        # Position sizing на основе риска
        risk_amount = 1000 * (self.config.risk_percent / 100)  # Пример: $1000 account  # type: ignore
        lot = self._calculate_lot_size(risk_amount, sl_distance)

        # Генерируем Signal ID
        sid = f"{self.symbol}_{signal_type}_{int(time.time())}"

        signal = UnifiedSignal(
            sid=sid,
            symbol=self.symbol,
            side=signal_type,
            lot=lot,
            entry=entry_price,
            sl=sl,
            tp_levels=tp_levels,
            reason=reason,
            confidence=confidence,
            indicators=indicators,
            orderflow=orderflow,
            timestamp=get_ny_time_millis()
        )

        return signal

    def publish_signal(self, signal: UnifiedSignal) -> bool:
        """
        Публикует сигнал в Redis streams.
        
        Публикует в два потока:
        1. signals:audit:{SYMBOL} - для aggregation hub
        2. signals:unified - общий поток для всех символов
        """
        try:
            signal_dict = asdict(signal)
            signal_json = json.dumps(signal_dict)

            # Публикуем в audit stream (для aggregation hub)
            self.redis.xadd(
                self.audit_stream,
                {'data': signal_json},
                maxlen=MAXLEN_PER_SYMBOL
            )

            # Публикуем в unified stream (для общего мониторинга)
            self.redis.xadd(
                self.unified_stream,
                {
                    'symbol': signal.symbol,
                    'side': signal.side,
                    'confidence': str(signal.confidence),
                    'data': signal_json
                },
                maxlen=MAXLEN_GLOBAL
            )

            self.logger.info(
                f"📤 Signal published: {signal.symbol} {signal.side} "
                f"confidence={signal.confidence:.2f} reason={signal.reason}"
            )

            return True

        except Exception as e:
            self.logger.error(f"❌ Failed to publish signal: {e}")
            return False

    # ═════════════════════════════════════════════════════════════
    # HELPER METHODS - Technical Indicators
    # ═════════════════════════════════════════════════════════════

    def _get_gpu_service(self):
        """Получить GPU сервис (lazy initialization)"""
        if not hasattr(self, '_gpu_service_cache'):
            try:
                from services.gpu_compute_service import get_gpu_service
                self._gpu_service_cache = get_gpu_service()
            except Exception:
                self._gpu_service_cache = None
        return self._gpu_service_cache

    def _calculate_ema(self, prices: list[float], period: int) -> float:
        """Exponential Moving Average with GPU acceleration"""
        if len(prices) < period:
            return prices[-1] if prices else 0.0

        # ✅ GPU Support: используем GPU если доступен
        gpu_service = self._get_gpu_service()
        if gpu_service and gpu_service.is_gpu_available():
            try:
                prices_arr = np.array(prices, dtype=np.float32)
                ema_values = gpu_service.compute_ema_batch(prices_arr, period)
                return float(ema_values[-1])
            except Exception:
                pass  # Fallback to CPU

        # CPU fallback
        prices_arr = np.array(prices[-period:])
        weights = np.exp(np.linspace(-1., 0., period))
        weights /= weights.sum()

        return np.convolve(prices_arr, weights, mode='valid')[-1]

    def _calculate_rsi(self, prices: list[float], period: int = 14) -> float:
        """Relative Strength Index with GPU acceleration"""
        if len(prices) < period + 1:
            return 50.0

        # ✅ GPU Support: используем GPU если доступен
        gpu_service = self._get_gpu_service()
        if gpu_service and gpu_service.is_gpu_available():
            try:
                prices_arr = np.array(prices, dtype=np.float32)
                rsi_values = gpu_service.compute_rsi_batch(prices_arr, period)
                return float(rsi_values[-1])
            except Exception:
                pass  # Fallback to CPU

        # CPU fallback
        deltas = np.diff(prices[-period-1:])
        gains = np.where(deltas > 0, deltas, 0)
        losses = np.where(deltas < 0, -deltas, 0)

        avg_gain = gains.mean()
        avg_loss = losses.mean()

        if avg_loss == 0:
            return 100.0

        rs = avg_gain / avg_loss
        rsi = 100 - (100 / (1 + rs))

        return rsi

    def _calculate_macd(
        self,
        prices: list[float],
        fast=12,
        slow=26,
        signal=9
    ) -> tuple[float, float, float]:
        """MACD (Moving Average Convergence Divergence) with GPU acceleration"""
        if len(prices) < slow:
            return (0.0, 0.0, 0.0)

        # ✅ GPU Support: используем GPU если доступен
        gpu_service = self._get_gpu_service()
        if gpu_service and gpu_service.is_gpu_available():
            try:
                prices_arr = np.array(prices, dtype=np.float32)
                macd_line_arr, signal_line_arr, histogram_arr = gpu_service.compute_macd_batch(
                    prices_arr, fast, slow, signal
                )
                return (
                    float(macd_line_arr[-1]),
                    float(signal_line_arr[-1]),
                    float(histogram_arr[-1])
                )
            except Exception:
                pass  # Fallback to CPU

        # CPU fallback
        ema_fast = self._calculate_ema(prices, fast)
        ema_slow = self._calculate_ema(prices, slow)

        macd_line = ema_fast - ema_slow

        # Signal line (EMA of MACD)
        # Используем правильный расчет через EMA от MACD значений
        macd_prices = [macd_line]  # Упрощенная версия для CPU fallback
        signal_line = self._calculate_ema(macd_prices, signal) if len(macd_prices) >= signal else macd_line * 0.9

        histogram = macd_line - signal_line

        return (macd_line, signal_line, histogram)

    def _calculate_lot_size(self, risk_amount: float, sl_distance: float) -> float:
        """Вычисляет размер лота на основе риска"""
        if sl_distance <= 0:
            return self.specs.min_lot

        # Формула: lot = risk_amount / (sl_distance * contract_size)
        # Для криптовалют contract_size обычно = 1
        # Для  contract_size = 100 (oz)

        contract_size = 100 if self.symbol == "" else 1
        lot = risk_amount / (sl_distance * contract_size)

        # Округляем до step
        lot = round(lot / self.specs.lot_step) * self.specs.lot_step

        # Применяем limits
        lot = max(self.specs.min_lot, min(lot, self.specs.max_lot))

        return lot

