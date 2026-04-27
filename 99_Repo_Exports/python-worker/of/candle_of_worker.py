"""
Candle → OF features (delta/z/ratio/cvd/bodyATR/absorbed) → Redis:

ФУНКЦИОНАЛ:
- Вход: закрытые свечи из Redis Stream (stream:kline_1m)
- Обработка: вычисление Delta, CVD, z-score, deltaRatio, bodyATR, absorbed
- Выход: публикация в stream:of-bar (каждый бар) и stream:of-spike (спайки)

ОСНОВА:
- Delta (Δ): если есть takerBuyVolume -> Δ = (2*takerBuy - volume), buyVol=takerBuy, sellVol=volume-takerBuy
- Если takerBuyVolume пусто: используем прокси-сигнал (Up/Down Volume)
- CVD: кумулятивная сумма Δ в рамках TF/символа
- zDelta: z-score Δ по online-статистике (Welford) на окне windowBars
- deltaRatio: Δ/Volume
- bodyATR: |close-open| / ATR
- absorbed: эвристика длинной противоположной тени при маленьком теле

ATR:
- Берём из Redis-кэша key=f"atr:{symbol}:{tf}" (число)
- Если кэша нет, считаем локально Wilder(14) на лету
"""

from __future__ import annotations
import os
import json
import math
import time
import threading
import sys
from collections import deque
from typing import Dict, Any, Optional, List

from core.redis_client import get_redis
from core.dual_redis_client import get_dual_signals_redis
from core.config import (
    OF_WINDOW_BARS,
    OF_Z_THRESHOLD,
    OF_RATIO_THRESHOLD,
    OF_MIN_BODY_ATR,
    OF_MIN_VOLUME_Q,
    OF_Z_THRESHOLD_PROXY,
    OF_RATIO_THRESHOLD_PROXY,
    OF_MIN_VOLUME_Q_PROXY,
    OF_STREAM_BAR,
    OF_STREAM_SPIKE,
    SUBSCRIBE_STREAM,
    OF_CONSUMER_GROUP,
    OF_READ_COUNT,
    OF_READ_BLOCK_MS,
    STREAM_MAX_LENGTH
)

# ✅ GPU Support: импорт GPU сервиса для ускорения вычислений
try:
    from services.gpu_compute_service import get_gpu_service
    GPU_SERVICE_AVAILABLE = True
except ImportError:
    GPU_SERVICE_AVAILABLE = False
    get_gpu_service = None

# Добавляем путь к common для импорта time_utils
common_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'common')
if common_path not in sys.path:
    sys.path.append(common_path)

from common.time_utils import extract_binance_close_time, format_timestamp_for_redis, get_current_timestamp_ms


# ---------------------- Online статистика (Welford) ----------------------
class OnlineStats:
    """Welford для подсчёта online mean/variance + z-score."""
    
    def __init__(self, maxlen: int = 300):
        self.maxlen = maxlen
        self.buf = deque(maxlen=maxlen)
        self.n = 0
        self.mean = 0.0
        self.m2 = 0.0

    def update(self, x: float) -> None:
        """Обновляет статистику новым значением."""
        self.buf.append(x)
        self.n += 1
        d = x - self.mean
        self.mean += d / self.n
        self.m2 += d * (x - self.mean)

    def z(self, x: float) -> float:
        """Вычисляет z-score для значения x."""
        if self.n < 20:
            return 0.0
        var = self.m2 / max(self.n - 1, 1)
        sd = math.sqrt(var if var > 0 else 1e-12)
        return (x - self.mean) / (sd or 1e-9)


# ---------------------- Примитивный ATR (Wilder, fallback) ----------------------
class WilderATR:
    """
    Простейший Wilder ATR(14) по свече (close, high, low):
    - TR = max(h-l, |h-prevC|, |l-prevC|)
    - Инициализация: среднее TR за 'period' свечей
    - Обновление: ATR_t = (ATR_{t-1}*(period-1) + TR_t)/period
    """
    
    def __init__(self, period: int = 14, warmup: int = 14):
        self.period = period
        self.warmup = warmup
        self.prev_close: Optional[float] = None
        self.atr: Optional[float] = None
        self.warm_sum = 0.0
        self.warm_cnt = 0

    def tr(self, high: float, low: float, prev_close: Optional[float]) -> float:
        """Вычисляет True Range."""
        a = high - low
        if prev_close is None:
            return a
        b = abs(high - prev_close)
        c = abs(low - prev_close)
        return max(a, b, c)

    def update(self, high: float, low: float, close: float) -> float:
        """Обновляет ATR новой свечой и возвращает текущее значение."""
        tr_val = self.tr(high, low, self.prev_close)
        self.prev_close = close
        
        if self.atr is None:
            # warmup фаза
            self.warm_sum += tr_val
            self.warm_cnt += 1
            if self.warm_cnt >= self.warmup:
                self.atr = self.warm_sum / max(self.warm_cnt, 1)
            return self.atr or 0.0
        
        # Wilder smoothing
        self.atr = (self.atr * (self.period - 1) + tr_val) / self.period
        return self.atr


# ---------------------- Детектор Δ-спайков на свечах ----------------------
class CandleDeltaDetector:
    """Детектор Delta спайков на основе Order Flow анализа."""
    
    def __init__(self, window: int = 300):
        self.stats = OnlineStats(window)
        self.vols = deque(maxlen=window)
        self.cvd = 0.0  # cumulative volume delta на TF
        self.use_proxy_mode = False

    def _vol_quantile(self, v: float) -> float:
        """Вычисляет квантиль объёма относительно истории."""
        if not self.vols:
            return 100.0
        less = sum(1 for x in self.vols if x <= v)
        return 100.0 * less / len(self.vols)

    def on_closed_candle(
        self,
        open_: float,
        high: float,
        low: float,
        close: float,
        volume: float,
        atr: float,
        taker_buy_vol: Optional[float]
    ) -> Dict[str, Any]:
        """
        Обрабатывает закрытую свечу и возвращает OF метрики.
        
        Returns:
            Dict с buyVol, sellVol, delta, cvd, deltaRatio, zDelta, bodyATR, absorbed, isSpike, dir
        """
        # --- buy/sell/Δ ---
        if taker_buy_vol is not None:
            # Нормальный режим Binance: есть takerBuyBaseAssetVolume
            buy_vol = float(max(taker_buy_vol, 0.0))
            sell_vol = float(max(volume - buy_vol, 0.0))
            delta = buy_vol - sell_vol
            self.use_proxy_mode = False
        else:
            # Прокси-режим (нет takerBuy): Up/Down Volume
            sign = 1.0 if close >= open_ else -1.0
            buy_vol = volume if sign > 0 else 0.0
            sell_vol = volume if sign < 0 else 0.0
            delta = sign * volume
            self.use_proxy_mode = True

        # Online статистика
        self.stats.update(delta)
        self.vols.append(volume)
        self.cvd += delta

        z = self.stats.z(delta)
        ratio = 0.0 if volume <= 0 else delta / volume
        body_atr = 0.0 if atr <= 0 else abs(close - open_) / atr
        vol_q = self._vol_quantile(volume)
        vol_ok = vol_q >= (OF_MIN_VOLUME_Q_PROXY if self.use_proxy_mode else OF_MIN_VOLUME_Q)

        # Эвристика абсорбции (по теням/телу)
        wick_up = high - max(open_, close)
        wick_dn = min(open_, close) - low
        absorbed_long = (close > open_) and (body_atr < 0.1) and (wick_up > 2 * max(1e-9, close - open_))
        absorbed_short = (close < open_) and (body_atr < 0.1) and (wick_dn > 2 * max(1e-9, open_ - close))
        absorbed = absorbed_long or absorbed_short

        # Пороговые значения: строже в proxy режиме
        z_thr = OF_Z_THRESHOLD_PROXY if self.use_proxy_mode else OF_Z_THRESHOLD
        r_thr = OF_RATIO_THRESHOLD_PROXY if self.use_proxy_mode else OF_RATIO_THRESHOLD
        body_thr = OF_MIN_BODY_ATR

        spike_long = (z >= z_thr) and (ratio >= r_thr) and (close > open_) and (body_atr >= body_thr) and vol_ok
        spike_short = (z <= -z_thr) and (ratio <= -r_thr) and (close < open_) and (body_atr >= body_thr) and vol_ok
        direction = 'long' if spike_long else ('short' if spike_short else None)

        return {
            "buyVol": buy_vol,
            "sellVol": sell_vol,
            "delta": delta,
            "cvd": self.cvd,
            "deltaRatio": ratio,
            "zDelta": z,
            "bodyATR": body_atr,
            "absorbed": absorbed,
            "isSpike": direction is not None,
            "dir": direction,
            "volumeQ": vol_q
        }


# ---------------------- Главный Worker для Order Flow ----------------------
class CandleOrderFlowWorker:
    """
    Обработчик Order Flow метрик для закрытых свечей через Redis Streams.
    
    Читает закрытые свечи из stream:kline_1m и публикует OF метрики в:
    - stream:of-bar (каждый бар)
    - stream:of-spike (только спайки)
    """
    
    def __init__(self):
        """Инициализация клиентов Redis и внутренних структур."""
        self.redis_client = get_redis()  # Клиент для чтения (порт 6379)
        self.dual_redis = get_dual_signals_redis()  # Клиент для публикации (порты 6380, 6381)
        self.is_running = False
        
        # ✅ GPU Support: инициализация GPU сервиса
        self.gpu_service = None
        if GPU_SERVICE_AVAILABLE:
            try:
                self.gpu_service = get_gpu_service()
                if self.gpu_service.is_gpu_available():
                    device_info = self.gpu_service.get_device_info()
                    if device_info:
                        print(f"🚀 GPU acceleration enabled: {device_info.get('name', 'Unknown GPU')}")
                        sys.stdout.flush()
                else:
                    print("📊 GPU not available, using CPU (NumPy)")
                    sys.stdout.flush()
            except Exception as e:
                print(f"⚠️ GPU service initialization failed: {e}, using CPU")
                sys.stdout.flush()
                self.gpu_service = None
        
        # Детекторы и ATR по (symbol, timeframe)
        self.detectors: Dict[tuple, CandleDeltaDetector] = {}
        self.atrs: Dict[tuple, WilderATR] = {}
        
        # ✅ GPU Batch Processing
        # Global batching is now used in _consume_loop, no per-symbol buffer needed.
        self.batch_size = int(os.getenv('CANDLE_BATCH_SIZE', '10'))
        
        # Статистика
        self.processed_count = 0
        self.spike_count = 0
        self._stats_thread = None
        self._stats_interval_sec = 60
        
    def _get_detector(self, symbol: str, timeframe: str) -> CandleDeltaDetector:
        """Возвращает или создаёт детектор для пары (symbol, timeframe)."""
        key = (symbol, timeframe)
        if key not in self.detectors:
            self.detectors[key] = CandleDeltaDetector(window=OF_WINDOW_BARS)
        return self.detectors[key]
    
    def _get_atr(self, symbol: str, timeframe: str) -> WilderATR:
        """Возвращает или создаёт ATR калькулятор для пары (symbol, timeframe)."""
        key = (symbol, timeframe)
        if key not in self.atrs:
            self.atrs[key] = WilderATR(period=14, warmup=14)
        return self.atrs[key]
    
    def _get_atr_value(self, symbol: str, timeframe: str, open_: float, high: float, low: float, close: float) -> float:
        """
        Получает ATR из Redis кэша или вычисляет локально.
        
        1) Пытаемся взять из Redis: key=f"atr:{symbol}:{timeframe}"
        2) Если нет — считаем локальный Wilder(14)
        """
        try:
            key = f"atr:{symbol}:{timeframe}"
            cached = self.redis_client.get(key)
            if cached:
                val = float(cached)
                if val > 0:
                    return val
        except Exception:
            pass
        
        # Fallback: локальный расчёт
        atr_calc = self._get_atr(symbol, timeframe)
        return atr_calc.update(high, low, close)
    
    def _as_float(self, x: Any) -> float:
        """Безопасное преобразование в float."""
        if x is None:
            return 0.0
        if isinstance(x, (int, float)):
            return float(x)
        try:
            return float(str(x))
        except Exception:
            return 0.0
    
    def _as_int(self, x: Any) -> int:
        """Безопасное преобразование в int."""
        if x is None:
            return 0
        if isinstance(x, int):
            return x
        try:
            return int(str(x))
        except Exception:
            return 0
    
    def _as_bool(self, x: Any) -> bool:
        """Безопасное преобразование в bool."""
        if isinstance(x, bool):
            return x
        s = str(x).lower()
        if s in ("true", "1", "yes"):
            return True
        if s in ("false", "0", "no"):
            return False
        return False
    
    def _process_stream_message(self, message_id: str, fields: dict) -> Optional[Dict[str, Any]]:
        """
        Parses a stream message into a generic candle dict.
        Does NOT process or publish. Returns dict or None.
        """
        try:
            if 'data' not in fields:
                return None
            
            message_data = json.loads(fields['data'])
            kline = message_data.get('k') if isinstance(message_data, dict) and 'k' in message_data else message_data
            
            if not isinstance(kline, dict):
                return None
            
            # Use 'x' (final) from Binance or 'closed' from other sources
            is_closed = self._as_bool(kline.get('x')) or self._as_bool(message_data.get('closed'))
            if not is_closed:
                return None
            
            symbol = str(kline.get('s') or kline.get('symbol') or '')
            timeframe = str(kline.get('i') or kline.get('type') or '1m')
            ts_ms = self._as_int(kline.get('T') or kline.get('closeTime') or kline.get('timestamp'))
            
            o = self._as_float(kline.get('o') or kline.get('open'))
            h = self._as_float(kline.get('h') or kline.get('high'))
            l = self._as_float(kline.get('l') or kline.get('low'))
            c = self._as_float(kline.get('c') or kline.get('close'))
            v = self._as_float(kline.get('v') or kline.get('volume'))
            
            taker_buy_vol_raw = kline.get('V') or kline.get('takerBuyVolume')
            tb = self._as_float(taker_buy_vol_raw) if taker_buy_vol_raw is not None else None
            
            # Get ATR (needed for bodyATR/spike logic eventually, can be fetched batch-wise or here)
            # Fetching here is simpler for now, though batch fetch would be even better.
            atr = self._get_atr_value(symbol, timeframe, o, h, l, c)

            return {
                'symbol': symbol,
                'timeframe': timeframe,
                'ts': ts_ms,
                'open': o,
                'high': h,
                'low': l,
                'close': c,
                'volume': v,
                'takerBuyVolume': tb,
                'atr': atr,
                'message_id': message_id # Keep ID for acking if needed, though we ack in loop
            }
        except Exception:
            return None

    def _process_global_batch(self, batch_candles: List[Dict[str, Any]]) -> None:
        """
        Processes a mixed batch of candles (various symbols) via GPU if available,
        then publishes results.
        """
        if not batch_candles:
            return

        # 1. Try GPU processing
        results = {}
        processed_via_gpu = False

        if self.gpu_service and self.gpu_service.is_gpu_available():
            try:
                # Prepare flat lists for GPU
                gpu_inputs = []
                for c in batch_candles:
                    gpu_inputs.append({
                        'open': c['open'],
                        'high': c['high'],
                        'low': c['low'],
                        'close': c['close'],
                        'volume': c['volume'],
                        'takerBuyVolume': c['takerBuyVolume'],
                        'atr': c['atr']
                    })
                
                # Bulk compute
                gpu_out = self.gpu_service.process_candles_batch(gpu_inputs)
                
                if gpu_out and len(gpu_out.get('deltas', [])) == len(batch_candles):
                    # Unpack GPU results
                    for i, c in enumerate(batch_candles):
                        # Merge GPU metrics into candle dict or a results dict
                        # We use id(c) or index as key? Just iterate.
                        c['gpu_result'] = {
                            "delta": float(gpu_out['deltas'][i]),
                            "buyVol": float(gpu_out['buy_vols'][i]),
                            "sellVol": float(gpu_out['sell_vols'][i]),
                            "cvd": float(gpu_out['cvd'][i]), # Note: this is batch-local CVD accumulation usually
                            "deltaRatio": float(gpu_out['delta_ratio'][i]),
                            "zDelta": float(gpu_out['z_deltas'][i]),
                            "bodyATR": float(gpu_out['body_atr'][i]),
                            "atr": float(gpu_out['atr'][i]),
                        }
                    processed_via_gpu = True
            except Exception as e:
                print(f"⚠️ OrderFlow: GPU global batch error: {e}, falling back to CPU")
                sys.stdout.flush()

        # 2. Finalize and Publish
        for c in batch_candles:
            try:
                symbol = c['symbol']
                timeframe = c['timeframe']
                detector = self._get_detector(symbol, timeframe)

                res = {}
                if processed_via_gpu and 'gpu_result' in c:
                    g = c['gpu_result']
                    
                    # Update State with GPU data
                    # Important: "cvd" from GPU might be local sum. We need global CVD from detector.
                    detector.stats.update(g['delta'])
                    detector.vols.append(c['volume'])
                    detector.cvd += g['delta'] 
                    
                    # Re-calculate zDelta using updated stats? 
                    # The GPU 'z_deltas' is likely batch-local or approximation if state isn't synced.
                    # 'candle_of_worker' GPU code (RobustZ) implies it computes Z based on batch distribution.
                    # CPU code uses `detector.stats.z` (Welford). 
                    # Let's trust Welford for consistency or use GPU's if we trust it. 
                    # Existing code favored detector.stats.z(delta).
                    # Let's stick to detector logic for stateful metrics (CVD, Z) to ensure continuity,
                    # BUT use GPU for stateless heavy ops if any.
                    # Actually, the previous implementation did: `detector.stats.update(delta); z = detector.stats.z(delta)` 
                    # AFTER getting delta from GPU.
                    
                    z_val = detector.stats.z(g['delta'])  # Use Welford Z for consistency across updates
                    
                    res = {
                        "buyVol": g['buyVol'],
                        "sellVol": g['sellVol'],
                        "delta": g['delta'],
                        "cvd": detector.cvd,
                        "deltaRatio": g['deltaRatio'],
                        "zDelta": z_val, 
                        "bodyATR": g['bodyATR'],
                        "atr": g['atr'],
                        "volumeQ": detector._vol_quantile(c['volume']),
                        "type": "of_bar"
                    }
                else:
                    # CPU Fallback
                    res_cpu = detector.on_closed_candle(
                         open_=c['open'], high=c['high'], low=c['low'], close=c['close'],
                         volume=c['volume'], atr=c['atr'], taker_buy_vol=c['takerBuyVolume']
                    )
                    res = {**res_cpu, "type": "of_bar", "atr": c['atr']}

                # Post-processing (absorbed, spike) logic - lightweight
                # Re-use logic from on_closed_candle or extract it?
                # Extracted "Spike/Absorbed" check:
                body_atr = res.get("bodyATR", 0.0)
                vol_q = res.get("volumeQ", 0.0)
                vol_ok = vol_q >= (OF_MIN_VOLUME_Q_PROXY if detector.use_proxy_mode else OF_MIN_VOLUME_Q)
                
                o, h, l, cl = c['open'], c['high'], c['low'], c['close']
                wick_up = h - max(o, cl)
                wick_dn = min(o, cl) - l
                absorbed_long = (cl > o) and (body_atr < 0.1) and (wick_up > 2 * max(1e-9, cl - o))
                absorbed_short = (cl < o) and (body_atr < 0.1) and (wick_dn > 2 * max(1e-9, o - cl))
                absorbed = absorbed_long or absorbed_short
                res["absorbed"] = absorbed

                z = res["zDelta"]
                ratio = res["deltaRatio"]
                z_thr = OF_Z_THRESHOLD_PROXY if detector.use_proxy_mode else OF_Z_THRESHOLD
                r_thr = OF_RATIO_THRESHOLD_PROXY if detector.use_proxy_mode else OF_RATIO_THRESHOLD
                body_thr = OF_MIN_BODY_ATR
                
                spike_long = (z >= z_thr) and (ratio >= r_thr) and (cl > o) and (body_atr >= body_thr) and vol_ok
                spike_short = (z <= -z_thr) and (ratio <= -r_thr) and (cl < o) and (body_atr >= body_thr) and vol_ok
                direction = 'long' if spike_long else ('short' if spike_short else None)
                res["isSpike"] = (direction is not None)
                res["dir"] = direction

                # Construct final payload
                payload = {
                    "symbol": symbol,
                    "timeframe": timeframe,
                    "ts": c['ts'],
                    "o": o, "h": h, "l": l, "c": cl, "volume": c['volume'],
                    "buyVol": res["buyVol"],
                    "sellVol": res["sellVol"],
                    "delta": res["delta"],
                    "cvd": res["cvd"],
                    "deltaRatio": res["deltaRatio"],
                    "zDelta": res["zDelta"],
                    "bodyATR": res["bodyATR"],
                    "absorbed": res["absorbed"],
                    "windowN": detector.stats.n,
                    "type": "of_bar"
                }

                self._publish_to_stream(OF_STREAM_BAR, payload)
                self.processed_count += 1
                
                if res["isSpike"]:
                    spike_payload = {**payload, "direction": direction, "type": "of_spike"}
                    self._publish_to_stream(OF_STREAM_SPIKE, spike_payload)
                    self.spike_count += 1
                    print(f"🎯 OF Spike: {symbol} {direction} (z={z:.2f})")

            except Exception as e:
                print(f"❌ OrderFlow: Error processing candle {c.get('symbol')}: {e}")

    def _publish_to_stream(self, stream_name: str, data: Dict[str, Any]) -> Optional[tuple]:
        """
        Публикует сообщение в оба Redis Stream (6380, 6381).
        
        Args:
            stream_name: Имя стрима
            data: Данные для публикации
            
        Returns:
            tuple: (message_id_1, message_id_2) или None
        """
        try:
            # Извлекаем closeTime из данных свечи
            close_time = extract_binance_close_time(data)
            if close_time == 0:
                # Если не нашли closeTime, логируем warning и используем текущее время
                # print(f"⚠️ OrderFlow: closeTime не найден в данных: {data.get('symbol', 'unknown')}")
                close_time = get_current_timestamp_ms()
            
            message_data = {
                'data': json.dumps(data),
                'timestamp': format_timestamp_for_redis(close_time),  # Время события (UTC ms)
                'type': data.get('type', 'unknown'),
                'symbol': data.get('symbol', 'unknown')
            }
            
            message_id_1, message_id_2 = self.dual_redis.xadd(
                stream_name,
                message_data,
                maxlen=STREAM_MAX_LENGTH,
                approximate=True
            )
            
            return (message_id_1, message_id_2)
            
        except Exception as e:
            print(f"❌ OrderFlow: Ошибка публикации в {stream_name}: {e}")
            sys.stdout.flush()
            return None

    def _process_pending_messages(self, consumer_name: str) -> None:
        """Обрабатывает pending сообщения при старте."""
        try:
            pending_info = self.redis_client.xpending_range(
                SUBSCRIBE_STREAM,
                OF_CONSUMER_GROUP,
                min='-',
                max='+',
                count=100
            )
            
            if pending_info:
                print(f"📦 OrderFlow: Найдено {len(pending_info)} pending сообщений")
                sys.stdout.flush()
                
                batch = []
                ack_ids = []

                for p in pending_info:
                    message_id = p['message_id']
                    messages = self.redis_client.xclaim(
                        SUBSCRIBE_STREAM,
                        OF_CONSUMER_GROUP,
                        consumer_name,
                        min_idle_time=0,
                        message_ids=[message_id]
                    )
                    
                    for msg_id, fields in messages:
                        candle = self._process_stream_message(msg_id, fields)
                        if candle:
                            batch.append(candle)
                        ack_ids.append((SUBSCRIBE_STREAM, msg_id))

                if batch:
                    self._process_global_batch(batch)
                
                for s_name, m_id in ack_ids:
                    self.redis_client.xack(s_name, OF_CONSUMER_GROUP, m_id)
                        
        except Exception as e:
            print(f"❌ OrderFlow: Ошибка обработки pending: {e}")
            sys.stdout.flush()

    def _consume_loop(self, consumer_name: str) -> None:
        """
        Main loop: Read -> Collect Batch -> Process Batch -> Ack.
        """
        last_id = '>'
        
        while self.is_running:
            try:
                # Read a chunk of messages (e.g. 50-100)
                messages = self.redis_client.xreadgroup(
                    OF_CONSUMER_GROUP,
                    consumer_name,
                    {SUBSCRIBE_STREAM: last_id},
                    count=max(self.batch_size, OF_READ_COUNT), # Read enough to fill preferred batch
                    block=OF_READ_BLOCK_MS
                )
                
                if not messages:
                    # Idle cycle
                    time.sleep(0.01)
                    continue

                # Collect all candles from this read
                global_batch = []
                ack_ids = []

                for stream_name, stream_messages in messages:
                    for message_id, fields in stream_messages:
                        candle = self._process_stream_message(message_id, fields)
                        if candle:
                            global_batch.append(candle)
                        # Always ack processed message (even if invalid/skipped)
                        ack_ids.append((stream_name, message_id))

                # Process the batch (GPU or CPU fallback implicit)
                if global_batch:
                    self._process_global_batch(global_batch)

                # Batch Ack
                # Simplification: we ack everything we read. 
                # If crash happens during processing, we might lose data (at-most-once for processed),
                # but we are using 'xreadgroup', so they stay in PEL if not acked?
                # Wait, if we crash inside _process_global_batch, we haven't acked yet.
                # So we have at-least-once semantics. Good.
                for s_name, m_id in ack_ids:
                    self.redis_client.xack(s_name, OF_CONSUMER_GROUP, m_id)

            except Exception as e:
                print(f"❌ OrderFlow: Loop error: {e}")
                sys.stdout.flush()
                # Consumer group recovery logic...
                if "NOGROUP" in str(e).upper():
                     # ... same recovery code ...
                     try:
                        self.redis_client.xgroup_create(SUBSCRIBE_STREAM, OF_CONSUMER_GROUP, id='$', mkstream=True)
                     except Exception: pass
                
                time.sleep(1.0)

    
    def _handle_stream(self) -> None:
        """Главная функция обработки стрима."""
        try:
            print(f"🔄 OrderFlow: Подключение к стриму {SUBSCRIBE_STREAM}")
            sys.stdout.flush()
            
            # Создаём consumer group если не существует
            try:
                self.redis_client.xgroup_create(
                    SUBSCRIBE_STREAM,
                    OF_CONSUMER_GROUP,
                    id='$',
                    mkstream=True
                )
                print(f"✅ OrderFlow: Consumer group {OF_CONSUMER_GROUP} создана")
            except Exception as e:
                if "BUSYGROUP" in str(e):
                    print(f"ℹ️ OrderFlow: Consumer group {OF_CONSUMER_GROUP} уже существует")
                else:
                    print(f"❌ OrderFlow: Ошибка создания consumer group: {e}")
            
            consumer_name = f"of-consumer-{os.getpid()}-{int(time.time())}"
            
            # Обрабатываем pending сообщения
            self._process_pending_messages(consumer_name)
            
            # Основной цикл чтения
            print(f"🔄 OrderFlow: Запуск основного цикла чтения...")
            sys.stdout.flush()
            self._consume_loop(consumer_name)
            
        except Exception as e:
            print(f"❌ OrderFlow: Критическая ошибка: {e}")
            sys.stdout.flush()
    
    def _periodic_stats(self) -> None:
        """Периодический вывод статистики."""
        while self.is_running:
            try:
                time.sleep(self._stats_interval_sec)
                
                # Выводим статистику
                print(f"📊 OrderFlow Stats: Processed={self.processed_count}, Spikes={self.spike_count}, Detectors={len(self.detectors)}")
                sys.stdout.flush()
            except Exception:
                pass
    
    def start(self) -> None:
        """Запускает обработчик в отдельном потоке."""
        if self.is_running:
            print("⚠️ OrderFlow Worker уже запущен")
            return
        
        self.is_running = True
        
        # Запускаем поток обработки стрима
        thread = threading.Thread(target=self._handle_stream, daemon=True)
        thread.start()
        
        # Запускаем поток статистики
        self._stats_thread = threading.Thread(target=self._periodic_stats, daemon=True)
        self._stats_thread.start()
        
        print("🚀 OrderFlow Worker запущен")
        sys.stdout.flush()
    
    def stop(self) -> None:
        """Останавливает обработчик."""
        self.is_running = False
        print("⛔ OrderFlow Worker остановлен")
        sys.stdout.flush()

