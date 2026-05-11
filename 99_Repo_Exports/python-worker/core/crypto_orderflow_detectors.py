from utils.time_utils import get_ny_time_millis

"""
Набор детекторов Order Flow для криптовалютных рынков.

Содержит классы:
- RingBuffer
- DeltaSpikeDetector
- OBIDetector
- AbsorptionDetector
- IcebergDetector
- LevelProximityFilter
"""

import logging
import math
import os
import time
from collections import deque
from typing import Any

DEBUG_DELTAS = os.getenv("CRYPTO_OF_DEBUG_DELTAS", "false").strip().lower() in ("1", "true", "yes", "on")
logger = logging.getLogger("crypto_orderflow_detectors")


def classify_signed_qty(tick: dict[str, Any], override_qty: float | None = None) -> float:
    """
    Shared tick классификация в signed volume (delta_tick). Оптимизировано под Zero-Allocation.
    """
    if override_qty is not None:
        volume = override_qty
    else:
        try:
            vol_raw = tick.get("qty")
            if vol_raw is None:
                vol_raw = tick.get("volume", 0.0)
            volume = float(vol_raw or 0.0)
            if volume < 0:
                volume = abs(volume)
        except Exception:
            return 0.0

    if volume <= 0:
        return 0.0

    # 1) Fast path: Binance is_buyer_maker
    is_buyer_maker = tick.get("is_buyer_maker")
    if is_buyer_maker is not None:
        # is_buyer_maker=True => taker SELL => negative
        return -volume if bool(is_buyer_maker) else volume

    # 2) Fast path: Строгий контракт от Go ("BUY" / "SELL")
    side = tick.get("side")
    if side == "BUY":
        return volume
    if side == "SELL":
        return -volume

    # 3) Slow path (Fallback для сырых данных без нормализации)
    if side:
        try:
            side_lower = side.strip().lower()
            if side_lower in ("buy", "b", "long", "bid"):
                return volume
            if side_lower in ("sell", "s", "short", "ask"):
                return -volume
        except Exception:
            pass

    return 0.0


class RingBuffer:
    """Простой кольцевой буфер с ограничением по длине."""

    def __init__(self, maxlen: int):
        self.buf: deque[Any] = deque(maxlen=maxlen)

    def append(self, item: Any) -> None:
        self.buf.append(item)

    def __len__(self) -> int:
        return len(self.buf)

    def items(self) -> list[Any]:
        return list(self.buf)


class DeltaSpikeDetector:
    """
    Детектор всплесков дельты (агрессивные покупки/продажи).

    Использует z-score на скользящем окне объёмов.
    """

    def __init__(self, window: int = 60, z_threshold: float = 3.0, min_abs_volume: float = 0.0):
        self.window = window
        self.z_threshold = z_threshold
        self.min_abs_volume = min_abs_volume
        self.values: deque[float] = deque(maxlen=window)
        self._sum = 0.0
        self._sum_sq = 0.0

    def classify_tick(self, tick: dict[str, Any]) -> float:
        """
        NOTE: делегируем в shared функцию, чтобы TickCVDState и delta_spike
        не расходились по определению signed volume.
        """
        return classify_signed_qty(tick)

    def push(self, tick: dict[str, Any]) -> dict[str, Any] | None:
        """
        Добавляет тик и возвращает событие, если дельта превысила порог.

        Важно: Z-score считаем по *предыдущему* окну (без включения текущего delta),
        чтобы избежать bias (10–20% недооценки |z| при self-inclusion).
        """
        delta = self.classify_tick(tick)

        prev_n = len(self.values)
        # Keep legacy warm-up behavior: require ~10 total samples (including current).
        min_total = min(10, max(1, int(self.window)))
        min_prev = max(1, min_total - 1)

        if prev_n < min_prev:
            self.values.append(delta)
            self._sum += delta
            self._sum_sq += delta * delta
            # Логируем прогресс заполнения буфера (только первые несколько раз)
            if DEBUG_DELTAS and (len(self.values) in (1, 5, min_prev)):
                logger.debug(
                    "📊 Delta detector buffer: %d/%d тиков (нужно минимум %d)",
                    len(self.values), self.window, min_total
                )
            return None

        # Расчет за O(1)
        mean = self._sum / prev_n
        # max(0, ...) защищает от ошибок округления float64
        variance = max(0.0, (self._sum_sq / prev_n) - (mean * mean))
        std_dev = variance ** 0.5 if variance > 0 else 0.0

        # Floor protection
        # Приближенное среднее абсолютное отклонение: для скорости берем std_dev как прокси
        # В оригинале было sum(abs(val) for val in self.values) / prev_n, что есть O(N).
        # Переходим на std_dev protection или WMA. В запросе предложено 0.10 * std_dev.
        std_floor = max(1e-6, 0.10 * std_dev)
        std_eff = max(std_dev, std_floor)

        z_value = (delta - mean) / std_eff

        # Удаляем старое значение из сумм, если буфер полон (ensures no self-inclusion bias)
        if prev_n == self.window:
            old_val = self.values[0]
            self._sum -= old_val
            self._sum_sq -= old_val * old_val

        # Добавляем новое ПОСЛЕ расчета z (ensures no self-inclusion bias)
        self.values.append(delta)
        self._sum += delta
        self._sum_sq += delta * delta

        if abs(z_value) >= self.z_threshold and abs(delta) >= self.min_abs_volume:
            # Deterministic time from tick if possible
            ts_ms = int(tick.get("ts_ms") or tick.get("ts") or tick.get("E") or 0)
            return {
                "type": "delta_spike",
                "delta": delta,
                "z": z_value,
                "ts_ms": ts_ms,
            }
        return None


class OBIDetector:
    """
    Детектор Order Book Imbalance с фильтрацией по времени удержания.
    """

    def __init__(self, depth: int = 5, threshold: float = 0.5, hold_secs: float = 2.0,
                 z_alpha: float = 0.05, z_floor_var: float = 1e-6):
        self.depth = depth
        self.threshold = threshold
        self.hold_secs = hold_secs
        self.last_ok_ts: float | None = None
        self.last_direction: str | None = None
        # EWMA stats for OBI normalization
        self.z_alpha = float(z_alpha)
        self.z_mu = 0.0
        self.z_var = float(z_floor_var)
        self.z_floor_var = float(z_floor_var)
        # Raw snapshot (always updated on push for consumers)
        self._last_raw: dict[str, Any] | None = None

    @staticmethod
    def _stacking_score(levels: list[list[float]], k: int) -> float:
        """
        Score in [0..1]: share of non-decreasing sizes as we go deeper.
        Example: sizes [5,6,7,2,1] => (>=) holds for first 2 transitions => 2/(k-1).
        """
        if not levels or k <= 1:
            return 0.0
        sizes = []
        for lv in levels[:k]:
            try:
                sizes.append(float(lv[1]))
            except Exception:
                sizes.append(0.0)
        ok = 0
        for i in range(len(sizes) - 1):
            if sizes[i + 1] >= sizes[i]:
                ok += 1
        return float(ok) / float(max(1, len(sizes) - 1))

    @staticmethod
    def _concentration(levels: list[list[float]], k: int) -> float:
        if not levels:
            return 0.0
        tot = 0.0
        top = 0.0
        for i, lv in enumerate(levels[:k]):
            try:
                q = float(lv[1])
            except Exception:
                q = 0.0
            tot += max(0.0, q)
            if i == 0:
                top = max(0.0, q)
        if tot <= 1e-12:
            return 0.0
        return top / tot

    def snapshot(self) -> dict[str, Any] | None:
        """Return the latest raw OBI snapshot (always available after first push)."""
        return self._last_raw

    def push(self, book: dict[str, Any]) -> dict[str, Any] | None:
        bids = book.get("bids") or []
        asks = book.get("asks") or []

        bid_vol = sum(float(level[1]) for level in bids[: self.depth])
        ask_vol = sum(float(level[1]) for level in asks[: self.depth])

        if bid_vol + ask_vol == 0:
            return None

        obi = (bid_vol - ask_vol) / (bid_vol + ask_vol)

        # Update EWMA stats for z-score (do it always, regardless of threshold)
        try:
            a = self.z_alpha
            d = float(obi) - float(self.z_mu)
            self.z_mu = (1.0 - a) * self.z_mu + a * float(obi)
            # EW variance (approx)
            self.z_var = max(self.z_floor_var, (1.0 - a) * self.z_var + a * (d * d))
            obi_z = (float(obi) - float(self.z_mu)) / math.sqrt(self.z_var)
        except Exception:
            obi_z = 0.0

        bid_stack = self._stacking_score(bids, self.depth)
        ask_stack = self._stacking_score(asks, self.depth)
        stacking = (bid_stack - ask_stack)  # [-1..+1], positive favors bids

        conc_bid = self._concentration(bids, self.depth)
        conc_ask = self._concentration(asks, self.depth)
        concentration = (conc_bid - conc_ask)  # [-1..+1]

        # Deterministic time if book has timestamp
        # Accept common fields: ts (ms), ts_ms (ms), timestamp (ms)
        now_s: float
        ts_ms = None
        try:
            for k in ("ts_ms", "ts", "timestamp"):
                if k in book and book.get(k) is not None:
                    ts_ms = int(book.get(k))  # type: ignore
                    break
        except Exception:
            ts_ms = None
        if ts_ms is not None and ts_ms > 0:
            now_s = ts_ms / 1000.0
        else:
            now_s = time.time()

        # Always persist raw snapshot for consumers (even when no stable event fires)
        self._last_raw = {
            "obi": float(obi),
            "obi_z": float(obi_z),
            "direction": "long" if obi > 0 else "short",
            "stacking": float(stacking),
            "concentration": float(concentration),
            "ts_ms": int(ts_ms or 0),
            "above_threshold": bool(abs(obi) >= self.threshold),
        }

        if abs(obi) >= self.threshold:
            direction = "long" if obi > 0 else "short"
            if self.last_direction == direction and self.last_ok_ts:
                stable_secs = now_s - self.last_ok_ts
                if stable_secs >= self.hold_secs:
                    return {
                        "type": "obi",
                        "direction": direction,
                        "obi": obi,
                        "stable_secs": stable_secs,
                        "bid_vol": float(bid_vol),
                        "ask_vol": float(ask_vol),
                        "depth": int(self.depth),
                        "obi_z": float(obi_z),
                        "stacking": float(stacking),
                        "concentration": float(concentration),
                        "ts_ms": int(ts_ms or 0),
                    }
            else:
                self.last_direction = direction
                self.last_ok_ts = now_s

        return None


class AbsorptionDetector:
    """
    Детектор абсорбции (flat-продажи на уровне с большим объёмом).
    """

    def __init__(self, price_tolerance: float = 0.0, min_volume: float = 0.0, window_sec: float = 10.0):
        self.price_tolerance = price_tolerance
        self.min_volume = min_volume
        self.window_sec = window_sec
        self._ticks: deque[tuple[float, float, float]] = deque()
        self._running_volume = 0.0 # O(1) state

    def push(self, tick: dict[str, Any], book: dict[str, Any] | None, price: float) -> dict[str, Any] | None:
        """
        Анализирует поток тиков для выявления абсорбции.
        """
        ts_raw = tick.get("ts") or tick.get("E") or get_ny_time_millis()
        ts = float(ts_raw) / 1000.0
        volume = float(tick.get("qty") or tick.get("volume") or 0)
        self._ticks.append((ts, price, volume))
        self._running_volume += volume

        cutoff = ts - self.window_sec
        while self._ticks and self._ticks[0][0] < cutoff:
            old_ts, old_p, old_v = self._ticks.popleft()
            self._running_volume -= old_v

        # Lazy Evaluation: отсекаем 99% тиков за O(1)
        if self._running_volume < self.min_volume:
            return None

        # Ищем max/min только когда это действительно нужно
        prices = [item[1] for item in self._ticks]
        if max(prices) - min(prices) > self.price_tolerance:
            return None

        side = "unknown"
        if book:
            asks = book.get("asks") or []
            bids = book.get("bids") or []
            if asks and price >= float(asks[0][0]):
                side = "short"
            elif bids and price <= float(bids[0][0]):
                side = "long"

        return {
            "type": "absorption",
            "volume": self._running_volume,
            "side": side,
            "ts_ms": int(ts_raw or 0),
        }


class IcebergDetector:
    """
    Простейший детектор iceberg-ордеров по активности best level.
    """

    def __init__(self, min_refresh: int = 2, min_duration: float = 1.5, state_ttl_sec: float = 10.0, max_states: int = 5000):
        self.min_refresh = int(min_refresh)
        self.min_duration = float(min_duration)
        self.state_ttl_sec = float(state_ttl_sec)
        self.max_states = int(max_states)
        self._level_state: dict[tuple[str, float], dict[str, Any]] = {}
        self._last_cleanup = 0.0  # NEW: Таймер троттлинга

    def push(self, book: dict[str, Any]) -> dict[str, Any] | None:
        # Deterministic time if possible
        ts_ms = None
        try:
            for k in ("ts_ms", "ts", "timestamp"):
                if k in book and book.get(k) is not None:
                    ts_ms = int(book.get(k))  # type: ignore
                    break
        except Exception:
            ts_ms = None
        now = (ts_ms / 1000.0) if (ts_ms is not None and ts_ms > 0) else time.time()

        bids = book.get("bids") or []
        asks = book.get("asks") or []

        if not bids and not asks:
            return None

        events: list[dict[str, Any]] = []

        for side, levels in (("bid", bids), ("ask", asks)):
            if not levels:
                continue
            price = float(levels[0][0])
            qty = float(levels[0][1])
            state = self._level_state.get((side, price))

            if not state:
                self._level_state[(side, price)] = {
                    "start": now,
                    "last_qty": qty,
                    "refresh": 0,
                    "total_refresh_qty": 0.0,
                }
                continue

            if qty >= state["last_qty"]:
                diff = qty - state["last_qty"]
                state["refresh"] += 1
                if diff > 0:
                     state["total_refresh_qty"] += diff

            state["last_qty"] = qty

            if state["refresh"] >= self.min_refresh and (now - state["start"]) >= self.min_duration:
                events.append(
                    {
                        "type": "iceberg",
                        "side": side,
                        "price": price,
                        "duration": now - state["start"],
                        "refresh": state["refresh"],
                        "total_refresh_qty": state.get("total_refresh_qty", 0.0),
                        "start_ts": state["start"],
                    }
                )

                state["refresh"] = 0
                state["total_refresh_qty"] = 0.0

        # --- Throttled Cleanup (Только 1 раз в секунду, а не на каждое обновление!) ---
        if now - self._last_cleanup > 1.0:
            self._last_cleanup = now
            try:
                ttl = self.state_ttl_sec
                if ttl > 0:
                    cutoff = now - ttl
                    # list(keys) быстрее и безопаснее для удаления из словаря во время итерации
                    to_del = [k for k, st in self._level_state.items() if st.get("start", now) < cutoff]
                    for k in to_del:
                        del self._level_state[k]

                # size cap: drop arbitrary old keys if exploding
                if self.max_states > 0 and len(self._level_state) > self.max_states:
                    # drop oldest by start
                    items = sorted(self._level_state.items(), key=lambda kv: kv[1].get("start", now))
                    drop_n = len(items) - self.max_states
                    for i in range(max(0, drop_n)):
                        self._level_state.pop(items[i][0], None)
            except Exception:
                pass

        return events[0] if events else None


class LevelProximityFilter:
    """Проверяет близость цены к заранее заданным уровням."""

    def __init__(self, levels: list[float], max_dist: float):
        self.levels = levels
        self.max_dist = max_dist

    def is_near(self, price: float) -> bool:
        return any(abs(price - level) <= self.max_dist for level in self.levels)




