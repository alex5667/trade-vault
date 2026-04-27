# -*- coding: utf-8 -*-
"""
Order Book L2 Tracker - отслеживание изменений глубины рынка.

ФУНКЦИОНАЛ:
- Отслеживание изменений топ-глубины (refill/depletion detection)
- Расчёт относительных изменений объёма (ratio)
- Хранение последнего снимка L2-метрик
- Impact proxy через изменения depth

ИСПОЛЬЗОВАНИЕ:
    from signals.orderbook_l2_tracker import L2BookTracker
    
    tracker = L2BookTracker(k_small=5, k_large=20)
    
    # При каждом book update
    snap = tracker.feed(book_data)
    if snap:
        # Проверка depletion (объём уменьшился)
        if snap.ch.bid_top3_ratio < -0.2:
            print("⚠️ Bid depletion: -20% volume on top 3 levels")
        
        # Проверка refill (объём увеличился)
        if snap.ch.ask_top5_ratio > 0.3:
            print("📈 Ask refill: +30% volume on top 5 levels")
        
        # Доступ к полным метрикам
        print(f"OBI_5: {snap.m.obi_5:.3f}")
        print(f"Wall on bid: {snap.m.wall_bid}")

ИНТЕГРАЦИЯ:
- Может быть интегрирован в OrderFlow Handlers
- Используется для детекции absorption/breakout
- Помогает определить силу уровней поддержки/сопротивления

ПРИМЕРЫ СИГНАЛОВ:
1. Depletion + Delta spike = Absorption (слабая защита уровня)
2. Refill + OBI sustained = Strong support/resistance
3. Wall появился + OBI confirms = Potential reversal
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, List

from signals.orderbook_l2_metrics import L2Metrics, compute_l2_metrics, EPS

# ✅ GPU Support: lazy initialization
_gpu_service_cache = None

def _get_gpu_service():
    """Получить GPU сервис (lazy initialization)"""
    global _gpu_service_cache
    if _gpu_service_cache is None:
        try:
            from services.gpu_compute_service import get_gpu_service
            _gpu_service_cache = get_gpu_service()
        except Exception:
            _gpu_service_cache = None
    return _gpu_service_cache


@dataclass
class L2Change:
    """
    Относительные изменения топ-глубины книги заявок.
    
    Positive ratio = refill (объём увеличился)
    Negative ratio = depletion (объём уменьшился)
    
    Attributes:
        bid_top3_ratio: Изменение bid depth на 3 уровнях (ratio)
        ask_top3_ratio: Изменение ask depth на 3 уровнях (ratio)
        bid_top5_ratio: Изменение bid depth на 5 уровнях (ratio)
        ask_top5_ratio: Изменение ask depth на 5 уровнях (ratio)
        
    Example:
        bid_top3_ratio = 0.2  → bid depth увеличился на 20% (refill)
        ask_top3_ratio = -0.3 → ask depth уменьшился на 30% (depletion)
    """
    # относительные изменения топ-глубины (ratio), + => refill, - => depletion
    bid_top3_ratio: float = 0.0
    ask_top3_ratio: float = 0.0
    bid_top5_ratio: float = 0.0
    ask_top5_ratio: float = 0.0


@dataclass
class L2Snapshot:
    """
    Снимок L2-метрик с изменениями.
    
    Attributes:
        m: Полные L2-метрики (L2Metrics)
        ch: Изменения глубины относительно предыдущего снимка (L2Change)
    """
    m: L2Metrics
    ch: L2Change


class L2BookTracker:
    """
    Трекер Order Book L2-метрик с отслеживанием изменений.
    
    Хранит prev snapshot и вычисляет:
      - Полные L2 метрики (OBI, depth, slope, microprice, walls)
      - Изменения топ-глубины (refill/depletion proxy)
      - Impact proxy через delta depth
    
    Attributes:
        k_small: Количество уровней для "малой" глубины (default 5)
        k_large: Количество уровней для "большой" глубины (default 20)
        wall_mult: Множитель медианы для wall detection (default 3.0)
        wall_max_dist_bps: Максимальное расстояние для wall (default 15 bps)
        prev: Предыдущие метрики (для расчёта изменений)
        last: Последний снимок (L2Snapshot)
        
    Example:
        >>> tracker = L2BookTracker(k_small=5, k_large=20)
        >>> snap = tracker.feed(book_data)
        >>> if snap and snap.ch.bid_top3_ratio < -0.2:
        ...     print("Bid depletion detected!")
    """
    def __init__(
        self,
        *,
        k_small: int = 5,
        k_large: int = 20,
        wall_mult: float = 3.0,
        wall_max_dist_bps: float = 15.0,
    ):
        """
        Инициализация L2BookTracker.
        
        Args:
            k_small: Количество уровней для "малой" глубины (default 5)
            k_large: Количество уровней для "большой" глубины (default 20)
            wall_mult: Множитель медианы для wall detection (default 3.0)
            wall_max_dist_bps: Максимальное расстояние для wall в bps (default 15.0)
        """
        self.k_small = int(k_small)
        self.k_large = int(k_large)
        self.wall_mult = float(wall_mult)
        self.wall_max_dist_bps = float(wall_max_dist_bps)

        self.prev: Optional[L2Metrics] = None
        self.last: Optional[L2Snapshot] = None

    def feed(self, book: dict) -> Optional[L2Snapshot]:
        """
        Обрабатывает новый снимок Order Book.
        
        Вычисляет полные L2-метрики и изменения относительно предыдущего снимка.
        
        Args:
            book: Order book с ключами "bids", "asks", "ts"
                  bids/asks: [[price, volume], ...]
                  
        Returns:
            L2Snapshot с метриками и изменениями, или None если book невалидный
            
        Example:
            >>> snap = tracker.feed(book_data)
            >>> if snap:
            ...     print(f"OBI_5: {snap.m.obi_5:.3f}")
            ...     print(f"Bid depth change: {snap.ch.bid_top5_ratio:.2%}")
        """
        m = compute_l2_metrics(
            book,
            k_small=self.k_small,
            k_large=self.k_large,
            wall_mult=self.wall_mult,
            wall_max_dist_bps=self.wall_max_dist_bps,
        )
        if m is None:
            return None

        ch = L2Change()
        if self.prev is not None:
            # Расчёт относительных изменений (ratio)
            # Positive = refill (объём увеличился)
            # Negative = depletion (объём уменьшился)
            ch.bid_top3_ratio = (m.bid_top3 - self.prev.bid_top3) / max(self.prev.bid_top3, EPS)
            ch.ask_top3_ratio = (m.ask_top3 - self.prev.ask_top3) / max(self.prev.ask_top3, EPS)
            ch.bid_top5_ratio = (m.bid_top5 - self.prev.bid_top5) / max(self.prev.bid_top5, EPS)
            ch.ask_top5_ratio = (m.ask_top5 - self.prev.ask_top5) / max(self.prev.ask_top5, EPS)

        snap = L2Snapshot(m=m, ch=ch)
        self.prev = m
        self.last = snap
        return snap
    
    def feed_batch(self, books: List[dict]) -> List[Optional[L2Snapshot]]:
        """
        Батч обработка множества книг с GPU ускорением.
        
        ✅ ОПТИМИЗИРОВАНО: Использует GPU для параллельной обработки множества книг.
        
        Args:
            books: Список книг для обработки
            
        Returns:
            Список L2Snapshot для каждой книги (или None если книга невалидна)
        """
        if not books:
            return []
        
        # ✅ GPU Support: используем GPU батч если доступен и книг достаточно
        gpu_service = _get_gpu_service()
        use_gpu_batch = (
            gpu_service and 
            gpu_service.is_gpu_available() and 
            len(books) >= 5  # Используем GPU для батчей из 5+ книг
        )
        
        if use_gpu_batch:
            try:
                # Подготавливаем книги с mid price
                books_with_mid = []
                for book in books:
                    try:
                        bids = book.get("bids", [])
                        asks = book.get("asks", [])
                        if not bids or not asks:
                            continue
                        best_bid = float(bids[0][0]) if bids else 0.0
                        best_ask = float(asks[0][0]) if asks else 0.0
                        if best_bid > 0 and best_ask > 0:
                            mid = 0.5 * (best_bid + best_ask)
                            book_with_mid = book.copy()
                            book_with_mid["mid"] = mid
                            books_with_mid.append(book_with_mid)
                    except Exception:
                        continue
                
                if books_with_mid:
                    # Вызываем GPU батч метод
                    gpu_results = gpu_service.compute_l2_metrics_batch(
                        books_with_mid,
                        k_small=self.k_small,
                        k_large=self.k_large,
                        wall_mult=self.wall_mult,
                        wall_max_dist_bps=self.wall_max_dist_bps,
                    )
                    
                    # Конвертируем результаты в L2Snapshot
                    snapshots = []
                    for i, gpu_result in enumerate(gpu_results):
                        if gpu_result is None:
                            snapshots.append(None)
                            continue
                        
                        # Создаем L2Metrics из результата GPU
                        m = L2Metrics(
                            ts=gpu_result["ts"],
                            best_bid=gpu_result["best_bid"],
                            best_ask=gpu_result["best_ask"],
                            mid=gpu_result["mid"],
                            spread_bps=gpu_result["spread_bps"],
                            depth_bid_5=gpu_result["depth_bid_5"],
                            depth_ask_5=gpu_result["depth_ask_5"],
                            depth_bid_20=gpu_result["depth_bid_20"],
                            depth_ask_20=gpu_result["depth_ask_20"],
                            obi_5=gpu_result["obi_5"],
                            obi_20=gpu_result["obi_20"],
                            slope_bid_20=gpu_result["slope_bid_20"],
                            slope_ask_20=gpu_result["slope_ask_20"],
                            microprice_20=gpu_result["microprice_20"],
                            microprice_shift_bps_20=gpu_result["microprice_shift_bps_20"],
                            wall_bid=gpu_result["wall_bid"],
                            wall_ask=gpu_result["wall_ask"],
                            wall_bid_dist_bps=gpu_result["wall_bid_dist_bps"],
                            wall_ask_dist_bps=gpu_result["wall_ask_dist_bps"],
                            bid_top3=gpu_result["depth_bid_3"],
                            ask_top3=gpu_result["depth_ask_3"],
                            bid_top5=gpu_result["depth_bid_5"],
                            ask_top5=gpu_result["depth_ask_5"],
                        )
                        
                        # Вычисляем изменения
                        ch = L2Change()
                        if self.prev is not None:
                            ch.bid_top3_ratio = (m.bid_top3 - self.prev.bid_top3) / max(self.prev.bid_top3, EPS)
                            ch.ask_top3_ratio = (m.ask_top3 - self.prev.ask_top3) / max(self.prev.ask_top3, EPS)
                            ch.bid_top5_ratio = (m.bid_top5 - self.prev.bid_top5) / max(self.prev.bid_top5, EPS)
                            ch.ask_top5_ratio = (m.ask_top5 - self.prev.ask_top5) / max(self.prev.ask_top5, EPS)
                        
                        snap = L2Snapshot(m=m, ch=ch)
                        self.prev = m  # Обновляем prev для следующей итерации
                        snapshots.append(snap)
                    
                    return snapshots
            except Exception:
                # Fallback на CPU если GPU батч не удался
                pass
        
        # CPU fallback: обрабатываем по одной книге
        return [self.feed(book) for book in books]

    def get_last(self) -> Optional[L2Snapshot]:
        """
        Возвращает последний снимок без обработки нового book.
        
        Returns:
            Последний L2Snapshot или None если ещё не было обработано ни одного book
        """
        return self.last

    def reset(self) -> None:
        """
        Сбрасывает состояние трекера (prev и last).
        
        Используется при переподключении или смене символа.
        """
        self.prev = None
        self.last = None

