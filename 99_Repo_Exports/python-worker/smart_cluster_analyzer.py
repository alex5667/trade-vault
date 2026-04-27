# smart_cluster_analyzer.py
"""
Smart Cluster Analyzer - анализ кластеров и дисбалансов из DOM и тиков.
"""
from __future__ import annotations
from typing import Dict, List, Tuple
import math

class SmartClusterAnalyzer:
    """
    1) По DOM: stacked imbalance (верх/низ книги), absorption (слабый прогресс при больших ударах).
    2) По тикам: грубая оценка через скорость/дельту из MicrostructureSpikeDetector (передай сюда окно).
    Возвращает "cluster_score" 0..100 и признаки.
    """

    def __init__(self, top_n: int = 5, imb_ratio: float = 3.0):
        """
        Args:
            top_n: Количество верхних уровней для анализа
            imb_ratio: Минимальный коэффициент дисбаланса для детекции
        """
        self.top_n = top_n
        self.imb_ratio = imb_ratio

    # ---------- DOM-путь ----------
    def analyze_from_dom(self, levels: Dict) -> Dict:
        """
        Анализ кластеров из DOM данных.
        
        Args:
            levels: {"bids":[[p,size],...], "asks":[[p,size],...], "mid": float}
        
        Returns:
            Dict с полями:
                - available: bool - доступны ли данные
                - cluster_score: float - оценка кластера (0..100)
                - stacked_sell: bool - обнаружен ли sell stack
                - stacked_buy: bool - обнаружен ли buy stack
                - imb_up: float - дисбаланс продавцов
                - imb_dn: float - дисбаланс покупателей
        """
        bids: List[Tuple[float, float]] = levels.get("bids", [])
        asks: List[Tuple[float, float]] = levels.get("asks", [])
        if not bids or not asks:
            return {"available": False, "cluster_score": 0.0}

        tb = sum([s for _, s in bids[:self.top_n]])
        ta = sum([s for _, s in asks[:self.top_n]])
        if tb <= 0 or ta <= 0:
            return {"available": False, "cluster_score": 0.0}

        imb_up = ta / tb   # перевес продавцов (над головой)
        imb_dn = tb / ta   # перевес покупателей (под ценой)

        # stacked: если верхняя сторона в 3х раз плотнее — повышаем уверенность «контратаки»
        stacked_sell = imb_up >= self.imb_ratio
        stacked_buy = imb_dn >= self.imb_ratio

        # cluster_score: симметрично в 0..100
        # чем больше дисбаланс, тем выше доверие, но не бесконечно
        raw = max(imb_up, imb_dn)
        score = min(100.0, 20.0 * math.log(raw + 1.0, 1.7))  # мягкая лог-шкала

        return {
            "available": True,
            "cluster_score": round(score, 1),
            "stacked_sell": stacked_sell,
            "stacked_buy": stacked_buy,
            "imb_up": round(imb_up, 2),
            "imb_dn": round(imb_dn, 2),
        }

    # ---------- По окну тиков (fallback) ----------
    def analyze_from_ticks(self, window: List[Dict]) -> Dict:
        """
        Анализ на основе тиков (fallback когда DOM недоступен).
        
        Args:
            window: Список тиков
        
        Returns:
            Dict с полями available и cluster_score
        """
        if not window:
            return {"available": False, "cluster_score": 0.0}
        # здесь можно пересчитать footprint-метрики, но оставим легкий суррогат
        return {"available": True, "cluster_score": 0.0}
