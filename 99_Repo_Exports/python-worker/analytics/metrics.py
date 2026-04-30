"""
Metrics - Расчёт аналитических метрик для торговых сигналов.

Функции:
- ROC/AUC вычисления (vectorised NumPy, O(N log N))
- Precision/Recall
- Confusion matrix
- F1-score
- Youden Index

Интеграция с Signal Performance Tracker:
- Использует данные из Repository
- Работает с Signal и Order объектами
"""

from __future__ import annotations
import numpy as np
from typing import List, Tuple, Dict, Any, Optional
from dataclasses import dataclass

from common.log import setup_logger


@dataclass
class ROCResult:
    """Результат ROC анализа"""
    fpr: List[float]  # False Positive Rate
    tpr: List[float]  # True Positive Rate
    thresholds: List[float]
    auc: float

    def to_dict(self) -> Dict[str, Any]:
        return {
            "fpr": self.fpr
            "tpr": self.tpr
            "thresholds": self.thresholds
            "auc": round(self.auc, 4)
        }


logger = setup_logger("Metrics")


def calculate_roc_auc(
    scores: List[float]
    labels: List[int]
) -> ROCResult:
    """
    Вычисление ROC кривой и AUC.

    Vectorised реализация: O(N log N) — sort once, then cumsum.
    Предыдущая реализация: O(N² * T) — nested loop over thresholds × samples.

    Args:
        scores: Список оценок/confidence (0.0-1.0)
        labels: Список меток (0 или 1)

    Returns:
        ROCResult с FPR, TPR, thresholds и AUC
    """
    if len(scores) != len(labels):
        raise ValueError("Длины scores и labels должны совпадать")

    if len(scores) == 0:
        return ROCResult(fpr=[], tpr=[], thresholds=[], auc=0.0)

    scores_arr = np.asarray(scores, dtype=np.float64)
    labels_arr = np.asarray(labels, dtype=np.int32)

    total_pos = int(labels_arr.sum())
    total_neg = len(labels_arr) - total_pos

    if total_pos == 0 or total_neg == 0:
        logger.warning("⚠️ Все метки одного класса, AUC не определён")
        return ROCResult(fpr=[0.0, 1.0], tpr=[0.0, 1.0], thresholds=[1.0, 0.0], auc=0.5)

    # Sort descending by score — O(N log N)
    desc_idx = np.argsort(-scores_arr, kind="stable")
    sorted_labels = labels_arr[desc_idx]
    sorted_scores = scores_arr[desc_idx]

    # Cumulative TP / FP counts
    cum_tp = np.cumsum(sorted_labels)
    cum_fp = np.cumsum(1 - sorted_labels)

    # Threshold breakpoints: last index where score equals a unique value
    # We include one extra point at the beginning for (0,0) start
    unique_scores, last_idx = np.unique(sorted_scores[::-1], return_index=True)
    last_idx = len(sorted_scores) - 1 - last_idx  # flip back to descending order

    # Add boundary indices
    last_idx_sorted = np.concatenate([[0], np.sort(last_idx)])

    tpr_arr = np.concatenate([[0.0], cum_tp[last_idx_sorted] / total_pos])
    fpr_arr = np.concatenate([[0.0], cum_fp[last_idx_sorted] / total_neg])
    thr_arr = np.concatenate([[1.0], sorted_scores[last_idx_sorted]])

    # Ensure monotone for trapz (FPR may not be strictly increasing):
    order = np.argsort(fpr_arr, kind="stable")
    fpr_arr = fpr_arr[order]
    tpr_arr = tpr_arr[order]
    thr_arr = thr_arr[order]

    # AUC via trapezoidal rule — O(N)
    auc = float(np.trapz(tpr_arr, fpr_arr))

    return ROCResult(
        fpr=fpr_arr.tolist()
        tpr=tpr_arr.tolist()
        thresholds=thr_arr.tolist()
        auc=abs(auc)
    )


def roc_from_signals(
    signals: List[Any]
    order_by_signal: Dict[str, Any]
) -> Optional[ROCResult]:
    """
    Построение ROC кривой из сигналов и их результатов.

    Args:
        signals: Список объектов Signal
        order_by_signal: Словарь {signal_id: Order}

    Returns:
        ROCResult или None
    """
    try:
        scores = []
        labels = []

        for signal in signals:
            # Получаем score (приоритет: score > confidence)
            score_val = signal.score if signal.score is not None else signal.confidence

            if score_val is None:
                continue

            # Получаем результат сделки
            order = order_by_signal.get(signal.signal_id)

            if not order:
                continue

            # Метка: 1 если профит, 0 если убыток
            label = 1 if (order.pnl_usd is None or order.pnl_usd > 0) else 0

            scores.append(float(score_val))
            labels.append(label)

        if not scores:
            logger.warning("⚠️ Нет данных для ROC")
            return None

        return calculate_roc_auc(scores, labels)

    except Exception as e:
        logger.error(f"❌ Ошибка вычисления ROC: {e}")
        return None


def calculate_precision_recall(
    tp: int
    fp: int
    fn: int
) -> Tuple[float, float, float]:
    """
    Вычисление Precision, Recall и F1-score.

    Args:
        tp: True Positives
        fp: False Positives
        fn: False Negatives

    Returns:
        (precision, recall, f1)
    """
    precision = tp / (tp + fp) if (tp + fp) > 0 else 0.0
    recall = tp / (tp + fn) if (tp + fn) > 0 else 0.0
    f1 = 2 * precision * recall / (precision + recall) if (precision + recall) > 0 else 0.0

    return precision, recall, f1


def calculate_confusion_matrix(
    scores: List[float]
    labels: List[int]
    threshold: float
) -> Tuple[int, int, int, int]:
    """
    Вычисление confusion matrix для заданного порога.

    Args:
        scores: Список оценок
        labels: Список меток (0/1)
        threshold: Порог классификации

    Returns:
        (tp, fp, tn, fn)
    """
    scores_arr = np.asarray(scores, dtype=np.float64)
    labels_arr = np.asarray(labels, dtype=np.int32)
    preds = (scores_arr >= threshold).astype(np.int32)

    tp = int(((preds == 1) & (labels_arr == 1)).sum())
    fp = int(((preds == 1) & (labels_arr == 0)).sum())
    tn = int(((preds == 0) & (labels_arr == 0)).sum())
    fn = int(((preds == 0) & (labels_arr == 1)).sum())

    return tp, fp, tn, fn


def calculate_youden_index(tpr: float, fpr: float) -> float:
    """
    Вычисление Youden Index (J statistic).

    J = TPR - FPR

    Args:
        tpr: True Positive Rate
        fpr: False Positive Rate

    Returns:
        Youden Index
    """
    return tpr - fpr


def find_best_threshold(
    roc: ROCResult
    method: str = "youden"
) -> Tuple[float, Dict[str, float]]:
    """
    Поиск оптимального порога.

    Args:
        roc: Результат ROC анализа
        method: Метод выбора ('youden', 'f1', 'balanced')

    Returns:
        (best_threshold, metrics_dict)
    """
    best_threshold = 0.5
    best_score = -np.inf
    best_metrics: Dict[str, float] = {}

    for i, threshold in enumerate(roc.thresholds):
        tpr = roc.tpr[i]
        fpr = roc.fpr[i]

        if method == "youden":
            score = calculate_youden_index(tpr, fpr)
        elif method == "f1":
            # Approximation without requiring full confusion matrix
            score = 2 * tpr / (1 + tpr) if tpr > 0 else 0.0
        else:  # balanced
            score = (tpr + (1 - fpr)) / 2.0

        if score > best_score:
            best_score = score
            best_threshold = threshold
            best_metrics = {
                "tpr": tpr
                "fpr": fpr
                "youden_j": calculate_youden_index(tpr, fpr)
                "score": score
            }

    return best_threshold, best_metrics
