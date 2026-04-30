# handlers/experiment_metrics.py

from typing import List, Tuple
import math


def _mean(xs: List[float]) -> float:
    """Вычисляет среднее значение списка"""
    return float(sum(xs) / len(xs)) if xs else 0.0


def _std(xs: List[float]) -> float:
    """Вычисляет стандартное отклонение"""
    n = len(xs)
    if n < 2:
        return 0.0
    mu = _mean(xs)
    var = sum((x - mu) ** 2 for x in xs) / (n - 1)
    return float(math.sqrt(max(var, 0.0)))


def expectancy_r(rs: List[float]) -> float:
    """
    Математическое ожидание результата в R (средний результат сделки)

    Args:
        rs: список результатов сделок в R (может включать 0 для нетрейдов)

    Returns:
        средний R
    """
    return _mean(rs)


def sharpe_r(rs: List[float]) -> float:
    """
    Sharpe ratio для результатов в R

    Args:
        rs: список результатов сделок в R

    Returns:
        Sharpe ratio (mu / sigma)
    """
    if not rs:
        return 0.0
    mu = _mean(rs)
    sd = _std(rs)
    if sd <= 1e-9:
        return 0.0
    return mu / sd


def max_drawdown_r(rs: List[float]) -> float:
    """
    Максимальная просадка в R (отрицательное значение)

    Args:
        rs: последовательность результатов сделок в R

    Returns:
        максимальная просадка (отрицательное число или 0)
    """
    if not rs:
        return 0.0

    equity = 0.0
    peak = 0.0
    max_dd = 0.0

    for r in rs:
        equity += r
        if equity > peak:
            peak = equity
        dd = equity - peak  # <= 0
        if dd < max_dd:
            max_dd = dd

    return max_dd  # отрицательный


def cl_ratio(rs: List[float]) -> float:
    """
    C/L ratio = expectancy / |avg_loss_R|

    Args:
        rs: список результатов сделок в R

    Returns:
        C/L ratio (expectancy / abs(avg_loss))
    """
    if not rs:
        return 0.0

    wins = [r for r in rs if r > 0]
    losses = [r for r in rs if r < 0]

    exp_val = expectancy_r(rs)
    if not losses:
        return 0.0  # если нет убытков, то бесконечный C/L

    avg_loss = _mean(losses)   # отрицательное
    if avg_loss >= 0:
        return 0.0
    return exp_val / abs(avg_loss)


def winrate(rs: List[float]) -> float:
    """
    Доля прибыльных сделок

    Args:
        rs: список результатов сделок в R

    Returns:
        доля сделок с положительным результатом
    """
    if not rs:
        return 0.0
    wins = sum(1 for r in rs if r > 0)
    return wins / len(rs)


def precision_recall(
    success_flags: List[bool]
    traded_flags: List[bool]
) -> Tuple[float, float, float]:
    """
    Вычисляет precision, recall, f1-score для фильтра

    Предполагаем:
      - success_flags[i] = True, если сигнал был "хорошим" (R >= threshold)
      - traded_flags[i] = True, если мы реально вошли по этому сигналу.

    Args:
        success_flags: флаги успешных сигналов
        traded_flags: флаги реально отторгованных сигналов

    Returns:
        (precision, recall, f1)
    """
    assert len(success_flags) == len(traded_flags)

    tp = fp = fn = 0

    for success, traded in zip(success_flags, traded_flags):
        if traded and success:
            tp += 1
        elif traded and not success:
            fp += 1
        elif (not traded) and success:
            fn += 1

    precision = tp / (tp + fp) if (tp + fp) > 0 else 0.0
    recall = tp / (tp + fn) if (tp + fn) > 0 else 0.0
    f1 = (
        2 * precision * recall / (precision + recall)
        if (precision + recall) > 0
        else 0.0
    )
    return float(precision), float(recall), float(f1)


def calculate_experiment_metrics(
    pnl_rs: List[float]
    success_threshold_r: float = 0.2
) -> dict:
    """
    Вычисляет полный набор метрик для эксперимента

    Args:
        pnl_rs: результаты сделок в R (только реально отторгованные)
        success_threshold_r: порог для определения "успешной" сделки

    Returns:
        словарь с метриками
    """
    success_flags = [r >= success_threshold_r for r in pnl_rs]
    traded_flags = [True] * len(pnl_rs)  # все переданные сделки считаем отторгованными

    precision, recall, f1 = precision_recall(success_flags, traded_flags)

    return {
        "signals_total": len(pnl_rs)
        "traded_total": len(pnl_rs)
        "winners_total": sum(success_flags)
        "losers_total": len(pnl_rs) - sum(success_flags)
        "expectancy_r": expectancy_r(pnl_rs)
        "sharpe_r": sharpe_r(pnl_rs)
        "max_dd_r": max_drawdown_r(pnl_rs)
        "cl_ratio": cl_ratio(pnl_rs)
        "winrate": winrate(pnl_rs)
        "precision": precision
        "recall": recall
        "f1": f1
    }


def compare_variants(
    control_rs: List[float]
    treatment_rs: List[float]
    success_threshold_r: float = 0.2
) -> dict:
    """
    Сравнивает метрики control vs treatment вариантов

    Args:
        control_rs: результаты control группы
        treatment_rs: результаты treatment группы
        success_threshold_r: порог успеха

    Returns:
        словарь с метриками по вариантам и разницами
    """
    control_metrics = calculate_experiment_metrics(control_rs, success_threshold_r)
    treatment_metrics = calculate_experiment_metrics(treatment_rs, success_threshold_r)

    # Разницы (treatment - control)
    differences = {}
    for key in control_metrics:
        if key in treatment_metrics:
            differences[f"{key}_diff"] = treatment_metrics[key] - control_metrics[key]

    return {
        "control": control_metrics
        "treatment": treatment_metrics
        "differences": differences
    }


def is_experiment_successful(
    control_metrics: dict
    treatment_metrics: dict
    target_metric: str
    min_improvement: float = 0.05
    max_dd_worsening: float = 0.1
) -> Tuple[bool, str]:
    """
    Определяет, успешен ли эксперимент на основе целевой метрики

    Args:
        control_metrics: метрики control группы
        treatment_metrics: метрики treatment группы
        target_metric: целевая метрика ('expectancy_r', 'sharpe_r', etc.)
        min_improvement: минимальное улучшение для успеха
        max_dd_worsening: максимальное ухудшение drawdown (в абсолютном значении)

    Returns:
        (is_successful, reason)
    """
    if target_metric not in control_metrics or target_metric not in treatment_metrics:
        return False, f"Target metric {target_metric} not found in metrics"

    control_value = control_metrics[target_metric]
    treatment_value = treatment_metrics[target_metric]

    # Для drawdown сравниваем абсолютные значения (оба отрицательные)
    if "dd" in target_metric.lower():
        improvement = treatment_value - control_value  # если treatment > control (менее негативный), то положительная разница
        is_better = improvement >= min_improvement
    else:
        improvement = treatment_value - control_value
        is_better = improvement >= min_improvement

    # Проверяем, что drawdown не ухудшился слишком сильно
    control_dd = abs(control_metrics.get("max_dd_r", 0))
    treatment_dd = abs(treatment_metrics.get("max_dd_r", 0))
    dd_worsening = treatment_dd - control_dd

    if dd_worsening > max_dd_worsening:
        return False, ".2f"

    if not is_better:
        return False, ".3f"

    return True, ".3f"























































