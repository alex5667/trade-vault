"""
Утилиты для ведения истории диапазонов цен свечей.

Основная функция `update_and_check_history` поддерживает скользящее окно истории
и возвращает среднее значение диапазона, когда данных достаточно.
"""

from core.config import RANGE_EVAL_WINDOW
from utils.candle_utils import average


def update_and_check_history(history, value=None, update=True, eval_window=RANGE_EVAL_WINDOW):
    """
    Обновляет историю диапазонов цен свечей (по желанию) и возвращает средний диапазон,
    если собрано достаточно данных (len(history) >= eval_window).

    Параметры:
        history (list): Мутируемый список исторических диапазонов (единица — абсолютное изменение цены)
        value (float, optional): Диапазон текущей свечи. Если не задан и update=True,
                                 функция не будет добавлять значение
        update (bool): Добавлять ли значение в историю (True) или только рассчитать среднее (False)
        eval_window (int): Окно для расчёта среднего (число последних значений)

    Возвращает:
        float or None: Средний диапазон цен по последним eval_window значениям (без модификации истории,
                       если update=False). None, если данных недостаточно.
    """
    # Безопасная нормализация входного значения
    if value is not None:
        try:
            value = float(value)
        except (TypeError, ValueError):
            value = None

    # При необходимости обновляем историю
    if update and value is not None:
        # Убеждаемся, что диапазон не нулевой и не отрицательный
        safe_value = max(0.00000001, value)
        history.append(safe_value)
        # Поддерживаем размер окна — удаляем избыточные элементы слева
        if len(history) > eval_window:
            del history[:len(history) - eval_window]

    # Проверяем, достаточно ли данных для расчёта среднего
    if len(history) < eval_window:
        return None

    # Берём последние eval_window значений
    window = history[-eval_window:]

    # Фильтруем невалидные значения
    valid = [v for v in window if isinstance(v, (int, float)) and v > 0]
    if not valid:
        return 0.00000001

    # Вычисляем среднее
    avg_range = average(valid)
    return max(0.00000001, avg_range)
