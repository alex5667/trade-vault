def calc_volatility(kline: dict) -> float:
    """
    Рассчитывает волатильность свечи как процентное отношение диапазона цены к цене открытия.
    
    Args:
        kline (dict): Словарь с данными о свече от Binance
            'h' - максимальная цена (high)
            'l' - минимальная цена (low)
            'o' - цена открытия (open)
    
    Returns:
        float: Волатильность в процентах
    
    Формула расчета: (high - low) / open * 100
    """
    high = float(kline['h'])
    low = float(kline['l'])
    open_price = float(kline['o'])
    return (high - low) / open_price * 100


def average(values):
    """
    Возвращает среднее арифметическое значение списка.
    
    Args:
        values (list): Список числовых значений
    
    Returns:
        float: Среднее значение списка, или 0 если список пуст
    
    Особенности:
    - Защита от деления на ноль при пустом списке
    - Работает как с целыми числами, так и с числами с плавающей точкой
    """
    if not values:
        return 0
    return sum(values) / len(values)