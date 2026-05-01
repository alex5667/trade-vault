from __future__ import annotations
"""
MT5 Client - MetaTrader5 API Wrapper

Обертка над MetaTrader5 Python API для исполнения ордеров.
Обеспечивает подключение к терминалу, получение котировок и отправку ордеров.
"""


from dataclasses import dataclass
from datetime import datetime
from typing import Optional

import MetaTrader5 as mt5


@dataclass
class Mt5Config:
    """
    Конфигурация для подключения к MT5 терминалу.

    Содержит учетные данные и маппинг символов.
    """
    login: int
    password: str
    server: str
    symbol_map: dict[str, str]  # маппинг символов, e.g. {".m"}


class Mt5Client:
    """
    Тонкая обёртка над MetaTrader5 Python API.

    Предоставляет высокоуровневый интерфейс для:
    - Инициализации и подключения к терминалу
    - Получения текущих котировок (bid/ask)
    - Нормализации объемов под требования брокера
    - Отправки рыночных ордеров с SL/TP
    """

    def __init__(self, cfg: Mt5Config):
        """
        Инициализирует MT5 терминал и подключается к счету.

        Args:
            cfg: Конфигурация MT5 подключения

        Raises:
            RuntimeError: Если инициализация, логин или получение account_info неудачны
        """
        self.cfg = cfg

        # Инициализация MT5 терминала
        if not mt5.initialize():
            error = mt5.last_error()
            raise RuntimeError(f"MT5 initialize failed, error: {error}")

        # Подключение к счету
        if not mt5.login(cfg.login, password=cfg.password, server=cfg.server):
            error = mt5.last_error()
            mt5.shutdown()  # Очистка при неудаче
            raise RuntimeError(f"MT5 login failed, error: {error}")

        # Получаем информацию о счете для валюты
        acc = mt5.account_info()
        if acc is None:
            error = mt5.last_error()
            mt5.shutdown()
            raise RuntimeError(f"account_info() returned None, error: {error}")

        self.account_currency: str = acc.currency

    def map_symbol(self, symbol: str) -> str:
        """
        Маппит символ на MT5-compatible название.

        Args:
            symbol: Исходное название символа

        Returns:
            str: Маппированное название или оригинал
        """
        return self.cfg.symbol_map.get(symbol, symbol)

    def get_tick(self, symbol: str) -> tuple[float, float]:
        """
        Получает текущие котировки bid/ask для символа.

        Args:
            symbol: Название символа

        Returns:
            tuple[float, float]: (bid, ask) цены

        Raises:
            RuntimeError: Если не удалось получить котировки
        """
        mapped = self.map_symbol(symbol)
        tick = mt5.symbol_info_tick(mapped)
        if tick is None:
            raise RuntimeError(f"symbol_info_tick({mapped}) returned None")
        return tick.bid, tick.ask

    def normalize_volume(self, symbol: str, volume_lots: float) -> float:
        """
        Нормализует объем под требования брокера:
        - Округляет до volume_step
        - Ограничивает min/max объемом

        Args:
            symbol: Название символа
            volume_lots: Желаемый объем в лотах

        Returns:
            float: Нормализованный объем

        Raises:
            RuntimeError: Если не удалось получить информацию о символе
        """
        mapped = self.map_symbol(symbol)
        info = mt5.symbol_info(mapped)
        if info is None:
            raise RuntimeError(f"symbol_info({mapped}) is None")

        step = info.volume_step
        min_vol = info.volume_min
        max_vol = info.volume_max

        # Округление до шага объема
        vol = round(volume_lots / step) * step
        vol = max(min_vol, min(max_vol, vol))
        return vol

    def send_market_order(
        self,
        symbol: str,
        is_buy: bool,
        volume_lots: float,
        sl_price: Optional[float],
        tp_price: Optional[float],
        comment: str = "",
    ):
        """
        Отправляет рыночный ордер с SL/TP.

        Args:
            symbol: Название символа
            is_buy: True для покупки (long), False для продажи (short)
            volume_lots: Объем в лотах
            sl_price: Цена стоп-лосса (None для отключения)
            tp_price: Цена тейк-профита (None для отключения)
            comment: Комментарий к ордеру

        Returns:
            OrderSendResult: Полный результат MT5 (содержит order, deal, price, volume и т.д.)

        Raises:
            RuntimeError: Если отправка ордера неудачна
        """
        mapped = self.map_symbol(symbol)

        # Получаем текущие котировки для определения цены исполнения
        bid, ask = self.get_tick(symbol)
        price = ask if is_buy else bid

        # Определяем тип ордера
        order_type = mt5.ORDER_TYPE_BUY if is_buy else mt5.ORDER_TYPE_SELL

        # Нормализуем объем
        volume = self.normalize_volume(symbol, volume_lots)

        # Формируем запрос на сделку
        request = {
            "action": mt5.TRADE_ACTION_DEAL,
            "symbol": mapped,
            "volume": volume,
            "type": order_type,
            "price": price,
            "sl": sl_price or 0.0,
            "tp": tp_price or 0.0,
            "deviation": 20,  # max slippage in points, подстрой под брокера
            "magic": 2025121501,  # магический номер для идентификации
            "comment": comment,
            "type_filling": mt5.ORDER_FILLING_IOC,  # Immediate or Cancel
            "type_time": mt5.ORDER_TIME_GTC,  # Good Till Cancel
        }

        # Отправляем ордер
        result = mt5.order_send(request)

        if result is None:
            raise RuntimeError(f"order_send returned None, last_error={mt5.last_error()}")

        if result.retcode != mt5.TRADE_RETCODE_DONE:
            raise RuntimeError(f"order_send failed, retcode={result.retcode}, comment={result.comment}")

        return result

    def shutdown(self) -> None:
        """
        Корректно завершает работу с MT5 терминалом.
        """
        mt5.shutdown()
