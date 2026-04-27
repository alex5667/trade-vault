"""
MT5 Deals Watcher - отслеживание реальных сделок

Читает историю сделок MT5 и публикует ExecutionEvent для сделок,
относящихся к сигналам (по comment с sig=<signal_id>).

Обеспечивает учет ФАКТИЧЕСКИХ результатов торговли:
- Фактическая цена исполнения
- Фактический объем
- Реальный PnL в валюте счета
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Set, Optional

import MetaTrader5 as mt5

from .mt5_client import Mt5Client
from .exec_events import ExecEventsPublisher, ExecutionEvent


class Mt5DealsWatcher:
    """
    Периодически читает историю сделок MT5 и по новым сделкам,
    помеченным как относящиеся к сигналам (comment содержит "sig=<signal_id>"),
    публикует ExecutionEvent в stream:signals:exec_events.

    Это обеспечивает учет ФАКТИЧЕСКОГО результата:
      - фактическая цена исполнения;
      - фактический объём;
      - реальный PnL в валюте счёта.
    """

    def __init__(
        self,
        mt5_client: Mt5Client,
        publisher: ExecEventsPublisher,
        history_window_minutes: int = 1440,  # заглядываем максимум на сутки назад при старте
    ):
        """
        Args:
            mt5_client: Подключенный MT5 клиент
            publisher: Паблишер для отправки событий в Redis
            history_window_minutes: Окно истории при старте (в минутах)
        """
        self._mt5 = mt5_client
        self._pub = publisher

        self._seen_deals: Set[int] = set()  # уже обработанные deal.ticket
        now = datetime.now(timezone.utc)
        self._from_time = now - timedelta(minutes=history_window_minutes)

    @staticmethod
    def _parse_signal_id(comment: str) -> Optional[str]:
        """
        Парсим signal_id из comment вида:
          "sig=XAU_2025-12-15_12:34:56"
          "something sig=XYZ other"
        Берём первое совпадение "sig=".

        Args:
            comment: Комментарий сделки

        Returns:
            Optional[str]: signal_id или None если не найден
        """
        if not comment:
            return None

        for part in comment.replace(";", " ").split():
            if part.startswith("sig="):
                return part[4:]  # убираем "sig="

        return None

    def _map_side(self, deal_type: int) -> str:
        """
        Маппим тип сделки MT5 в сторону позиции.

        Args:
            deal_type: mt5.DEAL_TYPE_*

        Returns:
            str: "long", "short" или "unknown"
        """
        if deal_type == mt5.DEAL_TYPE_BUY:
            return "long"
        if deal_type == mt5.DEAL_TYPE_SELL:
            return "short"
        return "unknown"

    def _map_event_type(self, entry: int) -> str:
        """
        Маппим тип входа сделки в event_type.

        На уровне одной сделки:
          - DEAL_ENTRY_IN       → "OPEN"
          - DEAL_ENTRY_OUT*     → "CLOSE"
          - остальное           → "DEAL"

        Args:
            entry: mt5.DEAL_ENTRY_*

        Returns:
            str: Тип события
        """
        if entry == mt5.DEAL_ENTRY_IN:
            return "OPEN"
        if entry in (mt5.DEAL_ENTRY_OUT, mt5.DEAL_ENTRY_OUT_BY):
            return "CLOSE"
        return "DEAL"

    def step(self) -> None:
        """
        Один проход по истории сделок:
          - history_deals_get(from_time, now)
          - по новым deal.ticket → ExecutionEvent(kind="fill") → XADD в Redis.
        """
        now = datetime.now(timezone.utc)
        # небольшой запас по времени вперёд, чтобы не отсеять пограничные сделки
        to_time = now + timedelta(seconds=5)

        deals = mt5.history_deals_get(self._from_time, to_time)
        if deals is None:
            # ничего не нашли / ошибка — логируйте при необходимости
            self._from_time = now
            return

        # на следующем шаге начинаем с текущего времени
        self._from_time = now

        for deal in deals:
            if deal.ticket in self._seen_deals:
                continue
            self._seen_deals.add(deal.ticket)

            comment = deal.comment or ""
            signal_id = self._parse_signal_id(comment)
            if not signal_id:
                # не ваш сигнал — игнорируем
                continue

            side = self._map_side(deal.type)
            event_type = self._map_event_type(deal.entry)

            ts_event = datetime.fromtimestamp(deal.time, tz=timezone.utc)

            ev = ExecutionEvent(
                signal_id=signal_id,
                symbol=deal.symbol,
                side=side,
                venue="mt5",
                kind="fill",
                event_type=event_type,
                ts_event=ts_event,
                price=deal.price,
                qty_lots=deal.volume,
                pnl_ccy=deal.profit,
                account_ccy=self._mt5.account_currency,
                mt5_deal=deal.ticket,
                mt5_order=deal.order,
                mt5_position_id=deal.position_id,
                comment=comment or None,
                meta={
                    "swap": deal.swap,
                    "commission": deal.commission,
                    "fee": deal.fee if hasattr(deal, 'fee') else 0.0,
                },
            )

            self._pub.publish(ev)
