from __future__ import annotations

"""
Plan Executor - Execution Logic for MT5 Bridge

Реализует логику исполнения сигналов:
- Time-to-decay (TTL) по expiry_bars
- Entry zones - вход только при попадании цены в зону
- Partial exits - разбиение позиции по tp_levels и partials
- Position management
"""


from dataclasses import dataclass, field
from datetime import UTC, datetime

from .models import Mt5ExecutionPlan

# Import MT5 client only if available
try:
    from .mt5_client import Mt5Client
    MT5_AVAILABLE = True
except ImportError:
    MT5_AVAILABLE = False
    Mt5Client = None


@dataclass
class ActivePlanState:
    """
    Состояние активного плана исполнения.

    Отслеживает:
    - Был ли вход в позицию
    - Номера открытых ордеров/tickets
    - Время входа
    """
    plan: Mt5ExecutionPlan
    entered: bool = False
    tickets: list[int] = field(default_factory=list)  # MT5 ticket numbers
    entered_at: datetime | None = None


class PlanExecutor:
    """
    Исполнитель планов сигналов для MT5.

    Реализует ключевую логику:
      - Time-to-decay: TTL по expiry_bars (в секундах)
      - Execution zone: вход только при попадании цены в entry_zone
      - Partial exits: разбиение позиции по partials и tp_levels
      - Position management: сопровождение открытых позиций

    Основной цикл: step() вызывается периодически для обработки всех активных планов.
    """

    def __init__(self, mt5: Mt5Client, bar_seconds: int = 60):
        """
        Args:
            mt5: Подключенный MT5 клиент
            bar_seconds: Длительность 1 бара в секундах (для расчета TTL)
        """
        if not MT5_AVAILABLE:
            raise ImportError("MetaTrader5 is not available. Install with: pip install MetaTrader5")

        self.mt5 = mt5
        self.bar_seconds = bar_seconds
        self._plans: dict[str, ActivePlanState] = {}

    def add_plan(self, plan: Mt5ExecutionPlan) -> None:
        """
        Добавляет новый план для исполнения.

        Если план с таким signal_id уже существует,
        он будет обновлен (или можно игнорировать дубликаты).

        Args:
            plan: План для исполнения
        """
        if plan.signal_id in self._plans:
            # Уже есть - можно обновить или игнорировать
            # Пока игнорируем дубликаты
            return
        self._plans[plan.signal_id] = ActivePlanState(plan=plan)

    def _is_expired(self, plan: Mt5ExecutionPlan, now: datetime) -> bool:
        """
        Проверяет, истек ли TTL сигнала.

        TTL = expiry_bars * bar_seconds

        Args:
            plan: План сигнала
            now: Текущее время

        Returns:
            bool: True если сигнал истек
        """
        ttl_seconds = plan.expiry_bars * self.bar_seconds
        elapsed = (now - plan.ts_signal).total_seconds()
        return elapsed > ttl_seconds

    def _price_in_zone(self, plan: Mt5ExecutionPlan, price: float) -> bool:
        """
        Проверяет, находится ли цена в entry zone.

        Args:
            plan: План сигнала
            price: Текущая цена (bid для long, ask для short)

        Returns:
            bool: True если цена в зоне входа
        """
        low = min(plan.entry_zone_low, plan.entry_zone_high)
        high = max(plan.entry_zone_low, plan.entry_zone_high)
        return low <= price <= high

    def _open_position(self, st: ActivePlanState) -> None:
        """
        Открывает позицию согласно плану.

        Разбивает общий объем по partials и tp_levels.
        Каждый partial открывается отдельным ордером с соответствующим TP.

        Args:
            st: Состояние плана
        """
        plan = st.plan
        symbol = plan.symbol

        # Общий объем позиции
        total_volume = plan.position_size_lots
        partials = plan.partials or [1.0]
        tp_prices = plan.tp_levels or [None]

        # Синхронизируем partials и tp_prices
        # Если partials больше TP - лишние partials без TP
        # Если TP больше partials - лишние TP игнорируем
        n = min(len(partials), len(tp_prices))
        partials = partials[:n]
        tp_prices = tp_prices[:n]

        comment_base = f"sig={plan.signal_id}"

        # Открываем каждый partial отдельно
        for frac, tp in zip(partials, tp_prices):
            vol = total_volume * frac
            if vol <= 0:
                continue

            result = self.mt5.send_market_order(
                symbol=symbol,
                is_buy=plan.is_long,
                volume_lots=vol,
                sl_price=plan.stop_price,
                tp_price=tp,
                comment=comment_base,
            )

            # фиксируем order id из результата MT5
            if hasattr(result, 'order') and result.order:
                st.tickets.append(result.order)

    def step(self) -> None:
        """
        Один шаг цикла исполнения.

        Выполняет:
        1. Удаляет протухшие планы (TTL истек и не вошли в позицию)
        2. Для активных планов проверяет условия входа
        3. При попадании цены в entry zone открывает позицию
        4. Может сопровождать открытые позиции (расширение)

        Вызывается в основном цикле с небольшой периодичностью.
        """
        now = datetime.now(UTC)

        # 1) Удаляем протухшие планы (TTL истек и не вошли)
        to_delete: list[str] = []
        for signal_id, st in self._plans.items():
            if self._is_expired(st.plan, now) and not st.entered:
                to_delete.append(signal_id)

        for signal_id in to_delete:
            del self._plans[signal_id]

        # 2) Обрабатываем оставшиеся планы
        for signal_id, st in list(self._plans.items()):
            if st.entered:
                # Позиция уже открыта
                # Здесь можно добавить сопровождение:
                # - Trailing stop
                # - Break-even
                # - Manual closing after N bars
                continue

            plan = st.plan

            # Получаем текущие котировки
            bid, ask = self.mt5.get_tick(plan.symbol)
            # Для входа используем консервативную цену:
            # - Для long: bid (чтобы купить по лучшей цене)
            # - Для short: ask (чтобы продать по лучшей цене)
            ref_price = bid if plan.is_long else ask

            # Проверяем, в зоне ли цена
            if not self._price_in_zone(plan, ref_price):
                continue

            # Цена в entry zone → открываем позицию
            self._open_position(st)
            st.entered = True
            st.entered_at = now

            # Опционально: можно удалить план после входа
            # (если не нужно дальнейшее сопровождение)
            # del self._plans[signal_id]

    def get_active_plans_count(self) -> int:
        """
        Возвращает количество активных планов.

        Returns:
            int: Число планов в обработке
        """
        return len(self._plans)

    def get_entered_positions_count(self) -> int:
        """
        Возвращает количество планов с открытыми позициями.

        Returns:
            int: Число планов с entered=True
        """
        return sum(1 for st in self._plans.values() if st.entered)
