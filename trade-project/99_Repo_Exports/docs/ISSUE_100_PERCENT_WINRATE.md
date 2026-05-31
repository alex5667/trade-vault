# Проблема: 100% Winrate - Комиссии не учитываются

## Проблема

В отчетах показывается 100% winrate, что выглядит подозрительно. 

## Корневая причина

**Комиссии не рассчитываются и не учитываются** при определении win/loss:

1. `Position.fees` всегда остается `0.0` (нигде не устанавливается)
2. `net_pnl = gross_pnl - fees = gross_pnl - 0 = gross_pnl`
3. Win/loss определяется по `pnl`, который равен `net_pnl`, но без учета комиссий
4. Если все сделки закрываются по TP (а не по SL), они все покажутся прибыльными, даже если после комиссий некоторые должны быть убыточными

## Где проблема

### В коде:

```python
# trade_monitor.py: Position.finalize()
def finalize(self) -> None:
    """Финализация позиции: расчет net_pnl."""
    self.net_pnl = self.realized_pnl_gross - self.fees  # fees всегда 0.0!
```

```python
# trade_monitor.py: _finalize_position()
total_pnl = pos.net_pnl  # Используем net_pnl
trade_result = "win" if total_pnl > 1e-9 else ("loss" if total_pnl < -1e-9 else "breakeven")
```

```python
# periodic_reporter.py: _accumulate_trade_metrics()
pnl = self._safe_float(trade.get("pnl"))  # Это net_pnl из order:*
if pnl > 1e-9:
    aggregates["wins"] += 1  # Но fees не учитываются!
```

## Решение

### Вариант 1: Добавить расчет комиссий при финализации позиции

1. Добавить конфигурацию комиссий в `SymbolSpec` или `instrument_config.py`
2. Рассчитывать `fees` при закрытии позиции на основе:
   - Комиссии на вход (например, 0.1% от объема входа)
   - Комиссии на выход (например, 0.1% от объема выхода)
   - Swap (если есть, для удержания позиции)
3. Обновить `Position.finalize()` для расчета `fees` перед вычислением `net_pnl`

### Вариант 2: Использовать данные из MT5 (если доступны)

Если сделки проходят через MT5, можно получать фактические комиссии и swap из MT5 и сохранять их в `Position`.

### Вариант 3: Добавить конфигурацию комиссий через env переменные

```bash
# В .env или docker-compose.yml
CRYPTO_COMMISSION_RATE=0.001  # 0.1% на каждую сторону
CRYPTO_SWAP_RATE=0.0          # Swap для крипты обычно 0
FOREX_COMMISSION_RATE=0.0005  # 0.05% для Forex
FOREX_SWAP_RATE=0.0001        # Swap для Forex
```

## Рекомендации

1. **Немедленно**: Добавить расчет комиссий при финализации позиции
2. **Для виртуальных позиций (paper trading)**: Использовать реалистичные комиссии (например, 0.1% на каждую сторону для крипты)
3. **Для реальных сделок**: Получать фактические комиссии и swap из брокера/MT5

## Пример расчета комиссий

```python
def calculate_fees(self, spec: SymbolSpec, entry_price: float, exit_price: float) -> float:
    """Расчет комиссий для позиции."""
    commission_rate = getattr(spec, 'commission_rate', 0.001)  # 0.1% по умолчанию
    
    # Комиссия на вход
    entry_value = entry_price * self.lot * spec.contract_size
    entry_commission = entry_value * commission_rate
    
    # Комиссия на выход
    exit_value = exit_price * self.lot * spec.contract_size
    exit_commission = exit_value * commission_rate
    
    # Swap (если есть, для удержания позиции)
    swap_rate = getattr(spec, 'swap_rate', 0.0)
    duration_days = (self.close_time - self.entry_time) / 86400.0
    swap = entry_value * swap_rate * duration_days
    
    return entry_commission + exit_commission + swap
```

## Проверка

После исправления:
- Winrate должен стать более реалистичным (не 100%)
- Убыточные сделки после комиссий будут правильно классифицированы как losses
- В отчетах будет видно влияние комиссий на итоговый P/L












