# Анализатор Trailing vs Baseline для PostgreSQL

## Описание

Этот скрипт анализирует эффективность trailing стратегий по сравнению с baseline (фиксированным выходом) на основе данных из таблицы `trades_closed` в PostgreSQL.

## Основные возможности

- **Сравнение стратегий**: Managed (с трейлингом) vs Baseline (фиксированный выход)
- **Метрики риска**: Sharpe ratio, Sortino ratio, Maximum Drawdown
- **Анализ по тегам**: Группировка по `entry_tag` для выявления лучших паттернов
- **Метрики эффективности трейлинга**: Giveback, Missed profit, MFE/MAE
- **Временные фильтры**: Анализ за последние N дней

## Требования

- Python 3.8+
- PostgreSQL с таблицей `trades_closed`
- Зависимости: `psycopg2-binary` (уже включен в requirements.txt)

## Структура таблицы trades_closed

Скрипт ожидает следующие столбцы:
- `source` - источник стратегии
- `symbol` - торговая пара
- `entry_tag` - тег входа
- `pnl_net` - чистый P&L с трейлингом
- `pnl_if_fixed_exit` - P&L при фиксированном выходе (baseline)
- `one_r_money` - размер одного R в валюте
- `mfe_pnl` - Maximum Favorable Excursion
- `mae_pnl` - Maximum Adverse Excursion
- `giveback` - откат от пика
- `missed_profit` - упущенная прибыль
- `trailing_started` - запущен ли трейлинг
- `trailing_active` - активен ли трейлинг на момент закрытия
- `close_reason` - причина закрытия
- `close_reason_raw` - сырая причина закрытия
- `close_reason_detail` - детальная причина закрытия
- `notional_usd` - номинал в USD
- `exit_ts_ms` - время выхода (timestamp в ms)

## Использование

```bash
cd python-worker

# Базовый запуск
python scripts/analyze_trailing_vs_baseline_postgres.py \
  --dsn "postgresql://user:pass@host:5432/db" \
  --source CryptoOrderFlow \
  --symbols "ETHUSDT,BTCUSDT" \
  --limit 200

# С временным фильтром (последние 30 дней)
python scripts/analyze_trailing_vs_baseline_postgres.py \
  --dsn "postgresql://user:pass@host:5432/db" \
  --source CryptoOrderFlow \
  --symbols "ETHUSDT" \
  --limit 1000 \
  --since-days 30 \
  --min-trades-per-tag 5
```

## Параметры

- `--dsn`: Обязательный. DSN для подключения к PostgreSQL
- `--source`: Источник стратегии (по умолчанию: CryptoOrderFlow)
- `--symbols`: Список символов через запятую (по умолчанию: ETHUSDT,BTCUSDT)
- `--limit`: Максимальное количество сделок на символ (по умолчанию: 200)
- `--since-days`: Анализировать сделки за последние N дней (по умолчанию: 0 - все)
- `--min-trades-per-tag`: Минимальное количество сделок для показа статистики по тегу (по умолчанию: 10)

## Вывод

Скрипт выводит:

1. **Глобальную статистику** по всем сделкам:
   - Win/Loss/Break-even ratio для managed и baseline стратегий
   - Expectancy в R для обеих стратегий
   - Risk-adjusted метрики (Sharpe, Sortino)
   - Maximum Drawdown для equity кривых
   - Метрики giveback/missed profit
   - Статистика использования трейлинга

2. **Статистику по entry_tag** (топ по количеству сделок):
   - WR, Expectancy для каждой группы
   - Delta expectancy (managed - baseline)
   - Процент использования трейлинга
   - Средние giveback и missed profit

## Тестирование

Для тестирования логики без подключения к БД:

```bash
cd python-worker/scripts
python3 test_analyzer.py
```

## Интеграция с проектом

Скрипт добавлен как дополнение к существующим анализаторам:
- `deep_trailing_vs_baseline_pg.py` - версия для Redis Stream
- `analyze_trades_from_postgres_advanced.py` - продвинутый анализ с markdown выводом

## Примеры метрик

- **Expectancy**: средний R на сделку
- **Delta Expectancy**: насколько трейлинг улучшает результат
- **Giveback Ratio**: доля упущенного движения от MFE
- **Trailing Share**: процент сделок с запущенным трейлингом
- **Sharpe Ratio**: риск-скорректированная доходность
