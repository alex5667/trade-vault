# Интеграция анализа Trailing vs Baseline в Periodic Reporter

## Обзор изменений

В `periodic_reporter.py` добавлена интеграция с анализатором trailing vs baseline для включения результатов анализа в регулярные отчеты.

## Новые возможности

### 1. Автоматический анализ PostgreSQL данных
- При формировании отчета периодический репортер может автоматически выполнять анализ trailing vs baseline
- Использует данные из таблицы `trades_closed` PostgreSQL
- Анализ выполняется с настраиваемым интервалом (по умолчанию каждые 10 отчетов)

### 2. Расширенные метрики в отчетах
В отчеты добавлена новая секция **"🎯 Trailing vs Baseline"** с метриками:
- Сравнение Win Rate managed vs baseline стратегий
- Expectancy R для обеих стратегий и дельта
- Sharpe/Sortino ratios по R-метрикам
- Maximum Drawdown для equity кривых
- Статистика использования трейлинга (share, WR, expectancy)
- Giveback и Missed profit метрики
- MFE/MAE (Maximum Favorable/Ex adverse Excursion)
- Анализ по топ entry_tag

## Конфигурационные переменные

### Основные настройки
```bash
# Включение анализа trailing vs baseline в отчеты
PERIODIC_REPORT_TRAILING_VS_BASELINE_ENABLED=true

# Интервал выполнения анализа (каждые N отчетов)
TRAILING_VS_BASELINE_REPORTS_INTERVAL=10

# Минимальное количество сделок для запуска анализа
PERIODIC_REPORT_MIN_TRADES_FOR_TRAILING_ANALYSIS=50
```

### Настройки базы данных
```bash
# DSN для подключения к PostgreSQL
DATABASE_URL="postgresql://user:pass@host:5432/db"
# или
POSTGRES_DSN="postgresql://user:pass@host:5432/db"
```

## Структура отчета

### Новая секция в Telegram отчете
```
🎯 Trailing vs Baseline (анализ 127 сделок)
WR(managed): 65.4% | WR(baseline): 62.2%
Exp_R(managed): +0.234 | Exp_R(baseline): +0.156
ΔExp_R: +0.078
Sharpe(R): +1.45 | Sortino(R): +1.67
MDD(managed): 125.30$ | MDD(baseline): 145.80$
Trailing запущен: 78.7% | Закрыт по трейлу: 72.4%
Trailing WR: 68.9%
Trailing Exp_R: +0.312 (Δ: +0.089)
Giveback: -0.045R (23.6%) | Missed: +0.067R (12.3%)
MFE/MAE: +1.234R / -0.789R

📊 По entry_tag (топ):
• bullish_signal: n=45, ΔExp_R=+0.145, trailing=82.2%
• bearish_setup: n=32, ΔExp_R=+0.098, trailing=75.0%
```

## Логика работы

1. **Проверка условий**: Анализ выполняется только если:
   - Включен флаг `PERIODIC_REPORT_TRAILING_VS_BASELINE_ENABLED`
   - Доступны функции анализа (импорт успешен)
   - Прошло достаточное количество отчетов (интервал)
   - Достаточно сделок для анализа

2. **Подключение к БД**: Использует PostgreSQL DSN из переменных окружения

3. **Загрузка данных**: Загружает сделки из `trades_closed` с теми же фильтрами, что и основной отчет

4. **Анализ**: Выполняет полный анализ managed vs baseline стратегий

5. **Формирование отчета**: Добавляет результаты в секцию отчета

## Зависимости

- `psycopg2-binary` - для работы с PostgreSQL
- `analyze_trailing_vs_baseline_postgres.py` - анализатор (уже создан)

## Диагностика

### Логи
```
✅ Анализ trailing vs baseline выполнен для CryptoOrderFlow/ETHUSDT: 127 сделок
✅ Добавлена секция trailing vs baseline в отчет для CryptoOrderFlow/ETHUSDT
```

### Возможные проблемы
- `psycopg2 not available` - отсутствует драйвер PostgreSQL
- `DSN not configured` - не настроено подключение к БД
- `Insufficient trades` - недостаточно сделок для анализа

## Производительность

- Анализ выполняется не при каждом отчете, а с интервалом
- Ограничение на максимум 500 сделок для анализа
- Опциональное выполнение (можно отключить)

## Интеграция с существующими отчетами

- Не влияет на существующие метрики
- Добавляет новую секцию в конец отчета
- Совместимо с существующими настройками trailing edge анализа
