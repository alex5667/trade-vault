# Руководство по анализу трейлинга и управлению им

## Обзор
Система анализа трейлинга позволяет оценивать эффективность trailing stop по сравнению с baseline (фиксацией прибыли на TP1) и принимать решения о настройке трейлинга.

## 1. Использование скрипта analyze_trades_from_redis_advanced.py

### Примеры команд для реального анализа:

```bash
# ETHUSDT, последние 1500 сделок за декабрь
python -m scripts.analyze_trades_from_redis_advanced \
  --redis-url "redis://localhost:6379/0" \
  --stream "trades:closed" \
  --source CryptoOrderFlow \
  --symbol ETHUSDT \
  --count 1500 \
  --from "2025-12-01" \
  --markdown

# BTCUSDT, аналогично
python -m scripts.analyze_trades_from_redis_advanced \
  --redis-url "redis://localhost:6379/0" \
  --stream "trades:closed" \
  --source CryptoOrderFlow \
  --symbol BTCUSDT \
  --count 1500 \
  --from "2025-12-01" \
  --markdown
```

### Что смотреть в отчёте:

#### Global metrics:
- **Expectancy R (managed) vs baseline** - средняя доходность с трейлингом vs без
- **ΔExpR** - разница в expectancy (положительная = трейлинг улучшает)
- **WR (managed) vs WR (baseline)** - сравнение win rate
- **Better/Worse shares** - доля сделок, где трейлинг помог/навредил

#### Entry-tag metrics:
- **ΔExpR** по каждому entry_tag
- **share_better/worse** - качество трейлинга для конкретных паттернов

## 2. Новые метрики в TagStats

### Счётчики сравнения managed vs baseline:
```python
@dataclass
class TagStats:
    # ...
    better_count: int = 0  # managed > baseline
    worse_count: int = 0   # managed < baseline
    equal_count: int = 0   # managed ≈ baseline
```

### Метрики в finalize():
```python
# Доли сравнения
share_better: float  # r_managed > r_baseline
share_worse: float   # r_managed < r_baseline
share_equal: float   # r_managed ≈ r_baseline
```

## 3. Интеграция с Postgres (analyze_entry_tags_advanced.py)

Скрипт теперь считает те же метрики для данных из Postgres:
- `delta_expectancy_r`
- `share_better/worse/equal`
- `trailing_delta_expectancy_r`

## 4. Принятие решений по трейлингу

### Критерии оценки:

#### Глобальный уровень (ETH/BTC):
- **ΔExpR_global > 0.05R** и **share_better > 0.55** → трейлинг полезен
- **ΔExpR_global < -0.05R** → трейлинг вреден, рассмотреть отключение

#### По entry_tag:
- **ΔExpR_tag > 0.1R** и **share_better > 0.6** → усиливать трейлинг
- **ΔExpR_tag < -0.1R** → ослаблять/отключать трейлинг

### Настройки трейлинга:

```python
# В symbol specs или конфиге
ETHUSDT:
  default_trailing:
    trail_after_tp1: true
    trail_profile: "rocket_v1"
    trailing_tp1_offset_atr: 0.6

  entry_tags:
    deltaSpikeZ:
      trail_after_tp1: true
      trailing_tp1_offset_atr: 0.5
    pullback_to_fvg:
      trail_after_tp1: false  # отключено по анализу
```

### Действия по результатам анализа:

1. **ETH показывает ΔExpR = +0.08R, share_better = 62%**
   - Оставить текущий трейлинг
   - Возможно протестировать более агрессивный профиль

2. **BTC показывает ΔExpR = -0.12R, share_worse = 58%**
   - Увеличить `trailing_tp1_offset_atr` до 0.8-1.0
   - Или снизить `trailing_share` до 70-80%

3. **Entry tag "pullback" показывает ΔExpR = -0.15R**
   - Отключить трейлинг для этого паттерна

## 5. Мониторинг и итерации

1. **Еженедельно** запускать анализ за последние 1000-2000 сделок
2. **Месячно** анализировать за больший период через Postgres
3. **При изменениях** тестировать на небольшой доле трафика
4. **Документировать** все изменения настроек с обоснованием

## 6. Техническая реализация

### Обновлённые файлы:
- `analytics/tag_stats.py` - новые счётчики better/worse/equal
- `scripts/analyze_trades_from_redis_advanced.py` - обновлённый рендеринг
- `services/entry_tag_analytics.py` - интеграция с Postgres

### Ключевые поля в Trade:
```python
pnl_net: float           # фактический P&L
pnl_if_fixed_exit: float # P&L при фиксации на TP1
one_r_money: float       # размер 1R в деньгах
trailing_started: bool   # был ли запущен трейлинг
trailing_active: bool    # активен ли трейлинг при закрытии
```

Все изменения совместимы с существующей инфраструктурой и не требуют миграций.
