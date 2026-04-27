# Reliability Calibration Analysis Guide

Этот гайд объясняет как анализировать данные калибровки надежности сигналов, собранные через `reliability_calibrator.py`.

## 📊 Что измеряется

Система собирает данные о том, насколько точно confidence score (0-100%) предсказывает реальную вероятность успеха сигнала.

### Outcomes (что измеряется):
- **`tp1`** - TP1 достигнута (entry quality - базовый)
- **`tp2`** - TP2 достигнута (компромисс entry quality)
- **`win`** - P&L > 0 (финансовый результат)
- **`nosl_after_tp1`** - TP1 достигнута И сделка закрыта НЕ по SL (management quality)
- **`nosl_after_tp1_t500`** - То же + trade survived ≥500ms после TP1 (strict short-term hold)
- **`nosl_after_tp1_t2000`** - То же + trade survived ≥2000ms после TP1 (strict long-term hold)

## 🔍 Как анализировать данные

### 1. Быстрый анализ через скрипт

```bash
cd python-worker/scripts
python analyze_reliability_calibration.py
```

**Что показывает:**
- Общую статистику по outcomes
- Калибровочные кривые (confidence → hit rate)
- Лучшие конфигурации
- Сравнение по символам

### 2. Экспорт данных для детального анализа

```bash
python export_reliability_calibration.py
```

**Создает файлы:**
- `relcal_summary.csv` - сводка по конфигурациям
- `relcal_buckets.csv` - данные по confidence buckets
- `relcal_outcomes.csv` - агрегированная статистика outcomes
- `calibration_curve_*.csv` - кривые для построения графиков

### 3. Визуализация (опционально)

```bash
pip install matplotlib pandas
python plot_reliability_calibration.py
```

**Создает графики:**
- `calibration_curves.png` - все кривые на одном графике
- `outcome_comparison.png` - сравнение outcomes

## 📈 Интерпретация результатов

### Калибровочная кривая

```
Идеальная калибровка: прямая линия y=x
Над диагональю: underconfident (консервативные оценки)
Под диагональю: overconfident (оптимистичные оценки)
```

### Примеры интерпретации

```
✅ Хорошая калибровка:
   confidence 50% → hit rate 48% (близко к диагонали)

❌ Overconfident:
   confidence 80% → hit rate 45% (значительно ниже)

❌ Underconfident:
   confidence 30% → hit rate 65% (значительно выше)
```

### Сравнение outcomes

```python
# Ожидаемые паттерны:
tp2_hit_rate > nosl_after_tp1_hit_rate  # TP2 проще достичь чем удержать
nosl_after_tp1 > nosl_after_tp1_t500    # Strict outcomes фильтруют шум
nosl_after_tp1_t500 > nosl_after_tp1_t2000  # Дольше = сложнее
```

## 💡 Практическое применение

### 1. Выбор outcome для разных стратегий

```python
# Быстрые скальперские сигналы
if confidence > 70 and cal.get_hit_rate('nosl_after_tp1_t500', confidence) > 0.6:
    trade()

# Трендовые сигналы с удержанием
if confidence > 60 and cal.get_hit_rate('nosl_after_tp1_t2000', confidence) > 0.5:
    trade()
```

### 2. Калибровка confidence scores

```python
# Если модель overconfident на 80% confidence
calibrated_confidence = adjust_for_overconfidence(confidence)

# Если модель underconfident
calibrated_confidence = adjust_for_underconfidence(confidence)
```

### 3. Оптимизация порогов

```python
# Вместо фиксированного порога 70%
dynamic_threshold = find_confidence_where_hit_rate_exceeds(0.6, outcome)
```

## 🔧 Технические детали

### Структура данных в Redis

```
Key: relcal:{outcome}:{kind}:{symbol}:{venue}:{session}:{tf}:{regime}
Hash fields:
  samples_total    - общее количество samples
  hits_total       - общее количество hits
  b{bucket}:n      - samples в confidence bucket (0,5,10,...,100)
  b{bucket}:h      - hits в confidence bucket
  last_ts_ms       - timestamp последнего обновления
```

### Confidence bucketing

```python
# Confidence 42% попадает в bucket 40 (step=5)
bucket = (42 // 5) * 5  # = 40
```

### TTL и cleanup

- Данные хранятся 30 дней (`REL_CAL_TTL_SEC=2592000`)
- Автоматическая очистка старых данных

## 🎯 Следующие шаги

1. **Накопите данные** - подождите 1-2 недели работы системы
2. **Проанализируйте кривые** - найдите over/underconfidence
3. **Скорректируйте confidence scores** в сигналах
4. **Настройте пороги** на основе калибровочных данных
5. **Повторяйте анализ** регулярно для отслеживания дрейфа

## 🚨 Важные замечания

- **Strict outcomes** (t500, t2000) требуют больше данных для надежной статистики
- **Sample size** критичен - не доверяйте выводам с <100 samples в bucket
- **Market conditions** влияют - переоценивайте кривые при изменении волатильности
- **Outcome selection** зависит от стратегии - tp2 для entry, nosl_after_tp1* для management

## 📞 Troubleshooting

**Нет данных в Redis:**
```bash
# Проверьте настройки
echo $REL_CAL_ENABLED  # Должно быть "1"
echo $REL_CAL_OUTCOMES # Проверьте список outcomes

# Проверьте логи
grep "relcal" logs/*.log
```

**Мало данных в buckets:**
- Увеличьте `REL_CAL_BUCKET_STEP_PCT` (с 5 до 10)
- Дайте системе больше времени на накопление данных

**Кривые выглядят странно:**
- Проверьте `REL_CAL_USE_*_DIM` - возможно слишком много dimensions
- Убедитесь что outcomes корректно рассчитываются
