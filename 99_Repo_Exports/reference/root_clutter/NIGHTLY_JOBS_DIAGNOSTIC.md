# Диагностика ночных джобов (02.02.2025)

## Выполнено вручную и проверено

### 1. of-nightly-calibrate (03:30 UTC)

**Статус:** ⚠️ SKIPPED - `no_calibration_result`

**Причина:**
- Джоба выполняется, но калибровка не находит оптимальных параметров
- Калибровка требует:
  - `pass_rate >= 0.15` (минимум 15% сделок должны пройти gate)
  - `tail_pass <= 0.18` (максимум 18% tail loss среди прошедших)
  - Максимизация `meanR_pass`

**Проблема:**
- В dataset либо все сделки имеют `ok=0` (не прошли gate) 
- Либо сделки еще не закрыты (`r_mult=0` для всех)
- Без закрытых сделок, которые прошли gate, калибровка не может найти параметры, удовлетворяющие ограничениям

**Последние запуски:**
- `nightly_20260204_182932`: 17121 inputs → dataset 174KB → SKIPPED (no_calibration_result)
- `nightly_20260204_180815`: 13879 inputs → dataset 161KB → SKIPPED (no_calibration_result)
- `nightly_20260204_180501`: 13315 inputs → dataset 281KB → SKIPPED (no_calibration_result)

**Рекомендации:**
1. Проверить, что сделки закрываются и записываются в `events:trades` stream
2. Проверить, что `build_of_dataset` правильно джойнит replay с trades по `sid`
3. Возможно, нужно увеличить `--since-hours` для получения большего количества закрытых сделок
4. Проверить логику gate - возможно, слишком строгие параметры блокируют все сделки

---

### 2. of-nightly-meta-train (04:10 UTC)

**Статус:** ⚠️ SKIPPED - `dataset_too_small`

**Причина:**
- Dataset после джойна с trades слишком мал
- Требуется минимум 300 строк для стабильного обучения LR модели
- Последний запуск: 286 строк (недостаточно)

**Последние запуски:**
- `nightly_meta_20260204_183602`: 19195 inputs → dataset 320 строк → SUCCESS ✅
- `nightly_meta_20260204_181435`: 14634 inputs → dataset 308 строк → SUCCESS ✅
- Ручной запуск: 286 строк → SKIPPED (dataset_too_small)

**Рекомендации:**
1. Увеличить `--since-hours` (сейчас 72 часа по умолчанию)
2. Проверить, что trades правильно джойнятся с replay по `sid`
3. Возможно, нужно расширить `CANARY_SYMBOLS` для получения больше данных

---

### 3. regress-safe (02:20 UTC)

**Статус:** ⚠️ FAILED - `mismatches=417 > max=0`

**Причина:**
- Джоба работает корректно, но обнаруживает расхождения с baseline
- 417 mismatches при max=0 означает, что engine replay дает другие результаты, чем baseline
- Это ожидаемое поведение при изменении логики engine

**Последние запуски:**
- `regress_safe_20260204_184444`: FAILED (mismatches=417 > max=0)
- `regress_safe_20260204_162325`: FAILED (mismatches=417 > max=0)
- Baseline файлы существуют: `/var/lib/trade/of_reports/baselines/inputs_canary.ndjson` (23187 строк)

**Что происходит:**
1. Джоба запускает engine replay на baseline inputs
2. Сравнивает результат с baseline output
3. Обнаруживает 417 расхождений
4. Создает emergency bundle для отключения ENFORCE mode (meta_model_mode=SHADOW)
5. Отправляет alert в Telegram

**Рекомендации:**
1. Если изменения в engine были намеренными - нужно обновить baseline:
   ```bash
   # Сгенерировать новый baseline
   docker exec scanner-of-timers-worker python3 -m tools.of_engine_replay_from_inputs \
     --inputs /var/lib/trade/of_reports/baselines/inputs_canary.ndjson \
     --out /var/lib/trade/of_reports/baselines/baseline_new.ndjson
   ```
2. Если изменения нежелательны - нужно откатить изменения в engine
3. Можно временно увеличить `REGRESS_MAX_MISMATCHES` для допущения некоторого количества расхождений

---

## Общие проблемы

### Пустые директории от 02.02.2025

**Причина:**
- Директории создавались, но джобы завершались с ошибками на ранних этапах
- Файлы не создавались из-за раннего выхода (`SystemExit`)

**Решение:**
- Джобы теперь создают `status.json` даже при ошибках
- Проверять `status.json` для понимания причины пустых директорий

---

## Команды для ручного запуска

```bash
# Калибровка
make of-nightly-calibrate-manual

# Meta train
make of-nightly-meta-train-manual

# Regress safe
make regress-safe-manual
```

---

## Следующие шаги

1. ✅ **Выполнено:** Ручной запуск всех трех джобов
2. ✅ **Выполнено:** Диагностика причин пустых результатов
3. 🔄 **Требуется:** Исправить проблемы с данными (trades не закрываются или не джойнятся)
4. 🔄 **Требуется:** Обновить baseline для regress-safe или исправить engine изменения

