# 🚀 Применение настроек GPU оптимизации

## ✅ Переменные окружения установлены

В файле `docker-compose.yml` для сервиса `multi-symbol-orderflow` установлены:

```yaml
- CANDLE_BATCH_SIZE=5
- CANDLE_BATCH_INTERVAL_SEC=2.0
- GPU_ENABLED=true
```

---

## 📋 Быстрый старт

### 1. Применить изменения:

```bash
# Перезапустить сервис
docker compose restart multi-symbol-orderflow

# Или полный перезапуск (если нужно)
docker compose down && docker compose up -d multi-symbol-orderflow
```

### 2. Проверить применение:

```bash
# Проверить переменные
docker exec scanner_infra-multi-symbol-orderflow-1 env | grep CANDLE

# Проверить логи
docker logs scanner_infra-multi-symbol-orderflow-1 | grep -E "(GPU|batch)"
```

### 3. Мониторить GPU:

```bash
watch -n 2 nvidia-smi
```

---

## 🎯 Ожидаемый результат

После применения настроек:
- Батч будет обрабатываться каждые **2 секунды** (вместо 5)
- Размер батча **5 свечей** (вместо 10)
- Использование GPU должно увеличиться до **30-50%**

---

**Готово!** Переменные установлены. Осталось перезапустить сервис.

