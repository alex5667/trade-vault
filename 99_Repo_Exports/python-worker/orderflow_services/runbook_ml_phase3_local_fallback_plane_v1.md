# Phase 3 Runbook: Local Fallback Plane (Gateway & Ollama)

## Цель
Обеспечение отказоустойчивости инференса LLM при деградации внешних провайдеров (Vertex AI). Использует локальный инстанс Ollama на RTX 3060.

## Модели
- **General / Code**: `deepseek-r1:14b` (quantized)

## Команды подготовки (на хосте)
```bash
ollama pull deepseek-r1:14b
```

## Режимы (ML_LOCAL_FALLBACK_MODE)
1. **DISABLED**: Обработка запросов отключена.
2. **FALLBACK_ONLY**: Обработка только если `vertex_unavailable=1` в запросе.
3. **LOCAL_ONLY**: Все входящие запросы обрабатываются локально (режим отладки/теста).

## Smoke Check
```bash
# Добавить тестовый запрос
redis-cli XADD stream:ml:local_fallback_requests * \
  request_id lf-1 \
  task_type emergency_summarize \
  severity critical \
  vertex_unavailable 1 \
  prompt "Summarize: service degraded, Vertex unavailable, fallback engaged."

# Проверить результат
redis-cli XREVRANGE stream:ml:local_fallback_results + - COUNT 1
# Проверить метрики
curl -s localhost:9916/metrics | grep accepted
```

## Алерты
- `MLLocalFallbackPlaneStale`: Гейтвей не обрабатывает сообщения. Проверьте логи контейнера.
- `MLLocalFallbackPlaneErrors`: Высокий уровень ошибок. Проверьте связь с `host.docker.internal:11434` и загруженность GPU.
- `MLLocalFallbackPlaneRejectionsSpike`: Много отклоненных запросов. Проверьте `task_allowlist` и длины промптов.
