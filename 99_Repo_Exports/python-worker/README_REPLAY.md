# Record & Replay (6.2)

## 1) Запись

Запись делается в `BaseOrderFlowHandler` на bucket boundary (после `_attach_modular_services_data(ctx)`).

### Минимальный набор env

```bash
export REPLAY_RECORD=1
export REPLAY_RECORD_PATH=/tmp/replay.jsonl
export REPLAY_RECORD_TYPES=ctx,signal
export REPLAY_RECORD_CTX_MODE=compact   # или full, если нужно больше полей
```

### Для строгих golden (опционально)

```bash
export REPLAY_STABLE_SIGNAL_ID=1
```

## 2) Replay локально

```bash
python -m tools.replay_local \
  --in /tmp/replay.jsonl --type ctx \
  --factory python_worker.handlers.replay_factory:create_adapter \
  --print_samples 3
```

## 3) Golden

1) Сгенерировать golden по записанным сигналам

```bash
python -m tools.make_golden --in /tmp/replay.jsonl --out /tmp/golden.json --samples 3
```

2) Прогнать replay и сравнить:

```bash
python -m tools.replay_local \
  --in /tmp/replay.jsonl --type ctx \
  --factory python_worker.handlers.replay_factory:create_adapter \
  --golden /tmp/golden.json
```

## 4) Важно про replay_factory

Файл `python-worker/handlers/replay_factory.py` — точка интеграции.
Сейчас он содержит TemplateAdapter (no-op). Его нужно заменить на реальный adapter,
который:

- создаёт in-memory outbox (OutboxCapture)
- создаёт emitter, который пишет в этот outbox
- делает process_ctx(payload):

ctx = SimpleNamespace(**payload) или OrderflowContext(**payload)

pipeline.process(ctx) (или ваш вызов)

возвращает adapter с .outbox
