# Record & Replay Tools

## Обзор

Инструменты для локального прогона записанных ctx через UnifiedSignalPipeline.

## replay_factory.py

Фабрика для сборки пайплайна с mocked зависимостями:

```python
from tools.replay.replay_factory import build_unified_pipeline_for_replay

bundle = build_unified_pipeline_for_replay(logger=logger)
pipeline = bundle.pipeline
publisher = bundle.publisher  # CapturePublisher
```

## capture_publisher.py

Захват сигналов в память + экспорт в JSONL:

```python
from tools.replay.capture_publisher import CapturePublisher

publisher = CapturePublisher(logger=logger)
# ... signals публикуются через publisher.publish(payload)
publisher.dump_jsonl("signals.jsonl")  # 1 сигнал = 1 JSON строка
```

## replay_runner.py

Основной runner для прогона ctx через пайплайн:

```python
from tools.replay.replay_runner import run_replay

result = run_replay(
    input_jsonl="ctx.jsonl",
    logger=logger,
    output_signals_jsonl="signals.jsonl"
)
print(f"Processed: {result.processed}, Published: {result.published}")
```

## replay_cli.py

CLI интерфейс для запуска replay:

```bash
python -m tools.replay.replay_cli --input ctx.jsonl --output signals.jsonl
```

## Использование

### Подготовка данных

1. Запишите ctx с помощью recorder:
   ```bash
   REPLAY_RECORD=1 REPLAY_RECORD_PATH=ctx.jsonl python worker.py
   ```

2. Запустите replay:
   ```bash
   python -m tools.replay.replay_cli --input ctx.jsonl --output signals.jsonl
   ```

3. Создайте golden:
   ```bash
   python -m tools.make_golden --in signals.jsonl --out golden.json
   ```

### Зависимости

Replay использует stub/fallback зависимости:
- StubScoringEngine для скоринга
- NoopGoldenPatternService для golden логики
- NoopExecFiltersGroup для фильтров
- CtxRegimeService для режимов

Для использования реальных зависимостей:
```bash
REPLAY_USE_REAL_DEPS=1 python -m tools.replay.replay_cli ...
```
