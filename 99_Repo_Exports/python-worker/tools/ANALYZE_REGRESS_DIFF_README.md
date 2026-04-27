# Regression Diff Analyzer

Комплексный инструмент для анализа расхождений в регрессионных тестах гейта подтверждения.

## Возможности

1. **Анализ mismatch_by_type_top** - показывает конкретные переходы значений (old->new)
2. **Проверка изменений в коде** - анализирует git history для релевантных файлов
3. **Анализ scenario/reason расхождений** - детектирует проблемы в логике гейта
4. **Предложение обновления baseline** - рекомендует обновить baseline если расхождения только в score и малы

## Использование

### Базовое использование (найти последний diff.json)

```bash
python -m tools.analyze_regress_diff --find-latest
```

### С указанием конкретного diff.json

```bash
python -m tools.analyze_regress_diff --diff /var/lib/trade/of_reports/out/regress_20250101_120000/diff.json
```

### С указанием baseline и candidate для детального анализа

```bash
python -m tools.analyze_regress_diff \
  --diff /path/to/diff.json \
  --baseline /path/to/baseline.ndjson \
  --candidate /path/to/candidate.ndjson
```

## Пример вывода

```
================================================================================
REGRESSION DIFF ANALYSIS
================================================================================

Summary:
  Overlap: 23,187 rows
  Total mismatches: 417
  Mismatch rate: 1.80%

Mismatches by field:
  score: 414
  need: 1
  scenario: 1
  reason: 1

================================================================================
MISMATCH TYPE TRANSITIONS (mismatch_by_type_top)
================================================================================

Score delta statistics:
  Mean absolute delta: 0.00001234
  Max absolute delta: 0.00012345
  Mean delta: 0.00000123
  Median delta: 0.00000098
  P95 delta: 0.00004567
  Min: -0.00012345, Max: 0.00012345

Top score transitions:
  score:0.123456->0.123467: 45
  score:0.234567->0.234578: 38
  ...

================================================================================
CODE CHANGES CHECK
================================================================================

Found 5 relevant commits (last 7 days):
  abc1234 Fix score calculation in ml_confirm_gate
  def5678 Update calibration parameters
  ...

Changes by file:
  python-worker/services/ml_confirm_gate.py: 3 commits
  python-worker/services/of_confirm_service.py: 2 commits

================================================================================
SCENARIO/REASON MISMATCH ANALYSIS (Gate Logic)
================================================================================

⚠️  WARNING: Gate logic issues detected!
  Scenario mismatches: 1
  Reason mismatches: 1

Top scenarios with mismatches:
  continuation: 345
  none: 72

Sample mismatches:
  Sample 1:
    Key: BTCUSDT|1704067200000|LONG
    Baseline: scenario=continuation, reason=ml_allow(score=0.123), ok=1
    Candidate: scenario=continuation, reason=ml_allow(score=0.124), ok=1

================================================================================
BASELINE UPDATE SUGGESTION
================================================================================

Analysis:
  Only score mismatches: True
  Small score deltas: True
  Mismatch rate: 1.80%

✓ SUGGESTION: Update baseline (confidence: high)
  Reason: Only score mismatches (414), small deltas (mean=0.00001234, max=0.00012345), low rate (1.80%)

  To update baseline:
    python -m tools.propose_baseline_update
    # or manually copy candidate to baseline
```

## Интерпретация результатов

### Score mismatches только

Если все расхождения только в `score` и они малы (mean < 0.01, max < 0.1), это обычно означает:
- Незначительные изменения в ML-модели или калибровке
- Различия в округлении/точности вычислений
- **Рекомендация**: Обновить baseline

### Scenario/Reason mismatches

Если есть расхождения в `scenario` или `reason`:
- **Критично**: Проблема в логике гейта
- Возможные причины:
  - Изменения в логике определения сценария
  - Изменения в порогах принятия решений
  - Баги в коде
- **Рекомендация**: НЕ обновлять baseline, расследовать причину

### Высокий mismatch rate (>5%)

Даже если только score:
- Возможны значительные изменения в модели
- **Рекомендация**: Проверить изменения в коде/конфигурации перед обновлением baseline

## Интеграция с CI/CD

Можно использовать в pipeline для автоматического анализа:

```bash
# В nightly regression test
python -m tools.nightly_regress_engine_replay_safe

# После теста - анализ
python -m tools.analyze_regress_diff --find-latest > regress_analysis.txt

# Проверить suggestion
if grep -q "should_update.*True" regress_analysis.txt; then
  echo "Baseline update recommended"
fi
```

## Переменные окружения

- `OUT_DIR` - директория для поиска diff.json (default: `/var/lib/trade/of_reports/out`)

