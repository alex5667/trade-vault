# Golden Replay (B5): train==serve parity

## Цель
Детерминированный replay: decision records (NDJSON) → повторный прогон `OFConfirmEngine.build()` → сравнение `OFConfirmV3` (+ опционально экспорт вектора meta-фич).

## Что даёт
- 100% фиксация «train==serve» на уровне runtime (поймает любые расхождения после рефакторов).
- Явный guard от «смешанных» SAFE/STRICT порогов внутри одного файла (policy hash).

## Prereqs
- B4 уже пишет `dq_policy_hash` и `dq_policy_feature_manifest_hash_v1` в indicators/decision record.
- Для сравнения meta-вектора включить экспорт на capture:
  - `GOLDEN_REPLAY_EXPORT_META_FEATURES=1`
  - `GOLDEN_REPLAY_EXPORT_META_FEATURES_MAX=256` (опционально)
- Чтобы replay мог работать даже если outer logger хранит только OFConfirmV3:
  - `GOLDEN_REPLAY_CAPTURE_ENABLE=1` (добавит `golden_replay_inputs_v1` в evidence)

## Run
```bash
python -m ml_analysis.tools.golden_replay_parity_v1 --input decisions.ndjson --outdir out_gr --limit 5000
cat out_gr/golden_replay_report.json | head
```

CI-режим:
```bash
python -m ml_analysis.tools.golden_replay_parity_v1 --input decisions.ndjson --fail-on-mismatch --evidence lite
```

Сравнение meta-фич:
```bash
python -m ml_analysis.tools.golden_replay_parity_v1 --input decisions.ndjson --compare-meta-features --evidence all
```
