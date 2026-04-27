# Примеры NDJSON строк для проверки консистентности

## 1. Примеры из stream:of:inputs (OFInputsV1)

### Пример 1: Reversal scenario

```json
{"v":1,"symbol":"BTCUSDT","ts_ms":1704067200000,"regime":"trend","direction":"LONG","scenario":"reversal","delta_z":2.5,"weak_progress":1,"sweep_recent":1,"reclaim_recent":0,"obi_stable":1,"iceberg_strict":0,"abs_lvl_ok":1,"trend_dir":"NONE","hidden_ctx_recent":0,"cont_ctx_recent":0,"cfg":{"min_delta_z":1.5,"require_sweep":true,"require_obi":true},"fp_eff_quote":43250.5,"fp_quote_delta":12.3,"fp_move_bp":0.28,"sid":"sig_abc123"}
```

### Пример 2: Continuation scenario

```json
{"v":1,"symbol":"ETHUSDT","ts_ms":1704067201000,"regime":"trend","direction":"SHORT","scenario":"continuation","delta_z":1.8,"weak_progress":0,"sweep_recent":0,"reclaim_recent":0,"obi_stable":0,"iceberg_strict":0,"abs_lvl_ok":0,"trend_dir":"SHORT","hidden_ctx_recent":1,"cont_ctx_recent":1,"cfg":{"min_delta_z":1.2,"require_trend_dir":true},"fp_eff_quote":2650.75,"fp_quote_delta":-5.2,"fp_move_bp":-0.20,"sid":"sig_def456"}
```

### Пример 3: Range regime

```json
{"v":1,"symbol":"SOLUSDT","ts_ms":1704067202000,"regime":"range","direction":"LONG","scenario":"reversal","delta_z":1.2,"weak_progress":1,"sweep_recent":1,"reclaim_recent":1,"obi_stable":1,"iceberg_strict":1,"abs_lvl_ok":1,"trend_dir":"NONE","hidden_ctx_recent":0,"cont_ctx_recent":0,"cfg":{"min_delta_z":1.0,"require_iceberg":true,"range_mode":true},"fp_eff_quote":98.45,"fp_quote_delta":0.15,"fp_move_bp":0.15,"sid":"sig_ghi789"}
```

### Пример 4: Thin regime (минимальный набор)

```json
{"v":1,"symbol":"ADAUSDT","ts_ms":1704067203000,"regime":"thin","direction":"SHORT","scenario":"reversal","delta_z":0.8,"weak_progress":0,"sweep_recent":0,"reclaim_recent":0,"obi_stable":0,"iceberg_strict":0,"abs_lvl_ok":0,"trend_dir":"NONE","hidden_ctx_recent":0,"cont_ctx_recent":0,"cfg":{"min_delta_z":0.5,"thin_mode":true},"fp_eff_quote":0.485,"fp_quote_delta":-0.001,"fp_move_bp":-0.21,"sid":"sig_jkl012"}
```

### Пример 5: С расширенными полями (если есть)

```json
{"v":1,"symbol":"BNBUSDT","ts_ms":1704067204000,"regime":"trend","direction":"LONG","scenario":"reversal","delta_z":3.2,"weak_progress":1,"sweep_recent":1,"reclaim_recent":1,"obi_stable":1,"iceberg_strict":1,"abs_lvl_ok":1,"trend_dir":"NONE","hidden_ctx_recent":0,"cont_ctx_recent":0,"cfg":{"min_delta_z":2.0,"require_sweep":true,"require_reclaim":true,"require_obi":true,"require_iceberg":true},"fp_eff_quote":315.8,"fp_quote_delta":2.1,"fp_move_bp":0.66,"sid":"sig_mno345","book_churn_hi":0,"sweep_kind":"aggressive","reclaim_kind":"partial","obi":1.05,"obi_stable_secs":3.0,"iceberg_score":0.95}
```

## 2. Примеры из replay output (of_engine_replay_from_inputs.py)

### Пример 1: Successful confirmation (reversal)

```json
{"v":3,"symbol":"BTCUSDT","ts_ms":1704067200000,"direction":"LONG","scenario":"reversal","ok":1,"score":0.85,"have":3,"need":2,"gate_bits":7,"reason":"A3_B2_C2","evidence":{"sweep_age_ms":150,"reclaim_age_ms":0,"obi_age_ms":200,"iceberg_age_ms":0,"delta_z":2.5,"fp_eff_quote":43250.5,"fp_move_bp":0.28},"contrib":{"delta_z":0.35,"sweep":0.25,"obi":0.15,"abs_lvl":0.10},"sid":"sig_abc123","legs_detail":{"A":{"delta_z_ok":true,"weak_progress":true,"abs_lvl_ok":true},"B":{"sweep_ok":true,"reclaim_ok":false},"C":{"obi_ok":true,"iceberg_ok":false}}}
```

### Пример 2: Veto (insufficient gates)

```json
{"v":3,"symbol":"ETHUSDT","ts_ms":1704067201000,"direction":"SHORT","scenario":"continuation","ok":0,"score":0.45,"have":1,"need":2,"gate_bits":1,"reason":"A_ONLY_NEED_2","evidence":{"trend_dir":"SHORT","hidden_ctx_age_ms":300,"cont_ctx_age_ms":250,"delta_z":1.8,"fp_eff_quote":2650.75,"fp_move_bp":-0.20},"contrib":{"delta_z":0.30,"trend_dir":0.15},"sid":"sig_def456","legs_detail":{"A":{"delta_z_ok":true,"weak_progress":false},"B":{"sweep_ok":false,"reclaim_ok":false},"C":{"obi_ok":false,"iceberg_ok":false}}}
```

### Пример 3: Successful confirmation (all gates passed)

```json
{"v":3,"symbol":"SOLUSDT","ts_ms":1704067202000,"direction":"LONG","scenario":"reversal","ok":1,"score":0.92,"have":4,"need":2,"gate_bits":15,"reason":"A3_B2_C2_D1","evidence":{"sweep_age_ms":100,"reclaim_age_ms":120,"obi_age_ms":80,"iceberg_age_ms":90,"delta_z":1.2,"fp_eff_quote":98.45,"fp_move_bp":0.15,"abs_lvl_score":0.95},"contrib":{"delta_z":0.25,"sweep":0.20,"reclaim":0.15,"obi":0.15,"iceberg":0.10,"abs_lvl":0.07},"sid":"sig_ghi789","legs_detail":{"A":{"delta_z_ok":true,"weak_progress":true,"abs_lvl_ok":true},"B":{"sweep_ok":true,"reclaim_ok":true},"C":{"obi_ok":true,"iceberg_ok":true},"D":{"abs_lvl_ok":true}}}
```

### Пример 4: Veto (low score despite gates)

```json
{"v":3,"symbol":"ADAUSDT","ts_ms":1704067203000,"direction":"SHORT","scenario":"reversal","ok":0,"score":0.35,"have":1,"need":2,"gate_bits":1,"reason":"A_ONLY_LOW_SCORE","evidence":{"delta_z":0.8,"fp_eff_quote":0.485,"fp_move_bp":-0.21,"thin_regime":true},"contrib":{"delta_z":0.20},"sid":"sig_jkl012","legs_detail":{"A":{"delta_z_ok":true,"weak_progress":false},"B":{"sweep_ok":false,"reclaim_ok":false},"C":{"obi_ok":false,"iceberg_ok":false}}}
```

### Пример 5: Continuation with trend

```json
{"v":3,"symbol":"BNBUSDT","ts_ms":1704067204000,"direction":"LONG","scenario":"continuation","ok":1,"score":0.78,"have":2,"need":2,"gate_bits":5,"reason":"A1_B0_C0_TREND","evidence":{"trend_dir":"LONG","hidden_ctx_age_ms":180,"cont_ctx_age_ms":200,"delta_z":3.2,"fp_eff_quote":315.8,"fp_move_bp":0.66},"contrib":{"delta_z":0.40,"trend_dir":0.25,"hidden_ctx":0.13},"sid":"sig_mno345","legs_detail":{"A":{"delta_z_ok":true,"weak_progress":false},"B":{"sweep_ok":false,"reclaim_ok":false},"C":{"obi_ok":true,"iceberg_ok":false}}}
```

## 3. Примеры из events:trades export (export_trade_closed_ndjson.py)

### Пример 1: Successful trade closure (profit)

```json
{"event_type":"POSITION_CLOSED","symbol":"BTCUSDT","ts_ms":1704067205000,"direction":"LONG","entry_price":43200.0,"exit_price":43280.0,"quantity":0.01,"pnl":0.8,"pnl_bps":1.85,"duration_ms":45000,"close_reason":"TP","signal_id":"sig_abc123","regime":"trend","of_confirm_ok":1,"of_confirm_score":0.85,"gate_bits":7,"entry_ts_ms":1704067160000}
```

### Пример 2: Trade closure (stop loss)

```json
{"event_type":"POSITION_CLOSED","symbol":"ETHUSDT","ts_ms":1704067206000,"direction":"SHORT","entry_price":2655.0,"exit_price":2660.0,"quantity":0.1,"pnl":-0.5,"pnl_bps":-1.88,"duration_ms":30000,"close_reason":"SL","signal_id":"sig_def456","regime":"trend","of_confirm_ok":0,"of_confirm_score":0.45,"gate_bits":1,"entry_ts_ms":1704067176000}
```

### Пример 3: Trade closure (time-based)

```json
{"event_type":"POSITION_CLOSED","symbol":"SOLUSDT","ts_ms":1704067207000,"direction":"LONG","entry_price":98.40,"exit_price":98.55,"quantity":1.0,"pnl":0.15,"pnl_bps":1.52,"duration_ms":120000,"close_reason":"TIME","signal_id":"sig_ghi789","regime":"range","of_confirm_ok":1,"of_confirm_score":0.92,"gate_bits":15,"entry_ts_ms":1704067087000}
```

### Пример 4: Trade closure (manual)

```json
{"event_type":"POSITION_CLOSED","symbol":"ADAUSDT","ts_ms":1704067208000,"direction":"SHORT","entry_price":0.485,"exit_price":0.484,"quantity":100.0,"pnl":0.1,"pnl_bps":0.21,"duration_ms":60000,"close_reason":"MANUAL","signal_id":"sig_jkl012","regime":"thin","of_confirm_ok":0,"of_confirm_score":0.35,"gate_bits":1,"entry_ts_ms":1704067148000}
```

### Пример 5: Trade closure (trailing stop)

```json
{"event_type":"POSITION_CLOSED","symbol":"BNBUSDT","ts_ms":1704067209000,"direction":"LONG","entry_price":315.5,"exit_price":316.2,"quantity":0.5,"pnl":0.35,"pnl_bps":2.22,"duration_ms":90000,"close_reason":"TRAILING","signal_id":"sig_mno345","regime":"trend","of_confirm_ok":1,"of_confirm_score":0.78,"gate_bits":5,"entry_ts_ms":1704067119000,"trailing_distance_bps":5.0}
```

## Проверка консистентности

### Ключевые поля для проверки:

1. **sid (signal_id)**: должен совпадать во всех трех источниках
   - `stream:of:inputs`: поле `sid` в OFInputsV1
   - `replay output`: поле `sid` в OFConfirmV3
   - `events:trades`: поле `signal_id` в trade event

2. **ts_ms**: должен быть близок (может отличаться на несколько миллисекунд из-за асинхронности)

3. **symbol**: должен совпадать

4. **direction**: должен совпадать

5. **Фичи и лейблы:**
   - `delta_z`, `sweep_recent`, `reclaim_recent`, `obi_stable`, `iceberg_strict` в inputs
   - Должны соответствовать `evidence` и `legs_detail` в replay output
   - `of_confirm_ok`, `of_confirm_score`, `gate_bits` в replay output
   - Должны соответствовать полям в trade event

### Пример проверки (Python):

```python
import json

# Загрузить inputs
with open("of_inputs.ndjson") as f:
    inputs = [json.loads(line) for line in f if line.strip()]

# Загрузить replay output
with open("replay.ndjson") as f:
    replay = {r["sid"]: r for r in [json.loads(line) for line in f if line.strip()]}

# Загрузить trades
with open("trades.ndjson") as f:
    trades = {t["signal_id"]: t for t in [json.loads(line) for line in f if line.strip()]}

# Проверить консистентность
for inp in inputs:
    sid = inp.get("sid")
    if not sid:
        continue
    
    r = replay.get(sid)
    t = trades.get(sid)
    
    if r and t:
        # Проверка совпадения
        assert inp["symbol"] == r["symbol"] == t["symbol"]
        assert inp["direction"] == r["direction"] == t["direction"]
        assert abs(inp["ts_ms"] - r["ts_ms"]) < 1000  # в пределах 1 секунды
        assert r["ok"] == t.get("of_confirm_ok", -1)
        assert abs(r["score"] - t.get("of_confirm_score", -1)) < 0.01
        assert r["gate_bits"] == t.get("gate_bits", -1)
        print(f"✓ {sid}: консистентен")
    else:
        print(f"✗ {sid}: отсутствует в replay или trades")
```

## Формат NDJSON

**Важно:** NDJSON (Newline Delimited JSON) - это формат, где каждый JSON объект находится на отдельной строке, без запятых между объектами и без внешних квадратных скобок.

**Правильный формат:**
```
{"key1":"value1"}
{"key2":"value2"}
{"key3":"value3"}
```

**Неправильный формат:**
```
[
  {"key1":"value1"},
  {"key2":"value2"}
]
```

Каждая строка должна быть валидным JSON объектом, и строки разделяются символом новой строки (`\n`).

