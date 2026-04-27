# Runbook: OFInputs V3 circuit breaker (P100)

## 1) Симптомы

- Prometheus alert `OFInputsV3CircuitDisabledAny|Many`.
- В логе tick path / of_inputs пайплайна увеличивается V3→V2 fallback.
- Auto-apply блокируется (fail-closed) ключами `cfg:of_inputs_v3:auto_apply_block_*`.

## 2) Быстрый triage (Redis)

### 2.1 Список отключённых символов

```bash
redis-cli --scan --pattern 'cfg:of_inputs:v3_disabled:*'
```

### 2.2 Посмотреть причину и until_ms

```bash
# пример
redis-cli GET 'cfg:of_inputs:v3_disabled:BTCUSDT'
redis-cli PTTL 'cfg:of_inputs:v3_disabled:BTCUSDT'
```

Ожидаемый JSON (пример):

```json
{"until_ms":1700000000000,"hard_until_ms":1699999999500,"cooldown_ms":60000,"reason":"seq_gap","trip_ts_ms":1699999999000,"count":3,"window_ms":60000}
```

### 2.3 Проверить счётчики даунгрейдов по причинам

ZSET ключи windowed (старые элементы удаляются при записи):

```bash
# Все причины/символы, по которым были даунгрейды
redis-cli --scan --pattern 'state:of_inputs:v3_downgrades:*'

# Для конкретного символа/причины
redis-cli ZCARD 'state:of_inputs:v3_downgrades:seq_gap:BTCUSDT'
redis-cli ZRANGE 'state:of_inputs:v3_downgrades:seq_gap:BTCUSDT' 0 -1
```

## 3) Проверка auto-apply блоков

Глобальный блок:

```bash
redis-cli --scan --pattern 'cfg:of_inputs_v3:auto_apply_block_global:*'
redis-cli GET  'cfg:of_inputs_v3:auto_apply_block_global:of_inputs_v3'
redis-cli PTTL 'cfg:of_inputs_v3:auto_apply_block_global:of_inputs_v3'
```

Per-symbol блок:

```bash
redis-cli --scan --pattern 'cfg:of_inputs_v3:auto_apply_block:*'
redis-cli GET  'cfg:of_inputs_v3:auto_apply_block:BTCUSDT:of_inputs_v3'
redis-cli PTTL 'cfg:of_inputs_v3:auto_apply_block:BTCUSDT:of_inputs_v3'
```

## 4) Что делать

### 4.1 Если это кратковременный спайк

Ничего не делать: ключ `cfg:of_inputs:v3_disabled:{sym}` истечёт сам (TTL по `disable_ms + cooldown_ms`, default 5+1 минут).

### 4.2 Если нужно досрочно включить V3 (осознанно)

```bash
redis-cli DEL 'cfg:of_inputs:v3_disabled:BTCUSDT'
```

Важно: включать досрочно только после устранения причины (book lag / missing fields / seq gaps), иначе символ снова отключится.

### 4.3 Если это системная деградация

- Сохранить fail-closed: оставить global auto-apply block.
- Проверить качество/свежесть V3 LOB snapshot в источнике, сетевые лаги, и publish path.
- Проверить Redis latency/CPU и ошибки сериализации.

## 5) Prometheus/Grafana

- Экспортёр состояния: `orderflow_services/of_inputs_v3_circuit_state_exporter_p100.py`.
- Dashboard: `orderflow_services/grafana/of_inputs_v3_circuit_p100.json`.
- Alerts: `orderflow_services/prometheus_alerts_of_inputs_v3_circuit_p100.yml`.

### Prometheus scrape target (exporter)

If you use the standalone exporter `of-inputs-v3-circuit-exporter` (port 9164), add a scrape job:

```yaml
scrape_configs:
  - job_name: of_inputs_v3_circuit
    static_configs:
      - targets: ['of-inputs-v3-circuit-exporter:9164']
```

## 6) Контракты и детерминизм

- Disable state хранится в `cfg:` (а не `state:`), чтобы survive любые reset'ы стейта.
- Downgrades ZSET разнесены по `reason`, чтобы агрегации были O(1) через `ZCARD/ZCOUNT`.
