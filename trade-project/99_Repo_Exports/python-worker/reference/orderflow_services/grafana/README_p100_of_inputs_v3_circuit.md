# Grafana: OFInputs V3 Circuit (P100)

Dashboard JSON: `of_inputs_v3_circuit_p100.json`

Что показывает:
- Сколько символов сейчас отключены для V3 (Redis cfg:of_inputs:v3_disabled:*).
- Суммарные даунгрейды в текущем окне по причинам (ZSET state:of_inputs:v3_downgrades:{reason}:{sym}).
- Возраст последнего poll exporter's state (health сигнал, чтобы не потерять наблюдаемость).

Источник метрик:
- `orderflow_services/of_inputs_v3_circuit_state_exporter_p100.py`

Рекомендация:
- В Prometheus лучше скрапить exporter отдельным job'ом (например job="of_inputs_v3_circuit").

Доп. метрика для hysteresis:
- `of_inputs_v3_circuit_cfg_hard_disabled_until_ms{symbol,reason}` — конец hard-disable фазы (до cooldown).
