#!/bin/bash
TEXT="Корректировка расчета ok_rate в SRE-мониторе (of_gate_sre_monitor.py):

- Добавлена фильтрация метрик через validate_of_gate_row. Системный монитор больше не учитывает мусорные или тестовые строки при расчете процентов.
- Баг с ok_rate: теперь при отсутствии подходящих данных (NoData) или если все события оказались dn_veto, параметр возвращает None (или \"NA\" в логах) вместо жесткого 0.0, что предотвращает ложное срабатывание алертов \"ok_rate_low\".
- Добавлены метрики качества: n_total_raw, n_invalid, а также топ причин попадания в карантин (dq_top)."

docker exec redis-worker-1 redis-cli XADD notify:telegram "*" type report subtype of_gate_update ts_ms "$(date +%s000)" text "$TEXT"
