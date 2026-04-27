from utils.time_utils import get_ny_time_millis
import redis
import time
import sys

r = redis.Redis.from_url('redis://localhost:6379/0')
text = '''Корректировка расчета ok_rate в SRE-мониторе (of_gate_sre_monitor.py):

Добавлена фильтрация метрик через validate_of_gate_row. Системный монитор больше не учитывает мусорные или тестовые строки при расчете процентов.
Баг с ok_rate: теперь при отсутствии подходящих данных (NoData) или если все события оказались dn_veto, параметр возвращает None (или "NA" в логах) вместо жесткого 0.0, что предотвращает ложное срабатывание алертов "ok_rate_low".
Добавлены метрики качества: n_total_raw, n_invalid, а также топ причин попадания в карантин (dq_top).'''

try:
    r.xadd('notify:telegram', {
        'type': 'report',
        'subtype': 'of_gate_update',
        'ts_ms': str(get_ny_time_millis()),
        'text': text
    }, maxlen=50000)
    print("Telegram message sent successfully.")
except Exception as e:
    print(f"Error sending message: {e}")
    sys.exit(1)
