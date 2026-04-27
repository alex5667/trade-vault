import re
path = 'services/orderflow/metrics.py'
with open(path, 'r') as f:
    text = f.read()

if 'feature_missing_total' not in text:
    text += "\nfeature_missing_total = _get_or_create_prom_counter('feature_missing_total', '', ['reason'])\n"

with open(path, 'w') as f:
    f.write(text)
