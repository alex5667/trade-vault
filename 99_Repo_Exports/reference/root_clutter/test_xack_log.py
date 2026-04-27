import re

with open("/home/alex/front/trade/scanner_infra/python-worker/services/posttrade/decision_snapshot_writer.py", "r") as f:
    text = f.read()

new_text = text.replace(
    'logger.warning("xack failed: %s", e)',
    'logger.warning("xack failed: %s with ids: %s", e, ids)'
)

with open("/home/alex/front/trade/scanner_infra/python-worker/services/posttrade/decision_snapshot_writer.py", "w") as f:
    f.write(new_text)

