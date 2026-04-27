import re

with open("/home/alex/front/trade/scanner_infra/python-worker/services/posttrade/decision_snapshot_writer.py", "r") as f:
    text = f.read()

new_text = text.replace(
    'logger.warning("pel reclaim fallback failed: %s", e)',
    'logger.warning("pel reclaim fallback failed (cursor=%s, ids=%s): %s", cursor, ids if "ids" in locals() else [], e, exc_info=True)'
)

with open("/home/alex/front/trade/scanner_infra/python-worker/services/posttrade/decision_snapshot_writer.py", "w") as f:
    f.write(new_text)

