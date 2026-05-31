import re

path = "/home/alex/front/trade/scanner_infra/python-worker/tests/test_cron_of_reports_recs.py"
with open(path, "r") as f:
    content = f.read()

if "no_data" not in content:
    content = content.replace("ok_rate=0.1,", "ok_rate=0.1,\n        no_data=0,")
    content = content.replace("ok_rate=0.4,", "ok_rate=0.4,\n        no_data=0,")
    content = content.replace("ok_rate=0.8,", "ok_rate=0.8,\n        no_data=0,")
    with open(path, "w") as f:
        f.write(content)
    print("test_cron_of_reports_recs.py patched")
