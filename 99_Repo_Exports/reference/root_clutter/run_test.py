import sys
sys.path.insert(0, ".")

import importlib
try:
    mod = importlib.import_module("python-worker.tests.test_of_gate_dlq_exporter_v1")
except ModuleNotFoundError:
    # it might not be a package if folder has hyphen
    pass

with open("python-worker/tests/test_of_gate_dlq_exporter_v1.py") as f:
    exec(f.read())

test_parse_streams()
test_id_to_ms()
test_exporter_poll_one_empty()
test_exporter_poll_one_has_data()
test_exporter_loop_iteration()

print("ALL PASS")
