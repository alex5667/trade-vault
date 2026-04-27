import py_compile
import os

def test_compile_enforce_health_tools_v82():
    py_compile.compile('orderflow_services/enforce_health_gates_v82.py', cfile='/tmp/1.pyc', doraise=True)
    py_compile.compile('orderflow_services/enforce_health_report_v82.py', cfile='/tmp/2.pyc', doraise=True)
    py_compile.compile('orderflow_services/enforce_bucket_state_exporter_v1.py', cfile='/tmp/3.pyc', doraise=True)
    py_compile.compile('services/of_timers_worker.py', cfile='/tmp/4.pyc', doraise=True)
