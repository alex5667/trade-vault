import py_compile


def test_tools_compile_p81():
    py_compile.compile('orderflow_services/refresh_exec_slip_stats_p80.py', doraise=True)
    py_compile.compile('orderflow_services/enforce_bucket_slo_freezer_p80.py', doraise=True)
    py_compile.compile('orderflow_services/enforce_bucket_state_exporter_v1.py', doraise=True)
