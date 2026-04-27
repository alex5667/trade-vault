import py_compile


def test_compile_main_and_mirror() -> None:
    py_compile.compile('orderflow_services/orchestration_composite_preflight_exporter_v1.py', doraise=True)
    py_compile.compile('tick_flow_full/orderflow_services/orchestration_composite_preflight_exporter_v1.py', doraise=True)
