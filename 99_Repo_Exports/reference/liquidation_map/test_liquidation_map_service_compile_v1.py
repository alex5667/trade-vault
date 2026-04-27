import py_compile


def test_compile_liquidation_map_service():
    py_compile.compile('services/liquidation_map_service.py', doraise=True)
    py_compile.compile('services/liquidation_map_core.py', doraise=True)

    # Mirror copy (train==serve / SoT compatibility)
    py_compile.compile('tick_flow_full/services/liquidation_map_service.py', doraise=True)
    py_compile.compile('tick_flow_full/services/liquidation_map_core.py', doraise=True)
