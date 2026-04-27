import py_compile


def test_tools_compile_p97_of_inputs_dlq_replay_tick_flow_full():
    py_compile.compile('tick_flow_full/orderflow_services/of_inputs_dlq_fixed_replay_p97.py', doraise=True)
    py_compile.compile('tick_flow_full/orderflow_services/of_inputs_dlq_exporter_v1.py', doraise=True)
