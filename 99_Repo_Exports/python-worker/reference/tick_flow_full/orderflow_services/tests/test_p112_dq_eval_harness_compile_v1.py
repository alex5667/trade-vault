import py_compile


def test_p112_dq_eval_harness_compile_v1():
    py_compile.compile(
        "tick_flow_full/orderflow_services/dq_threshold_eval_harness_p112.py",
        doraise=True,
    )
