import py_compile


def test_compile_of_timers_worker():
    py_compile.compile('services/of_timers_worker.py', doraise=True)
    py_compile.compile('tick_flow_full/services/of_timers_worker.py', doraise=True)
