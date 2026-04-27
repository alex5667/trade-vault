import py_compile


def test_compile_slippage_calibrator_wrapper_v1():
    py_compile.compile('ml_analysis/tools/nightly_slippage_calibrator_v1.py', doraise=True)
