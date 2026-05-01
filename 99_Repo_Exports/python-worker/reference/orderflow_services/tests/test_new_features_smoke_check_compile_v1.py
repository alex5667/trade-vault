import compileall


def test_new_features_smoke_check_compiles_v1() -> None:
    ok = compileall.compile_file(
        "orderflow_services/new_features_gauges_smoke_check_v1.py",
        quiet=1,
    )
    assert ok
