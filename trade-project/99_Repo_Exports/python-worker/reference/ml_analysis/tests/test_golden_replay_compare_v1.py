from ml_analysis.golden_replay.compare import diff_objects, float_close, stable_hash


def test_float_close_abs_rel_v1():
    assert float_close(1.0, 1.0 + 1e-7, abs_tol=1e-6, rel_tol=1e-6)
    assert not float_close(1.0, 1.0 + 1e-3, abs_tol=1e-6, rel_tol=1e-6)
    assert float_close(1000.0, 1000.0 + 1e-3, abs_tol=1e-6, rel_tol=1e-6)


def test_diff_objects_numeric_and_nested_v1():
    a = {"x": 1.0, "y": {"z": [1, 2, 3]}}
    b = {"x": 1.0 + 1e-7, "y": {"z": [1, 2, 4]}}
    diffs = diff_objects(a, b, abs_tol=1e-6, rel_tol=1e-6)
    assert any(d.path.endswith("[2]") for d in diffs)


def test_stable_hash_determinism_v1():
    a = {"b": 1, "a": {"z": 2, "y": 3}}
    b = {"a": {"y": 3, "z": 2}, "b": 1}
    assert stable_hash(a) == stable_hash(b)
