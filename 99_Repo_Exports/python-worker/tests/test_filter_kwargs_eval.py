from core.compat_utils import _filter_kwargs_for_callable


def test_filter_kwargs_basic():
    def target(a, b, *, c=1):
        return a + b + c

    kwargs = {"a": 10, "b": 20, "c": 5, "d": 999} # d is extra
    filtered = _filter_kwargs_for_callable(target, **kwargs)

    assert "a" in filtered
    assert "b" in filtered
    assert "c" in filtered
    assert "d" not in filtered
    assert len(filtered) == 3

def test_filter_kwargs_varkw():
    def target(a, **kwargs):
        pass

    kw = {"a": 1, "b": 2, "c": 3}
    filtered = _filter_kwargs_for_callable(target, **kw)
    assert len(filtered) == 3 # All passed because of **kwargs

def test_filter_kwargs_eval_reversal_stub():
    # Simulate old signature (no ofi_leg)
    def eval_reversal_old(direction, delta_z, weak_progress, sweep_recent, reclaim_recent, obi_stable, iceberg_strict, abs_lvl_ok, cfg):
        pass

    kw = {
        "direction": "L", "delta_z": 1.0, "weak_progress": False,
        "sweep_recent": True, "reclaim_recent": False,
        "obi_stable": True, "iceberg_strict": False, "abs_lvl_ok": False,
        "cfg": {},
        "ofi_leg": True, # Extra
        "fp_edge_absorb": True # Extra
    }

    filtered = _filter_kwargs_for_callable(eval_reversal_old, **kw)
    assert "ofi_leg" not in filtered
    assert "fp_edge_absorb" not in filtered
    assert "direction" in filtered

