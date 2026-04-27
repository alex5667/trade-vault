from types import SimpleNamespace

from common.dq_flags import ensure_dq_flags, append_dq_flag


def test_ensure_dq_flags_creates_list():
    ctx = SimpleNamespace()
    flags = ensure_dq_flags(ctx)
    assert isinstance(flags, list)
    assert getattr(ctx, "data_quality_flags") is flags


def test_ensure_dq_flags_converts_tuple_to_list():
    ctx = SimpleNamespace(data_quality_flags=("a", "b"))
    flags = ensure_dq_flags(ctx)
    assert flags == ["a", "b"]
    assert isinstance(getattr(ctx, "data_quality_flags"), list)


def test_append_dq_flag_dedup_and_trim():
    ctx = SimpleNamespace()
    append_dq_flag(ctx, "  x  ")
    append_dq_flag(ctx, "x")
    assert getattr(ctx, "data_quality_flags") == ["x"]
