from __future__ import annotations

from types import SimpleNamespace


def test_ensure_levels_sets_entry_from_price_aliases():
    from handlers.base_orderflow_handler import ensure_levels

    ctx = SimpleNamespace(price=100.0)
    ensure_levels(ctx, side="LONG")
    assert float(ctx.entry_price) == 100.0
    assert float(ctx.price) == 100.0
    assert getattr(ctx, "side_int", None) == 1
    assert getattr(ctx, "side", None) == "LONG"
    assert isinstance(getattr(ctx, "data_quality_flags", []), list)


def test_ensure_levels_sets_tp1_from_list_levels():
    from handlers.base_orderflow_handler import ensure_levels

    ctx = SimpleNamespace(entry_price=100.0, tp_levels=[101.5])
    ensure_levels(ctx, side=1)
    assert float(ctx.tp1_price) == 101.5


def test_ensure_levels_sets_sl_from_stop_alias():
    from handlers.base_orderflow_handler import ensure_levels

    ctx = SimpleNamespace(entry_price=100.0, stop_price=99.0)
    ensure_levels(ctx, side=-1)
    assert float(ctx.sl_price) == 99.0
    assert getattr(ctx, "side_int", None) == -1
    assert getattr(ctx, "side", None) == "SHORT"


def test_ensure_levels_missing_fields_add_flags_fail_open():
    from handlers.base_orderflow_handler import ensure_levels

    ctx = SimpleNamespace()  # no prices at all
    ensure_levels(ctx, side=None)
    flags = getattr(ctx, "data_quality_flags", [])
    assert "levels_missing_entry_price" in flags
    assert "levels_missing_price" in flags
    assert "levels_missing_tp1_price" in flags
    assert "levels_missing_sl_price" in flags
    assert "side_missing_or_unparsed" in flags


def test_side_normalization_accepts_buy_sell_variants():
    from handlers.base_orderflow_handler import normalize_side_int, side_int_to_payload

    assert normalize_side_int("BUY") == 1
    assert normalize_side_int("bid") == 1
    assert normalize_side_int("LONG") == 1
    assert normalize_side_int(1) == 1

    assert normalize_side_int("SELL") == -1
    assert normalize_side_int("ask") == -1
    assert normalize_side_int("SHORT") == -1
    assert normalize_side_int(-1) == -1

    assert side_int_to_payload(1) == "LONG"
    assert side_int_to_payload(-1) == "SHORT"
    assert side_int_to_payload(None) is None
