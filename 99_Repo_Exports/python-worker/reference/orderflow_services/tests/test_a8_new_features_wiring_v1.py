from pathlib import Path


def _read(rel: str) -> str:
    root = Path(__file__).resolve().parents[2]
    return (root / rel).read_text(encoding="utf-8")


def test_a8_metrics_gauges_declared_v1() -> None:
    txt = _read("tick_flow_full/services/orderflow/metrics.py")
    for name in (
        "trade_depth_total_10"
        "trade_gini_depth_10"
        "trade_vwap_roll_diff_bps"
        "trade_price_momentum_bps"
        "trade_realized_vol_bps"
        "trade_pressure_per_min"
        "trade_liquidity_pressure"
        "trade_info_flow"
        "trade_flag_state"
    ):
        assert name in txt


def test_a8_tick_processor_emits_stream_fields_v1() -> None:
    txt = _read("tick_flow_full/services/orderflow/components/tick_processor.py")
    for key in (
        '"depth_total_10"'
        '"gini_depth_10"'
        '"vwap_roll_diff_bps"'
        '"price_momentum_bps"'
        '"realized_vol_bps"'
        '"pressure_per_min"'
        '"liquidity_pressure"'
        '"info_flow"'
        '"realized_vol_no_data"'
        '"vwap_roll_no_data"'
    ):
        assert key in txt
