from unittest.mock import MagicMock

from domain.calculators import snapshot_tp1_excursions as func_snapshot


def test_snapshot_tp1_excursions_first_call():
    # Arrange
    pos = MagicMock()
    # Initial state
    pos.mfe_pnl = 150.0
    pos.mae_pnl = -50.0
    pos.max_favorable_price = 10100
    pos.max_favorable_ts = 100
    pos.max_adverse_price = 9950
    pos.max_adverse_ts = 50

    # Crucial for Mock: set them to None/0 so they aren't Mocks
    pos.tp1_hit_ts_ms = 0
    pos.mfe_pnl_at_tp1 = None
    pos.mae_pnl_before_tp1 = None

    # Act
    func_snapshot(pos, ts_ms=123456)

    # Assert
    assert pos.tp1_hit_ts_ms == 123456
    assert pos.mfe_pnl_at_tp1 == 150.0
    assert pos.mae_pnl_before_tp1 == -50.0
    assert pos.mfe_price_at_tp1 == 10100
    assert pos.mae_price_before_tp1 == 9950
    assert pos.mfe_ts_at_tp1 == 100
    assert pos.mae_ts_before_tp1 == 50

def test_snapshot_tp1_excursions_idempotent():
    # Arrange
    pos = MagicMock()
    # Already snapped
    pos.tp1_hit_ts_ms = 999
    pos.mfe_pnl_at_tp1 = 100.0
    pos.mae_pnl = -999.0 # changed since then (should be ignored)

    # Act
    func_snapshot(pos, ts_ms=2000)

    # Assert
    assert pos.tp1_hit_ts_ms == 999  # NOT changed
    assert pos.mfe_pnl_at_tp1 == 100.0 # NOT changed
