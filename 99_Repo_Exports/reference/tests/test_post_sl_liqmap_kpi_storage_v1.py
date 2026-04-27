import json
import pytest

pytest.importorskip("psycopg2")
import sys
from pathlib import Path

# Make the test runnable both from tick_flow_full root (CI) and directly.
# In CI, tick_flow_full/tests/conftest.py already inserts tick_flow_full/ into sys.path.
TICK_FLOW_ROOT = Path(__file__).resolve().parents[1]
if str(TICK_FLOW_ROOT) not in sys.path:
    sys.path.insert(0, str(TICK_FLOW_ROOT))

from services.archivers.stream_archiver import StreamArchiver


def _dummy_archiver():
    a = StreamArchiver.__new__(StreamArchiver)
    return a


def test_post_sl_liqmap_kpi_row_extracts_liqmap_subset_and_scalars():
    a = _dummy_archiver()
    payload = {
        "trade_id": "t-001",
        "symbol": "BTCUSDT",
        "side": "LONG",
        "regime": "trend",
        "event_ts_ms": 1700000000123,
        "liqmap_1h_peak_up_usd": 123456.0,
        "liqmap_1h_peak_up_dist_bps": 42.0,
        "liqmap_1h_age_ms": 90000,
        "sl_hit_near_liqmap_peak": 1,
        "sl_liqmap_peak_dist_bps": 7.5,
        "sl_liqmap_peak_usd": 250000.0,
        "tp1_anchored": 1,
        "tp1_anchored_and_hit": 0,
        "liqmap_levels_applied": 1,
        "liqmap_tp1_adj_bps": -5.0,
        "liqmap_levels_reason": "tp1_before_peak",
        "something_else": "ignored_in_liqmap_kpi",
    }

    row = a.post_sl_liqmap_kpi_row("1700000000123-0", payload)
    liqmap_kpi = json.loads(row[-2])
    full_payload = json.loads(row[-1])

    assert full_payload["trade_id"] == "t-001"
    assert liqmap_kpi["liqmap_1h_peak_up_usd"] == 123456.0
    assert liqmap_kpi["sl_hit_near_liqmap_peak"] == 1

    assert row[7] == 1
    assert row[8] == 1
    assert row[9] == 0


def test_post_sl_liqmap_kpi_row_requires_trade_symbol_side():
    a = _dummy_archiver()
    try:
        a.post_sl_liqmap_kpi_row("1700000000123-0", {"symbol": "BTCUSDT", "side": "LONG"})
        assert False, "expected ValueError"
    except ValueError:
        pass
import json
import pytest

pytest.importorskip("psycopg2")
import sys
from pathlib import Path

# Make the test runnable both from tick_flow_full root (CI) and directly.
# In CI, tick_flow_full/tests/conftest.py already inserts tick_flow_full/ into sys.path.
TICK_FLOW_ROOT = Path(__file__).resolve().parents[1]
if str(TICK_FLOW_ROOT) not in sys.path:
    sys.path.insert(0, str(TICK_FLOW_ROOT))

from services.archivers.stream_archiver import StreamArchiver


def _dummy_archiver():
    a = StreamArchiver.__new__(StreamArchiver)
    return a


def test_post_sl_liqmap_kpi_row_extracts_liqmap_subset_and_scalars():
    a = _dummy_archiver()
    payload = {
        "trade_id": "t-001",
        "symbol": "BTCUSDT",
        "side": "LONG",
        "regime": "trend",
        "event_ts_ms": 1700000000123,
        "liqmap_1h_peak_up_usd": 123456.0,
        "liqmap_1h_peak_up_dist_bps": 42.0,
        "liqmap_1h_age_ms": 90000,
        "sl_hit_near_liqmap_peak": 1,
        "sl_liqmap_peak_dist_bps": 7.5,
        "sl_liqmap_peak_usd": 250000.0,
        "tp1_anchored": 1,
        "tp1_anchored_and_hit": 0,
        "liqmap_levels_applied": 1,
        "liqmap_tp1_adj_bps": -5.0,
        "liqmap_levels_reason": "tp1_before_peak",
        "something_else": "ignored_in_liqmap_kpi",
    }

    row = a.post_sl_liqmap_kpi_row("1700000000123-0", payload)
    liqmap_kpi = json.loads(row[-2])
    full_payload = json.loads(row[-1])

    assert full_payload["trade_id"] == "t-001"
    assert liqmap_kpi["liqmap_1h_peak_up_usd"] == 123456.0
    assert liqmap_kpi["sl_hit_near_liqmap_peak"] == 1

    assert row[7] == 1
    assert row[8] == 1
    assert row[9] == 0


def test_post_sl_liqmap_kpi_row_requires_trade_symbol_side():
    a = _dummy_archiver()
    try:
        a.post_sl_liqmap_kpi_row("1700000000123-0", {"symbol": "BTCUSDT", "side": "LONG"})
        assert False, "expected ValueError"
    except ValueError:
        pass
