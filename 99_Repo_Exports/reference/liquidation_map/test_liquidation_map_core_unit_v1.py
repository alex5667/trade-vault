import os
import sys
from decimal import Decimal

# Ensure repo root is on sys.path for `services.*` imports when running tests from subfolders.
REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from services.liquidation_map_core import Bucketizer, LiqMapWindowAgg, format_decimal


def test_bucketizer_abs_round_trip():
    b = Bucketizer(mode='abs', abs_step=Decimal('10'))
    assert b.bucket_key(Decimal('100')) == '100'
    assert b.bucket_key(Decimal('104.9')) == '100'
    assert b.bucket_key(Decimal('105.0')) == '110'


def test_bucketizer_log_bps_monotonic():
    b = Bucketizer(mode='log_bps', bps=50)  # 0.5%
    k1 = int(b.bucket_key(Decimal('10000')))
    k2 = int(b.bucket_key(Decimal('10050')))
    k3 = int(b.bucket_key(Decimal('10100')))
    assert k1 <= k2 <= k3


def test_window_agg_add_and_evict():
    b = Bucketizer(mode='abs', abs_step=Decimal('1'))
    agg = LiqMapWindowAgg(window_ms=1000, bucketizer=b)

    agg.add(ts_event_ms=0, price=Decimal('100'), liq_side='long', notional=Decimal('10'))
    agg.add(ts_event_ms=500, price=Decimal('101'), liq_side='short', notional=Decimal('5'))

    # before eviction
    lv = agg.levels(max_levels=100, range_pct=0)
    assert len(lv) == 2

    # evict at now=1001 -> first event expires
    n = agg.evict(now_ms=1001)
    assert n == 1
    lv2 = agg.levels(max_levels=100, range_pct=0)
    assert len(lv2) == 1
    _p, _bk, l, s = lv2[0]
    assert format_decimal(l) == '0'
    assert format_decimal(s) == '5'
