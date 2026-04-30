from __future__ import annotations

import asyncio
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from services.orderflow.derivatives_context import aread_derivatives_context, partial_funding_payload_from_exchange


class FakeAsyncRedis:
    def __init__(self, mapping=None):
        self.mapping = dict(mapping or {})

    async def get(self, key):
        return self.mapping.get(key)


def test_aread_derivatives_context_reads_snapshot_from_redis():
    payload = {
        "schema_version": 1
        "symbol": "BTCUSDT"
        "ts_ms": 1000
        "venue": "binance"
        "funding_rate": 0.0010
        "funding_rate_abs": 0.0010
        "funding_rate_z": 4.0
        "premium_index": 0.0010
        "basis_bps": 12.0
        "open_interest": 10000.0
        "delta_oi_5m": 1000.0
        "oi_notional_usd": 1000000.0
        "funding_extreme": 1
        "basis_extreme": 1
        "oi_accel": 1
    }
    r = FakeAsyncRedis({"ctx:deriv:BTCUSDT": json.dumps(payload)})
    snap = asyncio.run(aread_derivatives_context(r, symbol="BTCUSDT"))
    assert snap is not None
    assert snap.funding_rate_z == 4.0
    assert snap.basis_extreme == 1


def test_partial_funding_payload_from_exchange_normalizes_legacy_stream_shape():
    out = partial_funding_payload_from_exchange(
        {
            "symbol": "ETHUSDT"
            "lastFundingRate": "0.0004"
            "fundingTime": 123456
        }
        venue="binance"
    )
    assert out["symbol"] == "ETHUSDT"
    assert out["funding_rate"] == 0.0004
    assert out["ts_ms"] == 123456
