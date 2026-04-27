import asyncio
import pytest
from services.position_sizer import KellyPositionSizer

class MockDbRow(dict):
    pass

class MockDbPool:
    def __init__(self, override_rows: list = None):
        if override_rows is None:
            override_rows = [{'win_rate': 0.6, 'avg_rr': 1.5, 'n_trades': 30}]
        self.rows = override_rows

    async def fetchrow(self, query, *args):
        if not self.rows:
            return None
        row = self.rows.pop(0)
        return MockDbRow(row)

def test_position_sizer():
    async def run():
        pool = MockDbPool()
        sizer = KellyPositionSizer(pool, min_size=0.01, max_size=0.1)
        res = await sizer.compute("BTCUSDT", "MOMENTUM", 1.0)
        assert res == 0.1, f"Expected clamped max_size (0.1), got {res}"
    asyncio.run(run())

def test_position_sizer_low_n_trades():
    async def run():
        pool = MockDbPool(override_rows=[{'win_rate': 0.9, 'avg_rr': 2.0, 'n_trades': 5}])
        sizer = KellyPositionSizer(pool, min_size=0.01, max_size=0.1)
        res = await sizer.compute("ETHUSDT", "MOMENTUM", 1.0)
        assert res == 0.01, f"Expected fallback min_size because n_trades < 20, got {res}"
    asyncio.run(run())

def test_position_sizer_negative_expectation():
    async def run():
        pool = MockDbPool(override_rows=[{'win_rate': 0.3, 'avg_rr': 1.0, 'n_trades': 30}])
        sizer = KellyPositionSizer(pool, min_size=0.01, max_size=0.1)
        res = await sizer.compute("SOLUSDT", "RANGING", 1.0)
        assert res == 0.01, f"Expected min_size due to negative half_kelly, got {res}"
    asyncio.run(run())

