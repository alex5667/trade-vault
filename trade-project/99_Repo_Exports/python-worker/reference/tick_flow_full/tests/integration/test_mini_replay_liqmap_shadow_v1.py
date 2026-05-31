# tick_flow_full/tests/integration/test_mini_replay_liqmap_shadow_v1.py
"""Integration-style smoke: LiqMap injection + liqmap gate (SHADOW).

This is a "mini-replay" in the sense that we:
  1) pull a LiqMap snapshot from a fake async Redis
  2) inject liqmap_* indicators
  3) run OFConfirmEngine.build with LIQMAP_GATE_MODE=shadow

We assert the output indicators contain both:
  - liqmap_* features (from injection)
  - liqmap_gate_* exports (from the engine)

No external services required.
"""

import asyncio
import json
from typing import Any


class _FakeAsyncRedis:
    def __init__(self, kv: dict[str, bytes | None]):
        self._kv = dict(kv)

    async def get(self, key: str):
        return self._kv.get(key)


def _snap(*, ts_ms: int, symbol: str, window: str) -> bytes:
    return json.dumps(
        {
            "v": 1,
            "ts_ms": int(ts_ms),
            "symbol": symbol,
            "window": str(window),
            "levels": [
                {"side": "ask", "price": 101.0, "usd": 400_000.0, "cnt": 10},
                {"side": "bid", "price": 99.5, "usd": 500_000.0, "cnt": 12},
            ],
        }
    ).encode("utf-8")


def test_mini_replay_liqmap_shadow_smoke():
    from core.of_confirm_engine import OFConfirmEngine
    from services.orderflow.components.tick_processor import TickProcessor

    # Build a "partial" TickProcessor for injection only.
    tp = TickProcessor.__new__(TickProcessor)
    tp._liqmap_cache = {}
    tp._liqmap_next_refresh_ts_ms = {}
    tp.redis = _FakeAsyncRedis(
        {
            "liqmap:snapshot:BTCUSDT:1h": _snap(ts_ms=1_000_000 - 200, symbol="BTCUSDT", window="1h")
        }
    )
    tp.liqmap_features_enable = True
    tp.liqmap_features_windows = ["1h"]
    tp.liqmap_features_refresh_ms = 1500
    tp.liqmap_features_fetch_interval_ms = 1500
    tp.liqmap_features_failopen_stale_ms = 120_000
    tp.liqmap_snapshot_key_prefix = "liqmap:snapshot"
    tp.liqmap_near_band_bps = 200.0
    tp.liqmap_peak_min_share = 0.05

    class _Runtime:
        symbol = "BTCUSDT"
        dynamic_cfg = {}
        config = {"micro_tf": "1m", "strategy_name": "cryptoorderflow"}
        last_regime = "bull"
        last_obi_event = None
        last_iceberg_event = None
        last_ofi_event = None
        last_sweep = None
        last_reclaim = None
        last_div = None
        last_wp = None
        last_fp_edge = None

    indicators: dict[str, Any] = {
        "price": 100.0,
        "atr_bps": 50.0,
        "tick_time_age_ms": 0.0,
        "data_health": 1.0,
        "book_health_ok": 1.0,
    }

    # 1) Inject liqmap_* from Redis snapshot.
    asyncio.run(
        tp._inject_liqmap_features(
            runtime=_Runtime(),
            now_ms=1_000_000,
            price=100.0,
            indicators=indicators,
        )
    )

    assert "liqmap_1h_total_usd" in indicators
    assert "liqmap_1h_age_ms" in indicators

    # 2) Run the engine with liqmap gate in SHADOW.
    engine = OFConfirmEngine()
    cfg = {
        "liqmap_gate_mode": "shadow",
        "liqmap_gate_window": "1h",
        "liqmap_gate_peak_min_usd": 250_000.0,
        "liqmap_gate_sl_band_mult": 2.0,
    }
    _confirm, _gate = engine.build(
        symbol="BTCUSDT",
        tf="1m",
        direction="LONG",
        tick_ts_ms=1_000_000,
        price=100.0,
        delta_z=1.2,
        runtime=_Runtime(),
        cfg=cfg,
        indicators=indicators,
        absorption=None,
    )

    assert "liqmap_gate_shadow_veto" in indicators
    assert "liqmap_gate_veto" in indicators
    assert "liqmap_gate_reason" in indicators
