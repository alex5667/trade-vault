from __future__ import annotations

import json
import unittest
from types import SimpleNamespace
from typing import Any


def _ns(d: dict[str, Any] | None) -> Any:
    if d is None:
        return None
    return SimpleNamespace(**d)


class TestOFCCaptureReplaySmoke(unittest.TestCase):
    def test_engine_build_from_snapshot_smoke(self):
        from core.of_confirm_engine import OFConfirmEngine

        eng = OFConfirmEngine(version=3)

        # Minimal row that resembles schema=2 capture
        row = {
            "schema": 3,
            "symbol": "BTCUSDT",
            "tf": "1s",
            "direction": "LONG",
            "tick_ts_ms": 1700000000000,
            "price": 50000.0,
            "delta_z": 2.5,
            "indicators": {"pressure_hi": 1, "book_churn_hi": 0},
            "absorption": None,
            "cfg": {},
            "runtime_snapshot": {
                "dynamic_cfg": {"pressure_hi": 1},
                "last_regime": "trend",
                "liq_regime": "hi",
                "book_churn_hi": 0,
                "pressure_hi": 0,
                "cont_ctx_ts_ms": 0,
                "last_wp": {"weak_any": False},
                "last_div": {"ts_ms": 955, "kind": "none"},
                "last_bar": {"id": 11, "ts_ms": 123456, "fp_enabled": 0, "fp_move_bp": 0},
            },
            "cancel_gate_state": None,
        }

        rs = row["runtime_snapshot"]
        runtime = SimpleNamespace(
            symbol=row["symbol"],
            dynamic_cfg=rs.get("dynamic_cfg", {}),
            last_regime=rs.get("last_regime", "na"),
            liq_regime=rs.get("liq_regime", "na"),
            book_churn_hi=rs.get("book_churn_hi", 0),
            pressure_hi=int(rs.get("pressure_hi", 0) or 0),
            cont_ctx_ts_ms=rs.get("cont_ctx_ts_ms", 0),
            last_obi_event=rs.get("last_obi_event"),
            last_iceberg_event=rs.get("last_iceberg_event"),
            last_ofi_event=rs.get("last_ofi_event"),
            last_sweep=_ns(rs.get("last_sweep")),
            last_reclaim=_ns(rs.get("last_reclaim")),
            last_wp=_ns(rs.get("last_wp")),
            last_div=_ns(rs.get("last_div")),
            last_fp_edge=_ns(rs.get("last_fp_edge")) if rs.get("last_fp_edge") is not None else None,
            last_bar=_ns(rs.get("last_bar")) if rs.get("last_bar") is not None else None,
        )

        # Should not raise
        ofc, dec = eng.build(
            symbol=row["symbol"],
            tf=row["tf"],
            direction=row["direction"],
            tick_ts_ms=row["tick_ts_ms"],
            price=row["price"],
            delta_z=row["delta_z"],
            runtime=runtime,
            cfg=row.get("cfg", {}),
            indicators=row.get("indicators", {}),
            absorption=row.get("absorption"),
        )

        # Ensure jsonable
        out = {
            "ok": int(getattr(ofc, "ok", 0) if ofc is not None else 0),
            "scenario": str(getattr(ofc, "scenario", "")),
            "reason": str(getattr(ofc, "reason", "")),
            "gate_allow": bool(getattr(dec, "allow", True) if dec is not None else True),
        }
        json.dumps(out, sort_keys=True)


if __name__ == "__main__":
    unittest.main()

