import json
import tempfile
import unittest
from pathlib import Path

from tools.ofc_replay_validate import main as replay_main


class TestReplayValidateToolSmoke(unittest.TestCase):
    def test_tool_runs_on_minimal_row(self) -> None:
        # Minimal row that should validate determinism
        row = {
            "symbol": "BTCUSDT",
            "direction": "LONG",
            "tick_ts_ms": 1700000000000,
            "price": 42000.0,
            "delta_z": 1.2,
            "cfg": {},
            "indicators": {},
            "runtime_snapshot": {"liq_regime": "na", "book_churn_hi": 0, "pressure_hi": 0, "cont_ctx_ts_ms": 0},
        }

        with tempfile.TemporaryDirectory() as td:
            p = Path(td) / "cap.ndjson"
            p.write_text(json.dumps(row) + "\n", encoding="utf-8")

            # Call via main() emulation: patch sys.argv inside tool
            import sys
            old = sys.argv[:]
            try:
                sys.argv = ["ofc_replay_validate.py", str(p), "--limit", "10"]
                rc = replay_main()
            finally:
                sys.argv = old

            self.assertEqual(rc, 0)


if __name__ == "__main__":
    unittest.main()

