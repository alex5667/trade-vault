import os
import sys
import unittest
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]  # python-worker/
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from tools.audit_side_sign_usage import scan_file


class TestAuditSideSignUsage(unittest.TestCase):
    def test_detects_ternary_else_minus1(self):
        with tempfile.TemporaryDirectory() as td:
            p = Path(td) / "x.py"
            p.write_text('sign = 1 if side == "BUY" else -1\n', encoding="utf-8")
            findings = scan_file(str(p))
            kinds = {f.kind for f in findings}
            self.assertIn("ternary_else_minus1", kinds)

    def test_detects_not_buy_else_plus1(self):
        with tempfile.TemporaryDirectory() as td:
            p = Path(td) / "x.py"
            p.write_text('sign = -1 if side != "BUY" else 1\n', encoding="utf-8")
            findings = scan_file(str(p))
            kinds = {f.kind for f in findings}
            self.assertIn("ternary_not_buy_else_plus1", kinds)

    def test_detects_default_buy(self):
        with tempfile.TemporaryDirectory() as td:
            p = Path(td) / "x.py"
            p.write_text('side = tick.get("side") or "BUY"\n', encoding="utf-8")
            findings = scan_file(str(p))
            kinds = {f.kind for f in findings}
            self.assertIn("default_buy", kinds)


if __name__ == "__main__":
    unittest.main()

