# -*- coding: utf-8 -*-
import json
import tempfile
import unittest
from pathlib import Path


def _import_by_path(mod_path: Path):
    import importlib.util
    spec = importlib.util.spec_from_file_location("patch_from_side_audit", str(mod_path))
    assert spec and spec.loader
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)  # type: ignore
    return m


class TestPatchFromSideAudit(unittest.TestCase):
    def test_generate_and_apply_patch(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)

            f = root / "python-worker" / "services" / "x.py"
            f.parent.mkdir(parents=True, exist_ok=True)
            f.write_text(
                'def f(tick):\n'
                '    side = tick.get("side") or "BUY"\n'
                '    s1 = 1 if side == "BUY" else -1\n'
                '    s2 = 1 if tick.get("side") == "BUY" else -1\n'
                '    s3 = -1 if tick.get("is_buyer_maker") else 1\n'
                '    return side, s1, s2, s3\n',
                encoding="utf-8",
            )

            audit = [
                {"path": "python-worker/services/x.py", "lineno": 2, "kind": "default_or_buy", "line": '    side = tick.get("side") or "BUY"'},
                {"path": "python-worker/services/x.py", "lineno": 3, "kind": "ternary_side_buy_else_minus1", "line": '    s1 = 1 if side == "BUY" else -1'},
                {"path": "python-worker/services/x.py", "lineno": 4, "kind": "ternary_tick_side", "line": '    s2 = 1 if tick.get("side") == "BUY" else -1'},
                {"path": "python-worker/services/x.py", "lineno": 5, "kind": "ternary_tick_ibm", "line": '    s3 = -1 if tick.get("is_buyer_maker") else 1'},
            ]
            audit_path = root / "audit.json"
            audit_path.write_text(json.dumps(audit), encoding="utf-8")

            # The module path in the real repo:
            mod_path = Path(__file__).resolve().parents[1] / "tools" / "patch_from_side_audit.py"
            if not mod_path.exists():
                self.skipTest("patch_from_side_audit.py not present in this environment")
            tool = _import_by_path(mod_path)

            findings = tool._load_audit_json(audit_path)  # noqa: SLF001
            patch = tool.generate_patch(root, findings)
            self.assertIn("side_sign_from_tick", patch)

            changed = tool.apply_patch_in_place(root, findings, backup=True)
            self.assertEqual(changed, 1)

            new_text = f.read_text(encoding="utf-8")
            self.assertIn('or "UNKNOWN"', new_text)
            self.assertIn('(-1 if side == "SELL" else 0)', new_text)
            self.assertIn('side_sign_from_tick(tick)', new_text)
            self.assertTrue((f.with_suffix(".py.bak")).exists())


if __name__ == "__main__":
    unittest.main()

