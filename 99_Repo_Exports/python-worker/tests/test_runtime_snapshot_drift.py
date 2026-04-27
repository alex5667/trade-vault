import unittest
from pathlib import Path

from core.of_confirm_engine import OFConfirmEngine
from tools.runtime_snapshot_deps import collect_requirements


class TestRuntimeSnapshotDrift(unittest.TestCase):
    def test_snapshot_schema_covers_code_deps(self):
        repo_root = Path(__file__).resolve().parents[1]
        req = collect_requirements(repo_root)
        schema = OFConfirmEngine.runtime_snapshot_schema()

        top_schema = set(schema.get("top") or [])
        nested_schema = {k: set(v) for k, v in (schema.get("nested") or {}).items()}

        missing_top = sorted(set(req.top) - top_schema)
        self.assertFalse(missing_top, f"runtime_snapshot schema missing top keys: {missing_top}")

        missing_nested = {}
        for k, fields in req.nested.items():
            have = nested_schema.get(k, set())
            miss = sorted(set(fields) - have)
            if miss:
                missing_nested[k] = miss

        self.assertFalse(missing_nested, f"runtime_snapshot schema missing nested keys: {missing_nested}")

