import unittest
from pathlib import Path

from tools.runtime_snapshot_deps import scan_file


class TestRuntimeSnapshotDrift(unittest.TestCase):
    def test_schema_covers_runtime_deps(self) -> None:
        engine_path = Path("core/of_confirm_engine.py")
        deps = scan_file(engine_path)

        from core.of_confirm_engine import OFConfirmEngine  # type: ignore

        schema = OFConfirmEngine.runtime_snapshot_schema()  # type: ignore
        missing = sorted(set(deps) - set(schema.keys()))
        # We allow 'pressure' object itself as dependency (live-only), but snapshot covers pressure_hi.
        allow = {"pressure"}
        missing2 = [m for m in missing if m not in allow]
        self.assertEqual(missing2, [], f"runtime deps missing in schema: {missing2}")


if __name__ == "__main__":
    unittest.main()

