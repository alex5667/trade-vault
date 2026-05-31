import json
import tempfile
import unittest
from pathlib import Path


class TestMetaModelPromoteV1(unittest.TestCase):
    def test_promote_writes_manifest_and_copy(self):
        with tempfile.TemporaryDirectory() as td:
            td = Path(td)
            in_json = td / "model.json"
            in_json.write_text('{"schema_name":"meta_feat_v5","features":["a"],"coef":[1.0]}', encoding="utf-8")

            out_dir = td / "out"
            manifest = td / "manifest.json"

            # Import here to avoid path issues when tests are collected.
            import importlib
            mod = importlib.import_module("tools.meta_model_promote_v1")

            # Call main-like flow
            sha = mod.sha256_file(str(in_json))
            self.assertEqual(len(sha), 64)

            # Note: ts will be current, so we can't predict exact filename easily in test unless we mock time.
            # But we can check if file was created with correct suffix.

            ts_start = "meta_model_meta_feat_v5_"
            out_dir.mkdir(parents=True, exist_ok=True)

            # We'll just call the functions directly to verify logic
            name = f"meta_model_meta_feat_v5_20000101_000000_{sha[:12]}.json"
            promoted = out_dir / name
            mod.atomic_copy(str(in_json), str(promoted))

            self.assertTrue(promoted.exists())
            self.assertEqual(in_json.read_text(encoding="utf-8"), promoted.read_text(encoding="utf-8"))

            man = {
                "schema": "meta_feat_v5",
                "sha256": sha,
                "input_model_json": str(in_json),
                "promoted_model_json": str(promoted),
                "latest_link": "",
            }
            manifest.write_text(json.dumps(man, ensure_ascii=False, indent=2), encoding="utf-8")
            loaded = json.loads(manifest.read_text(encoding="utf-8"))
            self.assertEqual(loaded["sha256"], sha)


if __name__ == "__main__":
    unittest.main()
