import unittest
import tempfile
import shutil
import os
import time
from tools.cleanup_promoted_models_v1 import cleanup_promoted_models
from tools.meta_promote_dir_check_v1 import check_promote_dir

class TestPromoteTools(unittest.TestCase):
    def setUp(self):
        self.test_dir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.test_dir)

    def create_dummy_file(self, name, age_seconds=0):
        path = os.path.join(self.test_dir, name)
        with open(path, 'w') as f:
            f.write("dummy")
        
        # Set mtime
        now = time.time()
        mtime = now - age_seconds
        os.utime(path, (mtime, mtime))
        return path

    def test_cleanup_retention(self):
        # Create artifacts
        # Policy: keep-last=2, keep-days=1 (86400s)
        
        # f1: now (newest)
        self.create_dummy_file("meta_model_1.json", 0)
        # f2: 1 hour ago
        self.create_dummy_file("meta_model_2.json", 3600)
        # f3: 25 hours ago (older than 1 day)
        self.create_dummy_file("meta_model_3.json", 90000)
        # f4: 26 hours ago
        self.create_dummy_file("meta_model_4.json", 93600)
        # f5: specific file that should NOT be deleted even if old (safety check usually checks name pattern)
        # But our script checks pattern meta_model_*.json
        # So create a file that doesn't match pattern
        self.create_dummy_file("ignored.txt", 99999)

        # Run cleanup
        # keep_last=2: f1, f2 kept.
        # f3: not in top 2, age 90000 > 86400 -> DELETE
        # f4: DELETE
        # ignored.txt: Pattern mismatch -> IGNORE (Keep)
        
        cleanup_promoted_models(self.test_dir, keep_last=2, keep_days=1)
        
        files = sorted(os.listdir(self.test_dir))
        # Expected: ignored.txt, meta_model_1.json, meta_model_2.json
        self.assertIn("meta_model_1.json", files)
        self.assertIn("meta_model_2.json", files)
        self.assertIn("ignored.txt", files)
        self.assertNotIn("meta_model_3.json", files)
        self.assertNotIn("meta_model_4.json", files)

    def test_dir_check(self):
        # Test existing dir
        metrics = check_promote_dir(self.test_dir)
        self.assertEqual(metrics["meta_promote_dir_exists"], 1)
        self.assertEqual(metrics["meta_promote_dir_writable"], 1)
        self.assertEqual(metrics["meta_promote_dir_ok"], 1)
        
        # Test non-existing dir
        fake_dir = os.path.join(self.test_dir, "nonexistent")
        metrics_bad = check_promote_dir(fake_dir)
        self.assertEqual(metrics_bad["meta_promote_dir_exists"], 0)
        self.assertEqual(metrics_bad["meta_promote_dir_ok"], 0)

if __name__ == "__main__":
    unittest.main()
