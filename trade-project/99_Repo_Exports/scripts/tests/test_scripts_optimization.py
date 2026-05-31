"""Unit tests for scripts/_redis_utils.py and scripts/benchmark_robust_z.py."""
from __future__ import annotations

import sys
import os

# Make scripts/ importable
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import unittest
from unittest.mock import MagicMock, patch

import redis


# ---------------------------------------------------------------------------
# _redis_utils tests
# ---------------------------------------------------------------------------

class TestMakeRedisClient(unittest.TestCase):
    """Tests for _redis_utils.make_redis_client and make_redis_client_from_env."""

    def test_make_redis_client_defaults(self) -> None:
        from _redis_utils import make_redis_client
        client = make_redis_client(host="myhost", port=1234)
        conn_kwargs = client.connection_pool.connection_kwargs
        self.assertEqual(conn_kwargs["host"], "myhost")
        self.assertEqual(conn_kwargs["port"], 1234)

    def test_make_redis_client_decode_responses(self) -> None:
        from _redis_utils import make_redis_client
        client = make_redis_client(decode_responses=True)
        self.assertTrue(client.connection_pool.connection_kwargs.get("decode_responses", False))

    def test_make_redis_client_from_env_defaults(self) -> None:
        from _redis_utils import make_redis_client_from_env
        env = {k: v for k, v in os.environ.items() if k not in ("REDIS_HOST", "REDIS_PORT")}
        with patch.dict(os.environ, env, clear=True):
            client = make_redis_client_from_env()
        conn_kwargs = client.connection_pool.connection_kwargs
        self.assertEqual(conn_kwargs["host"], "localhost")
        self.assertEqual(conn_kwargs["port"], 6379)

    def test_make_redis_client_from_env_override(self) -> None:
        from _redis_utils import make_redis_client_from_env
        with patch.dict(os.environ, {"REDIS_HOST": "myredis", "REDIS_PORT": "6380"}):
            client = make_redis_client_from_env()
        conn_kwargs = client.connection_pool.connection_kwargs
        self.assertEqual(conn_kwargs["host"], "myredis")
        self.assertEqual(conn_kwargs["port"], 6380)

    def test_ping_or_raise_success(self) -> None:
        from _redis_utils import ping_or_raise
        mock_client = MagicMock(spec=redis.Redis)
        mock_client.ping.return_value = True
        ping_or_raise(mock_client)  # must not raise

    def test_ping_or_raise_failure(self) -> None:
        from _redis_utils import ping_or_raise
        mock_client = MagicMock(spec=redis.Redis)
        mock_client.ping.side_effect = ConnectionError("refused")
        with self.assertRaises(redis.ConnectionError):
            ping_or_raise(mock_client, label="test")


# ---------------------------------------------------------------------------
# benchmark_robust_z CPU implementation tests
# ---------------------------------------------------------------------------

class TestBatchRollingRobustZCPU(unittest.TestCase):
    """Tests for BatchRollingRobustZ_CPU.update_and_calc."""

    def _get_class(self):
        from benchmark_robust_z import BatchRollingRobustZ_CPU
        return BatchRollingRobustZ_CPU

    def test_returns_zeros_for_single_sample(self) -> None:
        cls = self._get_class()
        obj = cls(num_streams=2, window_size=10)
        results = obj.update_and_calc([1.0, 2.0])
        self.assertEqual(results, [0.0, 0.0])

    def test_output_length_matches_num_streams(self) -> None:
        cls = self._get_class()
        obj = cls(num_streams=5, window_size=20)
        for _ in range(5):
            obj.update_and_calc([float(i) for i in range(5)])
        results = obj.update_and_calc([1.0] * 5)
        self.assertEqual(len(results), 5)

    def test_known_z_score(self) -> None:
        """Feed a known sequence and verify the z-score direction."""
        cls = self._get_class()
        obj = cls(num_streams=1, window_size=100)
        # Fill buffer with zeros
        for _ in range(100):
            obj.update_and_calc([0.0])
        # Inject a large positive outlier — z should be large and positive
        results = obj.update_and_calc([100.0])
        self.assertGreater(results[0], 1.0, "Outlier should produce z > 1.0")

    def test_symmetric_z_score(self) -> None:
        """Symmetric outliers should have equal absolute z-scores."""
        cls = self._get_class()
        obj_pos = cls(num_streams=1, window_size=50)
        obj_neg = cls(num_streams=1, window_size=50)
        for _ in range(50):
            obj_pos.update_and_calc([0.0])
            obj_neg.update_and_calc([0.0])
        z_pos = obj_pos.update_and_calc([5.0])[0]
        z_neg = obj_neg.update_and_calc([-5.0])[0]
        self.assertAlmostEqual(abs(z_pos), abs(z_neg), places=5)


class TestRollingRobustZCPU(unittest.TestCase):
    """Tests for RollingRobustZ_CPU single-stream implementation."""

    def _get_class(self):
        from benchmark_robust_z import RollingRobustZ_CPU
        return RollingRobustZ_CPU

    def test_zero_on_small_window(self) -> None:
        cls = self._get_class()
        obj = cls(window_size=10)
        obj.update(1.0)
        self.assertEqual(obj.z(1.0), 0.0)

    def test_nonzero_after_warmup(self) -> None:
        cls = self._get_class()
        obj = cls(window_size=10)
        for i in range(10):
            obj.update(float(i))
        # An outlier should give a nonzero z
        z = obj.z(1000.0)
        self.assertNotEqual(z, 0.0)


if __name__ == "__main__":
    unittest.main()
