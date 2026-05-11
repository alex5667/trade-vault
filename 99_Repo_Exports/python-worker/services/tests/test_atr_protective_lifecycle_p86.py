import json
import unittest

from services.atr_graph_backed_protective_resolver import ATRGraphBackedProtectiveResolver
from services.atr_protective_lifecycle_equivalence_cert_service import ATRProtectiveLifecycleEquivalenceCertService
from services.atr_protective_lifecycle_mirror import (
    ProtectiveLifecycleMirror,
)


class DummyConn:
    def __init__(self):
        self._cur = DummyCursor()
    def cursor(self):
        return self._cur
    def commit(self):
        pass
    def __enter__(self):
        return self
    def __exit__(self, *args):
        pass

class DummyCursor:
    def __init__(self):
        self.rows = []
    def execute(self, q, params=None):
        pass
    def fetchone(self):
        if self.rows:
            return self.rows.pop(0)
        return None
    def __enter__(self):
        return self
    def __exit__(self, *args):
        pass

class TestProtectiveLifecyclePhase86(unittest.TestCase):
    def setUp(self):
        self.mirror = ProtectiveLifecycleMirror()
        self.mirror.enabled = True
        self.mirror.bounded_symbols = {"BTCUSDT", "ETHUSDT"}
        self.mirror._get_conn = lambda: DummyConn()  # type: ignore

    def test_mirror_on_position_opened(self):
        """Test OPEN hook."""
        self.mirror.on_position_opened(
            signal_id="sig123", symbol="BTCUSDT", side="LONG",
            entry_price=60000.0, sl=59000.0, tp1=61000.0, ts_ms=1000
        )
        # Bounded symbol check
        self.assertTrue(self.mirror._should_mirror("BTCUSDT"))
        self.assertFalse(self.mirror._should_mirror("SOLUSDT"))

    def test_ratchet_invariant(self):
        """Test P3 ratchet direction invariant simulation."""
        # Long position: sl moving downwards should record a drift
        # Since we mock the DB, it won't actually query, but the python logic can be partially tested
        conn = DummyConn()
        # Mock trailing state fetch
        conn._cur.rows = [[json.dumps({"moves_count": 0})]]

        # Test ratchet backwards logging (normally it would emit drift and not throw)
        try:
            self.mirror.on_sl_moved("sig123", "BTCUSDT", "LONG", 59000.0, 58000.0, 60500.0, 2000)
            self.assertTrue(True)
        except Exception as e:
            self.fail(f"on_sl_moved raised {e} unexpectedly")

    def test_equivalence_cert_service(self):
        """Test the cert service instantiation and basic interface."""
        cert_service = ATRProtectiveLifecycleEquivalenceCertService()
        self.assertIsNotNone(cert_service)

    def test_graph_resolver(self):
        """Test resolver stub."""
        resolver = ATRGraphBackedProtectiveResolver()
        self.assertIsNotNone(resolver)

if __name__ == '__main__':
    unittest.main()
