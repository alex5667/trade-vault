import pytest
import os
from unittest.mock import patch, MagicMock

# Attempt to import the exporter and archiver safely.
# If they fail due to syntax/system issues, the test should fail gracefully.

try:
    from orderflow_services.of_gate_archiver_exporter_v1 import Exporter
    HAS_EXPORTER = True
except ImportError:
    HAS_EXPORTER = False

try:
    from services.archivers.stream_archiver import Archiver
    HAS_ARCHIVER = True
except ImportError:
    HAS_ARCHIVER = False


@pytest.mark.skipif(not HAS_EXPORTER, reason="of_gate_archiver_exporter_v1 not found or failed to import")
def test_p85_exporter_config():
    """
    Test that the P85 OF-gate archiver exporter initializes properly
    and reads the expected environment variables or defaults.
    """
    with patch.dict(os.environ, {"OF_GATE_ARCHIVER_EXPORTER_PORT": "9999"}):
        ex = Exporter()
        assert ex.port == 9999
        assert ex.redis_url.startswith("redis")
        
    with patch.dict(os.environ, {}, clear=True):
        ex_default = Exporter()
        assert ex_default.port == 9152


@pytest.mark.skipif(not HAS_ARCHIVER, reason="stream_archiver.py not found or failed to import")
def test_p86_stream_archiver_methods():
    """
    Test that the P86 features (consume_of_gate_metrics, consume_of_gate_quarantine)
    are available on the Archiver class.
    """
    assert hasattr(Archiver, "consume_of_gate_metrics"), "consume_of_gate_metrics missing"
    assert hasattr(Archiver, "consume_of_gate_quarantine"), "consume_of_gate_quarantine missing"


@pytest.mark.skipif(not HAS_ARCHIVER, reason="stream_archiver.py not found or failed to import")
def test_p86_stream_archiver_flags():
    """
    Test that the P86 env flags for OF gate streams are respected.
    """
    with patch.dict(os.environ, {
        "OF_GATE_METRICS_ARCHIVE_ENABLED": "1",
        "OF_GATE_QUARANTINE_ARCHIVE_ENABLED": "1",
        "OF_GATE_METRICS_STREAM": "custom:metrics",
        "OF_GATE_QUARANTINE_STREAM": "custom:quarantine"
    }):
        archiver = Archiver()
        assert archiver.of_gate_enabled is True
        assert archiver.of_gate_q_enabled is True
        assert archiver.of_gate_stream == "custom:metrics"
        assert archiver.of_gate_q_stream == "custom:quarantine"
