from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import orderflow_services.derivatives_context_exporter_v1 as mod
import contextlib


def test_derivatives_context_exporter_reads_snapshot_and_sets_metrics():
    mock_redis_cls = MagicMock()
    cli = MagicMock()
    mock_redis_cls.from_url.return_value = cli
    cli.keys.return_value = ["ctx:deriv:BTCUSDT"]
    cli.get.return_value = '{"schema_version":1,"symbol":"BTCUSDT","ts_ms":1000,"venue":"binance","funding_rate":0.001,"funding_rate_abs":0.001,"funding_rate_z":4.0,"premium_index":0.001,"basis_bps":12.0,"open_interest":1000.0,"delta_oi_5m":10.0,"oi_notional_usd":100000.0,"funding_extreme":1,"basis_extreme":1,"oi_accel":0}'

    with patch.object(mod, "redis", SimpleNamespace(Redis=mock_redis_cls)), \
         patch("orderflow_services.derivatives_context_exporter_v1.start_http_server"), \
         patch("orderflow_services.derivatives_context_exporter_v1.time.sleep", side_effect=KeyboardInterrupt):
        with contextlib.suppress(KeyboardInterrupt):
            mod.main()

    assert mod.g_funding_z.labels(symbol="BTCUSDT")._value.get() == 4.0
    assert mod.g_basis_bps.labels(symbol="BTCUSDT")._value.get() == 12.0
