from unittest.mock import AsyncMock, patch

import pytest

from services.orderflow.service_config import ServiceConfig


@pytest.mark.asyncio
async def test_shutdown_idempotency():
    with patch("services.crypto_orderflow_service.RedisPoolSet") as MockPoolSet:
        MockPoolSet.return_value.close_all = AsyncMock()

        from services.crypto_orderflow_service import CryptoOrderflowService

        cfg = ServiceConfig()
        cfg.pel.cleanup_on_startup = False
        cfg.lifecycle.supervisor_enable = False
        cfg.pel.sweeper_enable = False
        cfg.pel.cleanup_periodic_enable = False

        svc = CryptoOrderflowService(redis_dsn="redis://localhost")
        svc._svc_cfg = cfg
        svc._pools.close_all = AsyncMock()

        await svc.shutdown()
        assert svc._shutdown is True

        # Second call should return early, not crashing
        await svc.shutdown()
        assert svc._pools.close_all.call_count == 1
