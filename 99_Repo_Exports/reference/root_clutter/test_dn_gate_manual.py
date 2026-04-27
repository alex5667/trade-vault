import sys
import asyncio
import os
sys.path.append("/home/alex/front/trade/scanner_infra/python-worker")

from unittest.mock import AsyncMock, MagicMock, patch
from services.orderflow.components.tick_processor import TickProcessor

async def main():
    redis_mock = AsyncMock()
    ticks_mock = AsyncMock()
    publisher_mock = MagicMock()
    of_engine = MagicMock()
    calib_svc = MagicMock()
    atr_cache = MagicMock()
    atr_sanity = MagicMock()
    
    tp = TickProcessor(redis_mock, ticks_mock, publisher_mock, of_engine, calib_svc, atr_cache, atr_sanity)
    
    print(dir(tp))

if __name__ == "__main__":
    asyncio.run(main())
