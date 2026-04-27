import asyncio
import json
import logging
import os
import random
import time
from abc import ABC, abstractmethod
from typing import AsyncIterable, Dict, Any, Optional

import aiohttp

log = logging.getLogger("dom_feed_adapter")

class BaseDOMAdapter(ABC):
    def __init__(self, symbol: str, depth: int):
        self.symbol = symbol
        self.depth = depth
        
    @abstractmethod
    async def iter_levels(self) -> AsyncIterable[Dict[str, Any]]:
        """Yields snapshot dicts."""
        pass

class MockDOMAdapter(BaseDOMAdapter):
    async def iter_levels(self) -> AsyncIterable[Dict[str, Any]]:
        """Generates random mock data."""
        while True:
            mid = 2000.0 + random.uniform(-10, 10)
            bids = [[mid - i*0.1, 1.0 + i*0.1] for i in range(1, self.depth+1)]
            asks = [[mid + i*0.1, 1.0 + i*0.1] for i in range(1, self.depth+1)]
            yield {
                "ts": time.time(),
                "symbol": self.symbol,
                "mid": mid,
                "bids": bids,
                "asks": asks,
                "provider": "MOCK"
            }
            await asyncio.sleep(0.1)

class AsyncBinanceSpotDepthAdapter(BaseDOMAdapter):
    async def iter_levels(self) -> AsyncIterable[Dict[str, Any]]:
        """Connects to Binance WS and yields depth updates."""
        # Binance Stream: <symbol>@depth<levels>@100ms
        # levels: 5, 10, 20
        # symbol must be lowercase
        
        ws_url = f"wss://stream.binance.com:9443/ws/{self.symbol.lower()}@depth{self.depth}@100ms"
        
        while True:
            try:
                log.info(f"Connecting to Binance WS: {ws_url}")
                async with aiohttp.ClientSession() as session:
                    async with session.ws_connect(ws_url) as ws:
                        log.info("Connected to Binance WS")
                        async for msg in ws:
                            if msg.type == aiohttp.WSMsgType.TEXT:
                                data = json.loads(msg.data)
                                # Binance depth update format for partial book stream:
                                # {
                                #   "lastUpdateId": 160,
                                #   "bids": [ [ "4.00000000", "431.00000000" ] ],
                                #   "asks": [ [ "4.00000200", "12.00000000" ] ]
                                # }
                                
                                bids = [[float(p), float(q)] for p, q in data.get("bids", [])]
                                asks = [[float(p), float(q)] for p, q in data.get("asks", [])]
                                
                                if not bids or not asks:
                                    continue
                                    
                                best_bid = bids[0][0]
                                best_ask = asks[0][0]
                                mid = (best_bid + best_ask) / 2
                                
                                yield {
                                    "ts": time.time(),
                                    "symbol": self.symbol,
                                    "mid": mid,
                                    "bids": bids,
                                    "asks": asks,
                                    "provider": "BINANCE"
                                }
                            elif msg.type == aiohttp.WSMsgType.ERROR:
                                log.error("WS Error")
                                break
                                
            except Exception as e:
                log.error(f"Binance Adapter Error: {e}")
                await asyncio.sleep(5) # Reconnect delay

class CmeMdp3Adapter(BaseDOMAdapter):
    """
    Адаптер для CME MDP 3.0.
    В продакшене здесь должен быть парсер SBE (Simple Binary Encoding) и чтение мультикаста.
    Сейчас реализована эмуляция реалистичного потока данных (GC/ES).
    """
    async def iter_levels(self) -> AsyncIterable[Dict[str, Any]]:
        # TODO: yield self._normalize(bids, asks, "CME")
        raise NotImplementedError("Подключите реальный декодер CME MDP3.0")

def build_adapter(symbol: Optional[str] = None) -> BaseDOMAdapter:
    vendor = os.getenv("DOM_VENDOR", "MOCK").upper()
    if not symbol:
        symbol = os.getenv("SYMBOL", "XAUUSD")
    depth = int(os.getenv("DOM_DEPTH", "20"))
    
    if vendor == "BINANCE":
        return AsyncBinanceSpotDepthAdapter(symbol, depth)
    elif vendor == "CME":
        return CmeMdp3Adapter(symbol, depth)
    elif vendor == "MOCK":
        return MockDOMAdapter(symbol, depth)
    else:
        raise NotImplementedError(f"Vendor {vendor} not supported")
