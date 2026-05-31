import asyncio
import redis
import json
import httpx
from notify_worker import BotCallbackPoller
import os
from dotenv import load_dotenv

load_dotenv(".env")

r = redis.Redis(host='redis-worker-1', decode_responses=True)
poller = BotCallbackPoller(r)

class MockClient:
    async def post(self, url, json=None, data=None):
        print(f"POST {url} with {json} {data}")

async def test():
    # Setup pending state
    r.set("trail:calib:pending:test_123", json.dumps({
        "status": "PENDING",
        "n_params": 10,
        "symbols": ["BTCUSDT", "ETHUSDT"],
        "shadow_summary": {"n_better": 5, "n_neutral": 0, "n_worse": 1, "avg_delta_r": 1.2}
    }))
    
    update = {
        "callback_query": {
            "id": "cb123",
            "data": "trail_reject:test_123",
            "from": {"username": "testuser", "id": 123},
            "message": {
                "chat": {"id": 12345},
                "message_id": 678
            }
        }
    }
    
    await poller.handle_update(MockClient(), update)

if __name__ == '__main__':
    asyncio.run(test())
