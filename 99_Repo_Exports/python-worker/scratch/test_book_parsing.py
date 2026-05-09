
import asyncio
import sys

import redis.asyncio as aioredis

# Add paths
sys.path.append("/home/alex/front/trade/scanner_infra/python-worker")

from services.orderflow.utils import _fields_to_dict, _parse_book_payload


async def test():
    r = aioredis.from_url("redis://localhost:6379/0", decode_responses=True)
    # Get one message from stream:book_SOLUSDT
    res = await r.xrevrange("stream:book_SOLUSDT", count=1)
    if not res:
        print("No book messages found")
        return

    msg_id, fields = res[0]
    print(f"Raw fields keys: {list(fields.keys())}")
    print(f"Bids type: {type(fields.get('bids'))}")

    # Simulate BookProcessor logic
    raw = _fields_to_dict(fields)
    print(f"After _fields_to_dict Bids type: {type(raw.get('bids'))}")

    book_raw = _parse_book_payload(raw, "SOLUSDT")
    print(f"After _parse_book_payload Bids type: {type(book_raw.get('bids'))}")
    print(f"Bids first 50 chars/items: {(book_raw.get('bids'))[:50]}")

if __name__ == "__main__":
    asyncio.run(test())
