import asyncio
import redis.asyncio as aioredis

async def main():
    try:
        pool = aioredis.BlockingConnectionPool.from_url(
            "redis://localhost", max_connections=10, timeout=10
        )
        r = aioredis.Redis(connection_pool=pool)
        await r.ping()
        print("Success BlockingConnectionPool.from_url")
    except Exception as e:
        print(f"Error BlockingConnectionPool.from_url: {e}")

    try:
        r2 = aioredis.from_url("redis://localhost", connection_pool_class=aioredis.BlockingConnectionPool, max_connections=10, timeout=10)
        await r2.ping()
        print("Success from_url with connection_pool_class")
    except Exception as e:
        print(f"Error from_url with connection_pool_class: {e}")

asyncio.run(main())
