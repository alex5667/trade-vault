import asyncio
from services.persistence_manager import PersistenceManager

async def test():
    pm = PersistenceManager()
    bar = {'ts_ms': 1999999999000, 'open': 1.0, 'high': 2.0, 'low': 0.5, 'close': 1.5, 'vol': 100, 'cvd': 10}
    try:
        res = await pm.save_microbar('TESTUSDT', bar)
        print('Result:', res)
    finally:
        await pm.close()

if __name__ == "__main__":
    asyncio.run(test())
