import asyncio
import os
import tracemalloc
import time
import logging

from services.crypto_orderflow_service import CryptoOrderflowService

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("tracemalloc_debug")

async def memory_dumper():
    """Периодически печатает топ потребления памяти"""
    last_snapshot = None
    while True:
        await asyncio.sleep(10)
        snapshot = tracemalloc.take_snapshot()
        
        log.info(f"\n[{time.strftime('%H:%M:%S')}] --- Top 10 memory consuming lines ---")
        top_stats = snapshot.statistics('lineno')
        
        for stat in top_stats[:10]:
            log.info(str(stat))
            
        if last_snapshot:
            log.info(f"\n[{time.strftime('%H:%M:%S')}] --- Top 5 memory differences ---")
            top_diffs = snapshot.compare_to(last_snapshot, 'lineno')
            for stat in top_diffs[:5]:
                log.info(str(stat))
                
        last_snapshot = snapshot


async def main():
    log.info("Starting tracemalloc...")
    tracemalloc.start(25)  
    
    # Ограничиваем символы для теста
    os.environ["CRYPTO_SYMBOLS_SET_KEY"] = "crypto:symbols:debug"
    os.environ["CRYPTO_DEFAULT_SYMBOLS_ENABLED"] = "false"
    
    redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    
    log.info("Initializing CryptoOrderflowService...")
    service = CryptoOrderflowService(redis_dsn=redis_url)
    
    import redis.asyncio as aioredis
    temp_redis = aioredis.from_url(redis_url, decode_responses=True)
    await temp_redis.sadd("crypto:symbols:debug", "BTCUSDT") # Только один символ
    await temp_redis.close()

    # Запускаем фоновый дамп памяти
    asyncio.create_task(memory_dumper())
    
    log.info("Running service...")
    try:
        await asyncio.wait_for(service.run_forever(), timeout=25.0)
    except asyncio.TimeoutError:
        log.info("Traced for 25 seconds.. exiting")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as e:
        log.error(f"Error {e}")

