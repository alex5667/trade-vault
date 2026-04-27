import asyncio
import time
from persistence.postgres_pool import PostgresPool
from services.orderflow.calibration_service import CalibrationService
from persistence.calibration_repo import CalibrationRepo
class MockRuntime:
    symbol = "1000PEPEUSDT"
    tick_dn_calib = type("MockCalib", (), {
        "to_state": lambda self, x: {"quantiles": [1.0, 2.0, 3.0], "counts": 100}
    })()

async def run():
    pool = PostgresPool()
    await pool.connect()
    repo = CalibrationRepo(pool=pool, redis_client=None)
    svc = CalibrationService(repo)
    rt = MockRuntime()
    print("Testing stick_dn persistence...")
    await svc.persist_tick_dn(rt, "normal", int(time.time()*1000))
    print("Success. Executing check...")
    res = await pool.fetch("SELECT * FROM calibration_state WHERE kind='tick_dn' AND symbol='1000PEPEUSDT'")
    print("DB records:", len(res))

if __name__ == "__main__":
    asyncio.run(run())
