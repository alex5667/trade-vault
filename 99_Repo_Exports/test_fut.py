import concurrent.futures
import logging

logger = logging.getLogger()
logger.addHandler(logging.StreamHandler())

def db_task():
    try:
        raise ValueError("DB Error")
    except Exception:
        logger.exception("Failed %s")

def cb(fut):
    exc = fut.exception()
    if exc:
        print("FUT EXC:", type(exc).__name__, exc)

with concurrent.futures.ThreadPoolExecutor() as ex:
    f = ex.submit(db_task)
    f.add_done_callback(cb)

