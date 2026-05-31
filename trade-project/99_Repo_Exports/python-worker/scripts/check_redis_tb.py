import os

import redis

from utils.time_utils import get_ny_time_millis


def main():
    redis_url = os.getenv('REDIS_URL', 'redis://redis-worker-1:6379/0')
    r = redis.Redis.from_url(redis_url, decode_responses=True)
    now = get_ny_time_millis()
    print("Connecting to Redis:", redis_url)
    due = r.zrangebyscore('tb:jobs:due', min=0, max=now, start=0, num=10)
    jobs_in_set = r.zcard('tb:jobs:due')
    print('Jobs in ZSET:', jobs_in_set)
    print('Due now (first 10):', due)

    last_err = r.get('tb:last_err_ts_ms')
    last_lbl = r.get('tb:last_label_ts_ms')
    last_inp = r.get('tb:last_ts_ms')
    print('Last Err:', last_err, '(', now - int(last_err) if last_err else 'None', 'ms ago)')
    print('Last Lbl:', last_lbl, '(', now - int(last_lbl) if last_lbl else 'None', 'ms ago)')
    print('Last Inp:', last_inp, '(', now - int(last_inp) if last_inp else 'None', 'ms ago)')

if __name__ == '__main__':
    main()
