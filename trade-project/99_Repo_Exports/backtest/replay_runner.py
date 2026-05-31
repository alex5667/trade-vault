# -*- coding: utf-8 -*-
"""
Простой оффлайн-реплей для hub.
"""

import argparse
import time
import json
import csv
import redis

from infra.config import load_config
from infra.redis_client import get_redis


def write_tick(r: redis.Redis, last_key: str, stream: str, row: dict):
    tick = {"ts": int(row["ts"]), "bid": float(row["bid"]), "ask": float(row["ask"]), "last": float(row["last"]) }
    r.set(last_key, json.dumps(tick))
    r.xadd(stream, {"data": json.dumps(tick)}, maxlen=100000)


def write_book(r: redis.Redis, key: str, rows: list):
    r.set(key, json.dumps(rows))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", required=True)
    ap.add_argument("--book", default="")
    ap.add_argument("--symbol", default="XAUUSD")
    ap.add_argument("--rate", type=int, default=20, help="тик/сек")
    args = ap.parse_args()

    cfg = load_config()
    r = get_redis(cfg.redis_url)

    last_key = cfg.last_tick_key_tpl.replace("{SYMBOL}", args.symbol)
    stream = cfg.tick_stream_tpl.replace("{SYMBOL}", args.symbol)
    book_key = cfg.dom_levels_key_tpl.replace("{SYMBOL}", args.symbol)

    with open(args.csv, "r") as f:
        rdr = csv.DictReader(f)
        for row in rdr:
            write_tick(r, last_key, stream, row)
            if args.book:
                pass
            time.sleep(max(0.001, 1.0 / args.rate))

    print("Replay finished")


if __name__ == "__main__":
    main()


