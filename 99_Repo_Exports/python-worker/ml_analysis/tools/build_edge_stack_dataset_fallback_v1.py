import argparse
import json
import logging

import redis

from ml_analysis.tools.replay_inputs_reader_v1 import ReplayInputsReader
from core.redis_keys import RedisStreams as RS

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def build_dataset(
    redis_url: str,
    signal_stream: str,
    closed_stream: str,
    archive_dir: str | None = None,
    signals_count: int = 200000,
    closes_count: int = 200000,
    out_jsonl: str = "dataset.jsonl",
    fallback_enabled: bool = True
):
    """Build dataset by joining signals and closed trades."""
    r = redis.from_url(redis_url, decode_responses=True)

    logger.info(f"Fetching up to {closes_count} closed trades from {closed_stream}...")
    # Using XREVRANGE to get recent trades
    closed_raw = r.xrevrange(closed_stream, count=closes_count)
    if not closed_raw:
        logger.warning("No closed trades found in stream")
        return

    closed_trades = []
    min_ts_ms = None
    max_ts_ms = None

    for msg_id, data in closed_raw:
        record = json.loads(data['data']) if 'data' in data else data
        ts = record.get('ts_ms') or record.get('ts')
        if ts:
            if min_ts_ms is None or ts < min_ts_ms: min_ts_ms = ts
            if max_ts_ms is None or ts > max_ts_ms: max_ts_ms = ts
        closed_trades.append(record)

    logger.info(f"Collected {len(closed_trades)} trades. Time range: {min_ts_ms} -> {max_ts_ms}")

    # Collected signals from Redis
    logger.info(f"Fetching up to {signals_count} signals from Redis {signal_stream}...")
    signals_raw = r.xrevrange(signal_stream, count=signals_count)
    signals_by_sid = {}
    redis_min_ts = None

    for msg_id, data in signals_raw:
        record = json.loads(data['data']) if 'data' in data else data
        sid = record.get('sid')
        if sid:
            signals_by_sid[sid] = record

        ts = record.get('ts_ms') or record.get('ts')
        if ts:
            if redis_min_ts is None or ts < redis_min_ts:
                redis_min_ts = ts

    logger.info(f"Collected {len(signals_by_sid)} unique signals from Redis. Oldest Redis signal TS: {redis_min_ts}")

    # Fallback to archives if needed
    if fallback_enabled and archive_dir and min_ts_ms and (redis_min_ts is None or redis_min_ts > min_ts_ms):
        needed_start = min_ts_ms
        needed_end = redis_min_ts if redis_min_ts else max_ts_ms

        logger.info(f"Redis data insufficient. Reading from archives: {needed_start} -> {needed_end}")
        reader = ReplayInputsReader(archive_dir)
        archive_count = 0
        for record in reader.read_records(start_ts_ms=needed_start, end_ts_ms=needed_end):
            sid = record.get('sid')
            if sid and sid not in signals_by_sid:
                signals_by_sid[sid] = record
                archive_count += 1
        logger.info(f"Added {archive_count} signals from archives. Total signals: {len(signals_by_sid)}")

    # Join and write
    joined_count = 0
    with open(out_jsonl, 'w') as f:
        for trade in closed_trades:
            sid = trade.get('sid')
            if not sid:
                continue

            signal = signals_by_sid.get(sid)
            if signal:
                # Merge signal features into trade record or vice versa
                # Simple merge: trade data + signal data (prefixed if needed)
                combined = {**signal, **trade}
                f.write(json.dumps(combined) + "\n")
                joined_count += 1

    logger.info(f"Dataset built: {joined_count} joined records saved to {out_jsonl}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Edge Stack Dataset Builder with Fallback")
    parser.add_argument("--redis_url", default="redis://localhost:6379/0")
    parser.add_argument("--signal_stream", default=RS.OF_INPUTS)
    parser.add_argument("--closed_stream", default=RS.TRADES_CLOSED)
    parser.add_argument("--archive_dir", help="Directory with Replay Input archives")
    parser.add_argument("--signals_count", type=int, default=200000)
    parser.add_argument("--closes_count", type=int, default=200000)
    parser.add_argument("--out", default="edge_train.jsonl")
    parser.add_argument("--no_fallback", action="store_true", help="Disable archive fallback")

    args = parser.parse_args()

    build_dataset(
        redis_url=args.redis_url,
        signal_stream=args.signal_stream,
        closed_stream=args.closed_stream,
        archive_dir=args.archive_dir,
        signals_count=args.signals_count,
        closes_count=args.closes_count,
        out_jsonl=args.out,
        fallback_enabled=not args.no_fallback
    )
