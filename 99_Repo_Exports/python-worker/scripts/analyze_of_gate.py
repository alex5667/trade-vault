
from collections import Counter
from datetime import datetime

import redis
from core.redis_keys import RedisStreams as RS


def analyze_metrics():
    r = redis.Redis(host='localhost', port=6379, decode_responses=True)
    stream_name = RS.OF_GATE_METRICS

    # Fetch last 500 events
    events = r.xrevrange(stream_name, count=500)

    if not events:
        print("No events found in stream metrics:of_gate")
        return

    print(f"Fetched {len(events)} events from {stream_name}")

    parsed_events = []
    for entry_id, data in events:
        ts_ms = int(data.get('ts_ms', 0))
        symbol = data.get('symbol', 'unknown')
        parsed_events.append({
            'ts_ms': ts_ms,
            'symbol': symbol,
            'id': entry_id
        })

    if not parsed_events:
        return

    # Determine time range in the fetched data
    min_ts = min(e['ts_ms'] for e in parsed_events)
    max_ts = max(e['ts_ms'] for e in parsed_events)
    timespan_ms = max_ts - min_ts
    timespan_hours = timespan_ms / (1000 * 3600)

    print(f"Data spans {timespan_hours:.2f} hours (from {datetime.fromtimestamp(min_ts/1000)} to {datetime.fromtimestamp(max_ts/1000)})")

    symbol_counts = Counter(e['symbol'] for e in parsed_events)

    # Sampling rate is 0.10 (from strategy.py)
    sampling_rate = 0.10

    print("\nEstimation of N in 2h (sampling-corrected):")
    print(f"{'Symbol':<15} | {'Sampled N':<10} | {'Estimated N in 2h':<20}")
    print("-" * 50)

    for symbol, count in symbol_counts.most_common(20):
        # Estimated N in 2h = (count / timespan_hours) * 2 / sampling_rate
        if timespan_hours > 0:
            est_n_2h = (count / timespan_hours) * 2 / sampling_rate
            print(f"{symbol:<15} | {count:<10} | {int(est_n_2h):<20}")
        else:
            print(f"{symbol:<15} | {count:<10} | N/A (too short timespan)")

    # Print some raw data lines as requested
    print("\nFirst 10 raw events (as requested):")
    for i in range(min(10, len(events))):
        print(f"{events[i][0]}: {events[i][1]}")

if __name__ == "__main__":
    analyze_metrics()
