
import sys
from collections import Counter
from datetime import datetime


def parse_redis_xrange_output(lines):
    events = []
    current_event = {}
    i = 0
    while i < len(lines):
        line = lines[i].strip()
        # Entry ID like 1769829900860-0
        if '-' in line and line.replace('-', '').isdigit():
            if current_event:
                events.append(current_event)
            current_event = {'_id': line}
            i += 1
            continue

        # Key-Value pairs
        key = line
        if i + 1 < len(lines):
            val = lines[i+1].strip()
            current_event[key] = val
            i += 2
        else:
            i += 1

    if current_event:
        events.append(current_event)
    return events

def analyze(events):
    if not events:
        print("No events to analyze")
        return

    # Sampling rate from docker-compose
    sampling_rate = 0.05

    parsed = []
    for e in events:
        try:
            ts_ms = int(e.get('ts_ms', 0))
            symbol = e.get('symbol', 'unknown')
            if ts_ms > 0:
                parsed.append({'ts_ms': ts_ms, 'symbol': symbol})
        except Exception:
            continue

    if not parsed:
        print("No valid timestamps found")
        return

    min_ts = min(e['ts_ms'] for e in parsed)
    max_ts = max(e['ts_ms'] for e in parsed)
    timespan_ms = max_ts - min_ts
    timespan_hours = timespan_ms / (1000 * 3600)

    # We want N in 2h.
    # Estimated N in 2h = (Count / timespan_hours) * 2 / sampling_rate

    counts = Counter(e['symbol'] for e in parsed)

    print(f"Analyzed {len(parsed)} events spanning {timespan_hours:.4f} hours")
    print(f"Sampling rate: {sampling_rate*100}%")
    print(f"Time range: {datetime.fromtimestamp(min_ts/1000)} to {datetime.fromtimestamp(max_ts/1000)}")
    print("\n" + "="*60)
    print(f"{'Symbol':<15} | {'Hits':<8} | {'Est N (2h)':<12} | {'SPS (raw)':<8}")
    print("-" * 60)

    for symbol, hit_count in counts.most_common(30):
        if timespan_hours > 0:
            est_n_2h = (hit_count / timespan_hours) * 2 / sampling_rate
            # SPS = total events per second for this symbol
            # total_events = hit_count / sampling_rate
            # sps = total_events / (timespan_ms / 1000)
            sps = (hit_count / sampling_rate) / (timespan_ms / 1000) if timespan_ms > 0 else 0
            print(f"{symbol:<15} | {hit_count:<8} | {int(est_n_2h):<12} | {sps:.3f}")
        else:
            print(f"{symbol:<15} | {hit_count:<8} | N/A")

if __name__ == "__main__":
    lines = sys.stdin.readlines()
    events = parse_redis_xrange_output(lines)
    analyze(events)
