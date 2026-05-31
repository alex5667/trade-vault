
import redis
from core.redis_keys import RedisStreams as RS

r = redis.Redis(host='localhost', port=6379, db=0)
try:
    stream_name = RS.OF_GATE_METRICS
    entries = r.xrange(stream_name, count=1000)

    if not entries:
        print("No entries found in stream.")
        exit(0)

    print(f"Stats for last {len(entries)} entries:")
    have_need_pairs = {}
    ok_counts = {'0': 0, '1': 0}
    ok_soft_counts = {'0': 0, '1': 0}
    reasons = {}

    for entry_id, data in entries:
        decoded_data = {k.decode(): v.decode() for k, v in data.items()}

        have = decoded_data.get('have', 'N/A')
        need = decoded_data.get('need', 'N/A')
        ok = decoded_data.get('ok', 'N/A')
        ok_soft = decoded_data.get('ok_soft', 'N/A')
        reason = decoded_data.get('reason', 'N/A')

        pair = f"have={have}, need={need}"
        have_need_pairs[pair] = have_need_pairs.get(pair, 0) + 1

        if ok in ok_counts: ok_counts[ok] += 1
        if ok_soft in ok_soft_counts: ok_soft_counts[ok_soft] += 1

        reasons[reason] = reasons.get(reason, 0) + 1

    print("\nHave/Need distribution:")
    for pair, count in sorted(have_need_pairs.items()):
        print(f"  {pair}: {count}")

    print("\nOK distribution:")
    for val, count in ok_counts.items():
        print(f"  {val}: {count}")

    print("\nOK_SOFT distribution:")
    for val, count in ok_soft_counts.items():
        print(f"  {val}: {count}")

    print("\nTop Reasons:")
    for reason, count in sorted(reasons.items(), key=lambda x: x[1], reverse=True)[:5]:
        print(f"  {reason}: {count}")

except Exception as e:
    print(f"Error: {e}")
