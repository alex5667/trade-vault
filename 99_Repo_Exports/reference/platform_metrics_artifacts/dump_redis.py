import redis
import csv

def main():
    try:
        r = redis.Redis(host='127.0.0.1', port=6379, db=0, decode_responses=True)
        keys = r.keys('*')
        
        # We will group by pattern to avoid huge csv
        patterns = {}
        for k in keys:
            parts = k.split(':')
            if len(parts) > 1:
                pattern = ":".join(parts[:-1]) + ":*"
            else:
                pattern = k
            
            if pattern not in patterns:
                patterns[pattern] = {
                    "count": 0,
                    "type": r.type(k),
                    "ttl": r.ttl(k),
                    "sample": k
                }
            patterns[pattern]["count"] += 1

        with open('/home/alex/front/trade/scanner_infra/reference/platform_metrics_artifacts/redis_keys.csv', 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['pattern', 'type', 'ttl', 'count', 'sample_key'])
            for p, data in patterns.items():
                writer.writerow([p, data['type'], data['ttl'], data['count'], data['sample']])
        print("Redis map exported successfully.")

    except Exception as e:
        print(f"Failed to export redis map: {e}")

if __name__ == '__main__':
    main()
