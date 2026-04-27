#!/usr/bin/env python3
"""
Demo script for testing log filtering system.
Generates sample Prometheus logs and demonstrates filtering.
"""

import sys
import random
import time
from datetime import datetime

def generate_prometheus_logs(num_messages=50000):
    """Generate sample Prometheus log messages."""
    messages = [
        "write block completed",
        "Head GC started",
        "Head GC completed",
        "Creating checkpoint",
        "compact blocks",
        "Deleting obsolete block",
    ]

    components = ["tsdb", "head", "checkpoint", "compact"]
    sources = ["compact.go", "head.go", "checkpoint.go"]

    for i in range(num_messages):
        msg = random.choice(messages)
        component = random.choice(components)
        source = random.choice(sources)
        timestamp = datetime.now().isoformat() + "Z"

        # Add some variety to messages
        if msg == "compact blocks":
            count = random.randint(1, 5)
            duration = random.uniform(100, 500)
            print(f"scanner-prometheus | time={timestamp} level=INFO source={source} msg=\"{msg}\" component={component} count={count} duration={duration:.6f}ms")
        elif msg in ["Head GC started", "Head GC completed"]:
            caller = random.choice(["truncateMemory", "forceHeadGC"])
            duration = random.uniform(5, 20) if "completed" in msg else 0
            duration_str = f" duration={duration:.6f}ms" if duration > 0 else ""
            print(f"scanner-prometheus | time={timestamp} level=INFO source={source} msg=\"{msg}\" component={component} caller={caller}{duration_str}")
        elif msg == "write block completed":
            mint = random.randint(1766980800000, 1766988000000)
            maxt = mint + random.randint(3600000, 7200000)  # 1-2 hours
            ulid = f"01KDN{random.randint(100000, 999999)}ABCDEF{random.randint(100000, 999999)}"
            duration = random.uniform(50, 150)
            print(f"scanner-prometheus | time={timestamp} level=INFO source={source} msg=\"{msg}\" component={component} mint={mint} maxt={maxt} ulid={ulid} duration={duration:.5f}ms ooo=false")
        elif msg == "Creating checkpoint":
            segment_from = random.randint(50, 100)
            segment_to = segment_from + 1
            print(f"scanner-prometheus | time={timestamp} level=INFO source={source} msg=\"{msg}\" component={component} from_segment={segment_from} to_segment={segment_to} mint={random.randint(1766988000000, 1766990000000)}")
        elif msg == "Deleting obsolete block":
            ulid = f"01KD{random.choice(['HEC', 'HN7', 'M3Q'])}{random.randint(100000, 999999)}{random.choice(['XB9', '8Z3', 'WX4'])}{random.randint(100000, 999999)}"
            print(f"scanner-prometheus | time={timestamp} level=INFO source={source} msg=\"{msg}\" component={component} block={ulid}")

        # Small delay to simulate real-time logging
        if i % 1000 == 0:
            time.sleep(0.001)

def main():
    """Main demo function."""
    print("🔄 Generating sample Prometheus logs (50k messages)...")
    print("💡 Pipe this output to log_filter.py to see filtering in action:")
    print("   python test_log_filtering.py | ./log_filter.py prometheus")
    print("   python test_log_filtering.py | ./log_filter_advanced.py -t prometheus")
    print()

    generate_prometheus_logs(50000)

if __name__ == "__main__":
    main()
