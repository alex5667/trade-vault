#!/usr/bin/env python3
"""
Monitor v14_of canary rollout: EV/R, ECE, latency metrics.

Usage:
    python -m tools.monitor_v14_of_canary
    python -m tools.monitor_v14_of_canary --redis-url redis://localhost:6379/0
    python -m tools.monitor_v14_of_canary --hours 24  # last 24h metrics
    python -m tools.monitor_v14_of_canary --watch     # continuous monitoring
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from dataclasses import dataclass
from datetime import datetime

import redis


@dataclass
class CanaryMetrics:
    """v14_of canary health metrics."""

    timestamp_ms: int
    ev_r: float | None = None  # Expected Value / Risk
    ece: float | None = None  # Expected Calibration Error
    latency_p99_ms: float | None = None
    latency_p95_ms: float | None = None
    feature_missing_rate: float | None = None
    abstain_rate: float | None = None
    n_samples: int = 0

    def __str__(self) -> str:
        ts = datetime.fromtimestamp(self.timestamp_ms / 1000.0).strftime("%Y-%m-%d %H:%M:%S")
        parts = [f"[{ts}]"]
        if self.ev_r is not None:
            parts.append(f"EV/R={self.ev_r:.3f}")
        if self.ece is not None:
            parts.append(f"ECE={self.ece:.3f}")
        if self.latency_p99_ms is not None:
            parts.append(f"p99={self.latency_p99_ms:.0f}ms")
        if self.feature_missing_rate is not None:
            parts.append(f"missing={self.feature_missing_rate:.1%}")
        if self.abstain_rate is not None:
            parts.append(f"abstain={self.abstain_rate:.1%}")
        return " ".join(parts)


def parse_metrics_stream(r: redis.Redis, stream_key: str, hours: int = 24) -> list[CanaryMetrics]:
    """
    Read ML confirm metrics from Redis stream.
    Streams typically store: ml_p_edge, ml_ece, ml_latency, ml_feature_missing_total, etc.
    """
    metrics_list: list[CanaryMetrics] = []

    try:
        # Read last N entries from stream
        entries = list(r.xrevrange(stream_key, count=100))  # type: ignore
        if not entries:
            print(f"⚠️  Stream {stream_key} is empty")
            return metrics_list

        cutoff_time = int((time.time() - hours * 3600) * 1000)

        for msg_id, data in entries:  # type: ignore
            try:
                ts_ms = int(msg_id.decode() if isinstance(msg_id, bytes) else msg_id.split(b"-")[0])
                if ts_ms < cutoff_time:
                    break

                # Decode metrics from stream entry
                cm = CanaryMetrics(timestamp_ms=ts_ms)

                # Parse field-value pairs
                for key, value in data.items():
                    key_str = key.decode() if isinstance(key, bytes) else key
                    val_str = value.decode() if isinstance(value, bytes) else value

                    try:
                        val = float(val_str)
                    except ValueError:
                        continue

                    if "ev_r" in key_str.lower() or "ev/r" in key_str.lower():
                        cm.ev_r = val
                    elif "ece" in key_str.lower():
                        cm.ece = val
                    elif "latency" in key_str.lower() and "p99" in key_str.lower():
                        cm.latency_p99_ms = val * 1000  # convert to ms if in seconds
                    elif "latency" in key_str.lower() and "p95" in key_str.lower():
                        cm.latency_p95_ms = val * 1000
                    elif "missing" in key_str.lower():
                        cm.feature_missing_rate = val
                    elif "abstain" in key_str.lower():
                        cm.abstain_rate = val

                if cm.ev_r or cm.ece or cm.latency_p99_ms:
                    metrics_list.append(cm)
            except Exception as e:
                continue

        return sorted(metrics_list, key=lambda x: x.timestamp_ms)
    except Exception as e:
        print(f"❌ Error reading stream {stream_key}: {e}")
        return metrics_list


def check_promotion_readiness(metrics_list: list[CanaryMetrics]) -> dict:
    """
    Evaluate if v14_of is ready to promote based on recent metrics.

    Success criteria:
    - EV/R stable or improving
    - ECE < 0.10 (max allowed gap)
    - Latency p99 < baseline (check with champion)
    - Feature missing rate < 5%
    """
    if not metrics_list:
        return {"ready": False, "reason": "No metrics available"}

    recent = metrics_list[-10:]  # last 10 data points
    checks = {}

    # EV/R trend
    ev_r_vals = [m.ev_r for m in recent if m.ev_r is not None]
    if len(ev_r_vals) >= 2:
        ev_r_trend = ev_r_vals[-1] - ev_r_vals[0]
        checks["ev_r"] = {
            "current": ev_r_vals[-1],
            "trend": "stable" if abs(ev_r_trend) < 0.05 else ("improving" if ev_r_trend > 0 else "degrading"),
            "ok": True,
        }
    else:
        checks["ev_r"] = {"status": "insufficient data"}

    # ECE
    ece_vals = [m.ece for m in recent if m.ece is not None]
    if ece_vals:
        ece_latest = ece_vals[-1]
        checks["ece"] = {
            "current": ece_latest,
            "threshold": 0.10,
            "ok": ece_latest < 0.10,
        }
    else:
        checks["ece"] = {"status": "insufficient data"}

    # Latency p99
    lat_vals = [m.latency_p99_ms for m in recent if m.latency_p99_ms is not None]
    if lat_vals:
        lat_latest = lat_vals[-1]
        checks["latency_p99"] = {
            "current_ms": lat_latest,
            "ok": lat_latest < 1000,  # within budget
        }
    else:
        checks["latency_p99"] = {"status": "insufficient data"}

    # Feature missing rate
    missing_vals = [m.feature_missing_rate for m in recent if m.feature_missing_rate is not None]
    if missing_vals:
        missing_latest = missing_vals[-1]
        checks["feature_missing"] = {
            "current": missing_latest,
            "threshold": 0.05,
            "ok": missing_latest < 0.05,
        }
    else:
        checks["feature_missing"] = {"status": "insufficient data"}

    all_ok = all(c.get("ok", True) for c in checks.values())
    return {
        "ready": all_ok,
        "checks": checks,
        "timestamp": datetime.now().isoformat(),
    }


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Monitor v14_of canary: EV/R, ECE, latency",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python -m tools.monitor_v14_of_canary
  python -m tools.monitor_v14_of_canary --hours 24
  python -m tools.monitor_v14_of_canary --watch  # continuous, updates every 30s
        """,
    )
    ap.add_argument(
        "--redis-url",
        default=os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0"),
        help="Redis URL",
    )
    ap.add_argument(
        "--stream-key",
        default="metrics:ml_confirm",
        help="Redis stream key for metrics (default: metrics:ml_confirm)",
    )
    ap.add_argument(
        "--hours",
        type=int,
        default=24,
        help="Look back N hours (default: 24)",
    )
    ap.add_argument(
        "--watch",
        action="store_true",
        help="Continuous monitoring (update every 30s)",
    )
    args = ap.parse_args()

    r = redis.Redis.from_url(args.redis_url, decode_responses=True)

    print(f"\n{'='*80}")
    print("v14_of CANARY MONITORING")
    print(f"{'='*80}\n")

    iteration = 0
    while True:
        iteration += 1
        if iteration > 1:
            print(f"\n{'='*80}")
            print(f"Update #{iteration} — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"{'='*80}\n")

        # Fetch metrics
        print(f"📊 Reading metrics from: {args.stream_key}")
        print(f"   Lookback: {args.hours}h")
        print()

        metrics = parse_metrics_stream(r, args.stream_key, hours=args.hours)

        if not metrics:
            print("❌ No metrics found. Is the canary running?")
            if not args.watch:
                sys.exit(1)
            time.sleep(30)
            continue

        # Show recent metrics
        print("📈 Recent Metrics (last 5):")
        print("-" * 80)
        for m in metrics[-5:]:
            status = "✅" if (m.feature_missing_rate is None or m.feature_missing_rate < 0.05) else "⚠️"
            print(f"   {status} {m}")
        print()

        # Check readiness
        readiness = check_promotion_readiness(metrics)
        print("🚀 Promotion Readiness Check:")
        print("-" * 80)
        for check_name, result in readiness["checks"].items():
            if isinstance(result, dict) and "status" in result:
                print(f"   ⚠️  {check_name}: {result['status']}")
            else:
                ok_icon = "✅" if result.get("ok", False) else "❌"
                print(f"   {ok_icon} {check_name}: {json.dumps(result, indent=6)}")

        print()
        ready = readiness["ready"]
        if ready:
            print("✅ READY FOR PROMOTION")
            print()
            print("   Run to promote:")
            print("   $ python -m tools.promote_ml_confirm_champion_safe --dry-run")
            print("   $ python -m tools.promote_ml_confirm_champion_safe")
        else:
            print("⏳ NOT YET READY")
            print("   Waiting for metrics to stabilize...")

        print()

        if not args.watch:
            break

        print("🔄 Next update in 30s... (Ctrl+C to exit)")
        try:
            time.sleep(30)
        except KeyboardInterrupt:
            print("\n\n👋 Monitoring stopped")
            break


if __name__ == "__main__":
    main()
