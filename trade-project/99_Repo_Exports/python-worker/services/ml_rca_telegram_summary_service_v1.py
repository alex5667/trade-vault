import asyncio
import os
import time
import contextlib
from core.redis_keys import RedisStreams as RS

try:
    import redis.asyncio as redis
except ImportError:
    redis = None

APP_NAME = "ml_rca_telegram_summary_service_v1"
INPUT_STREAM = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_CONTROLLER_DECISIONS_STREAM",
    "stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_controller_decisions"
)
TG_OUT_STREAM = RS.NOTIFY_TELEGRAM
GROUP = "ml_rca_tg_summary"
CONSUMER = os.getenv("HOSTNAME", APP_NAME)

REDIS_URL = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
SUMMARY_INTERVAL_SEC = int(os.getenv("ML_RCA_TG_SUMMARY_INTERVAL_SEC", "3600"))

async def ensure_group(client, stream_key, group):
    with contextlib.suppress(Exception):
        await client.xgroup_create(stream_key, group, id="$", mkstream=True)

async def main():
    if redis is None:
        raise RuntimeError("redis.asyncio is required")

    print(f"Starting {APP_NAME}, listening to {INPUT_STREAM}, reporting every {SUMMARY_INTERVAL_SEC}s")

    r = redis.from_url(REDIS_URL)
    await ensure_group(r, INPUT_STREAM, GROUP)

    stats = {
        "total_evaluations": 0,
        "primary_shadow_promotions": 0,
        "single_arm_promotions": 0,
        "winners": {}
    }

    last_report_ts = time.time()

    while True:
        try:
            # Wake up every few seconds to drain the queue
            rows = await r.xreadgroup(GROUP, CONSUMER, {INPUT_STREAM: ">"}, count=100, block=5000)

            if rows:
                for stream, messages in rows:
                    for msg_id, payload in messages:
                        try:
                            # Convert bytes to string if needed
                            dec = {k.decode() if isinstance(k, bytes) else k: v.decode() if isinstance(v, bytes) else v for k, v in payload.items()}

                            stats["total_evaluations"] += 1
                            decision = dec.get("decision", "HOLD")
                            winner = dec.get("winner_arm", "none")

                            if decision == "APPLY_PRIMARY_ARM_SHADOW":
                                stats["primary_shadow_promotions"] += 1
                            elif decision == "APPLY_SINGLE_ARM":
                                stats["single_arm_promotions"] += 1

                            stats["winners"][winner] = stats["winners"].get(winner, 0) + 1

                        except Exception as e:
                            print(f"Error parsing msg {msg_id}: {e}")
                        finally:
                            await r.xack(INPUT_STREAM, GROUP, msg_id)

            # Check if it's time to report
            now = time.time()
            if now - last_report_ts >= SUMMARY_INTERVAL_SEC:
                if stats["total_evaluations"] > 0:
                    lines = [
                        "🤖 <b>ML RCA Hourly Governance Summary</b> 🤖",
                        f"🕒 Evaluations this hour: {stats['total_evaluations']}",
                        "",
                        "📈 <b>Promotions:</b>",
                        f"- Shadow Primary: {stats['primary_shadow_promotions']}",
                        f"- Single Arm: {stats['single_arm_promotions']}",
                        "",
                        "🏆 <b>Winner Breakdown:</b>"
                    ]
                    for w, cnt in sorted(stats["winners"].items(), key=lambda x: x[1], reverse=True):
                        lines.append(f"- <code>{w}</code>: {cnt}")

                    msg = "\n".join(lines)
                    print(f"Publishing summary to {TG_OUT_STREAM}:\n{msg}")

                    # Publish to standard telegram notifier stream
                    await r.xadd(TG_OUT_STREAM, {"message": msg, "parse_mode": "HTML"}, maxlen=10000, approximate=True)

                # Reset counters
                stats = {
                    "total_evaluations": 0,
                    "primary_shadow_promotions": 0,
                    "single_arm_promotions": 0,
                    "winners": {}
                }
                last_report_ts = now

        except Exception as e:
            print(f"Error in main loop: {e}")
            await asyncio.sleep(5)

if __name__ == "__main__":
    asyncio.run(main())
