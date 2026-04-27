import redis
import time

r = redis.from_url("redis://redis-worker-1:6379/0", decode_responses=True)
stream = "notify:telegram"

message_data = {
    "type": "report",
    "text": "🔍 TEST REPORT FROM ANTIGRAVITY 🔍\nIf you see this, the pipeline works.",
    "parse_mode": "HTML",
    "source": "AntigravityTest",
    "severity": "info",
    "timestamp": str(int(time.time() * 1000)),
}

print(f"Sending test report to {stream}...")
msg_id = r.xadd(stream, message_data, maxlen=2000)
print(f"Sent msg_id={msg_id}")
