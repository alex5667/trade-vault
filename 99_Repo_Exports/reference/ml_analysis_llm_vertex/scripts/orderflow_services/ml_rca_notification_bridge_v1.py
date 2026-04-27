import os
import json
import asyncio
import logging
from typing import Any, Dict

try:
    import redis.asyncio as redis
except ImportError:
    redis = None

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("ml_rca_notification_bridge")

INPUT_STREAM = os.getenv("ML_ANALYSIS_RESULTS_STREAM", "stream:ml:analysis_results")
NOTIFY_STREAM = os.getenv("ML_RCA_NOTIFY_STREAM", "notify:telegram")
GROUP = os.getenv("ML_RCA_NOTIFICATION_BRIDGE_GROUP", "cg_ml_rca_notification_bridge_v1")
CONSUMER = os.getenv("HOSTNAME", "ml_rca_bridge_worker")
MAXLEN = int(os.getenv("ML_RCA_NOTIFY_MAXLEN", "10000"))

def extract_summary(output: Dict[str, Any]) -> str:
    # Attempt to extract summary from common LLM output keys
    if "summary" in output and output["summary"]:
        return str(output["summary"])
    if "root_cause_summary" in output and output["root_cause_summary"]:
        return str(output["root_cause_summary"])
    if "insights" in output and output["insights"]:
        return str(output["insights"])
    
    # Fallback to taking the first relevant string
    for k, v in output.items():
        if isinstance(v, str) and len(v) > 20 and k not in {"reason_code", "category", "incident_id"}:
            return v
            
    return json.dumps(output, ensure_ascii=False)

def extract_reason_code(output: Dict[str, Any]) -> str:
    if "reason_code" in output:
        return str(output["reason_code"])
    if "primary_reason_codes" in output and isinstance(output["primary_reason_codes"], list):
        return ", ".join(map(str, output["primary_reason_codes"]))
    if "category" in output:
        return str(output["category"])
    return "UNKNOWN"

async def ensure_group(client: redis.Redis, stream_key: str, group: str) -> None:
    try:
        await client.xgroup_create(stream_key, group, id="$", mkstream=True)
        logger.info(f"Created consumer group {group} for {stream_key}")
    except Exception as e:
        if "BUSYGROUP" not in str(e):
            logger.error(f"Error creating group: {e}")

async def main() -> None:
    if redis is None:
        raise RuntimeError("redis.asyncio is required")
        
    redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    r = redis.from_url(redis_url, decode_responses=False)
    
    await ensure_group(r, INPUT_STREAM, GROUP)
    
    logger.info(f"Started RCA Notification Bridge. Listening to {INPUT_STREAM}, sending to {NOTIFY_STREAM}")
    
    while True:
        try:
            rows = await r.xreadgroup(GROUP, CONSUMER, {INPUT_STREAM: ">"}, count=10, block=5000)
            if not rows:
                continue
                
            for stream, messages in rows:
                for msg_id, payload in messages:
                    try:
                        # Decode payload keys and values
                        out_row = {
                            k.decode() if isinstance(k, bytes) else k: 
                            v.decode() if isinstance(v, bytes) else v 
                            for k, v in payload.items()
                        }
                        
                        task_type = out_row.get("task_type", "RCA")
                        provider = out_row.get("provider", "LLM")
                        request_id = out_row.get("request_id", "unknown")
                        
                        output_json_str = out_row.get("output_json", "{}")
                        try:
                            output_data = json.loads(output_json_str)
                        except json.JSONDecodeError:
                            output_data = {"raw": output_json_str}
                            
                        # Extract insights
                        reason_code = extract_reason_code(output_data)
                        summary = extract_summary(output_data)
                        
                        # Only send if it's not a trivial or fully rejected task
                        if "REJECT" in reason_code.upper():
                            await r.xack(INPUT_STREAM, GROUP, msg_id)
                            continue

                        # Format for Telegram
                        message = (
                            f"🤖 <b>[ML RCA Insight]</b>\n"
                            f"<b>ID:</b> {request_id}\n"
                            f"<b>Task:</b> {task_type}\n"
                            f"<b>Provider:</b> {provider}\n"
                            f"<b>Reason Code:</b> {reason_code}\n\n"
                            f"📝 <b>Summary:</b> {summary}\n"
                            f"<i>#ML_RCA_INSIGHT</i>"
                        )
                        
                        notify_payload = {
                            "type": "report",
                            "source": "ml_rca_bridge",
                            "symbol": "ML_RCA",
                            "level": "INFO",
                            "text": message
                        }
                        
                        await r.xadd(NOTIFY_STREAM, notify_payload, maxlen=MAXLEN, approximate=True)
                        logger.info(f"Published RCA Notification for {request_id} to {NOTIFY_STREAM}")
                        
                        await r.xack(INPUT_STREAM, GROUP, msg_id)
                        
                    except Exception as loop_e:
                        logger.error(f"Error processing message {msg_id}: {loop_e}")
                        # Ack it anyway to not block the pipeline
                        await r.xack(INPUT_STREAM, GROUP, msg_id)
                        
        except Exception as e:
            logger.error(f"Redis consumer loop error: {e}")
            await asyncio.sleep(1)

if __name__ == "__main__":
    asyncio.run(main())
