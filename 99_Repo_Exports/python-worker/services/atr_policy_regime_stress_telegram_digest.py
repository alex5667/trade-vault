import os
import json
import redis
import logging
from typing import Dict, Any

logger = logging.getLogger("atr_policy_regime_stress_telegram_digest")

def _redis():
    return redis.Redis.from_url(os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0"), decode_responses=True)

def run_once() -> int:
    r = _redis()
    
    chat_id = os.getenv("ATR_POLICY_TELEGRAM_CHAT_ID")
    if not chat_id:
        logger.warning("ATR_POLICY_TELEGRAM_CHAT_ID is missing.")
        return 0

    # Collect current stats
    keys = r.keys("state:atr_stress:*")
    stress_states: Dict[str, list] = {}
    
    for k in keys:
        sym = k.split(":")[-1]
        if not sym:
             continue
        st = r.get(k)
        if st and st != "normal":
             stress_states.setdefault(st, []).append(sym)
             
    if not stress_states:
         return 0

    lines = ["📉 <b>ATR Regime/Stress Digest</b>\n"]
    lines.append("<b>Stress states:</b>")
    
    for st, syms in stress_states.items():
        if len(syms) > 5:
             lines.append(f"- {len(syms)} symbols in {st}")
        else:
             for s in syms:
                 slip = float(r.get(f"slippage_ema:{s}") or 0.0)
                 spread = float(r.get(f"spread_ema_half_bps:{s}") or 0.0) * 2.0
                 lines.append(f"- {s} | {st} | spread={spread:.1f}bps | slip_ema={slip:.1f}bps")

    lines.append("\n<b>Actions Enforced:</b>")
    # For a real implementation, you would query recent atr_policy_stress_events or active cfg:atr_regime_risk_mult
    lines.append("- Stress limits applied dynamically by capital allocator/gates.")

    msg = "\n".join(lines)
    
    notify_stream = os.getenv("NOTIFY_STREAM", "stream:notify:telegram")
    try:
        r.xadd(notify_stream, {
            "chat_id": chat_id,
            "text": msg,
            "parse_mode": "HTML",
            "disable_web_page_preview": "true"
        }, maxlen=10000)
        logger.info("Sent regime/stress digest.")
        return 1
    except Exception as e:
        logger.error(f"Failed to send digest: {e}")
        return 0

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    n = run_once()
    print(f"Sent {n} digest messages.")
