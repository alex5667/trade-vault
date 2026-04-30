from __future__ import annotations
from utils.time_utils import get_ny_time_millis

import json
import os
import time
from typing import Any, Dict

import redis


def _coerce_hash_cfg(h: dict) -> dict:
    """Convert Redis hash to config dict with defaults."""
    cfg = {str(k): v for k, v in (h or {}).items()}
    cfg.setdefault("mode", "SHADOW")
    cfg.setdefault("fail_policy", "OPEN")
    cfg.setdefault("enforce_share", 0.05)
    cfg.setdefault("bootstrap_ms", get_ny_time_millis())
    return cfg


def _safe_loads(s: Any) -> Dict[str, Any]:
    """Safely load JSON from string/bytes/dict."""
    try:
        if s is None:
            return {}
        if isinstance(s, dict):
            return s
        if isinstance(s, bytes):
            s = s.decode("utf-8", "ignore")
        return json.loads(str(s))
    except Exception:
        return {}


def _is_valid_cfg(cfg: Dict[str, Any]) -> bool:
    if not isinstance(cfg, dict) or not cfg:
        return False
    rid = str(cfg.get("run_id", "") or "")
    return bool(rid)


def _notify(r: redis.Redis, stream: str, text: str, subtype: str = "ml_promo") -> None:
    try:
        r.xadd(stream, {"type": "alert", "subtype": subtype, "ts_ms": str(get_ny_time_millis()), "text": text}
               maxlen=200000, approximate=True)
    except Exception:
        pass


def main() -> None:
    """Worker that processes Telegram bot callbacks for ML TB v10.4 promotion."""
    r = redis.Redis.from_url(os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0"), decode_responses=True)

    callbacks_stream = os.getenv("BOT_CALLBACKS_STREAM", "bot:callbacks")
    group = os.getenv("ML_PROMO_GROUP", "ml-promo-tb-v10-4")
    consumer = os.getenv("ML_PROMO_CONSUMER", "c1")

    challenger_key = os.getenv("ML_CFG_CHALLENGER_KEY", "cfg:ml_confirm:challenger")
    champion_key = os.getenv("ML_CFG_CHAMPION_KEY", "cfg:ml_confirm:champion")
    cfg_hash_key = os.getenv("ML_CFG_HASH_KEY", "cfg:ml_confirm")
    notify_stream = os.getenv("NOTIFY_TELEGRAM_STREAM", "notify:telegram")
    processed_set = os.getenv("ML_PROMO_PROCESSED_SET", "ml:promo:processed:v10_4")
    processed_ttl_sec = int(os.getenv("ML_PROMO_PROCESSED_TTL_SEC", "604800"))

    try:
        r.xgroup_create(callbacks_stream, group, id="$", mkstream=True)
    except Exception:
        pass

    # Startup diagnostic: champion exists but is empty/invalid JSON => alert (prevents silent ERR_NO_CFG loops)
    try:
        champ = _safe_loads(r.get(champion_key))
        if not _is_valid_cfg(champ):
            _notify(r, notify_stream, f"ML champion cfg invalid/empty at {champion_key}. "
                                    f"TYPE={r.type(champion_key)} STRLEN={r.strlen(champion_key)}"
                    subtype="ml_champion_invalid")
    except Exception:
        pass

    # Bootstrap: if champion is missing but hash cfg exists, promote it to champion JSON once.
    try:
        if not r.get(champion_key):
            h = r.hgetall(cfg_hash_key)
            if isinstance(h, dict) and len(h) > 0:
                cfg = _coerce_hash_cfg(h)
                r.set(champion_key, json.dumps(cfg, ensure_ascii=False, separators=(",", ":")))
                r.xadd(notify_stream, {
                    "type": "info"
                    "subtype": "ml_cfg_bootstrap"
                    "ts_ms": str(get_ny_time_millis())
                    "text": f"Bootstrapped {champion_key} from hash {cfg_hash_key} (mode={cfg.get('mode')}, enforce_share={cfg.get('enforce_share')})"
                }, maxlen=200000, approximate=True)
    except Exception:
        pass

    while True:
        try:
            resp = r.xreadgroup(group, consumer, {callbacks_stream: ">"}, count=200, block=1000)
        except Exception:
            resp = None

        if not resp:
            time.sleep(0.05)
            continue

        for _stream, msgs in resp:
            for msg_id, fields in msgs:
                if r.sismember(processed_set, msg_id):
                    try:
                        r.xack(callbacks_stream, group, msg_id)
                    except Exception:
                        pass
                    continue

                cb = str(fields.get("callback", "") or "")
                if cb.startswith("approve:ml_tb_v10_4:"):
                    run_id = cb.split(":", 2)[2]
                    chal = _safe_loads(r.get(challenger_key))
                    if _is_valid_cfg(chal) and str(chal.get("run_id", "")) == run_id:
                        chal.setdefault("promoted_ms", get_ny_time_millis())
                        chal.setdefault("mode", "SHADOW")
                        chal.setdefault("fail_policy", "OPEN")
                        chal.setdefault("enforce_share", 0.05)
                        r.set(champion_key, json.dumps(chal, ensure_ascii=False, separators=(",", ":")))
                        r.delete(challenger_key)
                    else:
                        _notify(r, notify_stream
                                f"Approve requested for run_id={run_id}, but challenger missing/invalid at {challenger_key}. "
                                f"TYPE={r.type(challenger_key)} STRLEN={r.strlen(challenger_key)}"
                                subtype="ml_challenger_missing")
                elif cb.startswith("reject:ml_tb_v10_4:"):
                    run_id = cb.split(":", 2)[2]
                    chal = _safe_loads(r.get(challenger_key))
                    if _is_valid_cfg(chal) and str(chal.get("run_id", "")) == run_id:
                        chal["rejected_ms"] = get_ny_time_millis()
                        r.set(challenger_key + ":rejected:" + run_id, json.dumps(chal, ensure_ascii=False, separators=(",", ":")), ex=7*24*3600)
                        r.delete(challenger_key)

                try:
                    r.sadd(processed_set, msg_id)
                    r.expire(processed_set, processed_ttl_sec)
                except Exception:
                    pass
                try:
                    r.xack(callbacks_stream, group, msg_id)
                except Exception:
                    pass


if __name__ == "__main__":
    main()

