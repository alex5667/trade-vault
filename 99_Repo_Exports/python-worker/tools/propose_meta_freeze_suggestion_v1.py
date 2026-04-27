from __future__ import annotations
from utils.time_utils import get_ny_time_millis

import html
import json
import os
import secrets
import time
from typing import Any, Dict, List, Optional

import redis


def now_ms() -> int:
    return get_ny_time_millis()


def _notify(r: redis.Redis, text: str, sid: Optional[str] = None) -> None:
    stream = os.getenv("NOTIFY_TELEGRAM_STREAM", "notify:telegram")
    payload = {"type": "report", "text": text, "ts": str(now_ms())}
    if sid:
        payload["sid"] = sid
    r.xadd(stream, payload, maxlen=200000, approximate=True)


def emit_meta_freeze_suggestion(
    r: redis.Redis,
    *,
    prefix: str,
    scope: str,
    symbols: List[str],
    cfg_prefix: str,
    freeze: int,
    freeze_mode: str,
    report: Dict[str, Any],
    ttl_sec: int,
) -> str:
    """Emit a proposal into cfg:suggestions approval/apply contour.

    Keys written (all with TTL):
      {prefix}:meta:{sid}        -> JSON (ops+meta)
      {prefix}:approvals:{sid}   -> HASH (placeholder)
      {prefix}:latest:meta_freeze:{scope} -> sid
    """
    freeze = 1 if int(freeze) != 0 else 0
    freeze_mode = str(freeze_mode or "OPEN").upper()
    ttl_sec = int(ttl_sec or 86400)

    sid = f"meta_freeze:{now_ms()}:{secrets.token_hex(4)}"
    meta_key = f"{prefix}:meta:{sid}"
    appr_key = f"{prefix}:approvals:{sid}"
    latest_key = f"{prefix}:latest:meta_freeze:{scope}"

    ops: List[Dict[str, str]] = []
    for sym in symbols:
        hk = f"{cfg_prefix}{sym}"
        ops.append({"op": "HSET", "key": hk, "field": "meta_model_freeze", "value": str(freeze)})
        ops.append({"op": "HSET", "key": hk, "field": "meta_freeze_mode", "value": freeze_mode})

    payload = {
        "sid": sid,
        "created_ms": now_ms(),
        "ttl_sec": ttl_sec,
        "who": "meta_drift_guard_v1",
        "kind": "meta_model_freeze" if freeze == 1 else "meta_model_unfreeze",
        "scope": scope,
        "symbols": symbols,
        "ops": ops,
        "report": report,
    }

    r.set(meta_key, json.dumps(payload, ensure_ascii=False, separators=(",", ":")), ex=ttl_sec)
    # placeholder approvals container (approver writes fields here)
    r.hset(appr_key, mapping={"created_ms": str(payload["created_ms"]), "kind": payload["kind"]})
    r.expire(appr_key, ttl_sec)

    r.set(latest_key, sid, ex=ttl_sec)

    alerts_val = report.get("alerts")
    if isinstance(alerts_val, list):
        alerts_str = json.dumps(alerts_val, ensure_ascii=False)
    else:
        alerts_str = "[]"

    _notify(
        r,
        "<b>META_DRIFT proposal</b>\n"
        f"kind=<code>{html.escape(str(payload['kind']), quote=True)}</code> mode=<code>{html.escape(freeze_mode, quote=True)}</code>\n"
        f"sid=<code>{html.escape(sid, quote=True)}</code> scope=<code>{html.escape(scope, quote=True)}</code>\n"
        f"alerts=<code>{html.escape(alerts_str, quote=True)}</code> p50=<code>{float(report.get('p50') or 0.0):.3f}</code>",
        sid=sid,
    )
    return sid


def main() -> None:
    import argparse

    ap = argparse.ArgumentParser(description="Emit meta freeze/unfreeze proposal into cfg:suggestions contour")
    ap.add_argument("--redis-url", default=os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0"))
    ap.add_argument("--prefix", default=os.getenv("META_DRIFT_SUGGESTIONS_PREFIX", "cfg:suggestions:entry_policy"))
    ap.add_argument("--scope", default=os.getenv("META_DRIFT_SUGGESTIONS_SCOPE", "ALL"))
    ap.add_argument("--symbols", default=os.getenv("CANARY_SYMBOLS", "BTCUSDT,ETHUSDT"))
    ap.add_argument("--cfg-prefix", default=os.getenv("CFG_HASH_PREFIX", "config:orderflow:"))
    ap.add_argument("--freeze", type=int, default=1)
    ap.add_argument("--freeze-mode", default=os.getenv("META_FREEZE_MODE", "OPEN"))
    ap.add_argument("--ttl-sec", type=int, default=int(os.getenv("META_DRIFT_SUGGESTIONS_TTL_SEC", "86400") or 86400))
    ap.add_argument("--report-json", default="")

    args = ap.parse_args()
    r = redis.Redis.from_url(args.redis_url, decode_responses=True)
    syms = [s.strip().upper() for s in str(args.symbols or "").split(",") if s.strip()]
    scope = str(args.scope or "ALL").strip().upper()

    report: Dict[str, Any] = {}
    if args.report_json:
        try:
            report = json.loads(args.report_json)
        except Exception:
            report = {}

    emit_meta_freeze_suggestion(
        r,
        prefix=str(args.prefix),
        scope=scope,
        symbols=syms,
        cfg_prefix=str(args.cfg_prefix),
        freeze=int(args.freeze),
        freeze_mode=str(args.freeze_mode),
        report=report,
        ttl_sec=int(args.ttl_sec),
    )


if __name__ == "__main__":
    main()
