"""Auto-apply block guard.

Intended usage inside any ApplyRunner / auto-apply entrypoint:

    from services.orderflow.auto_apply_guard import assert_auto_apply_not_blocked
    assert_auto_apply_not_blocked()

Block keys (defaults):
  prefix = cfg:suggestions:entry_policy:auto_apply_block
  block  = {prefix}:{reason}
  meta   = {prefix}:{reason}:meta   (JSON)
  ts_ms  = {prefix}:{reason}:ts_ms

Default reasons (AUTO_APPLY_BLOCK_REASONS):
  tick_gate,enforce_bucket_promoter,meta_cov,prom_rules_bundle_smoke,prom_rules_loaded_probe,of_inputs_v3,of_inputs_exporters_smoke,of_gate_exporters_smoke
  tick_gate,enforce_bucket_promoter,meta_cov,prom_rules_bundle_smoke,prom_rules_loaded_probe,of_inputs_v3,of_inputs_exporters_smoke
"""

from __future__ import annotations
from utils.time_utils import get_ny_time_millis

import json
import os
import sys
import time
from typing import Any, Dict, Optional, Tuple


DEFAULT_PREFIX = "cfg:suggestions:entry_policy:auto_apply_block"
OF_INPUTS_V3_GLOBAL_PREFIX = "cfg:of_inputs_v3:auto_apply_block_global"
OF_INPUTS_V3_SYMBOL_PREFIX = "cfg:of_inputs_v3:auto_apply_block"
DEFAULT_REASONS = "tick_gate,enforce_bucket_promoter,meta_cov,prom_rules_bundle_smoke,prom_rules_loaded_probe,of_inputs_v3,of_inputs_exporters_smoke,of_gate_exporters_smoke"


def _now_ms() -> int:
    return get_ny_time_millis()


def _get_env(name: str, default: str) -> str:
    v = os.getenv(name)
    return v if (v is not None and str(v).strip() != "") else default


def _connect_redis(redis_url: str):
    try:
        import redis  # type: ignore
    except Exception as e:
        raise RuntimeError("redis-py is required to read auto-apply block keys") from e
    return redis.Redis.from_url(redis_url, decode_responses=True)


def _load_json(s: Optional[str]) -> Dict[str, Any]:
    if not s:
        return {}
    try:
        obj = json.loads(s)
        return obj if isinstance(obj, dict) else {}
    except Exception:
        return {}


def get_block_state(
    redis_url: Optional[str] = None,
    prefix: Optional[str] = None,
    max_meta_age_ms: int = 15 * 60 * 1000,
) -> Tuple[bool, Dict[str, Any]]:
    """Return (blocked, meta).

    Supports multiple reasons via AUTO_APPLY_BLOCK_REASONS env var.
    Default: tick_gate,enforce_bucket_promoter,meta_cov,prom_rules_bundle_smoke,prom_rules_loaded_probe,of_inputs_v3,of_inputs_exporters_smoke,of_gate_exporters_smoke
    Default: tick_gate,enforce_bucket_promoter,meta_cov,prom_rules_bundle_smoke,prom_rules_loaded_probe,of_inputs_v3,of_inputs_exporters_smoke
    """
    rurl = redis_url or os.getenv("REDIS_URL") or os.getenv("CRYPTO_NOTIFY_REDIS_URL") or ""
    if not rurl:
        return (False, {"status": "no_redis_url"})
    
    reasons_str = _get_env("AUTO_APPLY_BLOCK_REASONS", DEFAULT_REASONS)
    pfx = prefix or _get_env("AUTO_APPLY_BLOCK_PREFIX", DEFAULT_PREFIX)
    reasons_str = _get_env("AUTO_APPLY_BLOCK_REASONS", DEFAULT_REASONS)
    reasons = [r.strip() for r in reasons_str.split(",") if r.strip()]

    cli = _connect_redis(rurl)
    of_global = _get_env("OF_INPUTS_V3_AUTO_APPLY_BLOCK_GLOBAL_PREFIX", OF_INPUTS_V3_GLOBAL_PREFIX)
    of_sym_pfx = _get_env("OF_INPUTS_V3_AUTO_APPLY_BLOCK_SYMBOL_PREFIX", OF_INPUTS_V3_SYMBOL_PREFIX)
    symbol = os.getenv("AUTO_APPLY_BLOCK_SYMBOL") or os.getenv("SYMBOL")

    # One RTT via MGET: legacy {block,meta,ts} + of_inputs_v3 global + (optional) per-symbol.
    keys = []
    idx = {}  # rsn -> dict of indices
    for rsn in reasons:
        legacy_block = f"{pfx}:{rsn}"
        legacy_meta = f"{pfx}:{rsn}:meta"
        legacy_ts = f"{pfx}:{rsn}:ts_ms"
        v3_global = f"{of_global}:{rsn}"
        keys.extend([legacy_block, legacy_meta, legacy_ts, v3_global])
        idx[rsn] = {
            "legacy_block": len(keys) - 4,
            "legacy_meta": len(keys) - 3,
            "legacy_ts": len(keys) - 2,
            "v3_global": len(keys) - 1,
        }
        if symbol:
            v3_sym = f"{of_sym_pfx}:{symbol}:{rsn}"
            keys.append(v3_sym)
            idx[rsn]["v3_sym"] = len(keys) - 1

    results = cli.mget(keys) if keys else []
    now_ms = _now_ms()
    
    combined_meta: Dict[str, Any] = {"reasons_checked": reasons}
    
    for rsn in reasons:
        j = idx.get(rsn) or {}

        # v3 global hard block (preferred)
        v3g = results[j.get("v3_global", -1)] if j.get("v3_global", -1) >= 0 else None
        if v3g is not None:
            meta = _load_json(v3g)
            meta.setdefault("status", "blocked")
            meta.setdefault("reason", rsn)
            meta["block_key"] = f"{of_global}:{rsn}"
            meta["scope"] = "global"
            combined_meta.update(meta)
            return (True, combined_meta)

        # v3 per-symbol hard block
        v3s_idx = j.get("v3_sym", -1)
        v3s = results[v3s_idx] if v3s_idx >= 0 else None
        if v3s is not None:
            meta = _load_json(v3s)
            meta.setdefault("status", "blocked")
            meta.setdefault("reason", rsn)
            meta["block_key"] = f"{of_sym_pfx}:{symbol}:{rsn}"
            meta["scope"] = "symbol"
            combined_meta.update(meta)
            return (True, combined_meta)

        # legacy keys
        block_val = results[j.get("legacy_block", -1)] if j.get("legacy_block", -1) >= 0 else None
        meta_raw = results[j.get("legacy_meta", -1)] if j.get("legacy_meta", -1) >= 0 else None
        ts_raw = results[j.get("legacy_ts", -1)] if j.get("legacy_ts", -1) >= 0 else None

        
        meta = _load_json(meta_raw)
        
        # 1. Hard block via key existance
        if block_val is not None:
             meta.setdefault("status", "blocked")
             meta.setdefault("reason", rsn)
             meta["block_key"] = f"{pfx}:{rsn}"
             # Merge into combined
             combined_meta.update(meta)
             return (True, combined_meta)
        
        # 2. Soft block via fresh meta
        try:
            ts_ms = int(ts_raw) if ts_raw is not None else int(meta.get("ts_ms") or 0)
        except Exception:
            ts_ms = int(meta.get("ts_ms") or 0)
            
        if ts_ms > 0 and (now_ms - ts_ms) <= int(max_meta_age_ms):
            if str(meta.get("blocked") or "0") in ("1", "true", "True"):
                meta["reason"] = rsn
                combined_meta.update(meta)
                return (True, combined_meta)

    return (False, combined_meta)


def assert_auto_apply_not_blocked(
    redis_url: Optional[str] = None,
    prefix: Optional[str] = None,
    max_meta_age_ms: int = 15 * 60 * 1000,
    exit_code: int = 20,
) -> None:
    """Exit the process if auto-apply is blocked by any guard."""
    blocked, meta = get_block_state(redis_url=redis_url, prefix=prefix, max_meta_age_ms=max_meta_age_ms)
    if not blocked:
        return

    payload = {
        "blocked": True,
        "exit_code": exit_code,
        "meta": meta,
        "ts_ms": _now_ms()
    }
    sys.stderr.write(json.dumps(payload, ensure_ascii=False) + "\n")
    raise SystemExit(exit_code)
