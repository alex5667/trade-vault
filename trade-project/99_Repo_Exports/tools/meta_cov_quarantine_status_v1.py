#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""meta_cov_quarantine_status_v1.py

P34 helper: prints current per-bucket quarantine / recovery state from cfg2.

Reads:
  settings:dynamic_cfg (DYN_CFG_KEY)

Keys (cfg2)
  meta_cov_quarantine_{a,b,c,d}
  meta_cov_quarantine_until_ms_{a,b,c,d}
  meta_cov_quarantine_prev_share_{a,b,c,d}
  meta_cov_quarantine_reason_{a,b,c,d}
  meta_cov_recovery_target_share_{a,b,c,d}
  meta_enforce_share_cov_{a,b,c,d}

ENV:
  REDIS_URL (default redis://localhost:6379/0)
  DYN_CFG_KEY (default settings:dynamic_cfg)
"""

from __future__ import annotations

import json
import os
import time
from typing import Any, Dict

try:
    import redis  # type: ignore
except Exception:
    redis = None  # type: ignore


def now_ms() -> int:
    return int(time.time() * 1000)


def _i(x: Any, d: int = 0) -> int:
    try:
        return int(float(x))
    except Exception:
        return d


def _f(x: Any, d: float = 0.0) -> float:
    try:
        return float(x)
    except Exception:
        return d


def _loads(v: Any) -> Any:
    if v is None:
        return None
    if isinstance(v, (bytes, bytearray)):
        try:
            v = v.decode("utf-8", "replace")
        except Exception:
            return None
    if isinstance(v, str):
        s = v.strip()
        if not s:
            return None
        if (s.startswith("{") and s.endswith("}")) or (s.startswith("[") and s.endswith("]")):
            try:
                return json.loads(s)
            except Exception:
                return v
        return v
    return v


def _redis() -> Any:
    if redis is None:
        raise RuntimeError("redis library is not available")
    url = os.environ.get("REDIS_URL", "redis://localhost:6379/0")
    return redis.Redis.from_url(url, decode_responses=True)


def main() -> int:
    r = _redis()
    dyn_key = os.environ.get("DYN_CFG_KEY", "settings:dynamic_cfg")
    cfg2_raw = r.hgetall(dyn_key) or {}
    cfg2: Dict[str, Any] = {str(k): _loads(v) for k, v in cfg2_raw.items()}

    now_ts = now_ms()
    out: Dict[str, Any] = {"ts_ms": now_ts, "buckets": {}}

    for b in ("a", "b", "c", "d"):
        q = _i(cfg2.get(f"meta_cov_quarantine_{b}"), 0)
        until = _i(cfg2.get(f"meta_cov_quarantine_until_ms_{b}"), 0)
        prev_share = _f(cfg2.get(f"meta_cov_quarantine_prev_share_{b}"), 0.0)
        reason = str(cfg2.get(f"meta_cov_quarantine_reason_{b}") or "")
        target = _f(cfg2.get(f"meta_cov_recovery_target_share_{b}"), 0.0)
        share = _f(cfg2.get(f"meta_enforce_share_cov_{b}"), float("nan"))
        if share != share:
            share = _f(cfg2.get("meta_enforce_share"), 1.0)
        ttl_sec = max(0.0, (until - now_ts) / 1000.0) if (q == 1 and until > 0) else 0.0

        out["buckets"][b] = {
            "quarantine": int(q),
            "until_ms": int(until),
            "ttl_sec": float(ttl_sec),
            "prev_share": float(prev_share),
            "share": float(share),
            "recovery_target_share": float(target),
            "reason": reason,
        }

    print(json.dumps(out, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
