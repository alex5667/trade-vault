from __future__ import annotations

from utils.time_utils import get_ny_time_millis

"""Prometheus rules bundle smoke runner (standalone timer) — tick_flow_full mirror.

Why this exists
- Kept in both trees (services/ and tick_flow_full/services/) to match mixed-prod import paths.
- Executes `orderflow_services.prom_rules_bundle_health_check_v1` (syntax + optional
  promtool validation) against the repo rules bundle.
- On failure: sets a fail-closed auto-apply block key (legacy guard key) for a
  bounded hold time, so config/model auto-apply is paused.
- On success: clears the block it previously set.

Exit codes
- 0 OK
- 2 validation failed (or cannot run)

ENV
- REDIS_URL (default: redis://redis-worker-1:6379/0)
- PROM_RULES_REPO_ROOT (optional; default auto-detect in health-check)
- PROM_RULES_BUNDLE_SMOKE_PROMTOOL=auto|on|off (default: auto)

Auto-apply block
- AUTO_APPLY_BLOCK_PREFIX (default: cfg:suggestions:entry_policy:auto_apply_block)
- PROM_RULES_BUNDLE_SMOKE_BLOCK_REASON (default: prom_rules_bundle_smoke)
- AUTO_APPLY_BLOCK_HOLD_S (default: 300)

Block keys written
- {prefix}:{reason} = 1
- {prefix}:{reason}:ts_ms
- {prefix}:{reason}:meta  (small JSON)
"""

import json
import os
from typing import Any

try:
    import redis  # type: ignore
except Exception:  # pragma: no cover
    redis = None

from orderflow_services.prom_rules_bundle_health_check_v1 import main as health_main


def _now_ms() -> int:
    return get_ny_time_millis()


def _connect_redis() -> redis.Redis | None:
    if redis is None:
        return None
    url = (os.getenv("REDIS_URL") or os.getenv("CRYPTO_NOTIFY_REDIS_URL") or "redis://redis-worker-1:6379/0").strip()
    try:
        return redis.Redis.from_url(url, decode_responses=True)
    except Exception:
        return None


def _block_keys(prefix: str, reason: str) -> tuple[str, str, str]:
    k = f"{prefix}:{reason}"
    return k, f"{k}:ts_ms", f"{k}:meta"


def _set_block(*, reason: str, meta: dict[str, Any], hold_s: int) -> None:
    r = _connect_redis()
    if r is None:
        return

    prefix = (os.getenv("AUTO_APPLY_BLOCK_PREFIX") or "cfg:suggestions:entry_policy:auto_apply_block").strip()
    k, k_ts, k_meta = _block_keys(prefix, reason)

    now = _now_ms()

    pipe = r.pipeline(transaction=False)
    pipe.set(k, "1", ex=int(hold_s))
    pipe.set(k_ts, str(now), ex=int(hold_s))

    try:
        meta_blob = json.dumps(meta, ensure_ascii=False, separators=(",", ":"))
    except Exception:
        meta_blob = ""
    if meta_blob:
        pipe.set(k_meta, meta_blob, ex=int(hold_s))

    try:
        pipe.execute()
    except Exception:
        return


def _clear_block_if_owned(*, reason: str) -> None:
    r = _connect_redis()
    if r is None:
        return

    prefix = (os.getenv("AUTO_APPLY_BLOCK_PREFIX") or "cfg:suggestions:entry_policy:auto_apply_block").strip()
    k, k_ts, k_meta = _block_keys(prefix, reason)

    try:
        meta_blob = r.get(k_meta) or ""
        if meta_blob:
            meta = json.loads(meta_blob)
            if isinstance(meta, dict) and meta.get("owner") != "prom_rules_bundle_smoke_runner_v1":
                # do not delete blocks set by other components
                return
    except Exception:
        # if meta cannot be parsed, avoid deleting someone else's key
        return

    try:
        r.delete(k, k_ts, k_meta)
    except Exception:
        return


def main() -> int:
    repo_root = (os.getenv("PROM_RULES_REPO_ROOT") or "").strip() or None
    promtool_mode = (os.getenv("PROM_RULES_BUNDLE_SMOKE_PROMTOOL") or "auto").strip().lower()
    if promtool_mode not in ("auto", "on", "off"):
        promtool_mode = "auto"

    block_reason = (os.getenv("PROM_RULES_BUNDLE_SMOKE_BLOCK_REASON") or "prom_rules_bundle_smoke").strip() or "prom_rules_bundle_smoke"
    hold_s = int(os.getenv("AUTO_APPLY_BLOCK_HOLD_S", "300"))

    argv = []
    if repo_root:
        argv += ["--root", repo_root]
    argv += ["--promtool", promtool_mode]

    rc = 2
    try:
        rc = int(health_main(argv))
    except SystemExit as e:
        try:
            rc = int(e.code) if e.code is not None else 2
        except Exception:
            rc = 2
    except Exception:
        rc = 2

    if rc == 0:
        _clear_block_if_owned(reason=block_reason)
        return 0

    _set_block(
        reason=block_reason,
        meta={
            "owner": "prom_rules_bundle_smoke_runner_v1",
            "ts_ms": _now_ms(),
            "kind": "prom_rules_bundle_smoke",
        },
        hold_s=hold_s,
    )
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
