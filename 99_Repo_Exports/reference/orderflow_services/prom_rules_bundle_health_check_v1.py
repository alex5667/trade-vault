from __future__ import annotations

"""Prometheus rules bundle health check (nightly/orchestration).

Purpose
- Run the Python rules validator (and optional promtool) on the repo rules bundle.
- Persist last-run / last-ok state into Redis so exporters + alerts can track staleness.

State keys (Redis)
- state:prom_rules_bundle:last_run_ts_ms
- state:prom_rules_bundle:last_ok_ts_ms
- state:prom_rules_bundle:last_ok
- state:prom_rules_bundle:last_files_checked
- state:prom_rules_bundle:last_error_n
- state:prom_rules_bundle:last_error_head

Exit codes
- 0 OK
- 2 validation failed

ENV
- REDIS_URL (default redis://redis-worker-1:6379/0)
- PROM_RULES_BUNDLE_STATE_PREFIX (default state:prom_rules_bundle)
- REPO_ROOT (default /app if exists)
- PROM_RULES_BUNDLE_STATE_TTL_S (default 14 days)
"""

import argparse
import json
import os
import shutil
import time
from pathlib import Path

try:
    import redis  # type: ignore
except Exception:  # pragma: no cover
    redis = None

from orderflow_services.validate_prometheus_rules_bundle_v1 import validate_repo_rules


def _now_ms() -> int:
    return int(time.time() * 1000)


def _get_repo_root(arg_root: str | None) -> Path:
    if arg_root:
        return Path(arg_root).resolve()
    env_root = (os.getenv("REPO_ROOT") or "").strip()
    if env_root:
        return Path(env_root).resolve()
    if Path("/app").exists():
        return Path("/app").resolve()
    # file: <repo>/orderflow_services/prom_rules_bundle_health_check_v1.py
    return Path(__file__).resolve().parents[1]


def _connect_redis():
    if redis is None:
        return None
    url = os.getenv("REDIS_URL") or os.getenv("CRYPTO_NOTIFY_REDIS_URL") or "redis://redis-worker-1:6379/0"
    try:
        return redis.Redis.from_url(url, decode_responses=True)
    except Exception:
        return None


def _write_state(*, ok: bool, files_checked: int, errors: list[str]) -> None:
    r = _connect_redis()
    if r is None:
        return

    prefix = (os.getenv("PROM_RULES_BUNDLE_STATE_PREFIX") or "state:prom_rules_bundle").strip() or "state:prom_rules_bundle"
    now = _now_ms()

    err_n = int(len(errors))
    err_head = (errors[0] if errors else "")
    if len(err_head) > 240:
        err_head = err_head[:240] + "…"

    # Keep last_errors_json small (best-effort).
    err_blob = ""
    if errors:
        try:
            err_blob = json.dumps(errors[:8], ensure_ascii=False)
            if len(err_blob) > 1800:
                err_blob = err_blob[:1800] + "…"
        except Exception:
            err_blob = ""

    pipe = r.pipeline(transaction=False)
    pipe.set(f"{prefix}:last_run_ts_ms", str(now))
    pipe.set(f"{prefix}:last_ok", "1" if ok else "0")
    pipe.set(f"{prefix}:last_files_checked", str(int(files_checked)))
    pipe.set(f"{prefix}:last_error_n", str(int(err_n)))
    pipe.set(f"{prefix}:last_error_head", err_head)
    if err_blob:
        pipe.set(f"{prefix}:last_errors_json", err_blob)
        pipe.expire(f"{prefix}:last_errors_json", 7 * 24 * 3600)

    if ok:
        pipe.set(f"{prefix}:last_ok_ts_ms", str(now))

    # Keep state keys reasonably fresh, but do not delete them.
    ttl_s = int(os.getenv("PROM_RULES_BUNDLE_STATE_TTL_S", str(14 * 24 * 3600)))
    for k in (
        f"{prefix}:last_run_ts_ms",
        f"{prefix}:last_ok_ts_ms",
        f"{prefix}:last_ok",
        f"{prefix}:last_files_checked",
        f"{prefix}:last_error_n",
        f"{prefix}:last_error_head",
    ):
        pipe.expire(k, ttl_s)

    try:
        pipe.execute()
    except Exception as e:
        import traceback
        traceback.print_exc()
        # fail-open: do not break timers on redis issues
        return


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--root", default=None, help="Repo root (default: auto)")
    p.add_argument("--promtool", choices=("auto", "on", "off"), default="auto")
    args = p.parse_args(argv)

    repo_root = _get_repo_root(args.root)

    if args.promtool == "on":
        use_promtool = True
    elif args.promtool == "off":
        use_promtool = False
    else:
        use_promtool = bool(shutil.which("promtool"))

    res = validate_repo_rules(repo_root=repo_root, use_promtool=use_promtool)
    _write_state(ok=res.ok, files_checked=res.files_checked, errors=res.errors)

    if res.ok:
        print(f"OK: validated {res.files_checked} rules files")
        return 0

    print(f"FAIL: {len(res.errors)} error(s) while validating {res.files_checked} rules files")
    for e in res.errors[:10]:
        print(f"- {e}")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
