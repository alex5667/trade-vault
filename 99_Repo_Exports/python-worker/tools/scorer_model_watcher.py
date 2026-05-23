"""
Watches scorer_model.report.json for validation.pass=True.
On detection: restarts of-confirm-service + of-confirm-service-2 via docker,
sends Redis notification, exits 0.

Designed to run as a Docker service (socket mounted) or directly on host.
"""
from __future__ import annotations

import html
import json
import os
import subprocess
import sys
import time

REPORT_PATH = os.getenv("SCORER_REPORT_PATH", "/app/ml_models/scorer_model.report.json")
POLL_SEC = int(os.getenv("SCORER_WATCHER_POLL_SEC", "60"))
SERVICES = os.getenv("SCORER_WATCHER_SERVICES", "of-confirm-service of-confirm-service-2").split()
NOTIFY_STREAM = os.getenv("NOTIFY_TELEGRAM_STREAM", "notify:telegram")
REDIS_URL = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")


def _read_report(path: str) -> dict:
    try:
        with open(path) as f:
            return json.load(f)
    except Exception:
        return {}


def _validation_pass(path: str) -> bool:
    d = _read_report(path)
    return bool(d.get("validation", {}).get("pass", False))


def _report_mtime(path: str) -> float:
    try:
        return os.path.getmtime(path)
    except OSError:
        return 0.0


def _notify_redis(msg: str) -> None:
    try:
        import redis  # type: ignore
        r = redis.Redis.from_url(REDIS_URL, decode_responses=True)
        safe = html.escape(msg)
        r.xadd(
            NOTIFY_STREAM,
            {"type": "report", "subtype": "scorer_model_watcher", "text": safe},
            maxlen=200000,
            approximate=True,
        )
    except Exception as e:
        print(f"[WARN] Redis notify failed: {e}", flush=True)


def _docker_restart(services: list[str]) -> bool:
    cmd = ["docker", "restart"] + services
    print(f"[scorer-watcher] Running: {' '.join(cmd)}", flush=True)
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
    print(result.stdout, flush=True)
    if result.returncode != 0:
        print(f"[ERROR] docker restart failed:\n{result.stderr}", flush=True)
    return result.returncode == 0


def main() -> None:
    print(f"[scorer-watcher] Watching {REPORT_PATH}, poll={POLL_SEC}s", flush=True)
    print(f"[scorer-watcher] Will restart: {SERVICES}", flush=True)

    # Track initial mtime — only react to NEW validated model, not existing stub
    seen_mtime = _report_mtime(REPORT_PATH)
    print(f"[scorer-watcher] Initial mtime={seen_mtime:.0f} pass={_validation_pass(REPORT_PATH)}", flush=True)

    while True:
        time.sleep(POLL_SEC)
        mtime = _report_mtime(REPORT_PATH)
        if mtime <= 0:
            continue
        if mtime == seen_mtime:
            continue

        # Report file changed
        d = _read_report(REPORT_PATH)
        val = d.get("validation", {})
        passed = bool(val.get("pass", False))
        n = d.get("n_samples", "?")
        version = d.get("version", "?")
        print(f"[scorer-watcher] Report updated mtime={mtime:.0f} pass={passed} n={n} version={version}", flush=True)
        seen_mtime = mtime

        if not passed:
            reasons = val.get("reasons", [])
            print(f"[scorer-watcher] Validation NOT passed: {reasons} — waiting for next model", flush=True)
            continue

        # Validation passed — restart services
        msg = (
            f"scorer_model validated (n={n}, v={version}) — "
            f"restarting {', '.join(SERVICES)} with ML_SCORING_ENABLE=1"
        )
        print(f"[scorer-watcher] {msg}", flush=True)
        _notify_redis(msg)

        ok = _docker_restart(SERVICES)
        if ok:
            _notify_redis(f"scorer_model watcher: restart OK — {', '.join(SERVICES)} up with ML fusion active")
            print("[scorer-watcher] Done. Exiting.", flush=True)
            sys.exit(0)
        else:
            _notify_redis(f"scorer_model watcher: restart FAILED — check docker logs")
            print("[scorer-watcher] Restart failed — will retry on next model update", flush=True)


if __name__ == "__main__":
    main()
