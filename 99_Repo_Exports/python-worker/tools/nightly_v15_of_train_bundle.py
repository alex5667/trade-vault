"""Nightly retrain bundle for v15_of: gated wrapper over nightly_v14_of_train_bundle.

Behavior:
  1. Run check_v15_of_readiness.evaluate_readiness().
     - NOT READY → write metrics:v15_of_train:last with status=-2 (skipped),
       send a (throttled) Telegram skip notification, exit 0.
     - READY     → invoke v14 bundle main() with env override
       V14_FEATURE_SCHEMA_VER=v15_of so the underlying pipeline trains on
       515-key v15_of schema instead of 359-key v14_of.
  2. Defaults route work to a v15_of work dir + metrics key so v14_of and
     v15_of artifacts never collide.

This wrapper exists because v15_of upstream producers are still incomplete
(85 of 156 new keys perma-zero on golden fixture as of 2026-05-18 —
[[audit-v15-of-producer-readiness-2026-05-18]]). Without this gate, training
v15_of immediately yields a model that learns a constant-zero pattern.

Env vars (passed through to v14 bundle unless overridden here):
  V15_FORCE_TRAIN          0 | 1   bypass readiness gate (incident response)
  V15_WORK_DIR             /var/lib/trade/of_reports/v15_of_train_work
  V15_TRAIN_METRICS_KEY    metrics:v15_of_train:last
  NOTIFY_STREAM            notify:telegram
  REDIS_URL                redis://redis-worker-1:6379/0
  (any V14_* env)          inherited by the underlying bundle
"""
from __future__ import annotations

import json
import logging
import os
import sys
import time

logger = logging.getLogger("nightly_v15_of_train")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


def _env(key: str, default: str = "") -> str:
    return os.environ.get(key, default)


def _env_bool(key: str, default: bool = False) -> bool:
    v = _env(key, "1" if default else "0").strip().lower()
    return v in ("1", "true", "yes", "on")


def _write_skip_metrics(*, status: int, reason: str, oe: dict) -> None:
    """Persist a metrics:v15_of_train:last document for the exporter to scrape."""
    metrics_key = _env("V15_TRAIN_METRICS_KEY", "metrics:v15_of_train:last")
    redis_url = _env("REDIS_URL", "redis://redis-worker-1:6379/0")
    payload = {
        "status": status,
        "reason": reason,
        "finished_at_ms": int(time.time() * 1000),
        "feature_schema_ver": "v15_of",
        "readiness": oe,
    }
    try:
        import redis
        r = redis.Redis.from_url(redis_url, decode_responses=True)
        r.set(metrics_key, json.dumps(payload, separators=(",", ":")))
        logger.info("wrote skip metrics → %s", metrics_key)
    except Exception as e:
        logger.warning("failed to write skip metrics: %s", e)


def _notify_skip(*, reason: str, oe: dict) -> None:
    """Best-effort Telegram skip notification."""
    redis_url = _env("REDIS_URL", "redis://redis-worker-1:6379/0")
    stream = _env("NOTIFY_STREAM", "notify:telegram")
    try:
        import redis
        r = redis.Redis.from_url(redis_url, decode_responses=True)
        lines = [
            "🟡 *v15_of train skipped*",
            f"reason: `{reason}`",
        ]
        for g, cov in sorted((oe.get("group_coverage") or {}).items()):
            mark = "✓" if cov >= float(oe.get("min_coverage", 0.5)) else "✗"
            lines.append(f"  [{mark}] {g}: {cov:.1%}")
        if oe.get("span_days") is not None:
            lines.append(f"span: `{oe['span_days']:.1f}d` (need ≥ {oe.get('min_days', 7)}d)")
        text = "\n".join(lines)
        r.xadd(
            stream,
            {
                "type": "report",
                "subtype": "v15_of_train_skipped",
                "ts_ms": str(int(time.time() * 1000)),
                "text": text,
            },
            maxlen=5000,
            approximate=True,
        )
    except Exception as e:
        logger.debug("telegram skip notify failed: %s", e)


def _check_readiness() -> dict:
    """Return readiness result dict; on import failure, treat as blocking."""
    try:
        from tools.check_v15_of_readiness import evaluate_readiness
        return evaluate_readiness()
    except Exception as e:
        logger.error("v15_of readiness check crashed: %s — blocking train", e)
        return {
            "ready": False,
            "reasons": [f"check_crashed: {e}"],
            "group_coverage": {},
            "span_days": 0.0,
        }


def _run_v14_bundle_main() -> int:
    """Inject env overrides routing v14 bundle to train v15_of instead.

    Imports the v14 bundle main() and calls it. The bundle reads
    V14_FEATURE_SCHEMA_VER at module import (cached); to ensure the override
    takes effect, we set env BEFORE the import.
    """
    os.environ.setdefault("V14_FEATURE_SCHEMA_VER", "v15_of")
    # Route metrics + work-dir to v15_of namespace if user hasn't already.
    os.environ.setdefault(
        "V14_TRAIN_METRICS_KEY",
        _env("V15_TRAIN_METRICS_KEY", "metrics:v15_of_train:last"),
    )
    os.environ.setdefault(
        "V14_WORK_DIR",
        _env("V15_WORK_DIR", "/var/lib/trade/of_reports/v15_of_train_work"),
    )

    try:
        from tools.nightly_v14_of_train_bundle import main as _v14_main
    except Exception as e:
        logger.error("v14_of bundle import failed: %s", e)
        return 1

    try:
        return int(_v14_main() or 0)
    except SystemExit as e:
        return int(getattr(e, "code", 0) or 0)
    except Exception as e:
        logger.error("v14_of bundle main() raised: %s", e)
        return 1


def main() -> int:
    force = _env_bool("V15_FORCE_TRAIN", False)
    oe = _check_readiness() if not force else {"ready": True, "skipped_gate": True}

    if not oe.get("ready") and not force:
        reasons = "; ".join(oe.get("reasons", ["unknown"]))
        logger.warning("v15_of NOT ready: %s — skipping train", reasons)
        _write_skip_metrics(status=-2, reason=f"not_ready: {reasons}", oe=oe)
        _notify_skip(reason=reasons, oe=oe)
        return 0

    logger.info("v15_of ready (force=%s) — invoking v14 bundle with V14_FEATURE_SCHEMA_VER=v15_of", force)
    rc = _run_v14_bundle_main()
    logger.info("v15_of train finished rc=%d", rc)
    return rc


if __name__ == "__main__":
    sys.exit(main())
