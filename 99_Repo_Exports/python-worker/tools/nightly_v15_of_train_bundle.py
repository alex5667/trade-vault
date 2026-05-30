"""Nightly retrain bundle for v15_of: gated wrapper over nightly_v14_of_train_bundle.

Behavior:
  1. Run check_v15_of_readiness.evaluate_readiness().
     - NOT READY → write metrics:v15_of_train:last with status=-2 (skipped),
       send a (throttled) Telegram skip notification, exit 0.
     - READY     → invoke v14 bundle main() with env override
       V14_FEATURE_SCHEMA_VER=v15_of so the underlying pipeline trains on
       the full v15_of schema (count pinned by
       ``core.ml_feature_schema_v15_of._EXPECTED_KEYS``) instead of v14_of.
  2. Defaults route work to a v15_of work dir + metrics key so v14_of and
     v15_of artifacts never collide.
  3. Auto-promote: after successful training, compare v15_of AUC vs the
     global champion. Promote only if:
       - v15_of n_rows >= V15_AUTO_PROMOTE_MIN_ROWS (default 1000)
       - v15_of roc_auc_mean > champion roc_auc + V15_AUTO_PROMOTE_MIN_DELTA (default 0.005)
     Controlled by V15_AUTO_PROMOTE_TO_CHAMPION=1 (default).

This wrapper exists because v15_of upstream producers are still incomplete
(85 of 156 new keys perma-zero on golden fixture as of 2026-05-18 —
[[audit-v15-of-producer-readiness-2026-05-18]]). Without this gate, training
v15_of immediately yields a model that learns a constant-zero pattern.

Env vars (passed through to v14 bundle unless overridden here):
  V15_FORCE_TRAIN                  0 | 1   bypass readiness gate (incident response)
  V15_WORK_DIR                     /var/lib/trade/of_reports/v15_of_train_work
  V15_TRAIN_METRICS_KEY            metrics:v15_of_train:last
  V15_AUTO_PROMOTE_TO_CHAMPION     1 | 0   compare vs champion, promote if better
  V15_AUTO_PROMOTE_MIN_ROWS        5000    minimum n_rows in v15_of dataset (needs enough for reliable calibration)
  V15_AUTO_PROMOTE_MIN_DELTA       0.005   v15_of must beat champion AUC by at least this
  V15_GLOBAL_CHAMPION_KEY          cfg:ml_confirm:champion
  NOTIFY_STREAM                    notify:telegram
  REDIS_URL                        redis://redis-worker-1:6379/0
  (any V14_* env)                  inherited by the underlying bundle
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


def _notify_v15_promoted(
    *,
    r: object,
    v15_auc: float,
    v15_n_rows: int,
    cur_auc: float,
    cur_schema: str,
) -> None:
    """Best-effort Telegram notification when v15_of is promoted to champion."""
    stream = _env("NOTIFY_STREAM", "notify:telegram")
    try:
        text = (
            "✅ *v15_of auto-promoted to champion*\n"
            f"v15_of AUC=`{v15_auc:.4f}` (n={v15_n_rows})\n"
            f"prev champion: schema=`{cur_schema}` AUC=`{cur_auc:.4f}`"
        )
        import redis as _redis_mod
        assert isinstance(r, _redis_mod.Redis)
        r.xadd(
            stream,
            {
                "type": "report",
                "subtype": "v15_of_promoted",
                "ts_ms": str(int(time.time() * 1000)),
                "text": text,
            },
            maxlen=5000,
            approximate=True,
        )
    except Exception as e:
        logger.debug("telegram promote notify failed: %s", e)


def _maybe_promote_v15_to_champion() -> bool:
    """Compare v15_of vs current champion; promote if v15_of is better and has enough data."""
    if not _env_bool("V15_AUTO_PROMOTE_TO_CHAMPION", True):
        logger.info("V15_AUTO_PROMOTE_TO_CHAMPION=0 — skipping champion comparison")
        return False

    redis_url = _env("REDIS_URL", "redis://redis-worker-1:6379/0")
    try:
        import redis
        r = redis.Redis.from_url(redis_url, decode_responses=True)
    except Exception as e:
        logger.error("promote_v15: redis connect failed: %s", e)
        return False

    # v15_of training results written by v14 bundle into V15_TRAIN_METRICS_KEY
    metrics_key = _env("V15_TRAIN_METRICS_KEY", "metrics:v15_of_train:last")
    try:
        raw = r.get(metrics_key)
        if not raw:
            logger.warning("promote_v15: no train metrics at %s", metrics_key)
            return False
        v15_metrics = json.loads(str(raw))
    except Exception as e:
        logger.error("promote_v15: failed to read train metrics: %s", e)
        return False

    # Extract v15_of LR model metrics
    v15_lr = v15_metrics.get("lr") or {}
    v15_m = v15_lr.get("metrics") or {}
    v15_auc = float(v15_m.get("roc_auc_mean") or 0.0)
    v15_n_rows = int(v15_m.get("n_rows") or 0)

    min_rows = int(_env("V15_AUTO_PROMOTE_MIN_ROWS", "5000"))
    min_delta = float(_env("V15_AUTO_PROMOTE_MIN_DELTA", "0.005"))

    if v15_n_rows < min_rows:
        logger.info(
            "promote_v15: SKIP — n_rows=%d < %d (not enough data yet)", v15_n_rows, min_rows
        )
        return False

    if v15_auc <= 0.0:
        logger.warning("promote_v15: SKIP — v15_of AUC=0 (training failed?)")
        return False

    # Threshold-reachability gate: if <2% of OOF predictions reach p_min=0.5,
    # the model is too biased (missing class_weight=balanced or too few positives).
    pass_rate = float(v15_m.get("pass_rate_at_p_min", 1.0))
    min_pass_rate = float(_env("V15_AUTO_PROMOTE_MIN_PASS_RATE", "0.02"))
    if pass_rate < min_pass_rate:
        logger.warning(
            "promote_v15: SKIP — pass_rate_at_p_min=%.3f < %.3f (model too biased to produce actionable signals)",
            pass_rate, min_pass_rate,
        )
        return False

    # Read the freshly written v15_of lr_candidate for the full model config
    v15_cand_key = "cfg:ml_confirm:v15_of:lr_candidate"
    try:
        raw = r.get(v15_cand_key)
        if not raw:
            logger.warning("promote_v15: no v15_of lr_candidate at %s", v15_cand_key)
            return False
        v15_cand = json.loads(str(raw))
    except Exception as e:
        logger.error("promote_v15: failed to read v15 candidate: %s", e)
        return False

    # Read current global champion
    global_key = _env("V15_GLOBAL_CHAMPION_KEY", "cfg:ml_confirm:champion")
    cur_auc = 0.0
    cur_schema = "none"
    try:
        raw = r.get(global_key)
        if raw:
            cur = json.loads(str(raw))
            cur_schema = cur.get("feature_schema_ver", "unknown")
            cur_m = cur.get("metrics") or {}
            cur_auc = float(
                cur_m.get("roc_auc_mean") or cur_m.get("roc_auc_oof") or 0.0
            )
    except Exception as e:
        logger.warning("promote_v15: failed to read current champion: %s — treating as empty", e)

    if v15_auc < cur_auc + min_delta:
        logger.info(
            "promote_v15: SKIP — v15_of AUC=%.4f not better than champion (schema=%s AUC=%.4f + delta=%.3f)",
            v15_auc, cur_schema, cur_auc, min_delta,
        )
        return False

    logger.info(
        "promote_v15: PROMOTING v15_of AUC=%.4f n_rows=%d over %s AUC=%.4f",
        v15_auc, v15_n_rows, cur_schema, cur_auc,
    )

    try:
        prev = r.get(global_key)
        if prev:
            r.set(global_key + "_prev_v15_promote", str(prev))
    except Exception:
        pass

    champion_cfg = dict(v15_cand)
    champion_cfg["promoted_from"] = "v15_of_auto"
    champion_cfg["promoted_at_ms"] = int(time.time() * 1000)
    champion_cfg["prev_champion_schema"] = cur_schema
    champion_cfg["prev_champion_auc"] = cur_auc
    # class_weight="balanced" now corrects the LR bias term; calibration is safe.
    champion_cfg["calibrate_p_edge"] = True

    try:
        r.set(global_key, json.dumps(champion_cfg, separators=(",", ":")))
    except Exception as e:
        logger.error("promote_v15: failed to write champion: %s", e)
        return False

    _notify_v15_promoted(r=r, v15_auc=v15_auc, v15_n_rows=v15_n_rows, cur_auc=cur_auc, cur_schema=cur_schema)
    logger.info("promote_v15: ✅ v15_of promoted to global champion (%s)", global_key)
    return True


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
        return _v14_main() or 0
    except SystemExit as e:
        return int(getattr(e, "code", 0) or 0)
    except Exception as e:
        logger.error("v14_of bundle main() raised: %s", e)
        return 1


def main() -> int:
    force = _env_bool("V15_FORCE_TRAIN", False)
    oe = _check_readiness() if not force else {"ready": True, "skipped_gate": True}

    if not oe.get("ready") and not force:
        _raw_reasons = oe.get("reasons")
        reasons = "; ".join(str(r) for r in _raw_reasons) if isinstance(_raw_reasons, list) else "unknown"
        logger.warning("v15_of NOT ready: %s — skipping train", reasons)
        _write_skip_metrics(status=-2, reason=f"not_ready: {reasons}", oe=oe)
        _notify_skip(reason=reasons, oe=oe)
        return 0

    logger.info("v15_of ready (force=%s) — invoking v14 bundle with V14_FEATURE_SCHEMA_VER=v15_of", force)
    rc = _run_v14_bundle_main()
    logger.info("v15_of train finished rc=%d", rc)

    if rc == 0:
        _maybe_promote_v15_to_champion()

    return rc


if __name__ == "__main__":
    sys.exit(main())
