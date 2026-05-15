"""Prometheus exporter for v14_of nightly train metrics.

Polls Redis key `metrics:v14_of_train:last` (written by
`nightly_v14_of_train_bundle.py`) and exposes its content as Prometheus gauges
so Grafana / alerts can monitor retrain health and candidate model quality.

Exposed metrics (all `v14_of_train_*`):
  - v14_of_train_status                     1=ok, 0=fail, -1=missing
  - v14_of_train_elapsed_seconds
  - v14_of_train_finished_age_seconds       seconds since last finished_at_ms
  - v14_of_train_dataset_rows
  - v14_of_train_dataset_pos_rate
  - v14_of_train_dataset_label_flip_rate
  - v14_of_train_relabel_processed
  - v14_of_train_lr_metric{name}            roc_auc_mean | pr_auc_mean | brier_mean | log_loss_mean
  - v14_of_train_gbdt_metric{name}          roc_auc_oof | pr_auc_oof | brier_oof | log_loss_oof
  - v14_of_train_publish_candidate_count
  - v14_of_train_publish_promoted_count

Env:
  REDIS_URL                  redis://redis-worker-1:6379/0
  V14_TRAIN_METRICS_KEY      metrics:v14_of_train:last
  V14_EXPORTER_PORT          9836
  V14_EXPORTER_POLL_SEC      60
"""
from __future__ import annotations

import json
import logging
import os
import sys
import time
from typing import Any

from prometheus_client import Gauge, start_http_server
import redis

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("v14_of_train_metrics_exporter")


# ---------------------------------------------------------------------------
# Gauge definitions
# ---------------------------------------------------------------------------

g_status = Gauge("v14_of_train_status", "Last v14_of train cycle status (1=ok, 0=fail, -1=missing)")
g_elapsed = Gauge("v14_of_train_elapsed_seconds", "Last v14_of train cycle elapsed seconds")
g_finished_age = Gauge("v14_of_train_finished_age_seconds", "Seconds since last finished_at_ms")
g_dataset_rows = Gauge("v14_of_train_dataset_rows", "Dataset joined_rows from last cycle")
g_dataset_pos_rate = Gauge("v14_of_train_dataset_pos_rate", "Dataset y_edge positive rate")
g_dataset_flip_rate = Gauge("v14_of_train_dataset_label_flip_rate", "legacy ↔ cost-aware label flip rate")
g_relabel_processed = Gauge("v14_of_train_relabel_processed", "Cost-aware relabel processed rows")
g_lr_metric = Gauge("v14_of_train_lr_metric", "LR baseline CV metric", ["name"])
g_gbdt_metric = Gauge("v14_of_train_gbdt_metric", "edge_stack_v1 challenger OOF metric", ["name"])
g_publish_candidate_count = Gauge("v14_of_train_publish_candidate_count", "Count of candidate keys written")
g_publish_promoted_count = Gauge("v14_of_train_publish_promoted_count", "Count of cfgs auto-promoted")

# Status enum mapping
_STATUS_NUM = {"ok": 1, "fail_train": 0, "skipped_small_dataset": -2}


def _set_gauges(payload: dict[str, Any]) -> None:
    status_str = str(payload.get("status", ""))
    g_status.set(_STATUS_NUM.get(status_str, 0 if status_str else -1))
    g_elapsed.set(float(payload.get("elapsed_sec", 0.0) or 0.0))

    finished_ms = int(payload.get("finished_at_ms", 0) or 0)
    if finished_ms > 0:
        g_finished_age.set(max(0.0, (time.time() * 1000 - finished_ms) / 1000.0))

    ds = payload.get("dataset") or {}
    g_dataset_rows.set(int(ds.get("joined_rows", 0) or 0))
    g_dataset_pos_rate.set(float(ds.get("pos_rate", 0.0) or 0.0))
    g_dataset_flip_rate.set(float(ds.get("label_flip_rate", 0.0) or 0.0))

    relabel = payload.get("relabel") or {}
    g_relabel_processed.set(int(relabel.get("processed", 0) or 0))

    lr = (payload.get("lr") or {}).get("metrics") or {}
    for k in ("roc_auc_mean", "pr_auc_mean", "brier_mean", "log_loss_mean"):
        v = lr.get(k)
        if v is not None:
            try:
                g_lr_metric.labels(name=k).set(float(v))
            except Exception:
                pass

    gbdt = (payload.get("gbdt") or {}).get("metrics") or {}
    for k in ("roc_auc_oof", "pr_auc_oof", "brier_oof", "log_loss_oof"):
        v = gbdt.get(k)
        if v is not None:
            try:
                g_gbdt_metric.labels(name=k).set(float(v))
            except Exception:
                pass

    pub = payload.get("publish") or {}
    cand = pub.get("candidate_keys") or []
    promoted = pub.get("promoted") or []
    g_publish_candidate_count.set(len(cand) if isinstance(cand, list) else 0)
    g_publish_promoted_count.set(len(promoted) if isinstance(promoted, list) else 0)


def _poll_once(r: redis.Redis, key: str) -> bool:
    """Read latest metrics blob from Redis; update gauges. Return True if data fetched."""
    try:
        raw = r.get(key)
    except Exception as e:
        log.warning("redis get failed: %s", e)
        g_status.set(-1)
        return False

    if not raw:
        log.warning("key %s missing", key)
        g_status.set(-1)
        return False

    if isinstance(raw, bytes):
        raw = raw.decode("utf-8", errors="ignore")

    try:
        payload = json.loads(raw)
    except Exception as e:
        log.warning("json decode failed: %s", e)
        g_status.set(-1)
        return False

    _set_gauges(payload)
    return True


def main() -> None:
    redis_url = os.environ.get("REDIS_URL", "redis://redis-worker-1:6379/0")
    metrics_key = os.environ.get("V14_TRAIN_METRICS_KEY", "metrics:v14_of_train:last")
    port = int(os.environ.get("V14_EXPORTER_PORT", "9836"))
    poll_sec = int(os.environ.get("V14_EXPORTER_POLL_SEC", "60"))

    log.info("starting on :%d, polling %s every %ds", port, metrics_key, poll_sec)
    start_http_server(port)
    r = redis.Redis.from_url(redis_url, decode_responses=False)

    while True:
        try:
            ok = _poll_once(r, metrics_key)
            log.debug("poll: ok=%s", ok)
        except Exception as e:
            log.error("poll loop error: %s", e)
        time.sleep(poll_sec)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(0)
