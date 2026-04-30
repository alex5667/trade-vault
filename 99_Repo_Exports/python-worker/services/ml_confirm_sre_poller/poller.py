"""
SRE poller для метрик ML Confirm и labels:tb (async версия).
Проверяет cfg:ml_confirm:champion и labels:tb XLEN из Redis.
"""

import asyncio
import os
import time
import logging
from redis.asyncio import Redis

from redis.exceptions import BusyLoadingError, ConnectionError, TimeoutError
from services.ml_confirm.champion_cfg import validate_champion_cfg, ChampionCfgError
from services.observability.metrics_registry import (
    ml_confirm_cfg_present
    ml_confirm_cfg_valid
    ml_confirm_enforce_share
    ml_confirm_errors_total
    ml_missing_critical_total
)
from prometheus_client import Gauge

# tb_labels_xlen is defined locally (NOT imported from metrics_registry)
# to ensure it is ONLY exported by the ml-confirm-sre-poller job.
# Exporting it from python-worker/of-confirm-service caused false TBLabelsEmpty alerts.
tb_labels_xlen = Gauge("tb_labels_xlen", "XLEN of labels:tb stream")

log = logging.getLogger("ml_confirm_sre_poller")


CFG_KEY = os.getenv("ML_CONFIRM_CHAMPION_KEY", "cfg:ml_confirm:champion")
LABELS_STREAM = os.getenv("TB_LABELS_STREAM", "labels:tb")
POLL_INTERVAL_SEC = float(os.getenv("ML_CONFIRM_SRE_POLL_INTERVAL_SEC", "5"))
DEFAULT_KIND = os.getenv("ML_CONFIRM_KIND_DEFAULT", "util_mh_v1")


async def _read_xlen(r: Redis, stream: str) -> int:
    try:
        return int(await r.xlen(stream))
    except Exception:
        return -1


async def poll_loop(redis_url: str) -> None:
    r = Redis.from_url(redis_url, decode_responses=True)
    last_ok = 0.0
    while True:
        t0 = time.time()
        kind = DEFAULT_KIND
        try:
            try:
                raw = await r.get(CFG_KEY)
            except (BusyLoadingError, ConnectionError, TimeoutError) as e:
                log.warning(f"Redis is not ready ({type(e).__name__}), sleeping...")
                await asyncio.sleep(1.0)
                continue

            if not raw:
                ml_confirm_cfg_present.labels(kind=kind).set(0)
                ml_confirm_cfg_valid.labels(kind=kind).set(0)
                ml_confirm_errors_total.labels(kind=kind, reason="no_cfg").inc()
            else:
                ml_confirm_cfg_present.labels(kind=kind).set(1)
                try:
                    cfg, info = validate_champion_cfg(raw, allow_default_enforce_share=False)
                    kind = cfg.kind or kind
                    ml_confirm_cfg_valid.labels(kind=kind).set(1)
                    ml_confirm_enforce_share.labels(kind=kind).set(cfg.enforce_share)
                except ChampionCfgError as e:
                    # invalid cfg (bad json / missing enforce_share / invariants)
                    ml_confirm_cfg_valid.labels(kind=kind).set(0)
                    msg = str(e)
                    if msg.startswith("bad_json"):
                        ml_confirm_errors_total.labels(kind=kind, reason="bad_json").inc()
                    elif "enforce_share: missing" in msg:
                        ml_missing_critical_total.labels(field="champion.enforce_share").inc()
                        ml_confirm_errors_total.labels(kind=kind, reason="invalid_cfg").inc()
                    else:
                        ml_confirm_errors_total.labels(kind=kind, reason="invalid_cfg").inc()

            xlen = await _read_xlen(r, LABELS_STREAM)
            if xlen >= 0:
                tb_labels_xlen.set(xlen)

            last_ok = time.time()
        except Exception as e:
            ml_confirm_errors_total.labels(kind=kind, reason="exception").inc()
            log.exception("poller error: %s", e)
        finally:
            dt = time.time() - t0
            sleep_s = max(0.0, POLL_INTERVAL_SEC - dt)
            await asyncio.sleep(sleep_s)










