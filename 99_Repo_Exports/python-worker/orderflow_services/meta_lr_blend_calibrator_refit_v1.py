#!/usr/bin/env python3
"""Periodic refit service for meta_lr_blend posterior calibrator.

Runs `tools.refit_meta_lr_blend_calibrator.main()` in-process on an
interval (default every 6h). Each run reads `trades:closed`, fits an
isotonic calibrator, and — if acceptance gates pass — atomically writes
`calibrator.json` next to the meta_lr_blend artifact. Loader sibling
discovery picks it up on the next ml_confirm config cache refresh.

ENV
---
META_LR_BLEND_REFIT_MODEL_PATH    REQUIRED. Path to meta_lr_blend.json
META_LR_BLEND_REFIT_INTERVAL_S    Refit cadence (default 21600 = 6h)
META_LR_BLEND_REFIT_LOOKBACK_H    Window in hours (default 168 = 7d)
META_LR_BLEND_REFIT_MIN_N         Min samples (default 300)
META_LR_BLEND_REFIT_TARGET_VER    Version substr match (default "meta_lr_blend")
META_LR_BLEND_REFIT_BRIER_DELTA   Required Brier improvement (default 0.005)
META_LR_BLEND_REFIT_ECE_DELTA     Required ECE improvement (default 0.01)
META_LR_BLEND_REFIT_PORT          Prometheus :port (default 9867)
"""
from __future__ import annotations

import logging
import os
import sys
import time

log = logging.getLogger("meta_lr_blend_refit")

# Module-level metric registry; lazily initialised on first emit so unit
# imports stay side-effect-free (and so failures stay isolated).
_METRICS: dict[str, object] = {}


def _init_metrics() -> None:
    if _METRICS:
        return
    try:
        from prometheus_client import Counter, Gauge
        _METRICS["last_ts"] = Gauge(
            "meta_lr_blend_refit_last_run_ts", "Unix ts of last refit attempt",
        )
        _METRICS["accepted"] = Gauge(
            "meta_lr_blend_refit_accepted", "Whether last refit was accepted (0/1)",
        )
        _METRICS["n"] = Gauge(
            "meta_lr_blend_refit_n", "Sample count of last refit",
        )
        _METRICS["brier_delta"] = Gauge(
            "meta_lr_blend_refit_brier_delta", "Brier improvement (raw - cal)",
        )
        _METRICS["ece_delta"] = Gauge(
            "meta_lr_blend_refit_ece_delta", "ECE improvement (raw - cal)",
        )
        _METRICS["runs"] = Counter(
            "meta_lr_blend_refit_runs_total",
            "Refit attempts by outcome",
            ["outcome"],
        )
    except Exception as e:
        log.warning("metric init failed: %s", e)


def _emit_in_process_metrics(
    *, accepted: bool, n: int, reason: str, brier_delta: float, ece_delta: float,
) -> None:
    _init_metrics()
    try:
        if _METRICS:
            _METRICS["last_ts"].set(int(time.time()))  # type: ignore[attr-defined]
            _METRICS["accepted"].set(1 if accepted else 0)  # type: ignore[attr-defined]
            _METRICS["n"].set(int(n))  # type: ignore[attr-defined]
            _METRICS["brier_delta"].set(float(brier_delta))  # type: ignore[attr-defined]
            _METRICS["ece_delta"].set(float(ece_delta))  # type: ignore[attr-defined]
            outcome = "accepted" if accepted else (reason or "rejected").split("(")[0]
            _METRICS["runs"].labels(outcome=outcome).inc()  # type: ignore[attr-defined]
    except Exception as e:
        log.warning("metrics emission failed: %s", e)


def _run_once(model_path: str) -> None:
    """One refit pass — fail-safe; logs and swallows exceptions."""
    try:
        from tools.refit_meta_lr_blend_calibrator import (
            _read_trades_closed,
            _atomic_write_json,
            fit_and_evaluate,
        )
        from core.redis_client import get_redis

        r = get_redis()
        pairs = _read_trades_closed(
            r,
            stream=os.getenv("REFIT_STREAM", "trades:closed"),
            lookback_hours=int(os.getenv("META_LR_BLEND_REFIT_LOOKBACK_H", "168")),
            target_version_substr=os.getenv("META_LR_BLEND_REFIT_TARGET_VER", "meta_lr_blend"),
        )
        result = fit_and_evaluate(
            pairs,
            min_n=int(os.getenv("META_LR_BLEND_REFIT_MIN_N", "300")),
            require_brier_improvement=float(os.getenv("META_LR_BLEND_REFIT_BRIER_DELTA", "0.005")),
            require_ece_improvement=float(os.getenv("META_LR_BLEND_REFIT_ECE_DELTA", "0.01")),
        )
        accepted = bool(result.get("accepted"))
        n = int(result.get("n", 0))
        log.info(
            "refit n=%d accepted=%s reason=%s brier_delta=%.4f ece_delta=%.4f",
            n, accepted, result.get("reason"),
            float(result.get("brier_delta", 0.0) or 0.0),
            float(result.get("ece_delta", 0.0) or 0.0),
        )
        _emit_in_process_metrics(
            accepted=accepted,
            n=n,
            reason=str(result.get("reason", "unknown")),
            brier_delta=float(result.get("brier_delta", 0.0) or 0.0),
            ece_delta=float(result.get("ece_delta", 0.0) or 0.0),
        )

        # Write report regardless of acceptance (audit trail).
        report_dir = os.getenv("REFIT_REPORT_DIR", "/var/lib/trade/of_reports")
        try:
            os.makedirs(report_dir, exist_ok=True)
            report_path = os.path.join(report_dir, f"meta_lr_blend_refit_{int(time.time()*1000)}.json")
            _atomic_write_json(report_path, {**result, "model_path": model_path})
        except Exception as e:
            log.warning("report write failed: %s", e)

        if not accepted:
            return

        cal_path = os.path.join(os.path.dirname(model_path), "calibrator.json")
        artifact = dict(result["calibrator"])
        artifact["meta"] = {
            "kind": "meta_lr_blend_posterior",
            "n": n,
            "brier_raw": result["brier_raw"],
            "brier_cal": result["brier_cal"],
            "ece_raw": result["ece_raw"],
            "ece_cal": result["ece_cal"],
            "pos_rate": result["pos_rate"],
            "fit_ts_ms": int(time.time() * 1000),
            "lookback_hours": int(os.getenv("META_LR_BLEND_REFIT_LOOKBACK_H", "168")),
            "service": "meta_lr_blend_calibrator_refit_v1",
        }
        _atomic_write_json(cal_path, artifact)
        log.info("promoted calibrator: %s", cal_path)
    except Exception as e:
        log.exception("refit run failed: %s", e)


def main() -> int:
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
        stream=sys.stderr,
    )

    model_path = os.getenv("META_LR_BLEND_REFIT_MODEL_PATH", "").strip()
    if not model_path:
        log.error("META_LR_BLEND_REFIT_MODEL_PATH not set; nothing to refit.")
        return 2
    if not os.path.exists(model_path):
        log.error("model_path does not exist: %s", model_path)
        return 2

    try:
        from prometheus_client import start_http_server
        port = int(os.getenv("META_LR_BLEND_REFIT_PORT", "9867"))
        start_http_server(port)
        log.info("prometheus on :%d", port)
    except Exception as e:
        log.warning("prometheus startup failed: %s", e)

    interval = max(60, int(os.getenv("META_LR_BLEND_REFIT_INTERVAL_S", "21600")))
    log.info("meta_lr_blend refit loop interval=%ds model=%s", interval, model_path)

    # Run once at startup, then on cadence.
    while True:
        _run_once(model_path)
        time.sleep(interval)


if __name__ == "__main__":
    raise SystemExit(main())
