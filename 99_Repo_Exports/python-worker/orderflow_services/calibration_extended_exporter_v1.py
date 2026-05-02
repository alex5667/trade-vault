#!/usr/bin/env python3
from __future__ import annotations
"""Prometheus exporter for extended confidence-calibration diagnostics.

Reads proof/status JSON files written by conf_cal_promotion_manager_v1.py
and exports per-arm / delta metrics to Prometheus.

Arms: active, champion, challenger
Metrics: ece_cal, mce_cal, brier_cal, calibration_slope, calibration_intercept,
         sharpness_mean, sharpness_entropy, prob_mass_near_half, precision_top5p

ENV:
  CONF_CAL_PROOF_STATE_PATH         path to proof JSON (default /tmp/conf_cal_proof_state.json)
  CONF_CAL_PROMOTION_STATUS_PATH    path to status JSON (default /tmp/conf_cal_promo_status.json)
  CALIBRATION_EXT_EXPORTER_PORT     HTTP port (default 9138)
  CALIBRATION_EXT_EXPORTER_REFRESH_SEC  poll interval seconds (default 10)
""",
import json
import os
import signal
import time
from typing import Any, Dict, Optional

from prometheus_client import Counter, Gauge, start_http_server  # type: ignore


def _load_json(path: str) -> Optional[Dict[str, Any]]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            obj = json.load(f)
        return obj if isinstance(obj, dict) else None
    except Exception:
        return None


def _age_sec(path: str) -> float:
    try:
        return max(0.0, time.time() - os.path.getmtime(path))
    except Exception:
        return float("nan")


def _metric_value(obj: Dict[str, Any], arm: str, metric: str) -> float:
    arms = obj.get("arms") or {}
    if not isinstance(arms, dict):
        return float("nan")
    arm_obj = arms.get(arm) or {}
    if not isinstance(arm_obj, dict):
        return float("nan")
    try:
        return float(arm_obj.get(metric, float("nan")))
    except Exception:
        return float("nan")


def _delta_value(obj: Dict[str, Any], metric: str) -> float:
    arms = obj.get("arms") or {}
    delta = arms.get("delta") if isinstance(arms, dict) else {}
    if not isinstance(delta, dict):
        return float("nan")
    try:
        return float(delta.get(metric, float("nan")))
    except Exception:
        return float("nan")


g_up = Gauge("conf_cal_extended_exporter_up", "extended calibration exporter loop up")
g_read_ok = Gauge("conf_cal_extended_read_ok", "proof/status read ok (1/0)")
g_proof_age = Gauge("conf_cal_extended_proof_age_sec", "proof json age in seconds")
g_status_age = Gauge("conf_cal_extended_status_age_sec", "status json age in seconds")
g_degrade_review = Gauge("conf_cal_extended_degrade_review", "degrade-review requested by promotion manager")
g_promoted = Gauge("conf_cal_extended_promoted_last_run", "promotion manager promoted on last run")
g_metric = Gauge("conf_cal_extended_metric", "extended calibration metric by arm", ["arm", "metric"])
g_delta = Gauge("conf_cal_extended_delta", "challenger - champion delta for extended calibration metrics", ["metric"])
read_errors_total = Counter("conf_cal_extended_read_errors_total", "proof/status read errors")
parse_errors_total = Counter("conf_cal_extended_parse_errors_total", "proof/status parse/shape errors")


class Exporter:
    def __init__(self) -> None:
        self.proof_path = os.getenv("CONF_CAL_PROOF_STATE_PATH", "/tmp/conf_cal_proof_state.json")
        self.status_path = os.getenv("CONF_CAL_PROMOTION_STATUS_PATH", "/tmp/conf_cal_promo_status.json")
        self.refresh_sec = max(1.0, float(os.getenv("CALIBRATION_EXT_EXPORTER_REFRESH_SEC", "10")))
        self.running = True
        signal.signal(signal.SIGINT, self._stop)
        signal.signal(signal.SIGTERM, self._stop)

    def _stop(self, signum, frame) -> None:
        self.running = False

    def step(self) -> None:
        proof = _load_json(self.proof_path)
        status = _load_json(self.status_path)
        if proof is None or status is None:
            g_read_ok.set(0.0)
            read_errors_total.inc()
            return
        if not isinstance(proof.get("arms"), dict):
            g_read_ok.set(0.0)
            parse_errors_total.inc()
            return

        g_read_ok.set(1.0)
        g_proof_age.set(_age_sec(self.proof_path))
        g_status_age.set(_age_sec(self.status_path))
        g_degrade_review.set(1.0 if bool(status.get("degrade_review") or proof.get("degrade_review")) else 0.0)
        g_promoted.set(1.0 if bool(status.get("promoted")) else 0.0)

        metric_names = [
            "ece_cal",
            "mce_cal",
            "brier_cal",
            "calibration_slope",
            "calibration_intercept",
            "sharpness_mean",
            "sharpness_entropy",
            "prob_mass_near_half",
            "precision_top5p",
        ]
        for arm in ("active", "champion", "challenger"):
            for metric in metric_names:
                g_metric.labels(arm=arm, metric=metric).set(_metric_value(proof, arm, metric))
        for metric in ("ece_cal", "mce_cal", "brier_cal", "precision_top5p", "sharpness_mean", "prob_mass_near_half"):
            g_delta.labels(metric=metric).set(_delta_value(proof, metric))

    def run(self) -> None:
        port = int(os.getenv("CALIBRATION_EXT_EXPORTER_PORT", "9138"))
        start_http_server(port)
        while self.running:
            g_up.set(1.0)
            try:
                self.step()
            except Exception:
                g_read_ok.set(0.0)
                read_errors_total.inc()
            time.sleep(self.refresh_sec)


def main() -> None:
    Exporter().run()


if __name__ == "__main__":
    main()
