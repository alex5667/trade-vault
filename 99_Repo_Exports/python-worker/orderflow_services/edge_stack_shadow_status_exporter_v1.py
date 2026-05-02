#!/usr/bin/env python3
from __future__ import annotations
"""Prometheus exporter for Edge Stack shadow evaluation status.

Reads a JSON status file produced by `tools.edge_stack_shadow_eval_bundle_v1`
(and/or similar jobs) and exposes key metrics.

This keeps monitoring decoupled from the trading runtime. If the status file is stale
or cannot be parsed, `edge_stack_shadow_status_up` will be 0.
""",
import json
import os
import time
from dataclasses import dataclass
from typing import Any, Dict, Optional

from prometheus_client import Gauge, start_http_server


def _now_s() -> float:
    return time.time()


def _read_json(path: str) -> Optional[Dict[str, Any]]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


@dataclass
class Config:
    status_file: str
    port: int
    interval_s: float
    stale_s: float


UP = Gauge("edge_stack_shadow_status_up", "1 if status file is readable and not stale")
AGE = Gauge("edge_stack_shadow_eval_age_seconds", "Age of last status file in seconds")
NROWS = Gauge("edge_stack_shadow_eval_rows", "Number of rows evaluated")
PROMOTE_REC = Gauge("edge_stack_shadow_promote_recommended", "1 if guard recommends promotion")
PROMOTE_APPLIED = Gauge("edge_stack_shadow_promote_applied", "1 if this run applied promotion")

BRIER = Gauge("edge_stack_shadow_brier", "Brier score", ["model", "cal"])
ECE = Gauge("edge_stack_shadow_ece", "ECE", ["model", "cal"])
P5 = Gauge("edge_stack_shadow_precision_top5pct", "Precision@top5%", ["model", "cal"])
EXP5 = Gauge("edge_stack_shadow_expectancy_r_top5pct", "Expectancy R@top5%", ["model", "cal"])


# Compatibility gauges for P60 alert rules (no labels)
# Names must match prometheus_alerts_edge_stack_shadow_p60.yml exactly
LAST_SUCCESS = Gauge("edge_stack_shadow_last_success", "1 if last shadow eval succeeded")
LAST_UPDATED_MS = Gauge("edge_stack_shadow_last_updated_ts_ms", "Last shadow eval updated_ts_ms")
CHAMP_BRIER = Gauge("edge_stack_shadow_champion_brier", "Champion brier (cal, no labels)")


def _set_model_metrics(blob: Dict[str, Any], model: str) -> None:
    # blob: {raw:{...}, cal:{...}}
    for cal in ("raw", "cal"):
        m = blob.get(cal) or {}
        try:
            BRIER.labels(model=model, cal=cal).set(float(m.get("brier", 0.0) or 0.0))
            ECE.labels(model=model, cal=cal).set(float(m.get("ece", 0.0) or 0.0))
            P5.labels(model=model, cal=cal).set(float(m.get("precision_top5pct", 0.0) or 0.0))
            EXP5.labels(model=model, cal=cal).set(float(m.get("expectancy_r_top5pct", 0.0) or 0.0))
        except Exception:
            # keep last good
            continue


def load_cfg() -> Config:
    return Config(
        status_file=os.environ.get("EDGE_STACK_SHADOW_STATUS_FILE", "/var/lib/trade/of_reports/out/edge_stack/shadow_status.json"),
        port=int(os.environ.get("EDGE_STACK_SHADOW_EXPORTER_PORT", "8012")),
        interval_s=float(os.environ.get("EDGE_STACK_SHADOW_EXPORTER_INTERVAL_S", "5")),
        stale_s=float(os.environ.get("EDGE_STACK_SHADOW_STATUS_STALE_S", "900")),
    )


def main() -> int:
    cfg = load_cfg()
    start_http_server(cfg.port)

    while True:
        data = _read_json(cfg.status_file)
        if not data:
            UP.set(0)
            AGE.set(cfg.stale_s + 1)
        else:
            ts_ms = int(data.get("ts_ms", 0) or 0)
            age = _now_s() - (ts_ms / 1000.0 if ts_ms > 0 else _now_s())
            AGE.set(float(age))
            ok = 1 if age <= cfg.stale_s else 0
            UP.set(ok)
            # P60 compat gauges (used by prometheus_alerts_edge_stack_shadow_p60.yml)
            LAST_SUCCESS.set(1.0 if ok == 1 else 0.0)
            LAST_UPDATED_MS.set(float(ts_ms))

            NROWS.set(float(data.get("n", 0) or 0))
            PROMOTE_REC.set(1.0 if int(data.get("promote_recommended", 0) or 0) == 1 else 0.0)
            PROMOTE_APPLIED.set(1.0 if int(data.get("promote_applied", 0) or 0) == 1 else 0.0)

            champ = data.get("champion") or {}
            chall = data.get("challenger") or {}
            _set_model_metrics(champ.get("metrics", {}) or {}, "champion")
            # Populate P60 compat champion_brier gauge from calibrated metrics
            try:
                cb = (champ.get('metrics', {}) or {}).get('cal', {}).get('brier', 0.0)
                CHAMP_BRIER.set(float(cb or 0.0))
            except Exception:
                pass
            _set_model_metrics(chall.get("metrics", {}) or {}, "challenger")

        time.sleep(cfg.interval_s)


if __name__ == "__main__":
    raise SystemExit(main())
