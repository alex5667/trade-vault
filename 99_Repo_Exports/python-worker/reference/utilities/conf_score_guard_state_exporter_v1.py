"""conf_score_guard_state_exporter_v1.py

Prometheus exporter for the confidence score guardrails state file.

Reads CONF_SCORE_GUARD_STATE_PATH (json) written by
orderflow_services/conf_score_guardrails_apply_v1.py and exposes:
  - conf_score_guard_freeze{symbol}
  - conf_score_guard_scale{symbol}
  - conf_score_guard_drift_max_abs_z{symbol}
  - conf_score_guard_status_age_seconds
  - conf_score_guard_bundle_ts_ms
  - conf_score_guard_bundle_changed_symbols

Designed for low cardinality: exports only symbols present in the state file.
"""

import json
import logging
import os
import time
from typing import Any

from prometheus_client import Gauge, start_http_server

logger = logging.getLogger(__name__)

FREEZE = Gauge("conf_score_guard_freeze", "Guardrails freeze active (1/0)", ["symbol"])
SCALE = Gauge("conf_score_guard_scale", "Guardrails confidence scale", ["symbol"])
DRIFTZ = Gauge("conf_score_guard_drift_max_abs_z", "Max absolute drift z across confidence parts", ["symbol"])
LATCH = Gauge("conf_score_guard_latch_remaining_sec", "Remaining freeze latch time (sec)", ["symbol"])
STREAK = Gauge("conf_score_guard_stable_streak", "Consecutive stable runs (for recovery)", ["symbol"])
CANARY = Gauge("conf_score_guard_canary", "Symbol selected for canary application (1/0)", ["symbol"])

STATUS_AGE = Gauge("conf_score_guard_status_age_seconds", "Age of the guard state file (seconds)")

APPLIED = Gauge("conf_score_guard_apply_applied", "How many symbols were applied to Redis in the last run")
SKIPPED = Gauge("conf_score_guard_apply_skipped", "How many symbols were skipped (canary=0) in the last run")
SYMBOLS = Gauge("conf_score_guard_symbols", "How many symbols are present in the state file")
CANARY_SYMBOLS = Gauge("conf_score_guard_canary_symbols", "How many symbols have canary=1")

BUNDLE_TS = Gauge("conf_score_guard_bundle_ts_ms", "Timestamp of the current bundle (ms)")
BUNDLE_CHANGED = Gauge("conf_score_guard_bundle_changed_symbols", "Count of symbols with decision changes in bundle")


STAGE_PRESENT = Gauge("conf_score_guard_stage_present", "1 if staged.json exists")
STAGE_AGE = Gauge("conf_score_guard_stage_pointer_age_seconds", "Age of staged.json in seconds")
PROMOTE_LAST_OK = Gauge("conf_score_guard_promote_last_ok", "1 if current.json is valid and recent")
PROMOTE_AGE = Gauge("conf_score_guard_promote_last_age_seconds", "Age of current.json in seconds")

def _load(path: str) -> dict[str, Any]:
    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}

def main() -> None:
    path = os.getenv("CONF_SCORE_GUARD_STATE_PATH", "/tmp/conf_score_guard_state.json")
    bundle_dir = os.getenv("CONF_SCORE_GUARD_BUNDLE_DIR", "/var/lib/trade/conf_score_guard_bundles")

    port = int(os.getenv("CONF_SCORE_GUARD_EXPORTER_PORT", "9135"))
    sleep_s = float(os.getenv("CONF_SCORE_GUARD_EXPORTER_INTERVAL_SEC", "15"))

    start_http_server(port)
    logger.info(f"Serving metrics on port {port}, reading {path}")

    # Pre-declare metrics that might not be in state
    STAGE_PRESENT.set(0)
    STAGE_AGE.set(0)
    PROMOTE_LAST_OK.set(0)
    PROMOTE_AGE.set(0)

    while True:
        try:
            # 1. State File Metrics
            if os.path.exists(path):
                mtime = os.path.getmtime(path)
                age = time.time() - mtime
                STATUS_AGE.set(age)

                data = _load(path)
                decisions = data.get("decisions") if isinstance(data.get("decisions"), dict) else {}

                SYMBOLS.set(float(len(decisions)))

                canary_count = 0
                for sym, d in decisions.items():
                    if isinstance(d, dict) and int(d.get("canary", 0) or 0) == 1:
                        canary_count += 1
                CANARY_SYMBOLS.set(float(canary_count))

                apply_info = data.get("apply") if isinstance(data.get("apply"), dict) else {}
                APPLIED.set(float(apply_info.get("applied", 0) or 0))
                # SKIPPED metric might need better definition, for now keep as is if used

                bundle_info = data.get("bundle") if isinstance(data.get("bundle"), dict) else {}
                ts = float(data.get("ts_ms") or 0)
                BUNDLE_TS.set(ts)
                BUNDLE_CHANGED.set(float(bundle_info.get("changed_count", 0) or 0))

                for sym, d in decisions.items():
                    if not isinstance(d, dict):
                        continue
                    sym_s = str(sym)
                    FREEZE.labels(symbol=sym_s).set(1.0 if float(d.get("freeze", 0) or 0) > 0 else 0.0)
                    SCALE.labels(symbol=sym_s).set(float(d.get("scale", 1.0) or 1.0))
                    DRIFTZ.labels(symbol=sym_s).set(float(d.get("max_abs_dz", 0.0) or 0.0))
                    LATCH.labels(symbol=sym_s).set(float(d.get("latch_remaining_sec", 0.0) or 0.0))
                    STREAK.labels(symbol=sym_s).set(float(d.get("stable_streak", 0) or 0))
                    CANARY.labels(symbol=sym_s).set(1.0 if int(d.get("canary", 0) or 0) == 1 else 0.0)
            else:
                STATUS_AGE.set(9999)

            # 2. Stage/Promote Metrics
            if bundle_dir:
                # Stage
                staged_path = os.path.join(bundle_dir, "staged.json")
                if os.path.exists(staged_path):
                    STAGE_PRESENT.set(1)
                    try:
                        sage = time.time() - os.path.getmtime(staged_path)
                        STAGE_AGE.set(sage)
                    except Exception:
                        STAGE_AGE.set(0)
                else:
                    STAGE_PRESENT.set(0)
                    STAGE_AGE.set(0)

                # Promote (Current)
                current_path = os.path.join(bundle_dir, "current.json")
                if os.path.exists(current_path):
                    try:
                        cage = time.time() - os.path.getmtime(current_path)
                        PROMOTE_AGE.set(cage)
                        PROMOTE_LAST_OK.set(1)
                    except Exception:
                        PROMOTE_AGE.set(0)
                        PROMOTE_LAST_OK.set(0)
                else:
                    PROMOTE_AGE.set(0)
                    PROMOTE_LAST_OK.set(0)

        except Exception as e:
            logger.error(f"Error reading state: {e}")

        time.sleep(sleep_s)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
