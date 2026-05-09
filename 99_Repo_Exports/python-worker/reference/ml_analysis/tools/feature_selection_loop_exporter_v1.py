from __future__ import annotations

import os

import uvicorn
from fastapi import FastAPI
from fastapi.responses import PlainTextResponse

from utils.time_utils import get_ny_time_millis


def _env(name: str, default: str = "") -> str:
    return (os.getenv(name) or default).strip()


REDIS_URL = _env("REDIS_URL", "")
METRICS_KEY = _env("FEATURE_SELECTION_LOOP_METRICS_KEY", "metrics:feature_selection_loop:last")
PORT = int(_env("FEATURE_SELECTION_LOOP_EXPORTER_PORT", "9821"))

app = FastAPI()


def _read_redis() -> dict[str, str]:
    if not REDIS_URL:
        return {}
    try:
        import redis  # type: ignore

        r = redis.Redis.from_url(REDIS_URL)
        d = r.hgetall(METRICS_KEY)
        out: dict[str, str] = {}
        for k, v in d.items():
            try:
                out[k.decode("utf-8")] = v.decode("utf-8")
            except Exception:
                pass
        return out
    except Exception:
        return {}


@app.get("/metrics")
def metrics() -> PlainTextResponse:
    d = _read_redis()
    success = 0.0
    updated_ts = 0.0
    age_s = 0.0
    n_rows = 0.0
    n_features = 0.0
    noise_n = 0.0
    noise_share = 0.0
    auc_val = 0.0
    brier_val = 0.0

    if d:
        try:
            success = 1.0 if float(d.get("success", "0")) > 0 else 0.0
        except Exception:
            success = 0.0
        try:
            updated_ts = float(d.get("updated_ts_ms", "0") or 0)
        except Exception:
            updated_ts = 0.0
        try:
            n_rows = float(d.get("n_rows", "0") or 0)
        except Exception:
            n_rows = 0.0
        try:
            n_features = float(d.get("n_features", "0") or 0)
        except Exception:
            n_features = 0.0
        try:
            noise_n = float(d.get("noise_n", "0") or 0)
        except Exception:
            noise_n = 0.0
        try:
            auc_val = float(d.get("auc_val", "0") or 0)
        except Exception:
            auc_val = 0.0
        try:
            brier_val = float(d.get("brier_val", "0") or 0)
        except Exception:
            brier_val = 0.0

    if updated_ts > 0:
        age_s = max(0.0, (get_ny_time_millis() - updated_ts) / 1000.0)
    if n_features > 0:
        noise_share = max(0.0, min(1.0, noise_n / n_features))

    lines = []
    lines.append("# HELP feature_selection_loop_exporter_up Exporter up")
    lines.append("# TYPE feature_selection_loop_exporter_up gauge")
    lines.append("feature_selection_loop_exporter_up 1")

    lines.append("# HELP feature_selection_loop_last_success Last feature selection loop success (1/0)")
    lines.append("# TYPE feature_selection_loop_last_success gauge")
    lines.append(f"feature_selection_loop_last_success {success}")

    lines.append("# HELP feature_selection_loop_last_updated_ts_ms Last updated timestamp (ms)")
    lines.append("# TYPE feature_selection_loop_last_updated_ts_ms gauge")
    lines.append(f"feature_selection_loop_last_updated_ts_ms {updated_ts}")

    lines.append("# HELP feature_selection_loop_age_seconds Age since last update (s)")
    lines.append("# TYPE feature_selection_loop_age_seconds gauge")
    lines.append(f"feature_selection_loop_age_seconds {age_s}")

    lines.append("# HELP feature_selection_loop_rows Number of joined rows used")
    lines.append("# TYPE feature_selection_loop_rows gauge")
    lines.append(f"feature_selection_loop_rows {n_rows}")

    lines.append("# HELP feature_selection_loop_features Number of features in schema")
    lines.append("# TYPE feature_selection_loop_features gauge")
    lines.append(f"feature_selection_loop_features {n_features}")

    lines.append("# HELP feature_selection_loop_noise_n Number of noisy candidates flagged")
    lines.append("# TYPE feature_selection_loop_noise_n gauge")
    lines.append(f"feature_selection_loop_noise_n {noise_n}")

    lines.append("# HELP feature_selection_loop_noise_share Share of noisy candidates flagged")
    lines.append("# TYPE feature_selection_loop_noise_share gauge")
    lines.append(f"feature_selection_loop_noise_share {noise_share}")

    lines.append("# HELP feature_selection_loop_auc_val AUC on validation split (quick model)")
    lines.append("# TYPE feature_selection_loop_auc_val gauge")
    lines.append(f"feature_selection_loop_auc_val {auc_val}")

    lines.append("# HELP feature_selection_loop_brier_val Brier score on validation split (quick model)")
    lines.append("# TYPE feature_selection_loop_brier_val gauge")
    lines.append(f"feature_selection_loop_brier_val {brier_val}")

    return PlainTextResponse("\n".join(lines) + "\n", media_type="text/plain; version=0.0.4")


@app.get("/healthz")
def healthz() -> dict[str, str]:
    return {"ok": "1"}


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=PORT)
