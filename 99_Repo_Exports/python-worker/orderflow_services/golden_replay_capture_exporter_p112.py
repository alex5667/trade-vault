from __future__ import annotations

"""B7: Prometheus exporter for golden replay capture + nightly parity state.

Exposes gauges by scraping:
- <OFC_CAPTURE_DIR>/_state/ofc_capture_stats_<host>-<pid>.json  (from runtime workers)
- <GOLDEN_REPLAY_OUTDIR>/_state/gr_state_v1.json                (from nightly job)

Designed to run as a standalone process because production workers may not expose
/metrics endpoints.
""",
import json
import os
import time
from glob import glob
from pathlib import Path
from typing import Any

from prometheus_client import Counter, Gauge, start_http_server


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.environ.get(name, str(default)))
    except Exception:
        return default


def _read_json(path: Path) -> dict[str, Any] | None:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


G_CAPTURE_WRITTEN = Gauge("ofc_capture_written_total", "Captured NDJSON records written", ["host", "pid"])
G_CAPTURE_BYTES = Gauge("ofc_capture_bytes_total", "Captured NDJSON bytes written", ["host", "pid"])
G_CAPTURE_ERRORS = Gauge("ofc_capture_errors_total", "Capture write errors", ["host", "pid"])
G_CAPTURE_SAMPLED_OUT = Gauge("ofc_capture_sampled_out_total", "Records skipped by sampler", ["host", "pid"])
G_CAPTURE_LAST_WRITE = Gauge("ofc_capture_last_write_ts_ms", "Last successful capture write timestamp (ms)", ["host", "pid"])
G_CAPTURE_LAST_ERROR = Gauge("ofc_capture_last_error_ts_ms", "Last capture error timestamp (ms)", ["host", "pid"])

G_GR_POLICY_CNT = Gauge("golden_replay_policy_cnt", "Number of policy groups processed in latest nightly run")
G_GR_MISMATCHES = Gauge("golden_replay_mismatches_total", "Total mismatches across all policies in latest run")
G_GR_LAST_OK_DAY = Gauge("golden_replay_last_ok_day", "Last clean day (YYYYMMDD) as integer, 0 if unknown")
G_GR_UPDATED = Gauge("golden_replay_state_updated_ts_ms", "State update timestamp (ms)")

C_EXPORTER_SCRAPE_ERRORS = Counter("gr_capture_exporter_scrape_errors_total", "Exporter scrape errors", ["kind"])


def _emit_capture_stats(state_dir: Path) -> None:
    for p in glob(str(state_dir / "ofc_capture_stats_*.json")):
        js = _read_json(Path(p))
        if not js:
            C_EXPORTER_SCRAPE_ERRORS.labels(kind="capture_json").inc()
            continue
        host = (js.get("host", "unknown"))
        pid = (js.get("pid", "0"))
        G_CAPTURE_WRITTEN.labels(host=host, pid=pid).set(float(js.get("written_total", 0)))
        G_CAPTURE_BYTES.labels(host=host, pid=pid).set(float(js.get("bytes_total", 0)))
        G_CAPTURE_ERRORS.labels(host=host, pid=pid).set(float(js.get("errors_total", 0)))
        G_CAPTURE_SAMPLED_OUT.labels(host=host, pid=pid).set(float(js.get("sampled_out_total", 0)))
        G_CAPTURE_LAST_WRITE.labels(host=host, pid=pid).set(float(js.get("last_write_ts_ms", 0)))
        G_CAPTURE_LAST_ERROR.labels(host=host, pid=pid).set(float(js.get("last_error_ts_ms", 0)))


def _emit_gr_state(state_path: Path) -> None:
    js = _read_json(state_path)
    if not js:
        C_EXPORTER_SCRAPE_ERRORS.labels(kind="gr_state_json").inc()
        return
    G_GR_POLICY_CNT.set(float(js.get("policy_cnt", 0)))
    G_GR_MISMATCHES.set(float(js.get("mismatches_total", 0)))
    G_GR_UPDATED.set(float(js.get("updated_ts_ms", 0)))
    last_ok = js.get("last_ok_day") or ""
    try:
        G_GR_LAST_OK_DAY.set(float(int(last_ok)))
    except Exception:
        G_GR_LAST_OK_DAY.set(0.0)


def main() -> int:
    port = _env_int("GR_CAPTURE_EXPORTER_PORT", 9206)
    scrape_sec = _env_int("GR_CAPTURE_EXPORTER_SCRAPE_SEC", 5)

    capture_dir = Path(os.environ.get("OFC_CAPTURE_DIR", "/var/lib/scanner/ofc_capture"))
    capture_state_dir = capture_dir / "_state"

    gr_out = Path(os.environ.get("GOLDEN_REPLAY_OUTDIR", "/var/lib/scanner/golden_replay"))
    gr_state_path = gr_out / "_state" / "gr_state_v1.json"

    start_http_server(port)
    while True:
        try:
            if capture_state_dir.exists():
                _emit_capture_stats(capture_state_dir)
        except Exception:
            C_EXPORTER_SCRAPE_ERRORS.labels(kind="capture_scan").inc()

        try:
            if gr_state_path.exists():
                _emit_gr_state(gr_state_path)
        except Exception:
            C_EXPORTER_SCRAPE_ERRORS.labels(kind="gr_state_scan").inc()

        time.sleep(max(1, int(scrape_sec)))


if __name__ == "__main__":
    raise SystemExit(main())
