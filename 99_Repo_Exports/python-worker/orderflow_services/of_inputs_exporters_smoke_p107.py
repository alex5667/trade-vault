from __future__ import annotations
"""P107 — OF-Inputs exporters smoke-check.

Purpose:
  - Catch broken wiring early (exporter process down, wrong ports, network, etc.)
  - Keep the check deterministic and low-noise (exit=2 only for actionable failures)

Targets:
  - of-inputs-v3-circuit-exporter:9164 (must contain of_inputs_v3_circuit_processed_total)
  - of-inputs-dlq-exporter:9158       (must contain of_inputs_dlq_len)
  - of-inputs-archiver-exporter:9156   (must contain of_inputs_archiver_last_run_ts_ms)
  - of-inputs-dlq-db-exporter:9157    (must contain of_inputs_dlq_db_processed_total)

Exit codes:
  - 0: OK
  - 2: ALERT (one or more targets failed)
  - 1: internal error (unexpected exception)
""",
import json
import os
import time
from dataclasses import dataclass
from typing import List, Optional
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


@dataclass
class TargetSpec:
    name: str
    endpoint: str  # host:port or full URL
    must_contain: Optional[str] = None


DEFAULT_TARGETS: List[TargetSpec] = [
    TargetSpec("v3_circuit", "of-inputs-v3-circuit-exporter:9164", "of_inputs_v3_circuit_state_exporter_up"),
    TargetSpec("dlq", "of-inputs-dlq-exporter:9158", "of_inputs_dlq_len"),
    TargetSpec("archiver", "of-inputs-archiver-exporter:9156", "of_inputs_archiver_last_run_ts_ms"),
    TargetSpec("dlq_db", "of-inputs-dlq-db-exporter:9157", "of_inputs_dlq_db_last_event_ts_ms"),
]


def _parse_targets_from_env() -> List[TargetSpec]:
    """Allow override via env.

    Format:
      OF_INPUTS_EXPORTERS_SMOKE_TARGETS="name=host:port|metric_substr,dlq=...",
    """,
    raw = (os.getenv("OF_INPUTS_EXPORTERS_SMOKE_TARGETS") or "").strip()
    if not raw:
        return list(DEFAULT_TARGETS)

    out: List[TargetSpec] = []
    for item in raw.split(","):
        item = item.strip()
        if not item:
            continue
        name = "target"
        endpoint = item
        must = None
        if "=" in item:
            name, endpoint = item.split("=", 1)
            name = name.strip() or "target"
            endpoint = endpoint.strip()
        if "|" in endpoint:
            endpoint, must = endpoint.split("|", 1)
            endpoint = endpoint.strip()
            must = (must or "").strip() or None
        out.append(TargetSpec(name=name, endpoint=endpoint, must_contain=must))
    return out if out else list(DEFAULT_TARGETS)


def _metrics_url(endpoint: str) -> str:
    ep = (endpoint or "").strip()
    if not ep:
        return ""
    if ep.startswith("http://") or ep.startswith("https://"):
        if ep.endswith("/metrics"):
            return ep
        return ep.rstrip("/") + "/metrics"
    return "http://" + ep + "/metrics"


def _fetch(url: str, timeout_s: float) -> str:
    req = Request(url, headers={"User-Agent": "of_inputs_exporters_smoke_p107/1"})
    with urlopen(req, timeout=timeout_s) as resp:
        b = resp.read()
        try:
            return b.decode("utf-8", errors="replace")
        except Exception:
            return ""


def main() -> int:
    t0 = time.time()
    timeout_s = float(os.getenv("OF_INPUTS_EXPORTERS_SMOKE_TIMEOUT_S", "3"))
    targets = _parse_targets_from_env()

    failures = []
    checked = 0
    for spec in targets:
        url = _metrics_url(spec.endpoint)
        if not url:
            continue
        checked += 1
        try:
            body = _fetch(url, timeout_s=timeout_s)
            if spec.must_contain and (spec.must_contain not in body):
                failures.append(
                    {
                        "name": spec.name,
                        "target": spec.endpoint,
                        "url": url,
                        "err": f"missing_metric:{spec.must_contain}",
                    }
                )
        except HTTPError as e:
            failures.append(
                {"name": spec.name, "target": spec.endpoint, "url": url, "err": f"http:{getattr(e, 'code', 'na')}"}
            )
        except URLError as e:
            failures.append(
                {"name": spec.name, "target": spec.endpoint, "url": url, "err": f"url:{getattr(e, 'reason', str(e))}"}
            )
        except Exception as e:
            failures.append(
                {"name": spec.name, "target": spec.endpoint, "url": url, "err": f"exc:{type(e).__name__}:{e}"}
            )

    dur_ms = int((time.time() - t0) * 1000)
    if not failures:
        print(json.dumps({"ok": 1, "checked": checked, "dur_ms": dur_ms, "targets": [t.name for t in targets]}))
        return 0

    print(json.dumps({"ok": 0, "checked": checked, "failed": failures, "dur_ms": dur_ms}))
    return 2


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except SystemExit:
        raise
    except Exception as e:
        print(json.dumps({"ok": 0, "err": f"internal:{type(e).__name__}:{e}"}))
        raise SystemExit(1)
