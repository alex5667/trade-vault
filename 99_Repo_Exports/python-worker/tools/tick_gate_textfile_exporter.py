#!/usr/bin/env python3
"""Export tick-quality gate outcomes from Redis stream as Prometheus textfile metrics.

This is a low-friction way to put gate outcomes on dashboards/alerts without
modifying the running service:
 - reads ops:tick_quality_gate for a window (default 24h)
 - writes a .prom file compatible with node_exporter textfile collector

Metrics emitted (windowed counts):
  tick_gate_pass_total{window_h="24"}
  tick_gate_fail_total{window_h="24"}
  tick_gate_insufficient_total{window_h="24"}
  tick_gate_error_total{window_h="24"}
  tick_gate_fail_metric_total{metric="...",window_h="24"}
"""

from __future__ import annotations
from utils.time_utils import get_ny_time_millis

import argparse
import json
import os
import sys
import time
from typing import Any, Dict, List, Optional, Tuple


def _now_ms() -> int:
    return get_ny_time_millis()


def _safe_int(v: Any, default: int = 0) -> int:
    if v is None:
        return default
    if isinstance(v, bool):
        return int(v)
    if isinstance(v, int):
        return v
    if isinstance(v, (bytes, bytearray)):
        try:
            v = v.decode("utf-8", errors="replace")
        except Exception:
            return default
    try:
        return int(float(str(v).strip()))
    except Exception:
        return default


def _norm_str(v: Any, default: str = "") -> str:
    if v is None:
        return default
    if isinstance(v, (bytes, bytearray)):
        try:
            v = v.decode("utf-8", errors="replace")
        except Exception:
            return default
    try:
        s = str(v).strip()
    except Exception:
        return default
    return s if s else default


def _loads_json_if_possible(v: Any) -> Any:
    if v is None:
        return None
    if isinstance(v, (dict, list, int, float, bool)):
        return v
    if isinstance(v, (bytes, bytearray)):
        try:
            v = v.decode("utf-8", errors="replace")
        except Exception:
            return None
    if not isinstance(v, str):
        v = str(v)
    s = v.strip()
    if not s:
        return None
    if (s.startswith("{") and s.endswith("}")) or (s.startswith("[") and s.endswith("]")):
        try:
            return json.loads(s)
        except Exception:
            return v
    return v


def _parse_msg_id_ms(msg_id: str) -> int:
    try:
        return int(msg_id.split("-", 1)[0])
    except Exception:
        return 0


def _extract_status_and_failures(msg_id: str, fields: Dict[str, Any]) -> Tuple[str, List[str], int]:
    payload = None
    for k in ("json", "payload", "report", "gate", "result"):
        if k in fields:
            payload = _loads_json_if_possible(fields.get(k))
            if isinstance(payload, dict):
                break
    if not isinstance(payload, dict):
        payload = {}
    merged: Dict[str, Any] = {}
    merged.update(payload)
    merged.update(fields)

    status = _norm_str(merged.get("status") or merged.get("state"), "UNKNOWN").upper()
    if status in ("OK", "PASSING", "SUCCESS"):
        status = "PASS"
    if status in ("FAILED", "FAILURE"):
        status = "FAIL"
    if status in ("INSUFFICIENT", "NO_DATA", "INSUFFICIENTDATA"):
        status = "INSUFFICIENT_DATA"

    rc = _safe_int(merged.get("return_code") or merged.get("code"), 0)
    if status not in ("PASS", "FAIL", "INSUFFICIENT_DATA", "ERROR"):
        if rc == 0:
            status = "PASS"
        elif rc in (2, 20):
            status = "FAIL"
        elif rc in (1, 21):
            status = "INSUFFICIENT_DATA"
        elif rc:
            status = "ERROR"

    failures_raw = merged.get("failures") or merged.get("failed") or merged.get("reasons") or merged.get("violations")
    failures: List[str] = []
    fr = _loads_json_if_possible(failures_raw)
    if isinstance(fr, list):
        for x in fr:
            if isinstance(x, dict):
                failures.append(_norm_str(x.get("metric") or x.get("name") or x.get("reason"), "unknown"))
            else:
                failures.append(_norm_str(x, "unknown"))
    elif isinstance(fr, dict):
        for mk in fr.keys():
            failures.append(_norm_str(mk, "unknown"))
    return status, failures, rc


def _read_stream_events(redis_url: str, stream: str, start_ms: int, limit: int) -> List[Tuple[str, Dict[str, Any]]]:
    try:
        import redis  # type: ignore
    except Exception as e:
        raise RuntimeError("redis-py is required") from e

    r = redis.Redis.from_url(redis_url, decode_responses=False)
    raw = r.xrevrange(stream, max="+", min="-", count=int(limit))
    out: List[Tuple[str, Dict[str, Any]]] = []
    for msg_id_b, fields in raw:
        msg_id = msg_id_b.decode("utf-8", errors="replace") if isinstance(msg_id_b, (bytes, bytearray)) else str(msg_id_b)
        ts_ms = _parse_msg_id_ms(msg_id)
        if ts_ms < start_ms:
            break
        f2: Dict[str, Any] = {}
        for k, v in (fields or {}).items():
            kk = k.decode("utf-8", errors="replace") if isinstance(k, (bytes, bytearray)) else str(k)
            f2[kk] = v
        out.append((msg_id, f2))
    return out


def _escape_label_value(v: str) -> str:
    return v.replace("\\", "\\\\").replace("\n", "\\n").replace('"', '\\"')


def _render_prom(lines: List[str]) -> str:
    # Ensure trailing newline
    return "\n".join(lines) + "\n"


def main(argv: Optional[List[str]] = None) -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--redis-url", default=os.getenv("REDIS_URL", "redis://localhost:6379/0"))
    p.add_argument("--stream", default=os.getenv("TICK_GATE_REDIS_STREAM", "ops:tick_quality_gate"))
    p.add_argument("--hours", type=float, default=float(os.getenv("TICK_GATE_EXPORT_HOURS", "24")))
    p.add_argument("--limit", type=int, default=int(os.getenv("TICK_GATE_EXPORT_LIMIT", "20000")))
    p.add_argument("--out", default=os.getenv("TICK_GATE_TEXTFILE_OUT", "/var/lib/node_exporter/textfile_collector/tick_gate.prom"))
    args = p.parse_args(argv)

    start_ms = _now_ms() - int(args.hours * 3600 * 1000)
    try:
        items = _read_stream_events(args.redis_url, args.stream, start_ms, args.limit)
    except Exception as e:
        sys.stderr.write(f"ERROR: {e}\n")
        return 2

    pass_n = fail_n = ins_n = err_n = 0
    fail_metric: Dict[str, int] = {}
    for msg_id, fields in items:
        status, failures, rc = _extract_status_and_failures(msg_id, fields)
        if status == "PASS":
            pass_n += 1
        elif status == "FAIL":
            fail_n += 1
            if failures:
                for m in failures:
                    mm = m[:96] if m else "unknown"
                    fail_metric[mm] = fail_metric.get(mm, 0) + 1
            else:
                fail_metric["unknown"] = fail_metric.get("unknown", 0) + 1
        elif status == "INSUFFICIENT_DATA":
            ins_n += 1
        elif status == "ERROR":
            err_n += 1
        else:
            # unknown statuses bucket into error
            err_n += 1

    window_h = f"{args.hours:g}"
    lines: List[str] = []
    lines.append("# HELP tick_gate_pass_total Gate PASS count for window")
    lines.append("# TYPE tick_gate_pass_total gauge")
    lines.append(f'tick_gate_pass_total{{window_h="{window_h}"}} {pass_n}')
    lines.append("# HELP tick_gate_fail_total Gate FAIL count for window")
    lines.append("# TYPE tick_gate_fail_total gauge")
    lines.append(f'tick_gate_fail_total{{window_h="{window_h}"}} {fail_n}')
    lines.append("# HELP tick_gate_insufficient_total Gate INSUFFICIENT_DATA count for window")
    lines.append("# TYPE tick_gate_insufficient_total gauge")
    lines.append(f'tick_gate_insufficient_total{{window_h="{window_h}"}} {ins_n}')
    lines.append("# HELP tick_gate_error_total Gate ERROR count for window")
    lines.append("# TYPE tick_gate_error_total gauge")
    lines.append(f'tick_gate_error_total{{window_h="{window_h}"}} {err_n}')

    lines.append("# HELP tick_gate_fail_metric_total Gate FAIL count by failing metric/reason for window")
    lines.append("# TYPE tick_gate_fail_metric_total gauge")
    for mk, cnt in sorted(fail_metric.items(), key=lambda kv: (-kv[1], kv[0]))[:50]:
        mke = _escape_label_value(mk)
        lines.append(f'tick_gate_fail_metric_total{{metric="{mke}",window_h="{window_h}"}} {cnt}')

    prom = _render_prom(lines)
    # Write atomically
    out_path = args.out
    tmp_path = out_path + ".tmp"
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(tmp_path, "w", encoding="utf-8") as f:
        f.write(prom)
    os.replace(tmp_path, out_path)
    sys.stdout.write(out_path + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
