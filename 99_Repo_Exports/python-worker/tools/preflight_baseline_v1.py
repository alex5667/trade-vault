#!/usr/bin/env python3
"""preflight_baseline_v1.py — Step 0: capture system baseline.

Usage:
    python tools/preflight_baseline_v1.py \\
        --metrics-url http://localhost:8000/metrics \\
        --prom-url    http://localhost:9090 \\
        --compose     docker-compose.yml \\
        --out         /tmp/preflight_baseline.json

What it does (fail-open: network / parse errors produce partial output):
1. Fetch raw Prometheus text from --metrics-url (/metrics endpoint).
2. Fetch loaded alert rules from Prometheus /api/v1/rules.
3. Parse docker-compose.yml to find book-stream related env vars.
4. Write timestamped JSON snapshot to --out.

The output file is machine-readable and suitable for diffing across deployments.
"""

from __future__ import annotations
from utils.time_utils import get_ny_time_millis

import argparse
import json
import os
import sys
import time
import urllib.request
import urllib.error
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional


# ---------------------------------------------------------------------------
# --- Prometheus text parser (no dependencies) ---
# ---------------------------------------------------------------------------

def parse_prometheus_text(text: str) -> Dict[str, List[Dict[str, Any]]]:
    """Parse Prometheus /metrics text exposition format into a dict of metric families.

    Returns:
        {metric_name: [{"labels": {...}, "value": float, "help": str, "type": str}]}
    """
    families: Dict[str, List[Dict[str, Any]]] = {}
    help_map: Dict[str, str] = {}
    type_map: Dict[str, str] = {}

    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line:
            continue

        if line.startswith("# HELP "):
            rest = line[7:]
            parts = rest.split(" ", 1)
            name = parts[0].strip()
            help_map[name] = parts[1].strip() if len(parts) > 1 else ""
            continue

        if line.startswith("# TYPE "):
            rest = line[7:]
            parts = rest.split(" ", 1)
            name = parts[0].strip()
            type_map[name] = parts[1].strip() if len(parts) > 1 else ""
            continue

        if line.startswith("#"):
            continue

        # Sample line: metric_name{label="value",...} 1.23 [timestamp]
        try:
            # Split off trailing timestamp if present
            parts = line.rsplit(" ", 2)
            # Try to parse the last token as a timestamp (integer epoch ms)
            if len(parts) == 3:
                try:
                    int(parts[2])
                    line_no_ts = f"{parts[0]} {parts[1]}"
                except ValueError:
                    line_no_ts = line
            else:
                line_no_ts = line

            if "{" in line_no_ts:
                brace_open = line_no_ts.index("{")
                brace_close = line_no_ts.rindex("}")
                metric_name = line_no_ts[:brace_open]
                labels_str = line_no_ts[brace_open + 1:brace_close]
                value_str = line_no_ts[brace_close + 1:].strip()
            else:
                toks = line_no_ts.rsplit(" ", 1)
                if len(toks) != 2:
                    continue
                metric_name, value_str = toks[0].strip(), toks[1].strip()
                labels_str = ""

            try:
                value = float(value_str)
            except ValueError:
                continue  # NaN, +Inf, -Inf are valid — keep them
                if value_str in ("+Inf", "Inf"):
                    value = float("inf")
                elif value_str in ("-Inf",):
                    value = float("-inf")
                elif value_str == "NaN":
                    value = float("nan")
                else:
                    continue

            # Parse labels
            labels: Dict[str, str] = {}
            if labels_str:
                # Naive: split by comma that is not inside quotes
                cur_key = ""
                cur_val = ""
                in_val = False
                for ch in labels_str + ",":
                    if not in_val:
                        if ch == "=":
                            in_val = True
                            cur_val = ""
                        elif ch == ",":
                            pass
                        else:
                            cur_key += ch
                    else:
                        if ch == '"' and cur_val == "":
                            cur_val = ""  # open quote, start collecting
                        elif ch == "," and cur_val.endswith('"'):
                            labels[cur_key.strip()] = cur_val[:-1]
                            cur_key = ""
                            cur_val = ""
                            in_val = False
                        else:
                            cur_val += ch

            entry = {
                "labels": labels,
                "value": value,
                "help": help_map.get(metric_name, ""),
                "type": type_map.get(metric_name, ""),
            }
            families.setdefault(metric_name, []).append(entry)

        except Exception:
            continue

    return families


# ---------------------------------------------------------------------------
# --- Prometheus rules parser ---
# ---------------------------------------------------------------------------

def parse_prom_rules(rules_json: str) -> Dict[str, Any]:
    """Parse /api/v1/rules JSON response.

    Returns a simplified dict with:
        {
          "status": "success"|"error",
          "groups": [{"name": str, "file": str, "rules": [{"name": str, "type": str, ...}]}],
          "alert_count": int,
          "recording_count": int,
        }
    """
    try:
        raw = json.loads(rules_json)
    except Exception as exc:
        return {"status": "parse_error", "error": str(exc), "groups": []}

    status = raw.get("status", "unknown")
    data = raw.get("data", {}) or {}
    groups_raw = data.get("groups", []) or []

    groups: List[Dict[str, Any]] = []
    alert_count = 0
    recording_count = 0

    for g in groups_raw:
        rules_out: List[Dict[str, Any]] = []
        for r in (g.get("rules", []) or []):
            rtype = str(r.get("type", "") or "")
            rules_out.append({
                "name": str(r.get("name", "") or r.get("alert", "") or ""),
                "type": rtype,
                "query": str(r.get("query", "") or ""),
                "labels": r.get("labels", {}),
                "annotations": r.get("annotations", {}),
                "duration": r.get("duration", 0),
                "state": str(r.get("state", "") or ""),
            })
            if rtype == "alerting":
                alert_count += 1
            elif rtype == "recording":
                recording_count += 1

        groups.append({
            "name": str(g.get("name", "")),
            "file": str(g.get("file", "")),
            "interval": g.get("interval", 0),
            "rules": rules_out,
        })

    return {
        "status": status,
        "groups": groups,
        "alert_count": alert_count,
        "recording_count": recording_count,
    }


# ---------------------------------------------------------------------------
# --- Docker Compose parser ---
# ---------------------------------------------------------------------------

BOOK_STREAM_KEYS = (
    "BOOK_STREAM", "BINANCE_BOOK_STREAM", "STREAM_URL", "BINANCE_WS",
    "SYMBOLS", "BOOK_DEPTH", "BOOK_MISSING_SEQ_EMA_ALPHA",
    "DQ_BOOK_VETO_ENABLED", "DQ_BOOK_VETO_WARMUP_S",
    "DQ_GATE_ENABLE", "DQ_GATE_MODE", "DQ_MODE",
)


def parse_compose_book_env(compose_path: str) -> Dict[str, Any]:
    """Parse docker-compose.yml and return book-stream relevant env vars.

    Returns:
        {
          "services": {
             "service-name": {"env": {"KEY": "VALUE", ...}}
          },
          "error": str | None
        }
    """
    result: Dict[str, Any] = {"services": {}, "error": None}

    if not compose_path:
        result["error"] = "no compose path specified"
        return result

    try:
        try:
            import yaml  # type: ignore
            with open(compose_path, "r") as f:
                doc = yaml.safe_load(f)
        except ImportError:
            # Fallback: very naive key=value line-by-line search (no YAML dep required)
            doc = _parse_compose_naive(compose_path)
    except FileNotFoundError:
        result["error"] = f"file not found: {compose_path}"
        return result
    except Exception as exc:
        result["error"] = f"parse error: {exc}"
        return result

    if not isinstance(doc, dict):
        result["error"] = "unexpected compose structure"
        return result

    services = doc.get("services", {}) or {}
    for svc_name, svc in services.items():
        if not isinstance(svc, dict):
            continue
        env_section = svc.get("environment", {}) or {}
        env: Dict[str, str] = {}

        if isinstance(env_section, list):
            # YAML list form: ["KEY=VALUE", ...]
            for item in env_section:
                if "=" in str(item):
                    k, v = str(item).split("=", 1)
                    env[k.strip()] = v.strip()
        elif isinstance(env_section, dict):
            for k, v in env_section.items():
                env[str(k)] = str(v) if v is not None else ""

        # Filter to book-stream relevant keys
        filtered = {k: v for k, v in env.items() if any(bk in k.upper() for bk in BOOK_STREAM_KEYS)}
        if filtered:
            result["services"][str(svc_name)] = {"env": filtered}

    return result


def _parse_compose_naive(path: str) -> Dict[str, Any]:
    """Ultra-minimal YAML-subset parser (key-value environment lines only)."""
    doc: Dict[str, Any] = {"services": {}}
    current_service: Optional[str] = None
    in_environment = False

    with open(path, "r") as f:
        for line in f:
            stripped = line.rstrip()
            indent = len(stripped) - len(stripped.lstrip())
            content = stripped.strip()

            if indent == 2 and content and not content.startswith("-") and content.endswith(":"):
                current_service = content[:-1]
                in_environment = False
                doc["services"].setdefault(current_service, {"environment": {}})
            elif indent == 4 and content == "environment:":
                in_environment = True
            elif in_environment and indent >= 6 and "=" in content:
                if current_service:
                    k, v = content.lstrip("- ").split("=", 1)
                    doc["services"][current_service]["environment"][k.strip()] = v.strip()
            elif indent <= 4:
                in_environment = False

    return doc


# ---------------------------------------------------------------------------
# --- Network helpers ---
# ---------------------------------------------------------------------------

def fetch_url(url: str, timeout: int = 10) -> Optional[str]:
    """Fetch URL text. Returns None on any error."""
    try:
        req = urllib.request.Request(url, headers={"Accept": "text/plain,application/json"})
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.read().decode("utf-8", errors="replace")
    except Exception:
        return None


# ---------------------------------------------------------------------------
# --- Main ---
# ---------------------------------------------------------------------------

def run(
    metrics_url: str = "http://localhost:8000/metrics",
    prom_url: str = "http://localhost:9090",
    compose: str = "docker-compose.yml",
    out: str = "/tmp/preflight_baseline.json",
    timeout: int = 10,
) -> Dict[str, Any]:
    """Run the preflight snapshot. Always returns a snapshot dict (fail-open)."""
    ts_ms = get_ny_time_millis()
    ts_iso = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc).isoformat()

    snapshot: Dict[str, Any] = {
        "version": 1,
        "step": 0,
        "snapshot_ts_ms": ts_ms,
        "snapshot_ts_iso": ts_iso,
        "metrics_url": metrics_url,
        "prom_url": prom_url,
        "compose_path": compose,
    }

    # 1. Metrics endpoint
    metrics_text = fetch_url(metrics_url, timeout=timeout)
    if metrics_text is not None:
        try:
            parsed = parse_prometheus_text(metrics_text)
            snapshot["metrics_raw_len"] = len(metrics_text)
            snapshot["metrics_family_count"] = len(parsed)
            # Surface DQ-related metric families in a compact form
            dq_keys = [k for k in parsed if any(tag in k for tag in (
                "book_missing_seq", "tick_missing_seq", "tick_gap", "dq_level", "dq_veto",
            ))]
            snapshot["dq_metrics"] = {
                k: [{"labels": s["labels"], "value": s["value"]} for s in parsed[k]]
                for k in dq_keys
            }
            snapshot["metrics_ok"] = True
        except Exception as exc:
            snapshot["metrics_ok"] = False
            snapshot["metrics_error"] = str(exc)
    else:
        snapshot["metrics_ok"] = False
        snapshot["metrics_error"] = f"could not reach {metrics_url}"

    # 2. Prometheus rules
    rules_url = f"{prom_url.rstrip('/')}/api/v1/rules"
    rules_text = fetch_url(rules_url, timeout=timeout)
    if rules_text is not None:
        try:
            snapshot["prom_rules"] = parse_prom_rules(rules_text)
            snapshot["prom_rules_ok"] = True
        except Exception as exc:
            snapshot["prom_rules_ok"] = False
            snapshot["prom_rules_error"] = str(exc)
    else:
        snapshot["prom_rules_ok"] = False
        snapshot["prom_rules_error"] = f"could not reach {rules_url}"

    # 3. Docker Compose book-stream env
    try:
        snapshot["compose_book_env"] = parse_compose_book_env(compose)
        snapshot["compose_ok"] = True
    except Exception as exc:
        snapshot["compose_ok"] = False
        snapshot["compose_error"] = str(exc)

    # 4. Write output
    try:
        with open(out, "w") as f:
            json.dump(snapshot, f, indent=2, default=str)
        snapshot["out_written"] = True
        snapshot["out_path"] = out
    except Exception as exc:
        snapshot["out_written"] = False
        snapshot["out_error"] = str(exc)

    return snapshot


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--metrics-url",
        default=os.getenv("PREFLIGHT_METRICS_URL", "http://localhost:8000/metrics"),
        help="URL of the /metrics endpoint (default: http://localhost:8000/metrics)",
    )
    parser.add_argument(
        "--prom-url",
        default=os.getenv("PREFLIGHT_PROM_URL", "http://localhost:9090"),
        help="Prometheus base URL (default: http://localhost:9090)",
    )
    parser.add_argument(
        "--compose",
        default=os.getenv("PREFLIGHT_COMPOSE", "docker-compose.yml"),
        help="Path to docker-compose.yml (default: docker-compose.yml)",
    )
    parser.add_argument(
        "--out",
        default=os.getenv("PREFLIGHT_OUT", "/tmp/preflight_baseline.json"),
        help="Output file path (default: /tmp/preflight_baseline.json)",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=10,
        help="HTTP request timeout in seconds (default: 10)",
    )
    args = parser.parse_args()

    snap = run(
        metrics_url=args.metrics_url,
        prom_url=args.prom_url,
        compose=args.compose,
        out=args.out,
        timeout=args.timeout,
    )

    print(json.dumps({
        "ok": snap.get("metrics_ok") or snap.get("prom_rules_ok") or snap.get("compose_ok"),
        "snapshot_ts_iso": snap.get("snapshot_ts_iso"),
        "metrics_family_count": snap.get("metrics_family_count", 0),
        "dq_metric_families": list((snap.get("dq_metrics") or {}).keys()),
        "prom_alert_count": (snap.get("prom_rules") or {}).get("alert_count", 0),
        "compose_services": list((snap.get("compose_book_env") or {}).get("services", {}).keys()),
        "out_written": snap.get("out_written"),
        "out_path": snap.get("out_path"),
        "errors": {k: v for k, v in snap.items() if k.endswith("_error")},
    }, indent=2))

    sys.exit(0)


if __name__ == "__main__":
    main()
