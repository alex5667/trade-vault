#!/usr/bin/env python3
import glob
import json
import os
import re
import sys
from typing import Any

import yaml


REQUIRED_FOR = {"critical", "warning"}


def _as_list(x: Any) -> list[Any]:
    if x is None:
        return []
    if isinstance(x, list):
        return x
    return [x]


def _load_yaml(path: str) -> Any:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def _load_dashboard_uids() -> tuple[set[str], list[str]]:
    uids: set[str] = set()
    errors: list[str] = []
    for fp in sorted(glob.glob("monitoring/grafana/dashboards/*.json")):
        try:
            with open(fp, "r", encoding="utf-8") as f:
                doc = json.load(f)
            uid = str(doc.get("uid") or "").strip()
            if not uid:
                errors.append(f"[FAIL] dashboard missing uid: {fp}")
                continue
            uids.add(uid)
        except Exception as e:
            errors.append(f"[FAIL] dashboard json parse error: {fp}: {type(e).__name__}")
    return uids, errors


def _extract_uid(dashboard_path: str) -> str:
    p = (dashboard_path or "").strip()
    if not p:
        return ""
    # drop query
    p = p.split("?", 1)[0]
    # accept /d/<uid>/<slug> and /d-solo/<uid>/<slug>
    m = re.match(r"^/(d|d-solo)/([^/]+)/", p)
    if not m:
        return ""
    return m.group(2)


def _runbook_file_exists(runbook_path: str) -> bool:
    p = (runbook_path or "").strip()
    if not p:
        return False
    # runbook_path is a URL path like "/web_uptime.md"
    rel = p.lstrip("/")
    if not rel:
        return False
    fp = os.path.join("monitoring", "runbooks", rel)
    return os.path.isfile(fp)


def main() -> int:
    alerts_files = sorted(glob.glob("ok_rate_logic/prometheus_alerts_*.yml"))
    if not alerts_files:
        print("No ok_rate_logic/prometheus_alerts_*.yml files found", file=sys.stderr)
        return 2

    dash_uids, dash_errors = _load_dashboard_uids()
    for e in dash_errors:
        print(e)
    if dash_errors:
        return 1
    if not dash_uids:
        print("[FAIL] no dashboards found in monitoring/grafana/dashboards/*.json", file=sys.stderr)
        return 1

    bad = 0
    for fp in alerts_files:
        doc = _load_yaml(fp) or {}
        groups = _as_list(doc.get("groups"))
        for g in groups:
            rules = _as_list((g or {}).get("rules"))
            for r in rules:
                labels: dict[str, Any] = (r or {}).get("labels") or {}
                ann: dict[str, Any] = (r or {}).get("annotations") or {}
                alert = (r or {}).get("alert") or "<unknown>"
                sev = str(labels.get("severity") or "").strip().lower()
                if sev not in REQUIRED_FOR:
                    continue

                rb = str(ann.get("runbook_path") or "").strip()
                dp = str(ann.get("dashboard_path") or "").strip()

                if not rb or not rb.startswith("/"):
                    bad += 1
                    print(f"[FAIL] {fp}: alert={alert} severity={sev} invalid runbook_path={rb!r}")
                elif not _runbook_file_exists(rb):
                    bad += 1
                    print(f"[FAIL] {fp}: alert={alert} severity={sev} missing runbook file for {rb} -> monitoring/runbooks/{rb.lstrip('/')}")

                uid = _extract_uid(dp)
                if not dp or not dp.startswith("/"):
                    bad += 1
                    print(f"[FAIL] {fp}: alert={alert} severity={sev} invalid dashboard_path={dp!r}")
                elif not uid:
                    bad += 1
                    print(f"[FAIL] {fp}: alert={alert} severity={sev} dashboard_path does not match /d/<uid>/...: {dp!r}")
                elif uid not in dash_uids:
                    bad += 1
                    print(f"[FAIL] {fp}: alert={alert} severity={sev} dashboard uid not found: {uid!r} (from {dp})")

    if bad:
        print(f"FAILED: {bad} alerts have invalid runbook_path/dashboard_path references")
        return 1
    print("OK: all critical/warning alerts reference existing runbooks + dashboards")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
