#!/usr/bin/env python3
import glob
import sys
from typing import Any

import yaml


def _as_list(x: Any) -> list[Any]:
    if x is None:
        return []
    if isinstance(x, list):
        return x
    return [x]


def _load_yaml(path: str) -> Any:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def main() -> int:
    # Import the production builder logic (source of truth).
    try:
        from ml_analysis.tools.build_smoke_contract_targets_from_alerts_v1 import (  # type: ignore
            REQUIRED_FOR,
            _normalize_dashboard_path,
            _normalize_runbook_path,
            build_targets_from_alerts,
        )
    except Exception as e:
        print(f"[FAIL] cannot import build_smoke_contract_targets_from_alerts_v1: {type(e).__name__}", file=sys.stderr)
        return 2

    alerts_files = sorted(glob.glob("ok_rate_logic/prometheus_alerts_*.yml"))
    if not alerts_files:
        print("[FAIL] no ok_rate_logic/prometheus_alerts_*.yml found", file=sys.stderr)
        return 2

    # Expected targets from alert annotations (normalized).
    exp_runbooks: set[str] = set()
    exp_dashboards: set[str] = set()

    for fp in alerts_files:
        doc = _load_yaml(fp) or {}
        groups = _as_list(doc.get("groups"))
        for g in groups:
            rules = _as_list((g or {}).get("rules"))
            for r in rules:
                labels: dict[str, Any] = (r or {}).get("labels") or {}
                ann: dict[str, Any] = (r or {}).get("annotations") or {}
                sev = str(labels.get("severity") or "").strip().lower()
                if sev not in set([str(x) for x in REQUIRED_FOR]):
                    continue
                rb = _normalize_runbook_path(str(ann.get("runbook_path") or "").strip())
                dp = _normalize_dashboard_path(str(ann.get("dashboard_path") or "").strip())
                if rb:
                    exp_runbooks.add(rb)
                if dp:
                    exp_dashboards.add(dp)

    runbooks, dashboards = build_targets_from_alerts()
    act_runbooks = set(runbooks)
    act_dashboards = set(dashboards)

    bad = 0
    if not act_runbooks:
        bad += 1
        print("[FAIL] build_targets_from_alerts returned empty runbooks set")
    if not act_dashboards:
        bad += 1
        print("[FAIL] build_targets_from_alerts returned empty dashboards set")

    # Sanity formatting rules (public-proxy paths)
    for rb in act_runbooks:
        if not rb.startswith("/runbooks/"):
            bad += 1
            print(f"[FAIL] runbook path not under /runbooks/: {rb!r}")
    for dp in act_dashboards:
        if not (dp.startswith("/grafana/d/") or dp.startswith("/grafana/d-solo/") or dp.startswith("/grafana/")):
            bad += 1
            print(f"[FAIL] dashboard path not under /grafana/: {dp!r}")

    # Equality check vs expected annotations normalization
    missing_rb = sorted(exp_runbooks - act_runbooks)
    extra_rb = sorted(act_runbooks - exp_runbooks)
    missing_dp = sorted(exp_dashboards - act_dashboards)
    extra_dp = sorted(act_dashboards - exp_dashboards)

    if missing_rb:
        bad += 1
        print(f"[FAIL] missing runbook targets from builder ({len(missing_rb)}): {missing_rb[:10]}")
    if missing_dp:
        bad += 1
        print(f"[FAIL] missing dashboard targets from builder ({len(missing_dp)}): {missing_dp[:10]}")
    if extra_rb:
        bad += 1
        print(f"[FAIL] extra runbook targets in builder ({len(extra_rb)}): {extra_rb[:10]}")
    if extra_dp:
        bad += 1
        print(f"[FAIL] extra dashboard targets in builder ({len(extra_dp)}): {extra_dp[:10]}")

    if bad:
        print(f"FAILED: smoke contract targets autogen check failed ({bad} issues)")
        return 1
    print(f"OK: smoke contract targets autogen matches alert annotations (runbooks={len(act_runbooks)} dashboards={len(act_dashboards)})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
