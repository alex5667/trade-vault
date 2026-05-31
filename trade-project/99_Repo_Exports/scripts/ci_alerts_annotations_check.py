#!/usr/bin/env python3
import sys
from typing import Any
import yaml

REQUIRED_FOR = {"critical", "warning"}
REQ_ANNOT_KEYS = {"runbook_path", "dashboard_path"}

def _load_yaml(path: str) -> Any:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def _as_list(x: Any) -> list[Any]:
    if x is None:
        return []
    if isinstance(x, list):
        return x
    return [x]

def main() -> int:
    files = sys.argv[1:]
    if not files:
        print("No prometheus_alerts_*.yml files provided", file=sys.stderr)
        return 2

    bad = 0
    for fp in files:
        doc = _load_yaml(fp) or {}
        groups = _as_list(doc.get("groups"))
        for g in groups:
            rules = _as_list((g or {}).get("rules"))
            for r in rules:
                if "alert" not in (r or {}):
                    continue
                labels: dict[str, Any] = (r or {}).get("labels") or {}
                ann: dict[str, Any] = (r or {}).get("annotations") or {}
                alert = (r or {}).get("alert") or "<unknown>"
                sev = str(labels.get("severity") or "").strip().lower()
                if sev in REQUIRED_FOR:
                    missing = [k for k in sorted(REQ_ANNOT_KEYS) if not str(ann.get(k) or "").strip()]
                    if missing:
                        bad += 1
                        print(f"[FAIL] {fp}: alert={alert} severity={sev} missing annotations: {missing}")

    if bad:
        print(f"FAILED: {bad} alerts missing required annotations {sorted(REQ_ANNOT_KEYS)}")
        return 1
    print("OK: all critical/warning alerts have required annotations")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
