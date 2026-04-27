from __future__ import annotations

import argparse
import json
import os
import sys

def main():
    ap = argparse.ArgumentParser(description="Convert skew audit report to Meta Freeze Guard JSON.")
    ap.add_argument("--skew-json", required=True, help="Input machine-readable JSON report from audit tool")
    ap.add_argument("--out", required=True, help="Output meta_freeze.json path")
    ap.add_argument("--ab-warn-cap", type=float, default=0.05, help="AB share cap for WARN status (default 0.05)")
    ap.add_argument("--enf-warn-cap", type=float, default=0.25, help="Enforce share cap for WARN status (default 0.25)")
    
    args = ap.parse_args()

    if not os.path.exists(args.skew_json):
        print(f"ERROR: Skew report not found at {args.skew_json}")
        sys.exit(1)

    try:
        with open(args.skew_json, "r", encoding="utf-8") as f:
            report = json.load(f)
    except Exception as e:
        print(f"ERROR parsing skew report: {e}")
        sys.exit(1)

    is_critical = report.get("critical", False)
    bad_features = report.get("bad_features", [])

    freeze = 0
    ab_cap = 1.0
    enf_cap = 1.0
    comment = "ok"

    if is_critical:
        freeze = 1
        ab_cap = 0.0
        enf_cap = 0.0
        comment = f"critical_skew: {', '.join(bad_features)}"
    elif bad_features:
        # Significant skew but not critical (WARN)
        ab_cap = float(args.ab_warn_cap)
        enf_cap = float(args.enf_warn_cap)
        comment = f"warn_skew: {', '.join(bad_features)}"
    else:
        comment = "skew_audit_ok"

    guard_state = {
        "freeze": freeze,
        "ab_share_cap": ab_cap,
        "enforce_share_cap": enf_cap,
        "comment": comment,
        "ts": report.get("ts", 0),
        "source": "skew_audit_v7"
    }

    # Ensure atomic-ish write
    out_dir = os.path.dirname(os.path.abspath(args.out))
    if not os.path.exists(out_dir):
        os.makedirs(out_dir, exist_ok=True)
        
    temp_out = args.out + ".tmp"
    with open(temp_out, "w", encoding="utf-8") as f:
        json.dump(guard_state, f, indent=2)
    os.rename(temp_out, args.out)
    
    print(f"Written guard state to {args.out}: freeze={freeze}, ab_cap={ab_cap}, enf_cap={enf_cap}, comment={comment}")

if __name__ == "__main__":
    main()
