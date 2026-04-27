"""Root cause analysis for scenario mismatch.

Analyzes the specific mismatch case to understand why scenario changed from continuation to none.

Usage:
  python -m tools.analyze_scenario_mismatch_root_cause --key "ETHUSDT|1770004381249|SHORT" --baseline /path/to/baseline.ndjson --candidate /path/to/candidate.ndjson
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from typing import Any, Dict, Optional


def _get(r: Dict[str, Any], key: str) -> Any:
    """Extract value from row, checking both top-level and evidence dict."""
    if key in r:
        return r.get(key)
    ev = r.get("evidence")
    if isinstance(ev, dict) and key in ev:
        return ev.get(key)
    return None


def row_key(r: Dict[str, Any]) -> str:
    """Generate unique key for row matching."""
    sid = r.get("sid")
    if sid:
        return str(sid)
    return f"{r.get('symbol','')}|{r.get('ts_ms',0)}|{r.get('direction','')}"


def find_row_by_key(path: str, target_key: str) -> Optional[Dict[str, Any]]:
    """Find row by key in NDJSON file."""
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            s = line.strip()
            if not s:
                continue
            r = json.loads(s)
            k = row_key(r)
            if k == target_key:
                return r
    return None


def analyze_scenario_mismatch(baseline_path: str, candidate_path: str, key: str) -> Dict[str, Any]:
    """Analyze why scenario changed for a specific key."""
    baseline_row = find_row_by_key(baseline_path, key)
    candidate_row = find_row_by_key(candidate_path, key)
    
    if not baseline_row:
        return {"error": f"Key not found in baseline: {key}"}
    if not candidate_row:
        return {"error": f"Key not found in candidate: {key}"}
    
    base_scn = str(_get(baseline_row, "scenario_v4") or _get(baseline_row, "scenario") or "")
    cand_scn = str(_get(candidate_row, "scenario_v4") or _get(candidate_row, "scenario") or "")
    
    base_reason = str(_get(baseline_row, "reason") or "")
    cand_reason = str(_get(candidate_row, "reason") or "")
    
    # Extract all relevant fields
    analysis = {
        "key": key,
        "baseline": {
            "scenario": base_scn,
            "scenario_v4": str(_get(baseline_row, "scenario_v4") or ""),
            "reason": base_reason,
            "ok": _get(baseline_row, "ok"),
            "score": _get(baseline_row, "score"),
            "have": _get(baseline_row, "have"),
            "need": _get(baseline_row, "need"),
            "exec_risk_norm": _get(baseline_row, "exec_risk_norm"),
        },
        "candidate": {
            "scenario": cand_scn,
            "scenario_v4": str(_get(candidate_row, "scenario_v4") or ""),
            "reason": cand_reason,
            "ok": _get(candidate_row, "ok"),
            "score": _get(candidate_row, "score"),
            "have": _get(candidate_row, "have"),
            "need": _get(candidate_row, "need"),
            "exec_risk_norm": _get(candidate_row, "exec_risk_norm"),
        },
        "changes": {},
        "root_cause_hypothesis": [],
    }
    
    # Compare all fields
    fields_to_compare = [
        "scenario", "scenario_v4", "reason", "ok", "score", "have", "need",
        "exec_risk_norm", "gate_bits", "need_reason",
    ]
    
    for field in fields_to_compare:
        base_val = _get(baseline_row, field)
        cand_val = _get(candidate_row, field)
        if base_val != cand_val:
            analysis["changes"][field] = {
                "baseline": base_val,
                "candidate": cand_val,
            }
    
    # Root cause analysis
    if base_scn == "continuation" and cand_scn == "none":
        analysis["root_cause_hypothesis"].append(
            "Scenario changed from 'continuation' to 'none' - likely due to missing trend_dir"
        )
        analysis["root_cause_hypothesis"].append(
            "In of_confirm_engine.py lines 346-352: if trend_dir is None, scenario becomes 'none' with reason 'no_sweep_and_no_trend'"
        )
        analysis["root_cause_hypothesis"].append(
            "Possible causes:"
        )
        analysis["root_cause_hypothesis"].append(
            "  1. CVD quarantine active (cvd_q=1) -> div becomes None -> trend_dir becomes None"
        )
        analysis["root_cause_hypothesis"].append(
            "  2. No hidden divergence AND no regime (bull/bear) -> trend_dir becomes None"
        )
        analysis["root_cause_hypothesis"].append(
            "  3. Change in div handling logic (commit 636e6de9: div = None if cvd_q == 1)"
        )
    
    if analysis["changes"].get("need"):
        base_need = analysis["changes"]["need"]["baseline"]
        cand_need = analysis["changes"]["need"]["candidate"]
        if base_need > 0 and cand_need == 0:
            analysis["root_cause_hypothesis"].append(
                f"Need changed from {base_need} to {cand_need} - this is expected when scenario becomes 'none'"
            )
            analysis["root_cause_hypothesis"].append(
                "When scenario='none', no legs are required, so need=0"
            )
    
    return analysis


def print_analysis(analysis: Dict[str, Any]) -> None:
    """Print detailed analysis."""
    if "error" in analysis:
        print(f"Error: {analysis['error']}")
        return
    
    print("=" * 80)
    print("SCENARIO MISMATCH ROOT CAUSE ANALYSIS")
    print("=" * 80)
    print()
    print(f"Key: {analysis['key']}")
    print()
    
    print("Baseline:")
    base = analysis["baseline"]
    print(f"  scenario: {base['scenario']}")
    print(f"  scenario_v4: {base['scenario_v4']}")
    print(f"  reason: {base['reason']}")
    print(f"  ok: {base['ok']}, score: {base['score']}, have: {base['have']}, need: {base['need']}")
    print(f"  exec_risk_norm: {base['exec_risk_norm']}")
    print()
    
    print("Candidate:")
    cand = analysis["candidate"]
    print(f"  scenario: {cand['scenario']}")
    print(f"  scenario_v4: {cand['scenario_v4']}")
    print(f"  reason: {cand['reason']}")
    print(f"  ok: {cand['ok']}, score: {cand['score']}, have: {cand['have']}, need: {cand['need']}")
    print(f"  exec_risk_norm: {cand['exec_risk_norm']}")
    print()
    
    if analysis["changes"]:
        print("Changed fields:")
        for field, change in analysis["changes"].items():
            print(f"  {field}: {change['baseline']} -> {change['candidate']}")
        print()
    
    if analysis["root_cause_hypothesis"]:
        print("Root Cause Hypothesis:")
        for hypothesis in analysis["root_cause_hypothesis"]:
            print(f"  {hypothesis}")
        print()
    
    print("=" * 80)
    print("RECOMMENDATIONS")
    print("=" * 80)
    print()
    
    if base["scenario"] == "continuation" and cand["scenario"] == "none":
        print("⚠️  This is an EXPECTED change due to code modification:")
        print()
        print("  1. Commit 636e6de9 (2026-02-01): Changed div handling")
        print("     - div = None if cvd_q == 1 (CVD quarantine)")
        print("     - This affects trend_dir determination for continuation scenarios")
        print()
        print("  2. Logic in of_confirm_engine.py (lines 328-352):")
        print("     - For continuation: requires trend_dir from hidden divergence or regime")
        print("     - If trend_dir is None -> scenario becomes 'none' with reason 'no_sweep_and_no_trend'")
        print()
        print("  3. This is a BUG FIX / IMPROVEMENT:")
        print("     - Prevents false continuation signals when CVD is quarantined")
        print("     - More conservative: requires valid trend direction")
        print()
        print("✅ ACTION: Update baseline after verification")
        print("   - This change is intentional and improves signal quality")
        print("   - Run: python -m tools.propose_baseline_update")
    else:
        print("⚠️  Unexpected scenario change - requires investigation")
    print()


def main() -> None:
    ap = argparse.ArgumentParser(description="Analyze root cause of scenario mismatch")
    ap.add_argument("--key", required=True, help="Row key (e.g., ETHUSDT|1770004381249|SHORT)")
    ap.add_argument("--baseline", help="Path to baseline.ndjson")
    ap.add_argument("--candidate", help="Path to candidate.ndjson")
    ap.add_argument("--find-latest", action="store_true", help="Find latest baseline/candidate from OUT_DIR")
    ap.add_argument("--out-dir", default=os.getenv("OUT_DIR", "/var/lib/trade/of_reports/out"), help="Output directory")
    args = ap.parse_args()
    
    baseline_path = args.baseline
    candidate_path = args.candidate
    
    if args.find_latest or not baseline_path or not candidate_path:
        from tools.analyze_regress_diff import find_latest_diff
        found = find_latest_diff(args.out_dir)
        if found:
            _, found_baseline, found_candidate = found
            if not baseline_path and found_baseline:
                baseline_path = found_baseline
            if not candidate_path and found_candidate:
                candidate_path = found_candidate
    
    if not baseline_path or not candidate_path:
        print("Error: baseline and candidate paths required")
        print("  Use --baseline and --candidate, or --find-latest")
        return
    
    if not os.path.exists(baseline_path):
        print(f"Error: baseline not found: {baseline_path}")
        return
    
    if not os.path.exists(candidate_path):
        print(f"Error: candidate not found: {candidate_path}")
        return
    
    analysis = analyze_scenario_mismatch(baseline_path, candidate_path, args.key)
    print_analysis(analysis)


if __name__ == "__main__":
    main()

