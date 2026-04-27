"""Comprehensive regression diff analyzer.

Analyzes diff.json from regression tests to:
1. Show detailed mismatch_by_type_top transitions
2. Check code/config changes since baseline
3. Analyze scenario/reason mismatches (gate logic issues)
4. Suggest baseline update if only score mismatches are small

Usage:
  python -m tools.analyze_regress_diff --diff /path/to/diff.json [--baseline /path/to/baseline.ndjson] [--candidate /path/to/candidate.ndjson]
  python -m tools.analyze_regress_diff --find-latest  # finds latest diff.json in OUT_DIR
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from collections import Counter
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


def find_latest_diff(out_dir: str = "/var/lib/trade/of_reports/out") -> Optional[Tuple[str, Optional[str], Optional[str]]]:
    """Find latest diff.json in regress directories.
    
    Returns:
        (diff_path, baseline_path, candidate_path) or None
    """
    base = Path(out_dir)
    if not base.exists():
        return None
    
    diffs = []
    for regress_dir in sorted(base.glob("regress_*"), reverse=True):
        diff_file = regress_dir / "diff.json"
        if diff_file.exists():
            # Try to find baseline and candidate in same directory or parent
            baseline_path = None
            candidate_path = None
            
            # Check for baseline in common locations
            for baseline_file in [regress_dir.parent / "baseline.ndjson", 
                                  Path("/var/lib/trade/of_reports/baselines/baseline.ndjson"),
                                  Path("python-worker/of_reports_baselines/baseline.ndjson")]:
                if baseline_file.exists():
                    baseline_path = str(baseline_file)
                    break
            
            # Candidate is usually in same directory
            candidate_file = regress_dir / "candidate.ndjson"
            if candidate_file.exists():
                candidate_path = str(candidate_file)
            
            diffs.append((diff_file.stat().st_mtime, str(diff_file), baseline_path, candidate_path))
    
    if not diffs:
        # also check regress_safe_* directories
        for regress_dir in sorted(base.glob("regress_safe_*"), reverse=True):
            diff_file = regress_dir / "diff.json"
            if diff_file.exists():
                baseline_path = None
                candidate_path = None
                
                for baseline_file in [regress_dir.parent / "baseline.ndjson",
                                      Path("/var/lib/trade/of_reports/baselines/baseline.ndjson"),
                                      Path("python-worker/of_reports_baselines/baseline.ndjson")]:
                    if baseline_file.exists():
                        baseline_path = str(baseline_file)
                        break
                
                candidate_file = regress_dir / "candidate.ndjson"
                if candidate_file.exists():
                    candidate_path = str(candidate_file)
                
                diffs.append((diff_file.stat().st_mtime, str(diff_file), baseline_path, candidate_path))
    
    if not diffs:
        return None
    
    diffs.sort(reverse=True)
    return (diffs[0][1], diffs[0][2], diffs[0][3])


def load_diff(path: str) -> Dict[str, Any]:
    """Load diff.json report."""
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def analyze_mismatch_types(diff: Dict[str, Any]) -> Dict[str, Any]:
    """Analyze mismatch_by_type_top to show value transitions."""
    types = diff.get("mismatch_by_type_top", [])
    if not types:
        return {"summary": "No mismatch_by_type_top data", "transitions": []}
    
    # Group by field
    by_field: Dict[str, List[Tuple[str, int]]] = {}
    for trans, count in types:
        if ":" in trans:
            field, change = trans.split(":", 1)
            if field not in by_field:
                by_field[field] = []
            by_field[field].append((change, count))
    
    # Analyze score transitions (most critical)
    score_transitions = []
    for trans, count in types:
        if trans.startswith("score:"):
            change = trans[6:]  # remove "score:"
            score_transitions.append((change, count))
    
    # Calculate score delta statistics if possible
    score_deltas = []
    for change, count in score_transitions:
        if "->" in change:
            try:
                old_val, new_val = change.split("->", 1)
                old_f = float(old_val) if old_val and old_val != "None" else 0.0
                new_f = float(new_val) if new_val and new_val != "None" else 0.0
                delta = new_f - old_f
                score_deltas.append((delta, count))
            except Exception:
                pass
    
    return {
        "summary": f"Found {len(types)} transition types",
        "by_field": {k: sorted(v, key=lambda x: -x[1]) for k, v in by_field.items()},
        "score_transitions": sorted(score_transitions, key=lambda x: -x[1])[:20],
        "score_delta_stats": _calc_delta_stats(score_deltas) if score_deltas else None,
    }


def _calc_delta_stats(deltas: List[Tuple[float, int]]) -> Dict[str, float]:
    """Calculate statistics on score deltas."""
    if not deltas:
        return {}
    
    # Weight by count
    weighted_deltas = []
    total_count = sum(count for _, count in deltas)
    for delta, count in deltas:
        weighted_deltas.extend([delta] * count)
    
    if not weighted_deltas:
        return {}
    
    sorted_deltas = sorted(weighted_deltas)
    n = len(sorted_deltas)
    
    return {
        "min": min(sorted_deltas),
        "max": max(sorted_deltas),
        "mean": sum(sorted_deltas) / n,
        "median": sorted_deltas[n // 2] if n > 0 else 0.0,
        "p95": sorted_deltas[int(n * 0.95)] if n > 1 else sorted_deltas[0],
        "p99": sorted_deltas[int(n * 0.99)] if n > 1 else sorted_deltas[-1],
        "abs_mean": sum(abs(d) for d in sorted_deltas) / n,
        "abs_max": max(abs(d) for d in sorted_deltas),
    }


def check_code_changes(since_days: int = 7, paths: Optional[List[str]] = None) -> Dict[str, Any]:
    """Check git changes in recent commits for relevant files."""
    try:
        # Find project root (go up from tools/ to project root)
        script_dir = Path(__file__).parent
        project_root = script_dir.parent.parent
        
        # Check last N commits for relevant files
        relevant_paths = paths or [
            "python-worker/services/ml_confirm_gate.py",
            "python-worker/services/of_confirm_service.py",
            "python-worker/core/of_confirm_engine.py",
            "python-worker/tools/of_engine_replay_from_inputs.py",
            "python-worker/tools/of_confirm_replay_from_inputs.py",
            "python-worker/services/of_confirm_service.py",
        ]
        
        # Get commits in last N days
        cmd = ["git", "log", "--oneline", f"--since={since_days} days ago", "--", *relevant_paths]
        result = subprocess.run(cmd, capture_output=True, text=True, cwd=str(project_root))
        
        if result.returncode == 0:
            commits = [line for line in result.stdout.strip().split("\n") if line.strip()]
            
            # Also get file stats
            file_changes = {}
            for path in relevant_paths:
                full_path = project_root / path
                if full_path.exists():
                    cmd_stat = ["git", "log", "--oneline", f"--since={since_days} days ago", "--", str(path)]
                    stat_result = subprocess.run(cmd_stat, capture_output=True, text=True, cwd=str(project_root))
                    if stat_result.returncode == 0:
                        file_commits = [line for line in stat_result.stdout.strip().split("\n") if line.strip()]
                        if file_commits:
                            file_changes[path] = len(file_commits)
            
            return {
                "found": True,
                "commits": commits[:20],  # top 20
                "count": len(commits),
                "file_changes": file_changes,
                "since_days": since_days,
            }
        else:
            # Try without date filter
            cmd = ["git", "log", "--oneline", "-30", "--", *relevant_paths]
            result = subprocess.run(cmd, capture_output=True, text=True, cwd=str(project_root))
            if result.returncode == 0:
                commits = [line for line in result.stdout.strip().split("\n") if line.strip()]
                return {
                    "found": True,
                    "commits": commits[:20],
                    "count": len(commits),
                    "since_days": None,
                }
    except Exception as e:
        return {"found": False, "error": f"Could not check git history: {e}"}
    
    return {"found": False, "error": "Could not check git history"}


def analyze_scenario_reason_mismatches(diff: Dict[str, Any], baseline_path: Optional[str] = None, candidate_path: Optional[str] = None) -> Dict[str, Any]:
    """Analyze scenario and reason mismatches to detect gate logic issues."""
    scenario_top = diff.get("mismatch_by_scenario_v4_top", [])
    reason_top = diff.get("mismatch_by_reason_top", [])
    
    by_field = diff.get("mismatch_by_field", {})
    scenario_mismatches = by_field.get("scenario", 0) + by_field.get("scenario_v4", 0)
    reason_mismatches = by_field.get("reason", 0)
    
    analysis = {
        "scenario_mismatches": scenario_mismatches,
        "reason_mismatches": reason_mismatches,
        "has_logic_issues": scenario_mismatches > 0 or reason_mismatches > 0,
        "scenario_top": scenario_top[:10],
        "reason_top": reason_top[:10],
    }
    
    # If we have file paths, try to extract sample mismatches
    if baseline_path and candidate_path:
        try:
            # First, get scenario/reason mismatches (priority)
            scenario_samples = _extract_mismatch_samples(
                baseline_path, candidate_path, 
                max_samples=15, 
                focus_fields=["scenario", "scenario_v4", "reason"]
            ) if (scenario_mismatches > 0 or reason_mismatches > 0) else []
            
            # Also get other interesting mismatches (ok, need)
            other_samples = _extract_mismatch_samples(
                baseline_path, candidate_path,
                max_samples=10,
                focus_fields=["ok", "need"]
            ) if by_field.get("ok", 0) > 0 or by_field.get("need", 0) > 0 else []
            
            # Combine, prioritizing scenario/reason
            all_samples = scenario_samples + other_samples
            # Remove duplicates by key
            seen_keys = set()
            unique_samples = []
            for s in all_samples:
                if s["key"] not in seen_keys:
                    seen_keys.add(s["key"])
                    unique_samples.append(s)
            
            analysis["samples"] = unique_samples[:20]
        except Exception as e:
            analysis["sample_error"] = str(e)
    
    return analysis


def _extract_mismatch_samples(baseline_path: str, candidate_path: str, max_samples: int = 20, 
                               focus_fields: Optional[List[str]] = None) -> List[Dict[str, Any]]:
    """Extract sample rows with mismatches, focusing on scenario/reason or all fields.
    
    Args:
        baseline_path: Path to baseline NDJSON
        candidate_path: Path to candidate NDJSON
        max_samples: Maximum number of samples to return
        focus_fields: If provided, only show samples with mismatches in these fields
    """
    def _get(r: Dict[str, Any], key: str) -> Any:
        if key in r:
            return r.get(key)
        ev = r.get("evidence")
        if isinstance(ev, dict) and key in ev:
            return ev.get(key)
        return None
    
    def row_key(r: Dict[str, Any]) -> str:
        sid = r.get("sid")
        if sid:
            return str(sid)
        return f"{r.get('symbol','')}|{r.get('ts_ms',0)}|{r.get('direction','')}"
    
    # Fields to compare
    FIELDS = ["ok", "score", "have", "need", "scenario", "reason", "scenario_v4", "need_reason"]
    
    # Load baseline
    base = {}
    with open(baseline_path, "r", encoding="utf-8") as f:
        for line in f:
            s = line.strip()
            if not s:
                continue
            r = json.loads(s)
            base[row_key(r)] = r
    
    samples = []
    with open(candidate_path, "r", encoding="utf-8") as f:
        for line in f:
            if len(samples) >= max_samples:
                break
            s = line.strip()
            if not s:
                continue
            r = json.loads(s)
            k = row_key(r)
            b = base.get(k)
            if not b:
                continue
            
            # Find all mismatches
            diffs = {}
            for f in FIELDS:
                base_val = _get(b, f)
                cand_val = _get(r, f)
                
                # For score, use epsilon comparison
                if f == "score":
                    try:
                        if base_val is not None and cand_val is not None:
                            if abs(float(base_val) - float(cand_val)) < 1e-9:
                                continue
                    except Exception:
                        pass
                
                if base_val != cand_val:
                    diffs[f] = {"baseline": base_val, "candidate": cand_val}
            
            # If focus_fields specified, only include if mismatch in those fields
            if focus_fields:
                if not any(f in diffs for f in focus_fields):
                    continue
            
            # Only include if there are mismatches
            if not diffs:
                continue
            
            # Get scenario/reason for context
            base_scn = str(_get(b, "scenario_v4") or _get(b, "scenario") or "")
            cand_scn = str(_get(r, "scenario_v4") or _get(r, "scenario") or "")
            base_reason = str(_get(b, "reason") or "")
            cand_reason = str(_get(r, "reason") or "")
            
            # Priority: scenario/reason mismatches first
            priority = 0
            if "scenario" in diffs or "scenario_v4" in diffs:
                priority += 10
            if "reason" in diffs:
                priority += 10
            if "ok" in diffs:
                priority += 5
            if "need" in diffs:
                priority += 3
            
            samples.append({
                "priority": priority,
                "key": k,
                "symbol": str(r.get("symbol", "")),
                "ts_ms": int(r.get("ts_ms", 0) or r.get("tick_ts_ms", 0)),
                "direction": str(r.get("direction", "")),
                "diffs": diffs,
                "baseline": {
                    "scenario": base_scn,
                    "scenario_v4": str(_get(b, "scenario_v4") or ""),
                    "reason": base_reason,
                    "score": _get(b, "score"),
                    "have": _get(b, "have"),
                    "need": _get(b, "need"),
                    "ok": _get(b, "ok"),
                    "exec_risk_norm": _get(b, "exec_risk_norm"),
                },
                "candidate": {
                    "scenario": cand_scn,
                    "scenario_v4": str(_get(r, "scenario_v4") or ""),
                    "reason": cand_reason,
                    "score": _get(r, "score"),
                    "have": _get(r, "have"),
                    "need": _get(r, "need"),
                    "ok": _get(r, "ok"),
                    "exec_risk_norm": _get(r, "exec_risk_norm"),
                },
            })
    
    # Sort by priority (highest first), then return top N
    samples.sort(key=lambda x: -x["priority"])
    return samples[:max_samples]


def suggest_baseline_update(diff: Dict[str, Any], type_analysis: Dict[str, Any]) -> Dict[str, Any]:
    """Suggest if baseline should be updated based on mismatch analysis."""
    by_field = diff.get("mismatch_by_field", {})
    total_mismatches = diff.get("mismatches", 0)
    overlap_n = diff.get("n", 0)
    
    score_mismatches = by_field.get("score", 0)
    non_score_mismatches = total_mismatches - score_mismatches
    
    # Check if only score mismatches
    only_score = (non_score_mismatches == 0) and (score_mismatches > 0)
    
    # Check if score deltas are small
    score_delta_stats = type_analysis.get("score_delta_stats")
    small_deltas = False
    if score_delta_stats:
        abs_mean = score_delta_stats.get("abs_mean", 1e9)
        abs_max = score_delta_stats.get("abs_max", 1e9)
        # Consider small if mean < 0.01 and max < 0.1
        small_deltas = abs_mean < 0.01 and abs_max < 0.1
    
    # Calculate mismatch rate
    mismatch_rate = (total_mismatches / max(1, overlap_n)) * 100
    
    suggestion = {
        "should_update": False,
        "reason": "",
        "confidence": "low",
        "stats": {
            "total_mismatches": total_mismatches,
            "score_mismatches": score_mismatches,
            "non_score_mismatches": non_score_mismatches,
            "mismatch_rate_pct": mismatch_rate,
            "only_score": only_score,
            "small_score_deltas": small_deltas,
        },
    }
    
    if only_score and small_deltas and mismatch_rate < 5.0:
        suggestion["should_update"] = True
        suggestion["confidence"] = "high"
        suggestion["reason"] = f"Only score mismatches ({score_mismatches}), small deltas (mean={score_delta_stats.get('abs_mean', 0):.6f}, max={score_delta_stats.get('abs_max', 0):.6f}), low rate ({mismatch_rate:.2f}%)"
    elif only_score and mismatch_rate < 2.0:
        suggestion["should_update"] = True
        suggestion["confidence"] = "medium"
        suggestion["reason"] = f"Only score mismatches ({score_mismatches}), low rate ({mismatch_rate:.2f}%)"
    elif non_score_mismatches > 0:
        suggestion["should_update"] = False
        suggestion["confidence"] = "high"
        suggestion["reason"] = f"Non-score mismatches detected ({non_score_mismatches}): scenario={by_field.get('scenario', 0)}, reason={by_field.get('reason', 0)}, need={by_field.get('need', 0)}"
    else:
        suggestion["should_update"] = False
        suggestion["confidence"] = "medium"
        suggestion["reason"] = f"Score mismatches may be significant or rate too high ({mismatch_rate:.2f}%)"
    
    return suggestion


def print_analysis(diff: Dict[str, Any], type_analysis: Dict[str, Any], code_changes: Dict[str, Any], 
                   scenario_analysis: Dict[str, Any], baseline_suggestion: Dict[str, Any]) -> None:
    """Print comprehensive analysis report."""
    print("=" * 80)
    print("REGRESSION DIFF ANALYSIS")
    print("=" * 80)
    print()
    
    # Summary
    overlap_n = diff.get("n", 0)
    mismatches = diff.get("mismatches", 0)
    by_field = diff.get("mismatch_by_field", {})
    print(f"Summary:")
    print(f"  Overlap: {overlap_n:,} rows")
    print(f"  Total mismatches: {mismatches:,}")
    print(f"  Mismatch rate: {(mismatches / max(1, overlap_n) * 100):.2f}%")
    print()
    
    print(f"Mismatches by field:")
    for field, count in sorted(by_field.items(), key=lambda x: -x[1]):
        print(f"  {field}: {count:,}")
    print()
    
    # Mismatch type transitions
    print("=" * 80)
    print("MISMATCH TYPE TRANSITIONS (mismatch_by_type_top)")
    print("=" * 80)
    print()
    
    if type_analysis.get("score_delta_stats"):
        stats = type_analysis["score_delta_stats"]
        print("Score delta statistics:")
        print(f"  Mean absolute delta: {stats['abs_mean']:.8f}")
        print(f"  Max absolute delta: {stats['abs_max']:.8f}")
        print(f"  Mean delta: {stats['mean']:.8f}")
        print(f"  Median delta: {stats['median']:.8f}")
        print(f"  P95 delta: {stats['p95']:.8f}")
        print(f"  Min: {stats['min']:.8f}, Max: {stats['max']:.8f}")
        print()
    
    print("Top score transitions:")
    for trans, count in type_analysis.get("score_transitions", [])[:15]:
        print(f"  {trans}: {count:,}")
    print()
    
    by_field_types = type_analysis.get("by_field", {})
    for field in ["scenario", "scenario_v4", "reason", "need", "ok"]:
        if field in by_field_types:
            print(f"Top {field} transitions:")
            for trans, count in by_field_types[field][:10]:
                print(f"  {trans}: {count:,}")
            print()
    
    # Code changes
    print("=" * 80)
    print("CODE CHANGES CHECK")
    print("=" * 80)
    print()
    if code_changes.get("found"):
        since_info = f" (last {code_changes.get('since_days', 'N')} days)" if code_changes.get('since_days') else " (last 30 commits)"
        print(f"Found {code_changes['count']} relevant commits{since_info}:")
        for commit in code_changes.get("commits", [])[:10]:
            print(f"  {commit}")
        if code_changes['count'] > 10:
            print(f"  ... and {code_changes['count'] - 10} more")
        
        if code_changes.get("file_changes"):
            print()
            print("Changes by file:")
            for file_path, count in sorted(code_changes["file_changes"].items(), key=lambda x: -x[1]):
                print(f"  {file_path}: {count} commits")
    else:
        print(f"  {code_changes.get('error', 'Could not check git history')}")
    print()
    
    # Scenario/Reason analysis
    print("=" * 80)
    print("SCENARIO/REASON MISMATCH ANALYSIS (Gate Logic)")
    print("=" * 80)
    print()
    if scenario_analysis.get("has_logic_issues"):
        print("⚠️  WARNING: Gate logic issues detected!")
        print(f"  Scenario mismatches: {scenario_analysis['scenario_mismatches']}")
        print(f"  Reason mismatches: {scenario_analysis['reason_mismatches']}")
        print()
        
        if scenario_analysis.get("scenario_top"):
            print("Top scenarios with mismatches:")
            for scn, count in scenario_analysis["scenario_top"]:
                print(f"  {scn}: {count:,}")
            print()
        
        if scenario_analysis.get("reason_top"):
            print("Top reason transitions:")
            for trans, count in scenario_analysis["reason_top"][:10]:
                print(f"  {trans}: {count:,}")
            print()
        
        if scenario_analysis.get("samples"):
            print("Sample mismatches (showing up to 10 most critical):")
            for i, sample in enumerate(scenario_analysis["samples"][:10], 1):
                print(f"  Sample {i} (priority={sample.get('priority', 0)}):")
                print(f"    Key: {sample['key']}")
                print(f"    Symbol: {sample.get('symbol', 'N/A')}, TS: {sample.get('ts_ms', 'N/A')}, Direction: {sample.get('direction', 'N/A')}")
                print(f"    Changed fields: {', '.join(sample['diffs'].keys())}")
                print(f"    Baseline:")
                print(f"      scenario={sample['baseline']['scenario']}, scenario_v4={sample['baseline']['scenario_v4']}")
                print(f"      reason={sample['baseline']['reason']}")
                print(f"      ok={sample['baseline']['ok']}, score={sample['baseline']['score']}, have={sample['baseline']['have']}, need={sample['baseline']['need']}")
                if sample['baseline'].get('exec_risk_norm') is not None:
                    print(f"      exec_risk_norm={sample['baseline']['exec_risk_norm']}")
                print(f"    Candidate:")
                print(f"      scenario={sample['candidate']['scenario']}, scenario_v4={sample['candidate']['scenario_v4']}")
                print(f"      reason={sample['candidate']['reason']}")
                print(f"      ok={sample['candidate']['ok']}, score={sample['candidate']['score']}, have={sample['candidate']['have']}, need={sample['candidate']['need']}")
                if sample['candidate'].get('exec_risk_norm') is not None:
                    print(f"      exec_risk_norm={sample['candidate']['exec_risk_norm']}")
                
                # Show detailed field changes
                if sample['diffs']:
                    print(f"    Field changes:")
                    for field, change in sample['diffs'].items():
                        print(f"      {field}: {change['baseline']} -> {change['candidate']}")
                print()
            
            if len(scenario_analysis["samples"]) > 10:
                print(f"  ... and {len(scenario_analysis['samples']) - 10} more samples (total: {len(scenario_analysis['samples'])})")
            print()
    else:
        print("✓ No scenario/reason mismatches detected (gate logic appears stable)")
    print()
    
    # Baseline update suggestion
    print("=" * 80)
    print("BASELINE UPDATE SUGGESTION")
    print("=" * 80)
    print()
    stats = baseline_suggestion["stats"]
    print(f"Analysis:")
    print(f"  Only score mismatches: {stats['only_score']}")
    print(f"  Small score deltas: {stats['small_score_deltas']}")
    print(f"  Mismatch rate: {stats['mismatch_rate_pct']:.2f}%")
    print()
    
    if baseline_suggestion["should_update"]:
        print(f"✓ SUGGESTION: Update baseline (confidence: {baseline_suggestion['confidence']})")
        print(f"  Reason: {baseline_suggestion['reason']}")
        print()
        print("  To update baseline:")
        print("    python -m tools.propose_baseline_update")
        print("    # or manually copy candidate to baseline")
    else:
        print(f"✗ SUGGESTION: Do NOT update baseline (confidence: {baseline_suggestion['confidence']})")
        print(f"  Reason: {baseline_suggestion['reason']}")
        print()
        print("  Action required: Investigate root cause of mismatches")
    print()


def main() -> None:
    ap = argparse.ArgumentParser(description="Analyze regression diff.json report")
    ap.add_argument("--diff", help="Path to diff.json report")
    ap.add_argument("--find-latest", action="store_true", help="Find latest diff.json in OUT_DIR")
    ap.add_argument("--baseline", help="Path to baseline.ndjson (for sample extraction)")
    ap.add_argument("--candidate", help="Path to candidate.ndjson (for sample extraction)")
    ap.add_argument("--out-dir", default=os.getenv("OUT_DIR", "/var/lib/trade/of_reports/out"), help="Output directory for finding latest diff")
    ap.add_argument("--check-git", action="store_true", default=True, help="Check git history for code changes")
    args = ap.parse_args()
    
    # Find diff.json
    diff_path = args.diff
    baseline_path = args.baseline
    candidate_path = args.candidate
    
    if args.find_latest or not diff_path:
        found = find_latest_diff(args.out_dir)
        if not found:
            print(f"Error: Could not find diff.json in {args.out_dir}")
            print("  Try: python -m tools.analyze_regress_diff --diff /path/to/diff.json")
            raise SystemExit(1)
        diff_path, found_baseline, found_candidate = found
        if not baseline_path and found_baseline:
            baseline_path = found_baseline
        if not candidate_path and found_candidate:
            candidate_path = found_candidate
        print(f"Using latest diff.json: {diff_path}")
        if baseline_path:
            print(f"  Baseline: {baseline_path}")
        if candidate_path:
            print(f"  Candidate: {candidate_path}")
        print()
    
    if not diff_path or not os.path.exists(diff_path):
        print(f"Error: diff.json not found: {diff_path}")
        raise SystemExit(1)
    
    # Load diff
    diff = load_diff(diff_path)
    
    # Analyze mismatch types
    type_analysis = analyze_mismatch_types(diff)
    
    # Check code changes
    code_changes = {}
    if args.check_git:
        code_changes = check_code_changes(since_days=7)
    
    # Analyze scenario/reason mismatches
    scenario_analysis = analyze_scenario_reason_mismatches(
        diff,
        baseline_path=baseline_path,
        candidate_path=candidate_path,
    )
    
    # Suggest baseline update
    baseline_suggestion = suggest_baseline_update(diff, type_analysis)
    
    # Print analysis
    print_analysis(diff, type_analysis, code_changes, scenario_analysis, baseline_suggestion)


if __name__ == "__main__":
    main()

