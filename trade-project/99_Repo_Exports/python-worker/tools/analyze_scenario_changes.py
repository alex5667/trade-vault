from __future__ import annotations

"""Analyze scenario determination logic changes in of_confirm_engine.py.

Compares recent commits to understand what changed in scenario determination logic.
Helps identify if scenario mismatches are expected or bugs.

Usage:
  python -m tools.analyze_scenario_changes --days 7 --file python-worker/core/of_confirm_engine.py
  python -m tools.analyze_scenario_changes --commit-range HEAD~20..HEAD --file python-worker/core/of_confirm_engine.py
"""


import argparse
import subprocess
import sys
from pathlib import Path
from typing import Any


def get_project_root() -> Path:
    """Find project root directory."""
    script_dir = Path(__file__).parent
    return script_dir.parent.parent


def get_commits(file_path: str, days: int | None = None, commit_range: str | None = None) -> list[dict[str, Any]]:
    """Get commits for a specific file."""
    project_root = get_project_root()
    full_path = project_root / file_path

    if not full_path.exists():
        return []

    cmd = ["git", "log", "--format=%H|%ai|%an|%s", "--"]
    if days:
        cmd.insert(2, f"--since={days} days ago")
    elif commit_range:
        cmd.insert(2, commit_range)
    else:
        cmd.insert(2, "-30")  # default last 30

    cmd.append(str(full_path))

    result = subprocess.run(cmd, capture_output=True, text=True, cwd=str(project_root))
    if result.returncode != 0:
        return []

    commits = []
    for line in result.stdout.strip().split("\n"):
        if not line.strip():
            continue
        parts = line.split("|", 3)
        if len(parts) >= 4:
            commits.append({
                "hash": parts[0],
                "date": parts[1],
                "author": parts[2],
                "message": parts[3],
            })

    return commits


def get_file_diff(commit_hash: str, file_path: str) -> str | None:
    """Get diff for a specific commit and file."""
    project_root = get_project_root()
    full_path = project_root / file_path

    cmd = ["git", "show", f"{commit_hash}:{file_path}"]
    result = subprocess.run(cmd, capture_output=True, text=True, cwd=str(project_root))
    if result.returncode != 0:
        return None

    return result.stdout


def get_commit_diff(commit_hash: str, file_path: str) -> str | None:
    """Get diff for a specific commit."""
    project_root = get_project_root()
    full_path = project_root / file_path

    cmd = ["git", "show", commit_hash, "--", str(full_path)]
    result = subprocess.run(cmd, capture_output=True, text=True, cwd=str(project_root))
    if result.returncode != 0:
        return None

    return result.stdout


def find_scenario_logic(file_content: str) -> dict[str, Any]:
    """Extract scenario determination logic from file."""
    lines = file_content.split("\n")

    scenario_keywords = [
        "scenario",
        "continuation",
        "reversal",
        "none",
        "scenario_v4",
        "_determine_scenario",
        "_get_scenario",
        "eval_continuation",
        "eval_reversal",
    ]

    relevant_lines = []
    for i, line in enumerate(lines, 1):
        line_lower = line.lower()
        if any(kw in line_lower for kw in scenario_keywords):
            # Get context (3 lines before and after)
            start = max(0, i - 4)
            end = min(len(lines), i + 3)
            context = "\n".join(f"{j:5d}| {lines[j-1]}" for j in range(start + 1, end + 1))
            relevant_lines.append({
                "line": i,
                "content": line,
                "context": context,
            })

    return {
        "total_lines": len(lines),
        "relevant_sections": relevant_lines,
    }


def analyze_scenario_changes(commits: list[dict[str, Any]], file_path: str) -> list[dict[str, Any]]:
    """Analyze commits for scenario-related changes."""
    project_root = get_project_root()
    full_path = project_root / file_path

    analysis = []

    for commit in commits:
        commit_hash = commit["hash"]

        # Get diff for this commit
        diff = get_commit_diff(commit_hash, file_path)
        if not diff:
            continue

        # Check if diff contains scenario-related changes
        diff_lower = diff.lower()
        scenario_keywords = [
            "scenario",
            "continuation",
            "reversal",
            "none",
            "scenario_v4",
            "determine_scenario",
            "get_scenario",
            "eval_continuation",
            "eval_reversal",
        ]

        has_scenario_changes = any(kw in diff_lower for kw in scenario_keywords)

        # Extract changed lines related to scenario
        changed_lines = []
        if has_scenario_changes:
            for line in diff.split("\n"):
                if line.startswith("+") or line.startswith("-"):
                    line_stripped = line[1:].strip()
                    if any(kw in line_stripped.lower() for kw in scenario_keywords):
                        changed_lines.append(line[:200])  # limit length

        analysis.append({
            "commit": commit,
            "has_scenario_changes": has_scenario_changes,
            "changed_lines": changed_lines[:20],  # top 20
            "diff_size": len(diff),
        })

    return analysis


def compare_scenario_logic(baseline_commit: str | None, current_commit: str, file_path: str) -> dict[str, Any]:
    """Compare scenario logic between baseline and current version."""
    project_root = get_project_root()
    full_path = project_root / file_path

    # Get current version
    current_content = get_file_diff(current_commit, file_path)
    if not current_content:
        return {"error": "Could not get current version"}

    current_logic = find_scenario_logic(current_content)

    # Get baseline version if specified
    baseline_logic = None
    if baseline_commit:
        baseline_content = get_file_diff(baseline_commit, file_path)
        if baseline_content:
            baseline_logic = find_scenario_logic(baseline_content)

    return {
        "current": current_logic,
        "baseline": baseline_logic,
        "has_baseline": baseline_logic is not None,
    }


def print_commits_analysis(commits: list[dict[str, Any]], analysis: list[dict[str, Any]]) -> None:
    """Print commits analysis."""
    print("=" * 80)
    print("COMMITS ANALYSIS")
    print("=" * 80)
    print()

    scenario_commits = [a for a in analysis if a["has_scenario_changes"]]

    print(f"Total commits: {len(commits)}")
    print(f"Commits with scenario changes: {len(scenario_commits)}")
    print()

    if scenario_commits:
        print("⚠️  SCENARIO-RELATED CHANGES DETECTED:")
        print()
        for i, item in enumerate(scenario_commits[:10], 1):
            commit = item["commit"]
            print(f"{i}. Commit: {commit['hash'][:8]} ({commit['date'][:10]})")
            print(f"   Author: {commit['author']}")
            print(f"   Message: {commit['message']}")
            print(f"   Changed lines ({len(item['changed_lines'])}):")
            for line in item["changed_lines"][:5]:
                print(f"     {line}")
            if len(item["changed_lines"]) > 5:
                print(f"     ... and {len(item['changed_lines']) - 5} more")
            print()
    else:
        print("✓ No direct scenario-related changes found in recent commits")
        print("  (Changes might be indirect - e.g., in dependencies or config)")
        print()

    # Show all commits for context
    print("All recent commits:")
    for i, commit in enumerate(commits[:15], 1):
        marker = "⚠️" if any(a["commit"]["hash"] == commit["hash"] and a["has_scenario_changes"] for a in analysis) else "  "
        print(f"{marker} {i}. {commit['hash'][:8]} ({commit['date'][:10]}) - {commit['message'][:60]}")
    print()


def print_scenario_logic_comparison(comparison: dict[str, Any]) -> None:
    """Print scenario logic comparison."""
    print("=" * 80)
    print("SCENARIO LOGIC COMPARISON")
    print("=" * 80)
    print()

    if "error" in comparison:
        print(f"Error: {comparison['error']}")
        return

    current = comparison.get("current", {})
    baseline = comparison.get("baseline")

    print("Current version:")
    print(f"  Total lines: {current.get('total_lines', 0)}")
    print(f"  Scenario-related sections: {len(current.get('relevant_sections', []))}")
    print()

    if baseline:
        print("Baseline version:")
        print(f"  Total lines: {baseline.get('total_lines', 0)}")
        print(f"  Scenario-related sections: {len(baseline.get('relevant_sections', []))}")
        print()

    # Show relevant sections
    if current.get("relevant_sections"):
        print("Current scenario-related code sections:")
        for i, section in enumerate(current["relevant_sections"][:10], 1):
            print(f"  Section {i} (line {section['line']}):")
            print(f"    {section['content'][:100]}")
            if len(section['content']) > 100:
                print("    ... (truncated)")
        print()

    if baseline and baseline.get("relevant_sections"):
        print("Baseline scenario-related code sections:")
        for i, section in enumerate(baseline["relevant_sections"][:10], 1):
            print(f"  Section {i} (line {section['line']}):")
            print(f"    {section['content'][:100]}")
            if len(section['content']) > 100:
                print("    ... (truncated)")
        print()


def find_baseline_commit() -> str | None:
    """Try to find baseline commit (e.g., from baseline file or git tags)."""
    project_root = get_project_root()

    # Try to find baseline tag
    cmd = ["git", "tag", "-l", "*baseline*", "*BL*"]
    result = subprocess.run(cmd, capture_output=True, text=True, cwd=str(project_root))
    if result.returncode == 0 and result.stdout.strip():
        tags = result.stdout.strip().split("\n")
        if tags:
            # Get commit for latest baseline tag
            cmd = ["git", "rev-parse", tags[0]]
            result = subprocess.run(cmd, capture_output=True, text=True, cwd=str(project_root))
            if result.returncode == 0:
                return result.stdout.strip()

    # Try to find commit that created baseline file
    baseline_file = project_root / "python-worker" / "of_reports_baselines" / "baseline.ndjson"
    if baseline_file.exists():
        # Get file creation/modification time and find nearby commit
        mtime = baseline_file.stat().st_mtime
        # This is approximate - just get commits around that time
        cmd = ["git", "log", "--format=%H|%ct", "-100", "--", str(baseline_file.relative_to(project_root))]
        result = subprocess.run(cmd, capture_output=True, text=True, cwd=str(project_root))
        if result.returncode == 0:
            # Find commit closest to file mtime
            closest_commit = None
            min_diff = float('inf')
            for line in result.stdout.strip().split("\n"):
                if "|" in line:
                    commit_hash, commit_time = line.split("|", 1)
                    try:
                        diff = abs(int(commit_time) - int(mtime))
                        if diff < min_diff:
                            min_diff = diff
                            closest_commit = commit_hash
                    except Exception:
                        pass
            if closest_commit:
                return closest_commit

    return None


def main() -> None:
    ap = argparse.ArgumentParser(description="Analyze scenario determination logic changes")
    ap.add_argument("--file", default="python-worker/core/of_confirm_engine.py", help="File to analyze")
    ap.add_argument("--days", type=int, default=7, help="Number of days to look back")
    ap.add_argument("--commit-range", help="Git commit range (e.g., HEAD~20..HEAD)")
    ap.add_argument("--baseline-commit", help="Baseline commit hash to compare against")
    ap.add_argument("--find-baseline", action="store_true", help="Try to find baseline commit automatically")
    args = ap.parse_args()

    # Get commits
    commits = get_commits(args.file, days=args.days if not args.commit_range else None, commit_range=args.commit_range)

    if not commits:
        print(f"No commits found for {args.file}")
        print("Try: python -m tools.analyze_scenario_changes --days 30")
        sys.exit(1)

    # Analyze commits
    analysis = analyze_scenario_changes(commits, args.file)

    # Print commits analysis
    print_commits_analysis(commits, analysis)

    # Compare with baseline if requested
    baseline_commit = args.baseline_commit
    if args.find_baseline and not baseline_commit:
        baseline_commit = find_baseline_commit()
        if baseline_commit:
            print(f"Found baseline commit: {baseline_commit[:8]}")
            print()

    if baseline_commit:
        comparison = compare_scenario_logic(baseline_commit, "HEAD", args.file)
        print_scenario_logic_comparison(comparison)

    # Summary and recommendations
    print("=" * 80)
    print("SUMMARY & RECOMMENDATIONS")
    print("=" * 80)
    print()

    scenario_commits = [a for a in analysis if a["has_scenario_changes"]]

    if scenario_commits:
        print("⚠️  ACTION REQUIRED:")
        print(f"   Found {len(scenario_commits)} commits with scenario-related changes")
        print("   1. Review each commit to understand what changed")
        print("   2. Check if changes are expected (feature) or bug")
        print("   3. If expected: update baseline after verification")
        print("   4. If bug: fix and re-run regression test")
        print()
        print("   To see full diff for a commit:")
        print("     git show <commit_hash> -- python-worker/core/of_confirm_engine.py")
    else:
        print("✓ No direct scenario logic changes found")
        print("   Scenario mismatches might be caused by:")
        print("   - Changes in dependencies or configuration")
        print("   - Changes in input data processing")
        print("   - Non-deterministic behavior")
        print("   - Changes in other parts of the codebase that affect scenario determination")
    print()


if __name__ == "__main__":
    main()

