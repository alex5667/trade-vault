from __future__ import annotations
"""Golden regression harness: baseline inputs → candidate output → diff → alert/fail.

Compares baseline engine replay output with candidate output and reports mismatches.
Used in nightly regression tests to detect non-deterministic changes or bugs.

Usage:
  python -m tools.of_regress_baseline_check --baseline /path/to/baseline.ndjson --candidate /path/to/candidate.ndjson --out /path/to/diff.json
"""


import argparse
import json
import os
import time
from collections import Counter
from typing import Any, Dict, Iterator, List, Optional, Tuple


def iter_ndjson(path: str) -> Iterator[Dict[str, Any]]:
    """Iterator over NDJSON lines."""
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            s = line.strip()
            if not s:
                continue
            yield json.loads(s)


def _get(r: Dict[str, Any], key: str) -> Any:
    """Extract value from row, checking both top-level and evidence dict."""
    if key in r:
        return r.get(key)
    ev = r.get("evidence")
    if isinstance(ev, dict) and key in ev:
        return ev.get(key)
    return None


def row_key(r: Dict[str, Any]) -> str:
    """Generate unique key for row matching (sid or composite)."""
    sid = r.get("sid")
    if sid:
        return str(sid)
    return f"{r.get('symbol','')}|{r.get('ts_ms',0)}|{r.get('direction','')}"


# Fields to compare between baseline and candidate
FIELDS = ["ok", "score", "have", "need", "scenario", "reason", "scenario_v4", "need_reason"]


def diff(baseline_path: str, cand_path: str, *, symbol="") -> Dict[str, Any]:
    """
    Compare baseline and candidate outputs.
    
    Returns:
        Dict with mismatch statistics:
        - n: number of overlapping rows
        - mismatches: total number of field mismatches
        - mismatch_by_field: Counter of mismatches per field
        - mismatch_by_type_top: Top 15 type transitions (old->new)
        - mismatch_by_scenario_v4_top: Top 10 scenarios with mismatches
        - mismatch_by_reason_top: Top 10 reason transitions
    """
    base = {}
    for r in iter_ndjson(baseline_path):
        if symbol and str(r.get("symbol","")).upper() != symbol.upper():
            continue
        base[row_key(r)] = r

    n = 0
    mismatches = 0
    by_field = Counter()
    by_type = Counter()
    by_scn = Counter()
    by_reason = Counter()

    for r in iter_ndjson(cand_path):
        if symbol and str(r.get("symbol","")).upper() != symbol.upper():
            continue
        k = row_key(r)
        b = base.get(k)
        if not b:
            continue
        n += 1
        scn = str(_get(r, "scenario_v4") or _get(r, "scenario") or "na")
        for f in FIELDS:
            av = _get(b, f)
            bv = _get(r, f)
            # For score, use floating point comparison with epsilon
            if f == "score":
                try:
                    if av is not None and bv is not None and abs(float(av) - float(bv)) < 1e-9:
                        continue
                except Exception:
                    pass
            if av != bv:
                mismatches += 1
                by_field[f] += 1
                by_type[f"{f}:{av}->{bv}"] += 1
                by_scn[scn] += 1
                by_reason[f"{_get(b,'reason')}->{_get(r,'reason')}"] += 1

    return {
        "n": n,
        "mismatches": mismatches,
        "mismatch_by_field": dict(by_field),
        "mismatch_by_type_top": by_type.most_common(15),
        "mismatch_by_scenario_v4_top": by_scn.most_common(10),
        "mismatch_by_reason_top": by_reason.most_common(10),
    }


def main() -> None:
    ap = argparse.ArgumentParser(description="Compare baseline and candidate engine replay outputs")
    ap.add_argument("--baseline", required=True, help="baseline output NDJSON (engine replay)")
    ap.add_argument("--candidate", required=True, help="candidate output NDJSON (engine replay)")
    ap.add_argument("--out", required=True, help="diff JSON output path")
    ap.add_argument("--symbol", default="", help="filter by symbol (optional)")
    ap.add_argument("--fail-on-mismatch", type=int, default=1, help="exit code 2 if mismatches > max (default: 1)")
    ap.add_argument("--max-mismatches", type=int, default=int(os.getenv("REGRESS_MAX_MISMATCHES", "0") or 0), help="max allowed mismatches (default: 0, from REGRESS_MAX_MISMATCHES env)")
    args = ap.parse_args()

    rep = diff(args.baseline, args.candidate, symbol=args.symbol)
    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(rep, f, ensure_ascii=False, indent=2)

    if args.fail_on_mismatch == 1:
        if int(rep.get("mismatches", 0)) > int(args.max_mismatches):
            raise SystemExit(2)


if __name__ == "__main__":
    main()

