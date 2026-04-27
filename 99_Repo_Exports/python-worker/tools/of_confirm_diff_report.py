from __future__ import annotations

import argparse
import json
import os
from collections import Counter, defaultdict
from typing import Any, Dict, List


def _safe_loads(line: str) -> Dict[str, Any]:
    try:
        d = json.loads(line)
        return d if isinstance(d, dict) else {}
    except Exception:
        return {}


def _load_ndjson(path: str, *, max_rows: int = 2_000_000) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    n = 0
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            if n >= max_rows:
                break
            line = line.strip()
            if not line:
                continue
            row = _safe_loads(line)
            k = str(row.get("k", "") or "")
            if not k:
                continue
            out[k] = row
            n += 1
    return out


def _group_key(row: Dict[str, Any]) -> str:
    sym = str(row.get("symbol", "") or "NA")
    sc = str(row.get("scenario_v4", "") or "")
    return f"{sym}|{sc}"


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--baseline", required=True)
    ap.add_argument("--candidate", required=True)
    ap.add_argument("--out", default="")
    ap.add_argument("--score-eps", type=float, default=float(os.getenv("OF_REPLAY_SCORE_EPS", "1e-6")))
    ap.add_argument("--fail-on-mismatch", type=int, default=int(os.getenv("OF_REPLAY_FAIL_ON_MISMATCH", "1")))
    ap.add_argument("--max-samples", type=int, default=int(os.getenv("OF_REPLAY_MAX_SAMPLES", "30")))
    args = ap.parse_args()

    base = _load_ndjson(args.baseline)
    cand = _load_ndjson(args.candidate)

    keys = set(base.keys()) | set(cand.keys())
    miss_base = 0
    miss_cand = 0
    mism = 0
    mismatch_types = Counter()
    per_group = defaultdict(int)
    samples: List[Dict[str, Any]] = []

    eps = float(args.score_eps)

    for k in sorted(keys):
        a = base.get(k)
        b = cand.get(k)
        if a is None:
            miss_base += 1
            continue
        if b is None:
            miss_cand += 1
            continue

        diffs = []
        if int(a.get("ok", 0) or 0) != int(b.get("ok", 0) or 0):
            diffs.append("ok")
        if str(a.get("scenario_v4", "") or "") != str(b.get("scenario_v4", "") or ""):
            diffs.append("scenario_v4")
        if int(a.get("have", 0) or 0) != int(b.get("have", 0) or 0):
            diffs.append("have")
        if int(a.get("need", 0) or 0) != int(b.get("need", 0) or 0):
            diffs.append("need")
        if int(a.get("gate_bits", 0) or 0) != int(b.get("gate_bits", 0) or 0):
            diffs.append("gate_bits")
        try:
            if abs(float(a.get("score", 0.0) or 0.0) - float(b.get("score", 0.0) or 0.0)) > eps:
                diffs.append("score")
        except Exception:
            pass
        if diffs:
            mism += 1
            for d in diffs:
                mismatch_types[d] += 1
            per_group[_group_key(a)] += 1
            if len(samples) < int(args.max_samples):
                samples.append({"k": k, "diffs": diffs, "baseline": a, "candidate": b})

    top_groups = sorted(per_group.items(), key=lambda kv: kv[1], reverse=True)[:15]
    report = {
        "baseline_n": len(base),
        "candidate_n": len(cand),
        "missing_in_baseline": miss_base,
        "missing_in_candidate": miss_cand,
        "mismatches": mism,
        "mismatch_types": dict(mismatch_types),
        "top_groups": top_groups,
        "samples": samples,
    }

    if args.out:
        os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)
        with open(args.out, "w", encoding="utf-8") as f:
            json.dump(report, f, ensure_ascii=False, indent=2)
    else:
        print(json.dumps(report, ensure_ascii=False, indent=2))

    if int(args.fail_on_mismatch) == 1 and (miss_base > 0 or miss_cand > 0 or mism > 0):
        raise SystemExit(2)


if __name__ == "__main__":
    main()

