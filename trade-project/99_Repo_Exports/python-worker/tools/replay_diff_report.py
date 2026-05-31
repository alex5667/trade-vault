from __future__ import annotations

import argparse
import json
from collections import Counter
from pathlib import Path
from typing import Any

'''
Diff report for golden replay outputs (OFConfirmV3 ndjson).

Compares baseline vs candidate by key:
  (sid) or (symbol, ts_ms, direction)

Adds top-diff analytics:
  - mismatch counts by field
  - mismatch counts by "type" (e.g. ok:1->0)
  - mismatch counts by scenario_v4 (from evidence)
  - sample mismatches with compact context
'''


def _f(x: Any, d: float = 0.0) -> float:
    try:
        return float(x)
    except Exception:
        return d


def _get(d: dict, key: str) -> Any:
    if key in d:
        return d.get(key)
    ev = d.get("evidence") or {}
    if isinstance(ev, dict) and key in ev:
        return ev.get(key)
    return None


def row_key(r: dict) -> str:
    sid = r.get("sid")
    if sid:
        return str(sid)
    return f"{r.get('symbol', '')}|{r.get('ts_ms', 0)}|{r.get('direction', '')}"


def load_ndjson(path: Path, symbol_filter: str = "") -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            s = line.strip()
            if not s:
                continue
            r = json.loads(s)
            if symbol_filter:
                sym = (r.get("symbol") or "").upper()
                if sym != symbol_filter.upper():
                    continue
            out.append(r)
    return out


FIELDS = ["ok", "score", "have", "need", "reason", "scenario", "scenario_v4", "need_reason"]


def compare(
    *,
    base_rows: list[dict[str, Any]],
    cand_rows: list[dict[str, Any]],
    score_eps: float = 1e-6,
    max_samples: int = 50,
) -> dict[str, Any]:
    base = {row_key(r): r for r in base_rows}
    cand = {row_key(r): r for r in cand_rows}

    keys = sorted(set(base.keys()) | set(cand.keys()))
    missing_in_cand: list[str] = []
    missing_in_base: list[str] = []
    mismatches: list[dict[str, Any]] = []

    mismatch_by_field: Counter[str] = Counter()
    mismatch_by_type: Counter[str] = Counter()
    mismatch_by_scn_v4: Counter[str] = Counter()
    mismatch_by_reason: Counter[str] = Counter()

    for k in keys:
        b = base.get(k)
        c = cand.get(k)
        if b is None:
            missing_in_base.append(k)
            continue
        if c is None:
            missing_in_cand.append(k)
            continue

        diffs: dict[str, Any] = {}
        for f in FIELDS:
            val_b = _get(b, f)
            val_c = _get(c, f)

            if f == "score":
                sb = _f(val_b, 0.0)
                sc = _f(val_c, 0.0)
                if abs(sb - sc) > score_eps:
                    diffs[f] = {"base": sb, "cand": sc}
                    mismatch_by_field[f] += 1
                    mismatch_by_type[f"{f}:changed"] += 1
                continue

            if str(val_b) != str(val_c):
                diffs[f] = {"base": val_b, "cand": val_c}
                mismatch_by_field[f] += 1
                mismatch_by_type[f"{f}:{val_b}->{val_c}"] += 1

        if diffs:
            scn = _get(b, "scenario_v4") or _get(c, "scenario_v4") or ""
            mismatch_by_scn_v4[str(scn)] += 1
            if "reason" in diffs:
                mismatch_by_reason[f"{_get(b, 'reason')}->{_get(c, 'reason')}"] += 1

            # compact sample
            mismatches.append(
                {
                    "key": k,
                    "scenario_v4": scn,
                    "diffs": diffs,
                    "base": {f: _get(b, f) for f in ["ok", "score", "have", "need", "reason"]},
                    "cand": {f: _get(c, f) for f in ["ok", "score", "have", "need", "reason"]},
                }
            )

    return {
        "n_base": len(base_rows),
        "n_cand": len(cand_rows),
        "n_keys": len(keys),
        "missing_in_cand": len(missing_in_cand),
        "missing_in_base": len(missing_in_base),
        "mismatch": len(mismatches),
        "mismatch_rate": (len(mismatches) / max(1, len(keys))),
        "mismatch_by_field": dict(mismatch_by_field),
        "mismatch_by_type": dict(mismatch_by_type.most_common(100)),
        "mismatch_by_scenario_v4": dict(mismatch_by_scn_v4),
        "mismatch_by_reason": dict(mismatch_by_reason.most_common(50)),
        "samples": mismatches[:max_samples],
        "missing_in_cand_samples": missing_in_cand[:max_samples],
        "missing_in_base_samples": missing_in_base[:max_samples],
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", required=True)
    ap.add_argument("--cand", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--symbol-filter", default="")
    ap.add_argument("--score-eps", type=float, default=1e-6)
    ap.add_argument("--max-samples", type=int, default=50)
    args = ap.parse_args()

    base_rows = load_ndjson(Path(args.base), symbol_filter=args.symbol_filter)
    cand_rows = load_ndjson(Path(args.cand), symbol_filter=args.symbol_filter)
    rep = compare(base_rows=base_rows, cand_rows=cand_rows, score_eps=float(args.score_eps), max_samples=int(args.max_samples))
    Path(args.out).write_text(json.dumps(rep, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")


if __name__ == "__main__":
    main()
