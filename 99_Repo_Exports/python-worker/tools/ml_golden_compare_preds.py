from __future__ import annotations

import argparse
import json
from typing import Any

from core.ml_metrics_utils import ks_statistic, quantiles


def load_preds(path: str) -> dict[str, dict[str, Any]]:
    m = {}
    with open(path, encoding="utf-8") as f:
        for line in f:
            line=line.strip()
            if not line:
                continue
            j = json.loads(line)
            sid = j.get("sid")
            if not sid:
                continue
            m[str(sid)] = j
    return m

def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--baseline", required=True)
    ap.add_argument("--candidate", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--topk", type=int, default=30)
    args = ap.parse_args()

    b = load_preds(args.baseline)
    c = load_preds(args.candidate)

    sids = sorted(set(b.keys()) & set(c.keys()))
    pb = [float(b[s]["p_edge"]) for s in sids]
    pc = [float(c[s]["p_edge"]) for s in sids]

    qb = quantiles(pb, [0.5,0.9,0.99])
    qc = quantiles(pc, [0.5,0.9,0.99])

    ks = float(ks_statistic(pb, pc))

    deltas = []
    for s in sids:
        d = float(c[s]["p_edge"]) - float(b[s]["p_edge"])
        deltas.append((abs(d), d, s))
    deltas.sort(reverse=True)
    top = [{"sid": s, "delta": d, "p_base": float(b[s]["p_edge"]), "p_new": float(c[s]["p_edge"])} for _, d, s in deltas[: args.topk]]

    rep = {
        "n_join": len(sids),
        "p_base": {"p50": qb[0], "p90": qb[1], "p99": qb[2]},
        "p_new": {"p50": qc[0], "p90": qc[1], "p99": qc[2]},
        "ks": ks,
        "top_delta": top,
    }
    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(rep, f, ensure_ascii=False, indent=2)
    print(json.dumps(rep, ensure_ascii=False))

if __name__ == "__main__":
    main()
