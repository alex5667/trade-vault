from __future__ import annotations

"""Extract policy snapshot + feature manifest from decision records (NDJSON).

Usage:
  python -m ml_analysis.tools.extract_policy_snapshot_v1 --input decisions.ndjson --outdir out_meta

This tool is standalone: it does NOT assume a particular dataset builder.
It enables an "ironclad" Train==Serve contract by persisting the runtime policy snapshot
used to create the training dataset.
"""


import argparse
import json
from pathlib import Path


def _read_ndjson(path: Path):
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except Exception:
                continue


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True, help="NDJSON file with decision records")
    ap.add_argument("--outdir", required=True, help="Output directory for meta")
    ap.add_argument("--allow-multiple", action="store_true", help="Do not fail if multiple policy hashes exist")
    args = ap.parse_args()

    inp = Path(args.input)
    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    policy_counts: dict[str, int] = {}
    manifest_counts: dict[str, int] = {}
    any_policy = None
    any_manifest = None

    n = 0
    for rec in _read_ndjson(inp):
        n += 1
        ind = rec.get("indicators") if isinstance(rec, dict) else None
        if not isinstance(ind, dict):
            continue

        ph = ind.get("dq_policy_hash")
        if isinstance(ph, str) and ph:
            policy_counts[ph] = policy_counts.get(ph, 0) + 1
            if any_policy is None:
                any_policy = ind.get("dq_policy_snapshot_v1")

        mh = ind.get("dq_policy_feature_manifest_hash_v1")
        if isinstance(mh, str) and mh:
            manifest_counts[mh] = manifest_counts.get(mh, 0) + 1
            if any_manifest is None:
                any_manifest = ind.get("dq_policy_feature_manifest_v1")

    meta = {
        "records_seen": int(n),
        "dq_policy_hash_counts": dict(sorted(policy_counts.items(), key=lambda kv: (-kv[1], kv[0]))),
        "dq_policy_feature_manifest_hash_counts": dict(sorted(manifest_counts.items(), key=lambda kv: (-kv[1], kv[0]))),
    }
    (outdir / "policy_counts.json").write_text(json.dumps(meta, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")

    if not args.allow_multiple and len(policy_counts) > 1:
        raise SystemExit(
            f"Multiple dq_policy_hash values found: {list(policy_counts.keys())}. "
            f"Split dataset by policy or set --allow-multiple."
        )

    if any_policy is not None:
        (outdir / "dq_policy_snapshot_v1.json").write_text(json.dumps(any_policy, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    if any_manifest is not None:
        (outdir / "dq_policy_feature_manifest_v1.json").write_text(json.dumps(any_manifest, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
