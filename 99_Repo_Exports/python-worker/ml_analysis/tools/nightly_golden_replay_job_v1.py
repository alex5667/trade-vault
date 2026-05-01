from __future__ import annotations
"""Nightly golden replay job (B6).

This tool scans OFC_CAPTURE NDJSON captures, groups them by policy hash, and
runs golden replay parity (B5) per group.

Key property: we do NOT allow mixed policies in a single replay run.
"""


import argparse
import os
import sys
import json
import subprocess
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List


def _utc_yyyymmdd(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y%m%d")


def _iter_policy_dirs(base: Path, day: str) -> List[Path]:
    root = base / day
    if not root.exists():
        return []
    out: List[Path] = []
    for p in sorted(root.iterdir()):
        if p.is_dir() and p.name.startswith("policy_"):
            out.append(p)
    return out


def _concat_ndjson(files: List[Path], out_path: Path, limit: int) -> int:
    n = 0
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", encoding="utf-8") as out:
        for f in files:
            with f.open("r", encoding="utf-8") as inp:
                for line in inp:
                    if not line.strip():
                        continue
                    out.write(line)
                    n += 1
                    if limit > 0 and n >= limit:
                        return n
    return n


def _run_parity(inp: Path, outdir: Path, fail_on_mismatch: bool, evidence: str) -> int:
    cmd = [
        sys.executable,
        "-m",
        "ml_analysis.tools.golden_replay_parity_v1",
        "--input",
        str(inp),
        "--outdir",
        str(outdir),
        "--evidence",
        str(evidence),
    ]
    if fail_on_mismatch:
        cmd.append("--fail-on-mismatch")
    p = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    outdir.mkdir(parents=True, exist_ok=True)
    (outdir / "parity_stdout.txt").write_text(p.stdout or "", encoding="utf-8")
    return int(p.returncode)


def prune_old(base: Path, keep_days: int) -> None:
    if keep_days <= 0:
        return
    cutoff = datetime.now(tz=timezone.utc) - timedelta(days=int(keep_days))
    for p in sorted(base.iterdir()):
        if not p.is_dir():
            continue
        name = p.name
        if len(name) != 8 or not name.isdigit():
            continue
        try:
            dt = datetime.strptime(name, "%Y%m%d").replace(tzinfo=timezone.utc)
        except Exception:
            continue
        if dt < cutoff:
            try:
                # rm -rf equivalent (careful, but directory name is validated)
                for sub in p.rglob("*"):
                    if sub.is_file() or sub.is_symlink():
                        try:
                            sub.unlink()
                        except Exception:
                            pass
                for sub in sorted(p.rglob("*"), reverse=True):
                    if sub.is_dir():
                        try:
                            sub.rmdir()
                        except Exception:
                            pass
                p.rmdir()
            except Exception:
                pass


def main(argv: List[str]) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--capture-dir", default=os.getenv("OFC_CAPTURE_DIR", "/var/lib/scanner/ofc_capture"))
    ap.add_argument("--outdir", default=os.getenv("GOLDEN_REPLAY_OUTDIR", "/var/lib/scanner/golden_replay_reports"))
    ap.add_argument("--date", default="yesterday", help="UTC yyyymmdd or 'yesterday' (default)")
    ap.add_argument("--limit", type=int, default=int(os.getenv("GOLDEN_REPLAY_LIMIT", "0")), help="max rows per policy (0=all)")
    ap.add_argument("--fail-on-mismatch", action="store_true")
    ap.add_argument("--evidence", default=os.getenv("GOLDEN_REPLAY_EVIDENCE", "lite"))
    ap.add_argument("--keep-days", type=int, default=int(os.getenv("OFC_CAPTURE_KEEP_DAYS", "10")))
    args = ap.parse_args(argv)

    cap = Path(args.capture_dir)
    out = Path(args.outdir)

    if args.date == "yesterday":
        day = _utc_yyyymmdd(datetime.now(tz=timezone.utc) - timedelta(days=1))
    else:
        day = str(args.date).strip()

    policy_dirs = _iter_policy_dirs(cap, day)
    if not policy_dirs:
        out.mkdir(parents=True, exist_ok=True)
        (out / f"report_{day}.json").write_text(json.dumps({"day": day, "status": "no_data"}, indent=2), encoding="utf-8")
        prune_old(cap, args.keep_days)
        return 0

    summary: Dict[str, Dict[str, object]] = {"day": day, "policies": {}}  # type: ignore

    rc_all = 0
    for pd in policy_dirs:
        pol = pd.name.replace("policy_", "")
        files = sorted([p for p in pd.glob("*.ndjson") if p.is_file()])
        if not files:
            continue
        tmp_inp = out / day / pol / "merged.ndjson"
        n = _concat_ndjson(files, tmp_inp, limit=int(args.limit))
        rep_dir = out / day / pol
        rc = _run_parity(tmp_inp, rep_dir, bool(args.fail_on_mismatch), str(args.evidence))
        summary["policies"][pol] = {"rows": n, "returncode": rc, "dir": str(rep_dir)}  # type: ignore
        if rc != 0:
            rc_all = rc if rc_all == 0 else rc_all

    out.mkdir(parents=True, exist_ok=True)
    (out / f"report_{day}.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")

    prune_old(cap, args.keep_days)
    return int(rc_all)


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
