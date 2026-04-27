from __future__ import annotations

import argparse
import json
import os
import subprocess
from pathlib import Path


def run(cmd: list[str]) -> None:
    p = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    if p.stdout:
        print(p.stdout.rstrip())
    if p.returncode != 0:
        raise SystemExit(p.returncode)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--redis-url", default=os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0"))
    ap.add_argument("--out-dir", required=True)
    ap.add_argument("--since-hours", type=float, default=float(os.getenv("ML_REPLAY_SINCE_HOURS", "24")))
    ap.add_argument("--max-records", type=int, default=int(os.getenv("ML_REPLAY_MAX_RECORDS", "250000")))
    ap.add_argument("--baseline", default=os.getenv("ML_REPLAY_BASELINE", ""))
    ap.add_argument("--fail-on-mismatch", type=int, default=int(os.getenv("ML_REPLAY_FAIL_ON_MISMATCH", "1")))
    args = ap.parse_args()

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    inputs_path = str(out_dir / "ml_inputs.ndjson")
    cand_path = str(out_dir / "ml_replay_candidate.ndjson")
    diff_path = str(out_dir / "ml_replay_diff.json")

    run([
        "python", "-m", "tools.export_ml_confirm_inputs_ndjson",
        "--redis-url", str(args.redis_url),
        "--since-hours", str(args.since_hours),
        "--max-records", str(args.max_records),
        "--out", inputs_path,
    ])

    run([
        "python", "-m", "tools.ml_confirm_replay_from_inputs",
        "--inputs", inputs_path,
        "--out", cand_path,
        "--mode", "ENFORCE",
    ])

    if str(args.baseline or "").strip():
        run([
            "python", "-m", "tools.ml_confirm_diff_report",
            "--baseline", str(args.baseline),
            "--candidate", cand_path,
            "--out", diff_path,
            "--fail-on-mismatch", str(int(args.fail_on_mismatch)),
        ])

    print(json.dumps({"ok": True, "inputs": inputs_path, "candidate": cand_path, "diff": diff_path}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()


