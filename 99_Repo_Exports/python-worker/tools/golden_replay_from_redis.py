from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path

from tools.ndjson_canary import (
    filter_inputs,
    iter_ndjson,
    write_ndjson,
    list_symbols_in_inputs,
    pick_baseline_for_symbol,
)


def _run(cmd: list[str]) -> None:
    p = subprocess.run(cmd, stdout=sys.stdout, stderr=sys.stderr)
    if p.returncode != 0:
        raise SystemExit(p.returncode)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--redis-url", required=True)
    ap.add_argument("--out-dir", required=True, help="directory for inputs/replay/diff")
    ap.add_argument("--state-file", default="")
    ap.add_argument("--resume", action="store_true")
    ap.add_argument("--max-records", type=int, default=0)
    ap.add_argument("--batch", type=int, default=2000)
    ap.add_argument("--start-id", default="0-0")
    ap.add_argument("--end-id", default="+")
    ap.add_argument("--stream", default=os.getenv("OF_INPUTS_STREAM", "signals:of:inputs"))
    ap.add_argument("--field", default=os.getenv("OF_INPUTS_STREAM_FIELD", "payload"))
    ap.add_argument("--baseline", default="", help="baseline replay ndjson")
    ap.add_argument("--baseline-dir", default="", help="directory with baseline_<SYMBOL>.ndjson or baseline.ndjson")
    ap.add_argument("--fail-on-mismatch", action="store_true")
    ap.add_argument("--canary-symbols", default="", help="CSV allowlist, e.g. BTCUSDT,ETHUSDT")
    ap.add_argument("--canary-share", type=float, default=0.0, help="Deterministic share 0..1")
    ap.add_argument("--python", default=sys.executable)
    args = ap.parse_args()

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    inputs_path_raw = out_dir / f"of_inputs_{ts}.raw.ndjson"
    replay_path = out_dir / f"of_replay_{ts}.ndjson"
    report_path = out_dir / f"of_diff_{ts}.json"

    export_cmd = [
        args.python,
        "-m",
        "tools.export_of_inputs_ndjson_v2",
        "--redis-url",
        str(args.redis_url),
        "--out",
        str(inputs_path_raw),
        "--stream",
        str(args.stream),
        "--field",
        str(args.field),
        "--start-id",
        str(args.start_id),
        "--end-id",
        str(args.end_id),
        "--batch",
        str(int(args.batch)),
    ]
    if int(args.max_records) > 0:
        export_cmd += ["--max-records", str(int(args.max_records))]
    if args.state_file:
        export_cmd += ["--state-file", str(args.state_file)]
    if args.resume:
        export_cmd += ["--resume"]
    _run(export_cmd)

    # Canary filtering
    canary_symbols = [s.strip() for s in args.canary_symbols.split(",") if s.strip()] if args.canary_symbols else None
    inputs_path = inputs_path_raw
    if (canary_symbols and len(canary_symbols) > 0) or (args.canary_share and args.canary_share > 0):
        inputs_path_canary = out_dir / f"of_inputs_{ts}.canary.ndjson"
        n = write_ndjson(
            str(inputs_path_canary),
            filter_inputs(iter_ndjson(str(inputs_path_raw)), canary_symbols=canary_symbols, canary_share=float(args.canary_share)),
        )
        inputs_path = inputs_path_canary
        print(f"[canary] wrote {n} rows to {inputs_path}")

    replay_cmd = [
        args.python,
        "-m",
        "tools.of_replay_from_inputs",
        "--inputs",
        str(inputs_path),
        "--out",
        str(replay_path),
    ]
    _run(replay_cmd)

    # Baseline comparison
    any_bad = False

    def _run_diff(base_path: str, cand_path: str, out_path: str, symbol_filter: str = "") -> dict:
        diff_cmd = [
            args.python,
            "-m",
            "tools.replay_diff_report",
            "--base",
            base_path,
            "--cand",
            cand_path,
            "--out",
            out_path,
        ]
        if symbol_filter:
            diff_cmd += ["--symbol-filter", symbol_filter]
        _run(diff_cmd)
        return json.loads(Path(out_path).read_text(encoding="utf-8"))

    if args.baseline:
        rep = _run_diff(args.baseline, str(replay_path), str(report_path))
        if int(rep.get("mismatch", 0)) > 0 or int(rep.get("missing_in_cand", 0)) > 0:
            any_bad = True

    if args.baseline_dir:
        bdir = args.baseline_dir.strip()
        if not os.path.exists(bdir):
            raise SystemExit(f"baseline-dir not found: {bdir}")

        syms = list_symbols_in_inputs(str(inputs_path), limit=200000)
        summary = {"symbols": {}, "total_mismatches": 0, "total_records": 0}
        for sym in sorted(syms):
            bpath = pick_baseline_for_symbol(bdir, sym)
            if not bpath:
                continue

            out_sym = out_dir / f"of_diff_{ts}.{sym}.json"
            rep = _run_diff(bpath, str(replay_path), str(out_sym), symbol_filter=sym)
            summary["symbols"][sym] = rep
            summary["total_mismatches"] += int(rep.get("mismatch", 0))
            summary["total_records"] += int(rep.get("n_keys", 0))
            if int(rep.get("mismatch", 0)) > 0 or int(rep.get("missing_in_cand", 0)) > 0:
                any_bad = True

        sum_path = out_dir / f"of_diff_{ts}_summary.json"
        sum_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

    if args.fail_on_mismatch and any_bad:
        raise SystemExit(2)

    print(str(replay_path))


if __name__ == "__main__":
    main()
