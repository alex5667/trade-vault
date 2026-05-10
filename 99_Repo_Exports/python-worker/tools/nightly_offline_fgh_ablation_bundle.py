import argparse
import os
import subprocess
import sys
from pathlib import Path

from utils.time_utils import get_ny_time_millis
from core.redis_keys import RedisStreams as RS


def _now_ms() -> int:
    return get_ny_time_millis()


def _find_script(path: str) -> str:
    if os.path.exists(path):
        return path
    if path.startswith("python-worker/"):
        alt = path[len("python-worker/") :]
        if os.path.exists(alt):
            return alt
    return path


def _run(cmd: list[str]) -> None:
    print(f">> Running: {' '.join(cmd)}")
    p = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    print(p.stdout)
    if p.returncode != 0:
        raise RuntimeError(f"cmd_failed rc={p.returncode} cmd={' '.join(cmd)}\n{p.stdout}")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--redis_url", default=os.getenv("REDIS_URL", "redis://localhost:6379/0"))
    ap.add_argument("--signal_stream", default=os.getenv("ML_REPLAY_STREAM", RS.ML_REPLAY_INPUTS))
    ap.add_argument("--closed_stream", default=os.getenv("TRADES_CLOSED_STREAM", RS.TRADES_CLOSED))
    ap.add_argument("--tb_labels_stream", default=os.getenv("TB_LABELS_STREAM", RS.TB_LABELS))
    ap.add_argument("--label_source", choices=["closed", "tb_primary", "tb_util"], default=os.getenv("LABEL_SOURCE", "closed"))
    ap.add_argument("--tb_labels_count", type=int, default=200000)

    ap.add_argument("--out_dir", default=os.getenv("FGH_ABLATION_OUT_DIR", "/var/lib/trade/of_reports/fgh"))

    # We match the time windows that are typical for nightly builds (e.g. 30 days) if needed,
    # but by default just build what's available or specify limit via env.
    ap.add_argument("--since_ms", type=int, default=0)

    # derived F/G/H features settings
    ap.add_argument("--fgh_leader_symbol", default=os.getenv("FGH_LEADER_SYMBOL", "BTCUSDT"))
    ap.add_argument("--fgh_leader_max_lag_ms", type=int, default=int(os.getenv("FGH_LEADER_MAX_LAG_MS", "2000")))

    # scripts
    ap.add_argument("--builder_script", default="python-worker/ml_analysis/tools/build_edge_stack_dataset_from_redis.py")
    ap.add_argument("--ablation_script", default="python-worker/ml_analysis/tools/ablation_report_fgh_v1.py")

    args = ap.parse_args()

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    ds_path = str(out_dir / "dataset_with_fgh.jsonl")
    cols_path = str(out_dir / "feature_cols_with_fgh.json")
    report_path = str(out_dir / "ablation_fgh_report.json")

    print(f"[{_now_ms()}] Starting Offline FGH Dataset Build")

    # Step 1: Build dataset
    builder = _find_script(args.builder_script)
    cmd_build = [
        sys.executable, builder,
        "--redis_url", args.redis_url,
        "--signal_stream", args.signal_stream,
        "--closed_stream", args.closed_stream,
        "--tb_labels_stream", args.tb_labels_stream,
        "--label_source", args.label_source,
        "--tb_labels_count", str(args.tb_labels_count),
        "--out_jsonl", ds_path,
        "--emit_feature_cols_json", cols_path,
        "--derive_fgh", "1",
        "--fgh_append_feature_cols", "1",
        "--fgh_leader_symbol", args.fgh_leader_symbol,
        "--fgh_leader_max_lag_ms", str(args.fgh_leader_max_lag_ms)
    ]
    if args.since_ms > 0:
        cmd_build.extend(["--since_ms", str(args.since_ms)])

    _run(cmd_build)

    print(f"[{_now_ms()}] Completed building dataset '{ds_path}'. Starting Ablation Report")

    # Step 2: Run ablation report
    ablation = _find_script(args.ablation_script)
    cmd_ablate = [
        sys.executable, ablation,
        "--data_jsonl", ds_path,
        "--feature_cols_json", cols_path,
        "--out_json", report_path
    ]
    _run(cmd_ablate)

    print(f"[{_now_ms()}] Finished Ablation Report: {report_path}")

if __name__ == "__main__":
    main()
