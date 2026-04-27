from __future__ import annotations
from utils.time_utils import get_ny_time_millis

import argparse
import json
import os
import time
import uuid
from typing import Any, Dict, Optional

import redis

from tools.export_stream_payload_ndjson_v1 import export_stream_since
import subprocess


def _safe_json(obj: Any) -> str:
    """Serialize object to JSON string."""
    return json.dumps(obj, ensure_ascii=False, separators=(",", ":"))


def _read_json(path: str) -> Dict[str, Any]:
    """Read JSON file."""
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def send_telegram(r: redis.Redis, text: str, buttons: Optional[list] = None) -> None:
    """Send Telegram notification with optional buttons."""
    stream = os.getenv("TELEGRAM_NOTIFY_STREAM", "notify:telegram")
    fields = {"text": text}
    if buttons is not None:
        fields["buttons"] = json.dumps(buttons, ensure_ascii=False, separators=(",", ":"))
    r.xadd(stream, fields, maxlen=200000, approximate=True)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--since-hours", type=float, default=float(os.getenv("ML_TB_SINCE_HOURS", "72")))
    ap.add_argument("--label-col", default=os.getenv("ML_TB_LABEL_COL", "y_edge"))  # y_edge or y_util_pos
    ap.add_argument("--models-root", default=os.getenv("ML_MODELS_ROOT", "/var/lib/trade/ml_models"))
    args = ap.parse_args()

    redis_url = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
    r = redis.Redis.from_url(redis_url, decode_responses=True)

    inputs_stream = os.getenv("OF_INPUTS_STREAM", "signals:of:inputs")
    inputs_field = os.getenv("OF_INPUTS_FIELD", "payload")
    tb_stream = os.getenv("TB_LABELS_STREAM", "labels:tb")
    tb_field = os.getenv("TB_LABELS_FIELD", "payload")

    run_id = time.strftime("%Y%m%d_%H%M%S") + "_" + uuid.uuid4().hex[:6]
    out_dir = os.path.join(args.models_root, f"tb_stack_{run_id}")
    os.makedirs(out_dir, exist_ok=True)

    tmp_inputs = os.path.join(out_dir, "of_inputs.ndjson")
    tmp_tb = os.path.join(out_dir, "tb_labels.ndjson")
    tmp_ds = os.path.join(out_dir, "dataset.parquet")

    since_ms = get_ny_time_millis() - int(float(args.since_hours) * 3600_000)

    # Check streams before export
    try:
        inputs_len = r.xlen(inputs_stream)
    except Exception:
        inputs_len = 0
    
    try:
        tb_len = r.xlen(tb_stream)
    except Exception:
        tb_len = 0
    
    print(f"📊 Stream lengths: {inputs_stream}={inputs_len}, {tb_stream}={tb_len}")
    
    if inputs_len == 0:
        raise SystemExit(f"❌ Stream {inputs_stream} is empty. No data to train on.")
    
    if tb_len == 0:
        # Check if stream exists
        try:
            r.xinfo_stream(tb_stream)
            stream_exists = True
        except Exception:
            stream_exists = False
        
        if not stream_exists:
            raise SystemExit(
                f"❌ Stream {tb_stream} does not exist.\n"
                f"   TB labeler (v10.1) may not be running or has not processed any data yet.\n"
                f"   Check: docker ps | grep tb-labeler\n"
                f"   Check logs: docker logs scanner-tb-labeler-worker-v10-1\n"
                f"   The labeler needs to process signals from {inputs_stream} first."
            )
        else:
            raise SystemExit(
                f"❌ Stream {tb_stream} is empty (all data may have been trimmed by MAXLEN).\n"
                f"   Try reducing --since-hours or wait for TB labeler to process new data.\n"
                f"   Current window: {args.since_hours} hours"
            )

    w_in, _ = export_stream_since(r=r, stream=inputs_stream, payload_field=inputs_field, since_ms=since_ms, out_path=tmp_inputs, max_scan=800_000, ts_field_guess="ts_ms")
    w_tb, _ = export_stream_since(r=r, stream=tb_stream, payload_field=tb_field, since_ms=since_ms, out_path=tmp_tb, max_scan=800_000, ts_field_guess="created_ms")

    print(f"📥 Exported: {w_in} inputs, {w_tb} TB labels")
    
    if w_in == 0:
        raise SystemExit(f"❌ No inputs exported from {inputs_stream} (since {args.since_hours}h ago)")
    if w_tb == 0:
        raise SystemExit(f"❌ No TB labels exported from {tb_stream} (since {args.since_hours}h ago). TB labeler may not be running or lagging.")

    # build dataset using the CLI script (deterministic)
    try:
        subprocess.check_call([
            "python3", "-m", "tools.build_dataset_from_inputs_tb_labels_v2",
            "--inputs", tmp_inputs,
            "--tb", tmp_tb,
            "--out", tmp_ds,
            "--primary-h-ms", os.getenv("TB_PRIMARY_H_MS", "180000"),
            "--drop-no-ticks", "1",
        ], env={**os.environ, "PYTHONPATH": ".:.."})
    except subprocess.CalledProcessError as e:
        raise SystemExit(f"❌ Dataset build failed: {e}. Check inputs and TB labels format.")

    ds_summary = _read_json(tmp_ds + ".json")
    joined_rows = ds_summary.get("joined_rows", 0)
    print(f"📊 Dataset summary: {joined_rows} joined rows")
    
    if joined_rows == 0:
        raise SystemExit(f"❌ Dataset is empty after join. Check join_rate in summary: {ds_summary}")

    # train
    subprocess.check_call([
        "python3", "-m", "tools.train_ml_confirm_stack_tb_v1",
        "--dataset", tmp_ds,
        "--out-dir", out_dir,
        "--time-col", "ts_ms",
        "--label-col", args.label_col,
    ], env={**os.environ, "PYTHONPATH": ".:.."})

    meta = _read_json(os.path.join(out_dir, "meta.json"))
    metrics = (meta.get("metrics") or {})

    # Prepare challenger config payload (promotion worker uses it)
    challenger_key = os.getenv("ML_CFG_CHALLENGER_KEY", "cfg:ml_confirm:challenger")
    champion_key = os.getenv("ML_CFG_CHAMPION_KEY", "cfg:ml_confirm:champion")

    challenger = {
        "ver": meta.get("version", "tb_stack_v1"),
        "run_id": run_id,
        "created_ms": int(meta.get("created_ms") or get_ny_time_millis()),
        "label_col": args.label_col,
        "model_path": os.path.join(out_dir, "model.joblib"),
        "meta_path": os.path.join(out_dir, "meta.json"),
        "metrics": metrics,
    }
    r.set(challenger_key, json.dumps(challenger, ensure_ascii=False, separators=(",", ":")))

    # Telegram report with Approve/Reject
    pr_auc = float(metrics.get("pr_auc", 0.0) or 0.0)
    logloss = float(metrics.get("logloss", 0.0) or 0.0)
    brier = float(metrics.get("brier", 0.0) or 0.0)
    ece = float(metrics.get("ece", 0.0) or 0.0)
    n = int(metrics.get("n", ds_summary.get("joined_rows", 0)) or 0)
    join_rate = 0.0
    try:
        join_rate = float(ds_summary.get("joined_rows", 0)) / float(ds_summary.get("inputs_rows", 1))
    except Exception:
        join_rate = 0.0

    text = (
        f"🧪 ML TB Train (v10.2)\n"
        f"run_id={run_id}\n"
        f"since_hours={args.since_hours} label={args.label_col}\n"
        f"dataset: n={n} join_rate={join_rate:.2%} pos_rate={float(metrics.get('pos_rate', 0.0) or 0.0):.2%}\n"
        f"metrics: PR-AUC={pr_auc:.3f} logloss={logloss:.4f} brier={brier:.4f} ece={ece:.4f}\n"
        f"saved: {out_dir}\n"
        f"cfg: challenger_key={challenger_key}\n"
        f"\nApprove -> promote challenger to champion ({champion_key})"
    )

    buttons = [[
        {"text": "✅ Approve", "callback": f"approve:ml_tb:{run_id}"},
        {"text": "❌ Reject", "callback": f"reject:ml_tb:{run_id}"},
    ]]
    send_telegram(r, text, buttons=buttons)


if __name__ == "__main__":
    main()

