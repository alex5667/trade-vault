from __future__ import annotations

import argparse
import json
import os
import subprocess
import time
import uuid
from typing import Any

import redis

from tools.export_stream_payload_ndjson_v1 import export_stream_since
from utils.time_utils import get_ny_time_millis
from core.redis_keys import RedisStreams as RS


def _read_json(path: str) -> dict[str, Any]:
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def send_telegram(r: redis.Redis, text: str, buttons: list | None = None) -> None:
    stream = os.getenv("TELEGRAM_NOTIFY_STREAM", RS.NOTIFY_TELEGRAM)
    fields = {"text": text}
    if buttons is not None:
        fields["buttons"] = json.dumps(buttons, ensure_ascii=False, separators=(",", ":"))
    r.xadd(stream, fields, maxlen=200000, approximate=True)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--since-hours", type=float, default=float(os.getenv("ML_TB_SINCE_HOURS", "72")))
    ap.add_argument("--label-col", default=os.getenv("ML_TB_LABEL_COL", "y_util_pos"))
    ap.add_argument("--models-root", default=os.getenv("ML_MODELS_ROOT", "/var/lib/trade/ml_models"))
    ap.add_argument("--primary-h-ms", default=os.getenv("TB_PRIMARY_H_MS", "180000"))
    args = ap.parse_args()

    r = redis.Redis.from_url(os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0"), decode_responses=True)

    inputs_stream = os.getenv("OF_INPUTS_STREAM", RS.OF_INPUTS)
    inputs_field = os.getenv("OF_INPUTS_FIELD", "payload")
    tb_stream = os.getenv("TB_LABELS_STREAM", RS.TB_LABELS)
    tb_field = os.getenv("TB_LABELS_FIELD", "payload")

    run_id = time.strftime("%Y%m%d_%H%M%S") + "_" + uuid.uuid4().hex[:6]
    out_dir = os.path.join(args.models_root, f"tb_v10_3_{run_id}")
    os.makedirs(out_dir, exist_ok=True)

    tmp_inputs = os.path.join(out_dir, "of_inputs.ndjson")
    tmp_tb = os.path.join(out_dir, "tb_labels.ndjson")
    tmp_ds = os.path.join(out_dir, "dataset.parquet")
    thr_json = os.path.join(out_dir, "thresholds.json")

    since_ms = get_ny_time_millis() - int(float(args.since_hours) * 3600_000)

    export_stream_since(r=r, stream=inputs_stream, payload_field=inputs_field, since_ms=since_ms, out_path=tmp_inputs, max_scan=1_200_000, ts_field_guess="ts_ms")
    export_stream_since(r=r, stream=tb_stream, payload_field=tb_field, since_ms=since_ms, out_path=tmp_tb, max_scan=1_200_000, ts_field_guess="created_ms")

    # dataset builder v2 from v10.2 must exist in repo
    subprocess.check_call([
        "python3", "-m", "tools.build_dataset_from_inputs_tb_labels_v2",
        "--inputs", tmp_inputs,
        "--tb", tmp_tb,
        "--out", tmp_ds,
        "--primary-h-ms", str(args.primary_h_ms),
        "--drop-no-ticks", "1",
    ], env={**os.environ, "PYTHONPATH": ".:.."})

    ds_summary = _read_json(tmp_ds + ".json")

    subprocess.check_call([
        "python3", "-m", "tools.train_ml_confirm_tb_stack_v2_strict_oof",
        "--dataset", tmp_ds,
        "--out-dir", out_dir,
        "--time-col", "ts_ms",
        "--label-col", args.label_col,
    ], env={**os.environ, "PYTHONPATH": ".:.."})

    meta = _read_json(os.path.join(out_dir, "meta.json"))
    evalm = (meta.get("eval_last_split") or {})

    subprocess.check_call([
        "python3", "-m", "tools.optimize_thresholds_tb_v1",
        "--dataset", tmp_ds,
        "--model", os.path.join(out_dir, "model.joblib"),
        "--out", thr_json,
        "--time-col", "ts_ms",
    ], env={**os.environ, "PYTHONPATH": ".:.."})

    thr = _read_json(thr_json)

    challenger_key = os.getenv("ML_CFG_CHALLENGER_KEY", "cfg:ml_confirm:challenger")
    champion_key = os.getenv("ML_CFG_CHAMPION_KEY", "cfg:ml_confirm:champion")

    challenger = {
        "kind": "tb_v10_3",
        "run_id": run_id,
        "created_ms": int(meta.get("created_ms") or get_ny_time_millis()),
        "label_col": args.label_col,
        "primary_h_ms": int(args.primary_h_ms),
        "model_path": os.path.join(out_dir, "model.joblib"),
        "meta_path": os.path.join(out_dir, "meta.json"),
        "thresholds": thr,
        "dataset_summary": ds_summary,
    }
    r.set(challenger_key, json.dumps(challenger, ensure_ascii=False, separators=(",", ":")))

    join_rate = 0.0
    try:
        join_rate = float(ds_summary.get("joined_rows", 0)) / float(ds_summary.get("inputs_rows", 1))
    except Exception:
        join_rate = 0.0

    text = (
        f"🎯 ML TB v10.3 (higher accuracy)\n"
        f"run_id={run_id}\n"
        f"since_hours={args.since_hours} label={args.label_col} primary_h_ms={args.primary_h_ms}\n"
        f"dataset: joined={ds_summary.get('joined_rows')} join_rate={join_rate:.1%} pos_y_edge={ds_summary.get('pos_rate_y_edge', 'na')} pos_y_util_pos={ds_summary.get('pos_rate_y_util_pos', 'na')}\n"
        f"eval(last split): pr_auc={evalm.get('pr_auc', 0):.4f} logloss={evalm.get('logloss', 0):.4f} brier={evalm.get('brier', 0):.4f} ece={evalm.get('ece', 0):.4f}\n"
        f"thr(global): {thr.get('global', {}).get('thr', 'na')}  take_rate={thr.get('global', {}).get('take_rate', 'na')}  mean_util={thr.get('global', {}).get('mean_util', 'na')}\n"
        f"thr(range): {thr.get('thresholds', {}).get('range', {}).get('thr', 'na')}  thr(trend): {thr.get('thresholds', {}).get('trend', {}).get('thr', 'na')}\n"
        f"saved: {out_dir}\n"
        f"Approve -> set champion ({champion_key})"
    )

    buttons = [[
        {"text": "✅ Approve", "callback": f"approve:ml_tb_v10_3:{run_id}"},
        {"text": "❌ Reject", "callback": f"reject:ml_tb_v10_3:{run_id}"},
    ]]
    send_telegram(r, text, buttons=buttons)


if __name__ == "__main__":
    main()

