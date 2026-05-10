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
    """Read JSON file."""
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def send_telegram(r: redis.Redis, text: str, buttons: list | None = None) -> None:
    """Send Telegram notification with optional buttons."""
    stream = os.getenv("TELEGRAM_NOTIFY_STREAM", RS.NOTIFY_TELEGRAM)
    fields = {"text": text}
    if buttons is not None:
        fields["buttons"] = json.dumps(buttons, ensure_ascii=False, separators=(",", ":"))
    r.xadd(stream, fields, maxlen=200000, approximate=True)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--since-hours", type=float, default=float(os.getenv("ML_TB_SINCE_HOURS", "72")))
    ap.add_argument("--models-root", default=os.getenv("ML_MODELS_ROOT", "/var/lib/trade/ml_models"))
    ap.add_argument("--horizons", default=os.getenv("TB_HORIZONS_MS", "60000,180000,300000"))
    args = ap.parse_args()

    r = redis.Redis.from_url(os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0"), decode_responses=True)

    inputs_stream = os.getenv("OF_INPUTS_STREAM", RS.OF_INPUTS)
    inputs_field = os.getenv("OF_INPUTS_FIELD", "payload")
    tb_stream = os.getenv("TB_LABELS_STREAM", RS.TB_LABELS)
    tb_field = os.getenv("TB_LABELS_FIELD", "payload")

    run_id = time.strftime("%Y%m%d_%H%M%S") + "_" + uuid.uuid4().hex[:6]
    out_dir = os.path.join(args.models_root, f"tb_v10_4_{run_id}")
    os.makedirs(out_dir, exist_ok=True)

    tmp_inputs = os.path.join(out_dir, "of_inputs.ndjson")
    tmp_tb = os.path.join(out_dir, "tb_labels.ndjson")
    tmp_ds = os.path.join(out_dir, "dataset_mh.parquet")
    floors_json = os.path.join(out_dir, "util_floors.json")

    since_ms = get_ny_time_millis() - int(float(args.since_hours) * 3600_000)

    # Export streams
    export_stream_since(r=r, stream=inputs_stream, payload_field=inputs_field, since_ms=since_ms, out_path=tmp_inputs, max_scan=1_200_000, ts_field_guess="ts_ms")
    export_stream_since(r=r, stream=tb_stream, payload_field=tb_field, since_ms=since_ms, out_path=tmp_tb, max_scan=1_200_000, ts_field_guess="created_ms")

    # Check if we have enough data (prevent empty models)
    if not os.path.exists(tmp_tb) or os.path.getsize(tmp_tb) < 100:
        print(f"ERROR: Not enough labels found in {tmp_tb} (size: {os.path.getsize(tmp_tb) if os.path.exists(tmp_tb) else 'missing'}). Aborting.")
        exit(1)

    # Build multi-horizon dataset
    subprocess.check_call([
        "python3", "-m", "tools.build_dataset_from_inputs_tb_labels_v3_mh",
        "--inputs", tmp_inputs,
        "--tb", tmp_tb,
        "--out", tmp_ds,
        "--horizons", args.horizons,
        "--drop-no-ticks", "1",
        "--keep-scenario-raw", "1",
    ], env={**os.environ, "PYTHONPATH": ".:.."})

    ds_summary = _read_json(tmp_ds + ".json")

    # Train util mh model
    subprocess.check_call([
        "python3", "-m", "tools.train_ml_confirm_tb_util_mh_v1",
        "--dataset", tmp_ds,
        "--out-dir", out_dir,
        "--time-col", "ts_ms",
        "--horizons", args.horizons,
    ], env={**os.environ, "PYTHONPATH": ".:.."})

    meta = _read_json(os.path.join(out_dir, "meta.json"))

    # Optimize util floors
    subprocess.check_call([
        "python3", "-m", "tools.optimize_util_floor_mh_v1",
        "--dataset", tmp_ds,
        "--model", os.path.join(out_dir, "model.joblib"),
        "--out", floors_json,
        "--time-col", "ts_ms",
        "--horizons", args.horizons,
    ], env={**os.environ, "PYTHONPATH": ".:.."})

    floors = _read_json(floors_json)

    challenger_key = os.getenv("ML_CFG_CHALLENGER_KEY", "cfg:ml_confirm:challenger")
    champion_key = os.getenv("ML_CFG_CHAMPION_KEY", "cfg:ml_confirm:champion")

    challenger = {
        "kind": "util_mh_v1",
        "run_id": run_id,
        "created_ms": int(meta.get("created_ms") or get_ny_time_millis()),
        "model_path": os.path.join(out_dir, "model.joblib"),
        "meta_path": os.path.join(out_dir, "meta.json"),
        "util_floors": floors,
        "dataset_summary": ds_summary,
    }
    r.set(challenger_key, json.dumps(challenger, ensure_ascii=False, separators=(",", ":")))

    join_rate = 0.0
    try:
        join_rate = float(ds_summary.get("joined_rows", 0)) / float(ds_summary.get("inputs_rows", 1))
    except Exception:
        join_rate = 0.0

    evalm = (meta.get("metrics", {}).get("eval_last_split", {}) if isinstance(meta.get("metrics"), dict) else {})

    text = (
        f"🚀 ML TB v10.4 util_mh (next accuracy)\\n"
        f"run_id={run_id}\\n"
        f"since_hours={args.since_hours} horizons={args.horizons} unc_k={meta.get('unc_k')}\\n"
        f"dataset: joined={ds_summary.get('joined_rows')} join_rate={join_rate:.1%}\\n"
        f"eval_last_split(mae_util): {evalm}\\n"
        f"util_floor(global)={floors.get('global', {}).get('floor', 'na')} "
        f"take_rate={floors.get('global', {}).get('take_rate', 'na')} mean_util={floors.get('global', {}).get('mean_util', 'na')}\\n"
        f"floors_by_bucket={floors.get('by_bucket', {})}\\n"
        f"saved: {out_dir}\\n"
        f"Approve -> set champion ({champion_key})"
    )

    buttons = [[
        {"text": "✅ Approve", "callback": f"approve:ml_tb_v10_4:{run_id}"},
        {"text": "❌ Reject", "callback": f"reject:ml_tb_v10_4:{run_id}"},
    ]]
    send_telegram(r, text, buttons=buttons)


if __name__ == "__main__":
    main()

