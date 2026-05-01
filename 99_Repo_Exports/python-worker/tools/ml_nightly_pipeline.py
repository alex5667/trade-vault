from __future__ import annotations
\
from utils.time_utils import get_ny_time_millis

import argparse
import json
import os
import subprocess
import time
from pathlib import Path
from typing import Any, Dict, Tuple

import redis

def now_ms() -> int:
    return get_ny_time_millis()

def run(cmd: list[str]) -> None:
    p = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    if p.returncode != 0:
        raise RuntimeError(f"cmd failed: {' '.join(cmd)}\n{p.stdout}")
    if p.stdout:
        print(p.stdout)

def read_json(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def sign(bundle_id: str, secret: str) -> str:
    import hmac, hashlib
    d = hmac.new(secret.encode("utf-8"), bundle_id.encode("utf-8"), hashlib.sha256).hexdigest()
    return d[:8]

def notify(r: redis.Redis, text: str, buttons=None) -> None:
    fields = {"type": "report", "text": text, "ts": str(now_ms())}
    if buttons is not None:
        fields["buttons"] = json.dumps(buttons, ensure_ascii=False, separators=(",", ":"))
    r.xadd(os.getenv("NOTIFY_TELEGRAM_STREAM", "notify:telegram"), fields, maxlen=200000, approximate=True)

def create_rollout_bundle(r: redis.Redis, *, cfg_key: str, model_path: str, meta_path: str, model_ver: str, ttl: int, secret: str) -> Tuple[str,str]:
    import secrets
    bundle_id = secrets.token_hex(6)
    sig = sign(bundle_id, secret)
    ts = now_ms()
    ops = [
        {"op": "HSET", "key": cfg_key, "field": "model_path", "value": model_path},
        {"op": "HSET", "key": cfg_key, "field": "meta_path", "value": meta_path},
        {"op": "HSET", "key": cfg_key, "field": "model_ver", "value": model_ver},
        {"op": "HSET", "key": cfg_key, "field": "updated_ms", "value": str(ts)},
    ]
    bundle = {"id": bundle_id, "created_ms": ts, "ttl_sec": ttl, "who": "ml_nightly_pipeline", "ops": ops, "meta": {"kind":"ml_model_rollout","model_ver":model_ver}}
    r.set(f"recs:bundle:{bundle_id}", json.dumps(bundle, ensure_ascii=False, separators=(",", ":")), ex=ttl)
    r.set(f"recs:status:{bundle_id}", "PENDING", ex=ttl)
    return bundle_id, sig

def gate_candidate(meta_new: Dict[str, Any], meta_base: Dict[str, Any]) -> Tuple[bool, str]:
    """
    Simple safety gate:
      - candidate logloss not worse by >2%
      - candidate ece not worse by >0.01
      - candidate brier not worse by >0.01
    """
    m_new = meta_new.get("metrics", {})
    m_base = meta_base.get("metrics", {})
    if not m_base:
        return True, "no_baseline_metrics"
    ll_new = float(m_new.get("logloss", 1e9))
    ll_base = float(m_base.get("logloss", 1e9))
    ece_new = float(m_new.get("ece", 1e9))
    ece_base = float(m_base.get("ece", 1e9))
    br_new = float(m_new.get("brier", 1e9))
    br_base = float(m_base.get("brier", 1e9))

    if ll_new > ll_base * 1.02:
        return False, f"logloss_regress {ll_new:.4f}>{ll_base:.4f}*1.02"
    if ece_new > ece_base + 0.01:
        return False, f"ece_regress {ece_new:.4f}>{ece_base:.4f}+0.01"
    if br_new > br_base + 0.01:
        return False, f"brier_regress {br_new:.4f}>{br_base:.4f}+0.01"
    return True, "ok"

def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--redis-url", default=os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0"))
    ap.add_argument("--inputs-stream", default=os.getenv("OF_INPUTS_STREAM", "signals:of:inputs"))
    ap.add_argument("--closed-stream", default=os.getenv("TRADE_EVENTS_STREAM", "events:trades"))
    ap.add_argument("--since-hours-inputs", type=float, default=float(os.getenv("ML_PIPE_INPUTS_HOURS", "24") or 24))
    ap.add_argument("--since-hours-closed", type=float, default=float(os.getenv("ML_PIPE_CLOSED_HOURS", "168") or 168))
    ap.add_argument("--models-dir", default=os.getenv("MODELS_DIR", "/opt/models/ml_confirm"))
    ap.add_argument("--cfg-key", default=os.getenv("ML_CONFIRM_CFG_KEY", "cfg:ml_confirm"))
    ap.add_argument("--calib", choices=["sigmoid","isotonic"], default=os.getenv("ML_CALIB", "sigmoid"))
    ap.add_argument("--r-min", type=float, default=float(os.getenv("ML_LABEL_R_MIN", "0.5") or 0.5))
    ap.add_argument("--adv-max", type=float, default=float(os.getenv("ML_LABEL_ADV_MAX", "1.0") or 1.0))
    ap.add_argument("--auto-propose", action="store_true", default=True)
    args = ap.parse_args()

    r = redis.Redis.from_url(args.redis_url, decode_responses=True)

    models_dir = Path(args.models_dir)
    models_dir.mkdir(parents=True, exist_ok=True)
    run_dir = models_dir / time.strftime("run_%Y%m%d_%H%M%S")
    run_dir.mkdir(parents=True, exist_ok=True)

    inputs_path = str(run_dir / "of_inputs.ndjson")
    closed_path = str(run_dir / "closed.ndjson")
    ds_path = str(run_dir / "ml_ds.ndjson")
    model_path = str(run_dir / "ml_confirm.joblib")
    meta_path = str(run_dir / "ml_confirm_meta.json")
    preds_path = str(run_dir / "preds_candidate.ndjson")
    cmp_path = str(run_dir / "compare.json")

    # baseline pointers
    baseline_dir = models_dir / "baseline"
    baseline_dir.mkdir(parents=True, exist_ok=True)
    base_model = str(baseline_dir / "ml_confirm.joblib")
    base_meta = str(baseline_dir / "ml_confirm_meta.json")
    base_preds = str(baseline_dir / "preds_baseline.ndjson")

    # 1) export inputs/closed
    run(["python", "-m", "tools.export_of_inputs_from_redis",
         "--redis-url", args.redis_url, "--stream", args.inputs_stream,
         "--since-hours", str(args.since_hours_inputs), "--out", inputs_path])

    run(["python", "-m", "tools.export_trade_closed_from_redis",
         "--redis-url", args.redis_url, "--stream", args.closed_stream,
         "--since-hours", str(args.since_hours_closed), "--out", closed_path])

    # 2) build dataset
    run(["python", "-m", "tools.ml_build_dataset_from_ndjson_v2",
         "--inputs", inputs_path, "--closed", closed_path, "--out", ds_path,
         "--r-min", str(args.r_min), "--adv-max", str(args.adv_max)])

    # 3) train model
    run(["python", "-m", "tools.ml_train_lr_calibrated",
         "--dataset", ds_path, "--out-model", model_path, "--out-meta", meta_path,
         "--calib", args.calib])

    meta_new = read_json(meta_path)

    # 4) predict candidate
    run(["python", "-m", "tools.ml_predict_from_inputs_v2",
         "--model", model_path, "--inputs", inputs_path, "--out", preds_path,
         "--p-min", os.getenv("ML_CONFIRM_P_MIN_DEFAULT", "0.55")])

    # 5) baseline: if missing, initialize baseline and exit with info
    if not (Path(base_model).exists() and Path(base_meta).exists() and Path(base_preds).exists()):
        # set baseline = first run
        Path(base_model).write_bytes(Path(model_path).read_bytes())
        Path(base_meta).write_text(Path(meta_path).read_text(encoding="utf-8"), encoding="utf-8")
        Path(base_preds).write_text(Path(preds_path).read_text(encoding="utf-8"), encoding="utf-8")
        notify(r, "<b>ML baseline initialized</b>\n"
                 f"model=<code>{base_model}</code>\nmeta=<code>{base_meta}</code>\n"
                 f"metrics=<code>{meta_new.get('metrics',{})}</code>")
        return

    # 6) compare preds to baseline
    run(["python", "-m", "tools.ml_golden_compare_preds",
         "--baseline", base_preds, "--candidate", preds_path, "--out", cmp_path])

    cmp = read_json(cmp_path)
    meta_base = read_json(base_meta)

    ok, why = gate_candidate(meta_new, meta_base)

    # 7) propose rollout (two-phase)
    ttl = int(os.getenv("RECS_TTL_SEC", "86400") or 86400)
    secret = os.getenv("RECS_HMAC_SECRET", "CHANGE_ME")
    model_ver = run_dir.name

    if ok:
        bundle_id, sig = create_rollout_bundle(
            r,
            cfg_key=args.cfg_key,
            model_path=model_path,
            meta_path=meta_path,
            model_ver=model_ver,
            ttl=ttl,
            secret=secret,
        )
        buttons = [[
            {"text": "👀 Preview diff", "callback": f"recs:preview2:{bundle_id}:{sig}"},
            {"text": "✅ Confirm apply", "callback": f"recs:confirm:{bundle_id}:{sig}"},
            {"text": "❌ Reject", "callback": f"recs:reject:{bundle_id}:{sig}"},
        ]]
        notify(
            r,
            "<b>ML candidate ready</b>\n"
            f"ver=<code>{model_ver}</code>\n"
            f"gate=<code>PASS</code> why=<code>{why}</code>\n"
            f"test_metrics=<code>{meta_new.get('metrics',{})}</code>\n"
            f"baseline_metrics=<code>{meta_base.get('metrics',{})}</code>\n"
            f"compare=<code>{{'ks':{cmp.get('ks')},'p_new':{cmp.get('p_new')},'p_base':{cmp.get('p_base')}}}</code>\n"
            f"bundle_id=<code>{bundle_id}</code>\n",
            buttons=buttons,
        )
    else:
        notify(
            r,
            "<b>ML candidate blocked by safety gate</b>\n"
            f"ver=<code>{model_ver}</code>\n"
            f"gate=<code>FAIL</code> why=<code>{why}</code>\n"
            f"test_metrics=<code>{meta_new.get('metrics',{})}</code>\n"
            f"baseline_metrics=<code>{meta_base.get('metrics',{})}</code>\n"
            f"compare=<code>{{'ks':{cmp.get('ks')},'p_new':{cmp.get('p_new')},'p_base':{cmp.get('p_base')}}}</code>\n"
        )

if __name__ == "__main__":
    main()
