from __future__ import annotations
from utils.time_utils import get_ny_time_millis

import argparse
import json
import os
import sys
import time
import hmac
import hashlib
import secrets
import subprocess
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple
import html

import redis

from services.ml_calibration import fit_platt_logit, brier_score, ece_score, logloss


# -----------------------------
# Utils
# -----------------------------

def now_ms() -> int:
    return get_ny_time_millis()


def _i(x: Any, d: int = 0) -> int:
    try:
        return int(float(x))
    except Exception:
        return d


def _f(x: Any, d: float = 0.0) -> float:
    try:
        return float(x)
    except Exception:
        return d


def _safe_json_dumps(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, separators=(",", ":"))


def _stream_id_ms(msg_id: str) -> int:
    # Redis stream ID: "1700000000000-0"
    try:
        return int(msg_id.split("-", 1)[0])
    except Exception:
        return 0


def _mkdirp(p: str) -> None:
    os.makedirs(p, exist_ok=True)


def _read_json(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return json.loads(f.read())


def _write_text(path: str, s: str) -> None:
    with open(path, "w", encoding="utf-8") as f:
        f.write(s)


# -----------------------------
# Telegram notify (via Redis stream)
# -----------------------------

def notify_telegram(r: redis.Redis, text: str, buttons: Optional[List[List[Dict[str, str]]]] = None) -> None:
    fields: Dict[str, str] = {
        "type": "report",
        "text": text,
        "ts": str(now_ms()),
    }
    if buttons is not None:
        fields["buttons"] = _safe_json_dumps(buttons)
    stream = os.getenv("NOTIFY_TELEGRAM_STREAM", "notify:telegram")
    r.xadd(stream, fields, maxlen=200000, approximate=True)


# -----------------------------
# Recs bundle (Approve/Reject)
# -----------------------------

@dataclass
class RecsBundle:
    bundle_id: str
    sig: str
    bundle: Dict[str, Any]


def recs_sign(bundle_id: str, secret: str) -> str:
    return hmac.new(secret.encode(), bundle_id.encode(), hashlib.sha256).hexdigest()[:8]


def make_hset_bundle(*, cfg_key: str, changes: Dict[str, str], who: str, ttl_sec: int) -> RecsBundle:
    secret = os.getenv("RECS_HMAC_SECRET", "CHANGE_ME")
    bundle_id = secrets.token_hex(6)
    sig = recs_sign(bundle_id, secret)
    ts = now_ms()
    ops = [{"op": "HSET", "key": cfg_key, "field": k, "value": str(v)} for k, v in changes.items()]
    bundle = {
        "id": bundle_id,
        "created_ms": ts,
        "ttl_sec": ttl_sec,
        "who": who,
        "ops": ops,
        "meta": {"kind": "ml_train_register_challenger_v1"},
    }
    return RecsBundle(bundle_id=bundle_id, sig=sig, bundle=bundle)


def write_bundle(r: redis.Redis, b: RecsBundle, ttl_sec: int) -> None:
    r.set(f"recs:bundle:{b.bundle_id}", _safe_json_dumps(b.bundle), ex=ttl_sec)
    r.set(f"recs:status:{b.bundle_id}", "PENDING", ex=ttl_sec)


# -----------------------------
# Exporters from Redis streams -> NDJSON
# -----------------------------

def export_of_inputs_ndjson(
    *,
    r: redis.Redis,
    stream: str,
    out_path: str,
    since_ms: int,
    max_scan: int,
    payload_field: str = "payload",
) -> Tuple[int, int]:
    """
    Reads signals:of:inputs backwards, writes NDJSON in chronological order.
    Each line is JSON of the payload (expanded), with fallback ts_ms from stream-id.
    Returns (written, scanned).
    """
    scanned = 0
    rows: List[Dict[str, Any]] = []
    last_id = "+"

    while scanned < max_scan:
        batch = r.xrevrange(stream, max=last_id, min="-", count=2000)
        if not batch:
            break
        if len(batch) == 1 and batch[0][0] == last_id:
            break
        for msg_id, fields in batch:
            scanned += 1
            if msg_id == last_id:
                continue
            last_id = msg_id

            if not isinstance(fields, dict):
                continue

            raw = fields.get(payload_field)
            if not raw:
                continue
            if isinstance(raw, bytes):
                try:
                    raw = raw.decode("utf-8", "ignore")
                except Exception:
                    continue

            try:
                obj = json.loads(raw) if isinstance(raw, str) else {}
            except Exception:
                continue

            if not isinstance(obj, dict):
                continue

            ts = _i(obj.get("ts_ms", obj.get("ts", 0)), 0)
            if ts <= 0:
                ts = _stream_id_ms(msg_id)
                obj["ts_ms"] = ts

            if ts and ts < since_ms:
                scanned = max_scan
                break

            if "sid" not in obj:
                # Heuristic to reconstruct sid from symbol+time (matches older strategy logic)
                sym = str(obj.get("symbol") or "")
                ts_val = obj.get("ts_ms") or obj.get("ts")
                if sym and ts_val:
                    obj["sid"] = f"crypto-of:{sym}:{ts_val}"

            rows.append(obj)

    rows.reverse()
    _mkdirp(os.path.dirname(out_path) or ".")
    with open(out_path, "w", encoding="utf-8") as f:
        for obj in rows:
            f.write(_safe_json_dumps(obj) + "\n")

    return (len(rows), scanned)


def _is_closed_event(obj: Dict[str, Any]) -> bool:
    et = str(obj.get("event_type", obj.get("type", "")) or "").upper()
    return et in ("POSITION_CLOSED", "CLOSE")


def export_trades_closed_ndjson(
    *,
    r: redis.Redis,
    stream: str,
    out_path: str,
    since_ms: int,
    max_scan: int,
    payload_field: str = "payload",
) -> Tuple[int, int]:
    """
    Reads events:trades backwards, filters POSITION_CLOSED/CLOSE, writes NDJSON chronological.
    Handles both root fields and payload JSON string.
    Returns (written, scanned).
    """
    scanned = 0
    rows: List[Dict[str, Any]] = []
    last_id = "+"

    while scanned < max_scan:
        batch = r.xrevrange(stream, max=last_id, min="-", count=2000)
        if not batch:
            break
        if len(batch) == 1 and batch[0][0] == last_id:
            break
        for msg_id, fields in batch:
            scanned += 1
            if msg_id == last_id:
                continue
            last_id = msg_id

            if not isinstance(fields, dict):
                continue

            obj: Dict[str, Any] = dict(fields)

            # expand payload if present
            raw = fields.get(payload_field)
            if raw and isinstance(raw, (str, bytes)):
                if isinstance(raw, bytes):
                    try:
                        raw = raw.decode("utf-8", "ignore")
                    except Exception:
                        raw = ""
                raw_s = str(raw)
                if raw_s.strip().startswith("{"):
                    try:
                        p = json.loads(raw_s)
                        if isinstance(p, dict):
                            obj.update(p)
                    except Exception:
                        pass

            if not _is_closed_event(obj):
                continue

            ts = _i(obj.get("ts_ms", obj.get("ts", obj.get("timestamp", 0))), 0)
            if ts <= 0:
                ts = _stream_id_ms(msg_id)
                obj["ts_ms"] = ts

            if ts and ts < since_ms:
                scanned = max_scan
                break

            # must have sid to join
            if not str(obj.get("sid", "") or ""):
                continue

            rows.append(obj)

    rows.reverse()
    _mkdirp(os.path.dirname(out_path) or ".")
    with open(out_path, "w", encoding="utf-8") as f:
        for obj in rows:
            f.write(_safe_json_dumps(obj) + "\n")

    return (len(rows), scanned)


# -----------------------------
# Subprocess runner for existing tools
# -----------------------------

@dataclass
class CmdResult:
    code: int
    out: str
    err: str


def run_cmd(cmd: List[str], env: Optional[Dict[str, str]] = None, cwd: Optional[str] = None) -> CmdResult:
    p = subprocess.run(
        cmd,
        env=env,
        cwd=cwd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    return CmdResult(code=p.returncode, out=p.stdout[-8000:], err=p.stderr[-8000:])


def choose_best_model(lr_meta: Dict[str, Any], gbdt_meta: Dict[str, Any]) -> str:
    # Primary: lower Brier; tie-breaker: higher PR-AUC; then lower ECE
    lr = lr_meta.get("mean", {}) or {}
    gb = gbdt_meta.get("mean", {}) or {}

    lr_brier = _f(lr.get("brier", 1.0), 1.0)
    gb_brier = _f(gb.get("brier", 1.0), 1.0)
    if gb_brier + 1e-12 < lr_brier:
        return "gbdt"
    if lr_brier + 1e-12 < gb_brier:
        return "lr"

    lr_pr = _f(lr.get("pr_auc", 0.0), 0.0)
    gb_pr = _f(gb.get("pr_auc", 0.0), 0.0)
    if gb_pr > lr_pr + 1e-12:
        return "gbdt"
    if lr_pr > gb_pr + 1e-12:
        return "lr"

    lr_ece = _f(lr.get("ece", 1.0), 1.0)
    gb_ece = _f(gb.get("ece", 1.0), 1.0)
    return "gbdt" if gb_ece < lr_ece else "lr"


def format_model_summary(name: str, meta: Dict[str, Any]) -> str:
    mean = meta.get("mean", {}) or {}
    return (
        f"{name}: "
        f"pr_auc={_f(mean.get('pr_auc', 0.0), 0.0):.4f} | "
        f"logloss={_f(mean.get('logloss', 0.0), 0.0):.4f} | "
        f"brier={_f(mean.get('brier', 0.0), 0.0):.4f} | "
        f"ece={_f(mean.get('ece', 0.0), 0.0):.4f}"
    )


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--workdir", default=os.getenv("ML_RUN_DIR", "/var/lib/trade/ml_runs"))
    ap.add_argument("--since-hours", type=float, default=float(os.getenv("ML_SINCE_HOURS", "168") or 168))
    ap.add_argument("--inputs-stream", default=os.getenv("OF_INPUTS_STREAM", "signals:crypto:raw"))
    ap.add_argument("--events-stream", default=os.getenv("TRADE_EVENTS_STREAM", "events:trades"))
    ap.add_argument("--redis-url", default=os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0"))
    ap.add_argument("--max-scan", type=int, default=int(os.getenv("ML_EXPORT_MAX_SCAN", "600000") or 600000))
    ap.add_argument("--r-min", type=float, default=float(os.getenv("ML_LABEL_R_MIN", "0.5") or 0.5))
    ap.add_argument("--project-root", default=os.getenv("TRADE_PROJECT_ROOT", "/home/alex/front/trade/scanner_infra/python-worker"))
    args = ap.parse_args()

    run_id = time.strftime("%Y%m%d_%H%M%S", time.gmtime())
    run_dir = os.path.join(args.workdir, run_id)
    _mkdirp(run_dir)

    r = redis.Redis.from_url(args.redis_url, decode_responses=True)

    since_ms = now_ms() - int(args.since_hours * 3600_000)

    inputs_ndjson = os.path.join(run_dir, "of_inputs.ndjson")
    closed_ndjson = os.path.join(run_dir, "closed.ndjson")
    dataset_parquet = os.path.join(run_dir, "ml_dataset.parquet")

    lr_out = os.path.join(run_dir, "lr_v2")
    gbdt_out = os.path.join(run_dir, "gbdt_v2")
    _mkdirp(lr_out)
    _mkdirp(gbdt_out)

    try:
        w_in, s_in = export_of_inputs_ndjson(
            r=r,
            stream=args.inputs_stream,
            out_path=inputs_ndjson,
            since_ms=since_ms,
            max_scan=args.max_scan,
            payload_field="payload",
        )
        w_cl, s_cl = export_trades_closed_ndjson(
            r=r,
            stream=args.events_stream,
            out_path=closed_ndjson,
            since_ms=since_ms,
            max_scan=args.max_scan,
            payload_field="payload",
        )
    except Exception as e:
        notify_telegram(r, f"<b>ML Nightly: export FAILED</b>\\nerr=<code>{html.escape(str(e)[:400])}</code>")
        raise

    # Also drop copies to /tmp for compatibility/debug
    try:
        _write_text("/tmp/of_inputs.ndjson", open(inputs_ndjson, "r", encoding="utf-8").read())
        _write_text("/tmp/closed_7d.ndjson", open(closed_ndjson, "r", encoding="utf-8").read())
    except Exception:
        pass

    env = os.environ.copy()
    env["PYTHONPATH"] = ".:.."

    # A) build dataset
    cmd_a = [sys.executable, "-m", "tools.build_dataset_from_inputs_outcomes_v2",
             "--inputs", inputs_ndjson, "--closed", closed_ndjson, "--out", dataset_parquet, "--r-min", str(args.r_min)]
    ra = run_cmd(cmd_a, env=env, cwd=args.project_root)
    if ra.code != 0:
        notify_telegram(r, f"<b>ML Nightly: dataset build FAILED</b>\\n<code>{html.escape(ra.err)}</code>")
        raise SystemExit(ra.code)

    ds_summary_path = dataset_parquet + ".json"
    ds_summary = _read_json(ds_summary_path) if os.path.exists(ds_summary_path) else {}
    
    joined_rows = int(ds_summary.get("joined_rows", 0))
    if joined_rows < 50:
        msg = f"<b>ML Nightly: SKIPPED (Not enough data)</b>\\njoined_rows={joined_rows} &lt; 50\\nCheck DN-GATE thresholds or market activity."
        notify_telegram(r, msg)
        print(msg)
        return

    # B) train LR
    cmd_b = [sys.executable, "-m", "tools.train_ml_confirm_lr_v2",
             "--dataset", dataset_parquet, "--out-dir", lr_out, "--time-col", "ts_ms"]
    rb = run_cmd(cmd_b, env=env, cwd=args.project_root)
    if rb.code != 0:
        notify_telegram(r, f"<b>ML Nightly: LR train FAILED</b>\\n<code>{html.escape(rb.err)}</code>")
        raise SystemExit(rb.code)

    # C) train GBDT
    cmd_c = [sys.executable, "-m", "tools.train_ml_confirm_gbdt_v2",
             "--dataset", dataset_parquet, "--out-dir", gbdt_out, "--time-col", "ts_ms"]
    rc = run_cmd(cmd_c, env=env, cwd=args.project_root)
    if rc.code != 0:
        notify_telegram(r, f"<b>ML Nightly: GBDT train FAILED</b>\\n<code>{html.escape(rc.err)}</code>")
        raise SystemExit(rc.code)

    lr_meta = _read_json(os.path.join(lr_out, "meta.json"))
    gb_meta = _read_json(os.path.join(gbdt_out, "meta.json"))

    best = choose_best_model(lr_meta, gb_meta)
    best_dir = gbdt_out if best == "gbdt" else lr_out
    best_model = os.path.join(best_dir, "model.joblib")
    best_meta = os.path.join(best_dir, "meta.json")

    ver = f"{best}_v2_{run_id}"

    # ------------------------------------------------------------
    # Calibration layer (Platt on logit(p))
    # ------------------------------------------------------------
    # Load validation data from last fold for calibration
    # We need to recompute predictions on validation set to get raw probabilities
    try:
        import joblib
        import numpy as np
        import pandas as pd
        from core.ml_feature_schema_v2 import MLFeatureSchemaV2
        from ml_core.purged_cv import purged_kfold_time_series
        from sklearn.model_selection import TimeSeriesSplit

        df = pd.read_parquet(dataset_parquet).sort_values("ts_ms")
        schema = MLFeatureSchemaV2()
        X = np.array([schema.vectorize_row(r) for r in df.to_dict(orient="records")], dtype=np.float32)
        y = df["y"].astype(int).to_numpy()

        # Get last fold indices (same logic as in train scripts)
        use_purged = os.getenv("ML_PURGED_CV_ENABLE", "1").strip().lower() in {"1", "true", "yes"}
        embargo_ms = int(os.getenv("ML_PURGED_CV_EMBARGO_MS", "60000"))
        has_t1 = "tb_t_hit_ms" in df.columns

        if use_purged and has_t1:
            folds = purged_kfold_time_series(
                ts_ms=df["ts_ms"].astype("int64").to_numpy(),
                t1_ms=df["tb_t_hit_ms"].astype("int64").to_numpy(),
                n_splits=5,
                embargo_ms=int(embargo_ms),
            )
            last_fold = folds[-1] if folds else None
            if last_fold:
                val_idx = last_fold.test_idx
            else:
                val_idx = []
        else:
            tscv = TimeSeriesSplit(n_splits=5)
            splits = list(tscv.split(X))
            if splits:
                _, val_idx = splits[-1]
            else:
                val_idx = []

        if len(val_idx) > 0:
            # Load best model and get predictions
            model = joblib.load(best_model)
            
            # Get predictions from model (these are already calibrated by CalibratedClassifierCV)
            # We treat them as "raw" for our additional calibration layer
            # In practice, this adds a second calibration layer on top of sklearn's Platt scaling
            p_model = model.predict_proba(X[val_idx])[:, 1]
            
            y_val = y[val_idx].tolist()
            p_raw_list = [float(p) for p in p_model]

            # Fit additional calibrator (Platt on logit space)
            # This can further improve calibration or adapt to distribution shift
            cal = fit_platt_logit(p_raw_list, y_val, l2=1e-3, max_iter=50)
            p_cal = cal.apply(p_raw_list)

            # Compute metrics
            brier_raw = brier_score(p_raw_list, y_val)
            brier_cal = brier_score(p_cal, y_val)
            ece_raw, bins_raw = ece_score(p_raw_list, y_val, n_bins=15)
            ece_cal, bins_cal = ece_score(p_cal, y_val, n_bins=15)
            ll_raw = logloss(p_raw_list, y_val)
            ll_cal = logloss(p_cal, y_val)

            # Store calibration results in best_meta for reporting
            best_meta_obj = _read_json(best_meta)
            best_meta_obj["calibration"] = {
                "type": "platt_logit",
                "params": cal.to_dict(),
                "metrics": {
                    "brier_raw": float(brier_raw),
                    "brier_cal": float(brier_cal),
                    "ece_raw": float(ece_raw),
                    "ece_cal": float(ece_cal),
                    "logloss_raw": float(ll_raw),
                    "logloss_cal": float(ll_cal),
                },
                "bins_raw": bins_raw,
                "bins_cal": bins_cal,
            }
            with open(best_meta, "w", encoding="utf-8") as f:
                json.dump(best_meta_obj, f, ensure_ascii=False, indent=2)
        else:
            cal = None
    except Exception as e:
        # If calibration fails, continue without it (non-critical)
        cal = None
        import traceback
        print(f"WARNING: Calibration failed: {e}", file=sys.stderr)
        traceback.print_exc()

    # Compose analysis
    text = []
    text.append("<b>ML Nightly Train: DONE</b>")
    text.append(f"run=<code>{run_id}</code> since_hours=<code>{args.since_hours}</code>")
    text.append(f"inputs_stream=<code>{args.inputs_stream}</code> events_stream=<code>{args.events_stream}</code>")
    text.append(f"export inputs: written={w_in} scanned={s_in}")
    text.append(f"export closed: written={w_cl} scanned={s_cl}")
    if ds_summary:
        try:
            text.append(
                "dataset: joined=<code>{}</code> pos_rate=<code>{:.4f}</code> missing_closed=<code>{}</code>".format(
                    ds_summary.get("joined_rows"),
                    float(ds_summary.get("pos_rate", 0.0)),
                    ds_summary.get("missing_closed"),
                )
            )
        except Exception:
            pass
    text.append("")
    text.append("<b>CV mean metrics</b>")
    text.append(f"<code>{format_model_summary('LR', lr_meta)}</code>")
    text.append(f"<code>{format_model_summary('GBDT', gb_meta)}</code>")
    text.append("")
    text.append(f"<b>Recommend challenger:</b> <code>{best.upper()}</code> ver=<code>{ver}</code>")
    text.append(f"model=<code>{best_model}</code>")
    text.append(f"meta=<code>{best_meta}</code>")

    # ------------------------------------------------------------
    # Embed calibrator into champion cfg (so MLConfirmGate uses it)
    # ------------------------------------------------------------
    # champion_cfg is the JSON written to cfg:ml_confirm:champion
    champion_cfg = _read_json(best_meta)
    if cal is not None:
        try:
            champion_cfg["calibrator"] = cal.to_dict()
            # enable calibration by default for probability outputs
            champion_cfg["calibrate_p_edge"] = True
            if "calibration" in champion_cfg and "metrics" in champion_cfg["calibration"]:
                champion_cfg["calibration_metrics"] = champion_cfg["calibration"]["metrics"]
        except Exception:
            pass

    # Register challenger via recs bundle (Approve/Reject)
    cfg_key = os.getenv("ML_CONFIRM_CFG_KEY", "cfg:ml_confirm")
    ttl_sec = int(os.getenv("RECS_TTL_SEC", "86400") or 86400)

    changes = {
        "challenger_model_path": best_model,
        "challenger_meta_path": best_meta,
        "challenger_ver": ver,
        "updated_ms": str(now_ms()),
    }
    b = make_hset_bundle(cfg_key=cfg_key, changes=changes, who="ml_nightly_train_and_report_v1", ttl_sec=ttl_sec)
    write_bundle(r, b, ttl_sec=ttl_sec)

    buttons = [[
        {"text": "👀 Preview diff", "callback": f"recs:preview2:{b.bundle_id}:{b.sig}"},
        {"text": "✅ Approve challenger", "callback": f"recs:confirm:{b.bundle_id}:{b.sig}"},
        {"text": "❌ Reject", "callback": f"recs:reject:{b.bundle_id}:{b.sig}"},
    ]]

    notify_telegram(r, "\n".join(text), buttons)


if __name__ == "__main__":
    main()

