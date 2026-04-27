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

import redis


def now_ms() -> int:
    return get_ny_time_millis()


def _i(x: Any, d: int = 0) -> int:
    try:
        return int(float(x))
    except Exception:
        return d


def _safe_json_dumps(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, separators=(",", ":"))


def _stream_id_ms(msg_id: str) -> int:
    try:
        return int(msg_id.split("-", 1)[0])
    except Exception:
        return 0


def _mkdirp(p: str) -> None:
    os.makedirs(p, exist_ok=True)


def _read_json(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return json.loads(f.read())


def _f(x: Any, d: float = 0.0) -> float:
    try:
        return float(x)
    except Exception:
        return d


def notify_telegram(r: redis.Redis, text: str, buttons: Optional[List[List[Dict[str, str]]]] = None) -> None:
    fields: Dict[str, str] = {"type": "report", "text": text, "ts": str(now_ms())}
    if buttons is not None:
        fields["buttons"] = _safe_json_dumps(buttons)
    r.xadd(os.getenv("NOTIFY_TELEGRAM_STREAM", "notify:telegram"), fields, maxlen=200000, approximate=True)


@dataclass
class RecsBundle:
    bundle_id: str
    sig: str
    bundle: Dict[str, Any]


def make_hset_bundle(*, cfg_key: str, changes: Dict[str, str], who: str, ttl_sec: int) -> RecsBundle:
    secret = os.getenv("RECS_HMAC_SECRET", "CHANGE_ME")
    bundle_id = secrets.token_hex(6)
    sig = hmac.new(secret.encode(), bundle_id.encode(), hashlib.sha256).hexdigest()[:8]
    ts = now_ms()
    ops = [{"op": "HSET", "key": cfg_key, "field": k, "value": str(v)} for k, v in changes.items()]
    bundle = {"id": bundle_id, "created_ms": ts, "ttl_sec": ttl_sec, "who": who, "ops": ops, "meta": {"kind": "ml_nightly_v2_stack"}}
    return RecsBundle(bundle_id=bundle_id, sig=sig, bundle=bundle)


def write_bundle(r: redis.Redis, b: RecsBundle, ttl_sec: int) -> None:
    r.set(f"recs:bundle:{b.bundle_id}", _safe_json_dumps(b.bundle), ex=ttl_sec)
    r.set(f"recs:status:{b.bundle_id}", "PENDING", ex=ttl_sec)


def export_stream_payload_ndjson(*, r: redis.Redis, stream: str, out_path: str, since_ms: int, max_scan: int,
                                 payload_field: str = "payload") -> Tuple[int, int]:
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
            raw_s = str(raw)
            if not raw_s.strip().startswith("{"):
                continue
            try:
                obj = json.loads(raw_s)
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
            rows.append(obj)
    rows.reverse()
    _mkdirp(os.path.dirname(out_path) or ".")
    with open(out_path, "w", encoding="utf-8") as f:
        for obj in rows:
            f.write(_safe_json_dumps(obj) + "\n")
    return (len(rows), scanned)


@dataclass
class CmdResult:
    code: int
    out: str
    err: str


def run_cmd(cmd: List[str], env: Optional[Dict[str, str]] = None, cwd: Optional[str] = None) -> CmdResult:
    p = subprocess.run(cmd, env=env, cwd=cwd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    return CmdResult(code=p.returncode, out=p.stdout[-8000:], err=p.stderr[-8000:])


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--workdir", default=os.getenv("ML_RUN_DIR", "/var/lib/trade/ml_runs"))
    ap.add_argument("--since-hours", type=float, default=float(os.getenv("ML_SINCE_HOURS", "168") or 168))
    ap.add_argument("--inputs-stream", default=os.getenv("OF_INPUTS_STREAM", "signals:of:inputs"))
    ap.add_argument("--events-stream", default=os.getenv("TRADE_EVENTS_STREAM", "events:trades"))
    ap.add_argument("--redis-url", default=os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0"))
    ap.add_argument("--max-scan", type=int, default=int(os.getenv("ML_EXPORT_MAX_SCAN", "600000") or 600000))
    ap.add_argument("--project-root", default=os.getenv("TRADE_PROJECT_ROOT", "/home/alex/front/trade/scanner_infra/python-worker"))
    args = ap.parse_args()

    run_id = time.strftime("%Y%m%d_%H%M%S", time.gmtime())
    run_dir = os.path.join(args.workdir, run_id)
    _mkdirp(run_dir)

    r = redis.Redis.from_url(args.redis_url, decode_responses=True)
    since_ms = now_ms() - int(args.since_hours * 3600_000)

    inputs_ndjson = os.path.join(run_dir, "of_inputs.ndjson")
    closed_ndjson = os.path.join(run_dir, "closed.ndjson")
    dataset_parquet = os.path.join(run_dir, "ml_dataset_v3.parquet")
    stack_out = os.path.join(run_dir, "stack_v1")
    _mkdirp(stack_out)

    try:
        w_in, s_in = export_stream_payload_ndjson(r=r, stream=args.inputs_stream, out_path=inputs_ndjson, since_ms=since_ms, max_scan=args.max_scan, payload_field="payload")
        w_cl, s_cl = export_stream_payload_ndjson(r=r, stream=args.events_stream, out_path=closed_ndjson, since_ms=since_ms, max_scan=args.max_scan, payload_field="payload")
    except Exception as e:
        notify_telegram(r, f"<b>ML Nightly v2: export FAILED</b>\nerr=<code>{str(e)[:400]}</code>")
        raise

    env = os.environ.copy()
    env["PYTHONPATH"] = ".:.."

    cmd_a = [sys.executable, "-m", "tools.build_dataset_from_inputs_outcomes_v3", "--inputs", inputs_ndjson, "--closed", closed_ndjson, "--out", dataset_parquet]
    ra = run_cmd(cmd_a, env=env, cwd=args.project_root)
    if ra.code != 0:
        notify_telegram(r, f"<b>ML Nightly v2: dataset build FAILED</b>\n<code>{ra.err}</code>")
        raise SystemExit(ra.code)

    ds_summary = _read_json(dataset_parquet + ".json") if os.path.exists(dataset_parquet + ".json") else {}

    cmd_s = [sys.executable, "-m", "tools.train_ml_confirm_stack_v1", "--dataset", dataset_parquet, "--out-dir", stack_out]
    rs = run_cmd(cmd_s, env=env, cwd=args.project_root)
    if rs.code != 0:
        notify_telegram(r, f"<b>ML Nightly v2: STACK train FAILED</b>\n<code>{rs.err}</code>")
        raise SystemExit(rs.code)

    meta = _read_json(os.path.join(stack_out, "meta.json"))
    m = meta.get("stack_eval_last_split", {}) or {}
    best_model = os.path.join(stack_out, "model.joblib")
    best_meta = os.path.join(stack_out, "meta.json")
    ver = f"stack_v9_{run_id}"

    text = []
    text.append("<b>ML Nightly v2 (v9 labels+stack): DONE</b>")
    text.append(f"run=<code>{run_id}</code> since_hours=<code>{args.since_hours}</code>")
    text.append(f"inputs_stream=<code>{args.inputs_stream}</code> events_stream=<code>{args.events_stream}</code>")
    text.append(f"export inputs: written={w_in} scanned={s_in}")
    text.append(f"export closed: written={w_cl} scanned={s_cl}")
    if ds_summary:
        try:
            text.append(
                "dataset_v3: joined=<code>{}</code> pos_rate=<code>{:.4f}</code> util_mean=<code>{:.4f}</code> missing_closed=<code>{}</code>".format(
                    ds_summary.get("joined_rows"),
                    float(ds_summary.get("pos_rate", 0.0)),
                    float(ds_summary.get("util_mean", 0.0)),
                    ds_summary.get("missing_closed"),
                )
            )
        except Exception:
            pass
    text.append("")
    text.append("<b>STACK eval (last split, purged+embargo)</b>")
    text.append("<code>pr_auc={:.4f} logloss={:.4f} brier={:.4f} ece={:.4f}</code>".format(
        _f(m.get("pr_auc", 0.0), 0.0),
        _f(m.get("logloss", 0.0), 0.0),
        _f(m.get("brier", 0.0), 0.0),
        _f(m.get("ece", 0.0), 0.0),
    ))
    text.append("")
    text.append(f"<b>Recommend challenger:</b> <code>STACK</code> ver=<code>{ver}</code>")
    text.append(f"model=<code>{best_model}</code>")
    text.append(f"meta=<code>{best_meta}</code>")

    cfg_key = os.getenv("ML_CONFIRM_CFG_KEY", "cfg:ml_confirm")
    ttl_sec = int(os.getenv("RECS_TTL_SEC", "86400") or 86400)
    changes = {"challenger_model_path": best_model, "challenger_meta_path": best_meta, "challenger_ver": ver, "updated_ms": str(now_ms())}
    b = make_hset_bundle(cfg_key=cfg_key, changes=changes, who="ml_nightly_train_report_v2_stack", ttl_sec=ttl_sec)
    write_bundle(r, b, ttl_sec)

    buttons = [[
        {"text": "👀 Preview diff", "callback": f"recs:preview2:{b.bundle_id}:{b.sig}"},
        {"text": "✅ Approve challenger", "callback": f"recs:confirm:{b.bundle_id}:{b.sig}"},
        {"text": "❌ Reject", "callback": f"recs:reject:{b.bundle_id}:{b.sig}"},
    ]]
    notify_telegram(r, "\n".join(text), buttons)


if __name__ == "__main__":
    main()

