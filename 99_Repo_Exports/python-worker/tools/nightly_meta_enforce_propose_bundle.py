"""Nightly meta ENFORCE proposal with safety gates.

Offline pipeline:
1. Export inputs from stream
2. Filter canary symbols
3. Engine replay
4. Export trades
5. Build dataset (join replay + trades)
6. Evaluate meta_p_min thresholds
7. Check safety gates (streak + 24h health)
8. Create bundle for ENFORCE mode (manual confirm via Telegram)

Usage:
  python -m tools.nightly_meta_enforce_propose_bundle
  (reads ENV vars for streams, paths, thresholds)
"""

from __future__ import annotations
from utils.time_utils import get_ny_time_millis

import argparse
import glob
import json
import os
import secrets
import subprocess
import sys
import time
import hmac
import hashlib
from typing import Any, Dict, List

import redis

from common.log import setup_logger

from core.ok_fields import get_ts_ms
from tools.of_gate_metrics_contract import derive_ok_fields, is_gate_row, scenario_key

logger = setup_logger("NightlyMetaEnforcePropose")


def now_ms() -> int:
    """Returns current timestamp in milliseconds (epoch)."""
    return get_ny_time_millis()


def sign(bid: str, secret: str) -> str:
    """Generates short HMAC signature for bundle_id (8 hex characters)."""
    d = hmac.new(secret.encode("utf-8"), bid.encode("utf-8"), hashlib.sha256).hexdigest()
    return d[:8]


def _read_metrics_window(r: redis.Redis, stream: str, since_ms: int, max_scan: int) -> list[dict]:
    """
    Read metrics from Redis stream backwards until timestamp < since_ms.
    Returns rows in chronological order (oldest first).
    
    Args:
        r: Redis client
        stream: Stream name (e.g. "metrics:of_gate")
        since_ms: Minimum timestamp (ms) to read from
        max_scan: Maximum messages to scan
        
    Returns:
        List of metric dictionaries
    """
    rows = []
    last_id = "+"
    scanned = 0
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
            ts = get_ts_ms(fields)
            if ts and ts < since_ms:
                scanned = max_scan
                break
            rows.append(dict(fields))
    rows.reverse()
    return rows


def _pctl(xs: list[float], q: float) -> float:
    """
    Compute percentile from sorted list.
    
    Args:
        xs: List of float values
        q: Quantile (0.0 to 1.0)
        
    Returns:
        Percentile value
    """
    if not xs:
        return 0.0
    xs = sorted(xs)
    i = int(round((len(xs) - 1) * q))
    i = max(0, min(len(xs) - 1, i))
    return float(xs[i])


def _metrics_health(rows: list[dict]) -> dict:
    """
    Compute health metrics from metric rows.
    
    Args:
        rows: List of metric dictionaries from stream
        
    Returns:
        Dictionary with health metrics: n, ok_rate, soft_rate, lat_p99_us, exec_p90
    """
    gate_rows = [r for r in rows if is_gate_row(r)]
    valid_rows = [r for r in gate_rows if scenario_key(r) != "dn_veto"]
    n = len(valid_rows)
    if n == 0:
        return {"n": 0}
    ok = 0
    soft = 0
    lat = []
    ex = []
    
    for r in gate_rows:
        try:
            lat.append(float(r.get("latency_us", 0.0) or 0.0))
        except Exception:
            pass
        try:
            ex.append(float(r.get("exec_risk_norm", 0.0) or 0.0))
        except Exception:
            pass

    for r in valid_rows:
        ok_i, soft_i, _, _ = derive_ok_fields(r)
        ok += ok_i
        soft += soft_i
    return {
        "n": n,
        "n_total": len(gate_rows),
        "ok_rate": ok / n if n > 0 else 0.0,
        "soft_rate": soft / n if n > 0 else 0.0,
        "lat_p99_us": _pctl(lat, 0.99),
        "exec_p90": _pctl(ex, 0.90),
    }


def main() -> None:
    """Main entry point: build dataset, evaluate thresholds, check gates, propose bundle."""
    ap = argparse.ArgumentParser(description="Nightly meta ENFORCE proposal with safety gates")
    ap.add_argument("--since-hours", type=float, default=float(os.getenv("META_ENFORCE_SINCE_HOURS", "168") or 168))
    ap.add_argument("--canary-symbols", default=os.getenv("CANARY_SYMBOLS", "BTCUSDT,ETHUSDT"))
    ap.add_argument("--out-dir", default=os.getenv("OUT_DIR", "/var/lib/trade/of_reports/out"))
    ap.add_argument("--model-path", default=os.getenv("META_MODEL_PATH", ""))
    ap.add_argument("--models-latest", default=os.getenv("META_MODEL_LATEST", ""))  # optional stable path
    ap.add_argument("--min-streak", type=int, default=int(os.getenv("META_ENFORCE_MIN_STREAK", os.getenv("BASELINE_PROPOSE_MIN_STREAK", "3")) or 3))
    ap.add_argument("--notify-on-skip", type=int, default=int(os.getenv("META_ENFORCE_NOTIFY_ON_SKIP", "0") or 0))
    args = ap.parse_args()

    redis_url = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
    r = redis.Redis.from_url(redis_url, decode_responses=True)

    # -------- gate 1: regress PASS streak ----------
    streak_key = os.getenv("REGRESS_PASS_STREAK_KEY", "sre:regress:pass_streak")
    last_status_key = os.getenv("REGRESS_LAST_STATUS_KEY", "sre:regress:last_status")
    last_ts_key = os.getenv("REGRESS_LAST_TS_KEY", "sre:regress:last_ts_ms")
    max_age_h = float(os.getenv("BASELINE_PROPOSE_MAX_AGE_HOURS", "30") or 30.0)

    try:
        streak = int(r.get(streak_key) or "0")
    except Exception:
        streak = 0
    last_status = str(r.get(last_status_key) or "")
    try:
        last_ts = int(r.get(last_ts_key) or "0")
    except Exception:
        last_ts = 0
    age_ok = True
    if last_ts > 0:
        age_ok = (now_ms() - last_ts) <= int(max_age_h * 3600_000)

    if not (last_status == "PASS" and age_ok and streak >= args.min_streak):
        if args.notify_on_skip == 1:
            r.xadd(os.getenv("NOTIFY_TELEGRAM_STREAM", "notify:telegram"), {
                "type": "report",
                "text": f"<b>Meta ENFORCE skipped</b>\nreason=<code>streak_gate</code>\nstreak=<code>{streak}</code> need=<code>{args.min_streak}</code> last=<code>{last_status}</code> age_ok=<code>{int(age_ok)}</code>",
                "ts": str(now_ms()),
            }, maxlen=200000, approximate=True)
        return

    # -------- gate 2: 24h health ----------
    metrics_stream = os.getenv("OF_GATE_METRICS_STREAM", "metrics:of_gate")
    win_h = float(os.getenv("BASELINE_PROPOSE_HEALTH_WINDOW_HOURS", "24") or 24)
    min_n = int(os.getenv("BASELINE_PROPOSE_MIN_N", "200") or 200)

    lat_cap = float(os.getenv("BASELINE_PROPOSE_LAT_P99_US_MAX", "4000") or 4000)
    exec_cap = float(os.getenv("BASELINE_PROPOSE_EXEC_P90_MAX", "0.85") or 0.85)
    soft_cap = float(os.getenv("BASELINE_PROPOSE_SOFT_RATE_MAX", "0.35") or 0.35)
    ok_floor = float(os.getenv("BASELINE_PROPOSE_OK_RATE_MIN", "0.20") or 0.20)

    rows = _read_metrics_window(r, metrics_stream, now_ms() - int(win_h * 3600_000), max_scan=int(os.getenv("BASELINE_PROPOSE_MAX_SCAN", "400000") or 400000))
    mh = _metrics_health(rows)

    health_reasons = []
    if mh.get("n", 0) < min_n:
        health_reasons.append(f"low_n={mh.get('n',0)}<{min_n}")
    else:
        if float(mh.get("lat_p99_us", 0.0)) > lat_cap:
            health_reasons.append(f"lat_p99={mh.get('lat_p99_us',0.0):.0f}>{lat_cap:.0f}")
        if float(mh.get("exec_p90", 0.0)) > exec_cap:
            health_reasons.append(f"exec_p90={mh.get('exec_p90',0.0):.2f}>{exec_cap:.2f}")
        if float(mh.get("soft_rate", 0.0)) > soft_cap:
            health_reasons.append(f"soft_rate={mh.get('soft_rate',0.0):.2f}>{soft_cap:.2f}")
        if float(mh.get("ok_rate", 0.0)) < ok_floor:
            health_reasons.append(f"ok_rate={mh.get('ok_rate',0.0):.2f}<{ok_floor:.2f}")

    if health_reasons:
        if args.notify_on_skip == 1:
            r.xadd(os.getenv("NOTIFY_TELEGRAM_STREAM", "notify:telegram"), {
                "type": "report",
                "text": f"<b>Meta ENFORCE skipped</b>\nreason=<code>health_gate</code>\nreasons=<code>{','.join(health_reasons)}</code>\nmetrics=<code>{mh}</code>",
                "ts": str(now_ms()),
            }, maxlen=200000, approximate=True)
        return

    # -------- resolve model path ----------
    model_path = (args.models_latest or "").strip() or (args.model_path or "").strip()
    if not model_path:
        # try latest from MODELS_DIR
        models_dir = os.getenv("MODELS_DIR", "/var/lib/trade/of_reports/models")
        try:
            cand = sorted(glob.glob(os.path.join(models_dir, "meta_lr_*.json")))
            if cand:
                model_path = cand[-1]
        except Exception:
            pass
    if not model_path:
        raise SystemExit("meta_model_path_missing (set META_MODEL_PATH or META_MODEL_LATEST)")

    # -------- build dataset offline ----------
    ts = time.strftime("%Y%m%d_%H%M%S")
    run_dir = f"{args.out_dir}/meta_enforce_{ts}"
    os.makedirs(run_dir, exist_ok=True)

    inputs_raw = f"{run_dir}/of_inputs_raw.ndjson"
    inputs_can = f"{run_dir}/of_inputs_canary.ndjson"
    replay_out = f"{run_dir}/of_replay_engine.ndjson"
    trades_out = f"{run_dir}/closed_trades.ndjson"
    dataset_out = f"{run_dir}/dataset.ndjson"
    eval_out = f"{run_dir}/eval.json"

    inputs_stream = os.getenv("OF_INPUTS_STREAM", "signals:of:inputs")
    inputs_field = os.getenv("OF_INPUTS_STREAM_FIELD", "payload")
    trades_stream = os.getenv("TRADE_EVENTS_STREAM", "events:trades")

    # export inputs
    logger.info("Exporting inputs from stream=%s", inputs_stream)
    subprocess.check_call([
        sys.executable, "-m", "tools.export_of_inputs_ndjson_v2",
        "--redis-url", redis_url,
        "--out", inputs_raw,
        "--stream", inputs_stream,
        "--field", inputs_field,
        "--state-file", os.getenv("STATE_FILE", f"{args.out_dir}/of_inputs.state"),
        "--resume",
    ])

    # filter canary symbols
    allow = {s.strip().upper() for s in args.canary_symbols.split(",") if s.strip()}
    n = 0
    with open(inputs_raw, "r", encoding="utf-8") as f, open(inputs_can, "w", encoding="utf-8") as g:
        for line in f:
            if not line.strip():
                continue
            row = json.loads(line)
            p = row.get("payload")
            inp = json.loads(p) if isinstance(p, str) else (p if isinstance(p, dict) else row)
            sym = str(inp.get("symbol", "")).upper()
            if sym in allow:
                g.write(json.dumps(inp, ensure_ascii=False) + "\n")
                n += 1
    if n < 2000:
        raise SystemExit(f"too_few_inputs_canary n={n}")

    # engine replay
    logger.info("Running engine replay on %s", inputs_can)
    subprocess.check_call([sys.executable, "-m", "tools.of_engine_replay_from_inputs", "--inputs", inputs_can, "--out", replay_out])

    # export trades
    logger.info("Exporting trades from stream=%s", trades_stream)
    subprocess.check_call([
        sys.executable, "tools/export_trade_closed_ndjson.py",
        "--since-hours", str(args.since_hours),
        "--out", trades_out,
        "--stream", trades_stream,
        "--redis-url", redis_url,
        "--max-scan", os.getenv("TRADES_MAX_SCAN", "500000"),
    ])

    # dataset join
    logger.info("Building dataset from replay and trades")
    subprocess.check_call([sys.executable, "-m", "tools.build_of_dataset", "--replay", replay_out, "--trades", trades_out, "--out", dataset_out])

    # eval thresholds
    logger.info("Evaluating meta_p_min thresholds")
    subprocess.check_call([
        sys.executable, "-m", "tools.eval_meta_enforce",
        "--dataset", dataset_out,
        "--model", model_path,
        "--out", eval_out,
    ])

    ev = json.loads(open(eval_out, "r", encoding="utf-8").read()).get("best")
    if not ev:
        # no valid threshold -> no promotion
        r.xadd(os.getenv("NOTIFY_TELEGRAM_STREAM", "notify:telegram"), {
            "type": "report",
            "text": f"<b>Meta ENFORCE proposal</b>\nstatus=<code>NO_OP</code>\nreason=<code>no_valid_threshold</code>\nmodel=<code>{model_path}</code>",
            "ts": str(now_ms()),
        }, maxlen=200000, approximate=True)
        return

    meta_p_min = float(ev["meta_p_min"])

    # -------- create recs bundle (manual confirm) ----------
    secret = os.getenv("RECS_HMAC_SECRET", "CHANGE_ME")
    ttl = int(os.getenv("RECS_TTL_SEC", "86400") or 86400)
    prefix = os.getenv("CFG_HASH_PREFIX", "config:orderflow:")

    bundle_id = secrets.token_hex(6)
    sig = sign(bundle_id, secret)

    ops = []
    for sym in sorted(list(allow)):
        ops += [
            {"op": "HSET", "key": f"{prefix}{sym}", "field": "meta_model_enable", "value": "1"},
            {"op": "HSET", "key": f"{prefix}{sym}", "field": "meta_model_mode", "value": "ENFORCE"},
            {"op": "HSET", "key": f"{prefix}{sym}", "field": "meta_model_path", "value": model_path},
            {"op": "HSET", "key": f"{prefix}{sym}", "field": "meta_p_min", "value": f"{meta_p_min:.3f}"},
            {"op": "HSET", "key": f"{prefix}{sym}", "field": "meta_model_reload_sec", "value": "60"},
            {"op": "HSET", "key": f"{prefix}{sym}", "field": "meta_enforce_share", "value": str(os.getenv("META_ENFORCE_INITIAL_SHARE", "0.10"))},
            {"op": "HSET", "key": f"{prefix}{sym}", "field": "meta_enforce_salt", "value": str(os.getenv("META_ENFORCE_SALT", "enf_v1"))},
        ]

    bundle = {
        "id": bundle_id,
        "created_ms": now_ms(),
        "ttl_sec": ttl,
        "who": "nightly_meta_enforce_propose_bundle",
        "ops": ops,
        "meta": {
            "eval": ev,
            "model": model_path,
            "health": mh,
            "streak": streak,
        },
    }

    r.set(f"recs:bundle:{bundle_id}", json.dumps(bundle, ensure_ascii=False, separators=(",", ":")), ex=ttl)
    r.set(f"recs:status:{bundle_id}", "PENDING", ex=ttl)

    buttons = [[
        {"text": "✅ Approve (preview)", "callback": f"recs:preview:{bundle_id}:{sig}"},
        {"text": "❌ Reject", "callback": f"recs:reject:{bundle_id}:{sig}"},
    ]]

    msg = (
        "<b>Meta ENFORCE proposal</b>\n"
        f"id=<code>{bundle_id}</code>\n"
        f"symbols=<code>{','.join(sorted(list(allow)))}</code>\n"
        f"model=<code>{model_path}</code>\n"
        f"meta_p_min=<code>{meta_p_min:.3f}</code>\n"
        f"baseline=<code>{ev['baseline']}</code>\n"
        f"filtered=<code>{ev['filtered']}</code>\n"
        f"delta=<code>{ev['delta']}</code>\n"
        f"health=<code>{mh}</code> streak=<code>{streak}</code>"
    )

    r.xadd(os.getenv("NOTIFY_TELEGRAM_STREAM", "notify:telegram"), {
        "type": "report",
        "text": msg,
        "buttons": json.dumps(buttons, ensure_ascii=False, separators=(",", ":")),
        "ts": str(now_ms()),
    }, maxlen=200000, approximate=True)

    logger.info("Meta ENFORCE proposal created: bundle_id=%s", bundle_id)


if __name__ == "__main__":
    main()

