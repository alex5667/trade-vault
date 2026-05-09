from __future__ import annotations
from core.redis_keys import RedisStreams as RS

"""Nightly orchestrator: export inputs → engine replay → export trades → build dataset → calibrate → create bundle + Telegram preview.

Why:
  Automated calibration loop: nightly run that creates recs bundle with optimal gate parameters
  and sends Telegram message with preview button for 2-phase approval.

Usage:
  python -m tools.nightly_gate_calibrate_bundle --since-hours 24 --canary-symbols BTCUSDT,ETHUSDT
"""

import argparse
import json
import os
import secrets
import subprocess
import sys
import time

import redis

from utils.time_utils import get_ny_time_millis


def sign(bundle_id: str, secret: str) -> str:
    """Generate HMAC signature for bundle approval callbacks."""
    import hashlib
    import hmac
    d = hmac.new(secret.encode("utf-8"), bundle_id.encode("utf-8"), hashlib.sha256).hexdigest()
    return d[:8]


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--since-hours", type=float, default=float(os.getenv("TRADES_SINCE_HOURS", "24") or 24))
    ap.add_argument("--canary-symbols", default=os.getenv("CANARY_SYMBOLS", "BTCUSDT,ETHUSDT"))
    ap.add_argument("--out-dir", default=os.getenv("OUT_DIR", "/var/lib/trade/of_reports/out"))
    args = ap.parse_args()

    redis_url = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
    inputs_stream = os.getenv("OF_INPUTS_STREAM", RS.OF_INPUTS)
    inputs_field = os.getenv("OF_INPUTS_STREAM_FIELD", "payload")
    trades_stream = os.getenv("TRADE_EVENTS_STREAM", RS.EVENTS_TRADES)
    secret = os.getenv("RECS_HMAC_SECRET", "CHANGE_ME")

    out_dir = args.out_dir
    ts = time.strftime("%Y%m%d_%H%M%S")
    run_dir = f"{out_dir}/nightly_{ts}"
    os.makedirs(run_dir, exist_ok=True)

    inputs_raw = f"{run_dir}/of_inputs_raw.ndjson"
    inputs_can = f"{run_dir}/of_inputs_canary.ndjson"
    replay_out = f"{run_dir}/of_replay_engine.ndjson"
    trades_out = f"{run_dir}/closed_trades.ndjson"
    dataset_out = f"{run_dir}/dataset.ndjson"
    calib_out = f"{run_dir}/calib.json"
    status_out = f"{run_dir}/status.json"

    status_data = {"status": "UNKNOWN", "ts": ts}

    try:
        # 1) export inputs (v2)
        subprocess.run([
            sys.executable, "-m", "tools.export_of_inputs_ndjson_v2",
            "--redis-url", redis_url, "--out", inputs_raw,
            "--stream", inputs_stream, "--field", inputs_field,
            "--state-file", os.getenv("STATE_FILE", f"{out_dir}/of_inputs.state"),
            "--resume",
            "--max-records", os.getenv("MAX_RECORDS", "200000"),
        ], check=True, capture_output=True, text=True)

        # 2) canary filter (simple: keep only listed symbols)
        allow = {s.strip().upper() for s in args.canary_symbols.split(",") if s.strip()}
        n = 0
        with open(inputs_raw, encoding="utf-8") as f, open(inputs_can, "w", encoding="utf-8") as g:
            for line in f:
                if not line.strip():
                    continue
                row = json.loads(line)
                payload = row.get("payload")
                p = json.loads(payload) if isinstance(payload, str) else (payload if isinstance(payload, dict) else row)
                sym = (p.get("symbol", "")).upper()
                if sym in allow:
                    g.write(json.dumps(p, ensure_ascii=False) + "\n")
                    n += 1
            if n < 50:
                status_data["n_inputs"] = n
                raise SystemExit(f"too_few_inputs_canary n={n}")

        status_data["n_inputs"] = n

        # 3) engine replay
        subprocess.run([sys.executable, "-m", "tools.of_engine_replay_from_inputs", "--inputs", inputs_can, "--out", replay_out], check=True, capture_output=True, text=True)

        # 4) export closed trades
        subprocess.run([
            sys.executable, "tools/export_trade_closed_ndjson.py",
            "--since-hours", str(args.since_hours),
            "--out", trades_out,
            "--stream", trades_stream,
            "--redis-url", redis_url,
            "--max-scan", os.getenv("TRADES_MAX_SCAN", "500000"),
        ], check=True, capture_output=True, text=True)

        # 5) build dataset (join by sid)
        subprocess.run([sys.executable, "-m", "tools.build_of_dataset", "--replay", replay_out, "--trades", trades_out, "--out", dataset_out, "--pos-th", "0", "--neg-th", "0"], check=True, capture_output=True, text=True)

        # 6) calibrate
        subprocess.run([sys.executable, "-m", "tools.calibrate_gate_params", "--dataset", dataset_out, "--out", calib_out], check=True, capture_output=True, text=True)

        best = json.loads(open(calib_out, encoding="utf-8").read()).get("best") or {}
        if not best:
             raise SystemExit("no_calibration_result")

        # 7) create recs bundle (per symbol)
        bundle_id = secrets.token_hex(6)
        sig = sign(bundle_id, secret)
        ttl = int(os.getenv("RECS_TTL_SEC", "86400") or 86400)
        prefix = os.getenv("CFG_HASH_PREFIX", "config:orderflow:")

        w_exec = best.get("w_exec_risk")
        ref = best.get("exec_risk_ref_bps")
        smin = best.get("of_score_min")

        ops = []
        for sym in sorted(allow):
            ops += [
                {"op": "HSET", "key": f"{prefix}{sym}", "field": "w_exec_risk", "value": f"{float(w_exec):.3f}"},
                {"op": "HSET", "key": f"{prefix}{sym}", "field": "exec_risk_ref_bps", "value": f"{float(ref):.2f}"},
                {"op": "HSET", "key": f"{prefix}{sym}", "field": "of_score_min", "value": f"{float(smin):.3f}"},
            ]

        bundle = {
            "id": bundle_id,
            "created_ms": get_ny_time_millis(),
            "ttl_sec": ttl,
            "who": "nightly_gate_calibrate_bundle",
            "ops": ops,
            "meta": {"ts": ts, "best": best},
        }

        r = redis.Redis.from_url(redis_url, decode_responses=True)
        r.set(f"recs:bundle:{bundle_id}", json.dumps(bundle, ensure_ascii=False, separators=(",", ":")), ex=ttl)
        r.set(f"recs:status:{bundle_id}", "PENDING", ex=ttl)

        # 8) Telegram message with preview button (2-phase)
        buttons = [[
            {"text": "✅ Approve (preview)", "callback": f"recs:preview:{bundle_id}:{sig}"},
            {"text": "❌ Reject", "callback": f"recs:reject:{bundle_id}:{sig}"},
        ]]
        msg = (
            "<b>Nightly calibration</b>\n"
            f"id=<code>{bundle_id}</code>\n"
            f"symbols=<code>{','.join(sorted(allow))}</code>\n"
            f"w_exec_risk=<code>{float(w_exec):.3f}</code> exec_ref=<code>{float(ref):.2f}</code> score_min=<code>{float(smin):.3f}</code>\n"
            f"metrics={best.get('metrics')}"
        )
        r.xadd(os.getenv("NOTIFY_TELEGRAM_STREAM", RS.NOTIFY_TELEGRAM), {
            "type": "report",
            "text": msg,
            "buttons": json.dumps(buttons, ensure_ascii=False, separators=(",", ":")),
            "ts": str(get_ny_time_millis()),
        }, maxlen=200000, approximate=True)

        status_data["status"] = "SUCCESS"
        status_data["bundle_id"] = bundle_id

    except SystemExit as e:
        msg = str(e)
        if "too_few_inputs" in msg or "no_calibration_result" in msg or "dataset_too_small" in msg:
            status_data["status"] = "SKIPPED"
            status_data["reason"] = msg
            print(f"Skipped: {msg}")
        else:
            status_data["status"] = "FAILED"
            status_data["error"] = msg
            print(f"Failed (SystemExit): {msg}")
            raise
    except Exception as e:
        status_data["status"] = "FAILED"
        status_data["error"] = str(e)
        print(f"Failed: {e}")
        raise
    finally:
        with open(status_out, "w", encoding="utf-8") as f:
            json.dump(status_data, f, ensure_ascii=False)


if __name__ == "__main__":
    main()

