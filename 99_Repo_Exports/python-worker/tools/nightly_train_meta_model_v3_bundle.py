from __future__ import annotations

import argparse
import json
import os
import secrets
import subprocess
import sys
import time
from typing import Any

import redis

from utils.time_utils import get_ny_time_millis
from core.redis_keys import RedisStreams as RS


def sign(bundle_id: str, secret: str) -> str:
    import hashlib
    import hmac
    d = hmac.new(secret.encode("utf-8"), bundle_id.encode("utf-8"), hashlib.sha256).hexdigest()
    return d[:8]


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--since-hours", type=float, default=float(os.getenv("TRADES_SINCE_HOURS", "72") or 72))
    ap.add_argument("--canary-symbols", default=os.getenv("CANARY_SYMBOLS", "BTCUSDT,ETHUSDT"))
    ap.add_argument("--out-dir", default=os.getenv("OUT_DIR", "/var/lib/trade/of_reports/out"))
    ap.add_argument("--models-dir", default=os.getenv("MODELS_DIR", "/var/lib/trade/of_reports/models"))
    ap.add_argument("--meta-p-min", type=float, default=float(os.getenv("META_P_MIN", "0.55") or 0.55))
    ap.add_argument("--auto-confirm", action="store_true", help="Auto-confirm the bundle without waiting for user interaction")
    args = ap.parse_args()

    redis_url = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
    inputs_stream = os.getenv("OF_INPUTS_STREAM", RS.OF_INPUTS)
    inputs_field = os.getenv("OF_INPUTS_STREAM_FIELD", "payload")
    trades_stream = os.getenv("TRADE_EVENTS_STREAM", RS.EVENTS_TRADES)
    secret = os.getenv("RECS_HMAC_SECRET", "CHANGE_ME")

    ts = time.strftime("%Y%m%d_%H%M%S")
    run_dir = f"{args.out_dir}/nightly_meta_v3_{ts}"
    os.makedirs(run_dir, exist_ok=True)
    os.makedirs(args.models_dir, exist_ok=True)

    inputs_raw = f"{run_dir}/of_inputs_raw.ndjson"
    inputs_can = f"{run_dir}/of_inputs_canary.ndjson"
    trades_out = f"{run_dir}/closed_trades.ndjson"
    dataset_out = f"{run_dir}/dataset.parquet"
    model_path = f"{args.models_dir}/meta_lr_v3_{ts}.json"
    status_out = f"{run_dir}/status.json"

    status_data = {"status": "UNKNOWN", "ts": ts}

    try:
        # 1) export inputs
        print("Exporting OF inputs...")
        # 1) export inputs
        print("Exporting OF inputs...")
        export_log = f"{run_dir}/export_inputs.log"
        with open(export_log, "w") as log_f:
            subprocess.run([
                sys.executable, "-m", "tools.export_of_inputs_ndjson_v2",
                "--redis-url", redis_url,
                "--out", inputs_raw,
                "--stream", inputs_stream,
                "--field", inputs_field,
                "--state-file", os.getenv("STATE_FILE", f"{args.out_dir}/of_inputs.state"),
                "--resume",
            ], check=True, stdout=log_f, stderr=subprocess.STDOUT)

        # 2) filter canary
        allow = {s.strip().upper() for s in args.canary_symbols.split(",") if s.strip()}
        n = 0
        with open(inputs_raw, encoding="utf-8") as f, open(inputs_can, "w", encoding="utf-8") as g:
            for line in f:
                if not line.strip():
                    continue
                row = json.loads(line)
                p = row.get("payload")
                inp = json.loads(p) if isinstance(p, str) else (p if isinstance(p, dict) else row)
                sym = (inp.get("symbol", "")).upper()
                if sym in allow:
                    g.write(json.dumps(inp, ensure_ascii=False) + "\n")
                    n += 1

        status_data["n_inputs"] = n
        print(f"Canary inputs filtered: n={n}")

        # 3) export trades
        print("Exporting closed trades...")
        subprocess.run([
            sys.executable, "tools/export_trade_closed_ndjson.py",
            "--since-hours", str(args.since_hours),
            "--out", trades_out,
            "--stream", trades_stream,
            "--redis-url", redis_url,
            "--max-scan", os.getenv("TRADES_MAX_SCAN", "500000"),
        ], check=True, capture_output=True, text=True)

        # 4) dataset join (v2 builder -> parquet)
        print("Building dataset...")
        subprocess.run([
            sys.executable, "-m", "tools.build_dataset_from_inputs_outcomes_v2",
            "--inputs", inputs_can,
            "--closed", trades_out,
            "--out", dataset_out,
            "--r-min", "0.0"
        ], check=True, capture_output=True, text=True)

        # 5) train LR model (v3 trainer with Train==Serve self-check)
        print("Training meta-model v3...")
        subprocess.run([
            sys.executable, "-m", "tools.train_meta_model_lr_v3",
            "--in-parquet", dataset_out,
            "--label-col", "y",
            "--out-json", model_path,
            "--self-check", "1",
        ], check=True, capture_output=True, text=True)

        # Read report from model file (v3 doesn't have report yet, but we'll add basic stats)
        with open(model_path, encoding="utf-8") as f:
            model_data = json.load(f)

        # 6) create bundle
        bundle_id = secrets.token_hex(6)
        sig = sign(bundle_id, secret)
        ttl = int(os.getenv("RECS_TTL_SEC", "86400") or 86400)
        prefix = os.getenv("CFG_HASH_PREFIX", "config:orderflow:")

        ops = []
        for sym in sorted(allow):
            ops += [
                {"op": "HSET", "key": f"{prefix}{sym}", "field": "meta_model_enable", "value": "1"},
                {"op": "HSET", "key": f"{prefix}{sym}", "field": "meta_model_mode", "value": "SHADOW"},
                {"op": "HSET", "key": f"{prefix}{sym}", "field": "meta_model_path", "value": model_path},
                {"op": "HSET", "key": f"{prefix}{sym}", "field": "meta_p_min", "value": f"{args.meta_p_min:.3f}"},
                {"op": "HSET", "key": f"{prefix}{sym}", "field": "meta_model_reload_sec", "value": "60"},
            ]

        # Champion config
        champion_cfg = {
            "kind": "meta_lr",
            "model_path": model_path,
            "run_id": ts,
            "mode": "SHADOW",
            "updated_ms": get_ny_time_millis(),
            "source": "nightly_train_meta_model_v3_bundle",
        }
        champion_json = json.dumps(champion_cfg, ensure_ascii=False, separators=(",", ":"))
        champion_key = os.getenv("ML_CFG_CHAMPION_KEY", "cfg:ml_confirm:champion")
        ops.append({"op": "SET", "key": champion_key, "value": champion_json})
        ops.append({"op": "SET", "key": f"{champion_key}:meta_lr", "value": champion_json})

        bundle = {"id": bundle_id, "created_ms": get_ny_time_millis(), "ttl_sec": ttl, "who": "nightly_train_meta_model_v3_bundle", "ops": ops, "meta": {"ts": ts}}

        r = redis.Redis.from_url(redis_url, decode_responses=True)
        r.set(f"recs:bundle:{bundle_id}", json.dumps(bundle, ensure_ascii=False, separators=(",", ":")), ex=ttl)

        # Auto-confirm
        auto_confirm = int(os.getenv("ML_AUTO_CONFIRM_META_MODEL", "0") or 0) == 1
        if args.auto_confirm: auto_confirm = True

        if auto_confirm:
            def audit_push(r: redis.Redis, bundle_id: str, entry: dict[str, Any], ttl: int) -> None:
                r.rpush(f"recs:audit:{bundle_id}", json.dumps(entry, ensure_ascii=False, separators=(",", ":")))
                r.expire(f"recs:audit:{bundle_id}", ttl)

            def apply_ops_inline(r: redis.Redis, bundle: dict[str, Any], ttl: int, actor: dict[str, str]) -> int:
                ops = bundle.get("ops") or []
                ts = get_ny_time_millis()
                applied = 0
                pipe = r.pipeline()
                for op in ops:
                    typ = op.get("op"); key = op.get("key"); field = op.get("field")
                    if not typ or not key: continue
                    if typ == "HSET":
                        val = (op.get("value", ""))
                        pipe.hset(key, field, val)
                        applied += 1
                    elif typ == "SET":
                        val = (op.get("value", ""))
                        pipe.set(key, val)
                        applied += 1
                pipe.execute()
                return applied

            actor = {"who": "auto-confirm-v3", "ts": str(get_ny_time_millis())}
            n_ops = apply_ops_inline(r, bundle, ttl, actor)
            r.set(f"recs:status:{bundle_id}", "APPLIED", ex=ttl)

            msg = (
                "<b>Nightly meta-model V3 (LR) - AUTO APPLIED</b>\n"
                f"id=<code>{bundle_id}</code>\n"
                f"symbols=<code>{','.join(sorted(allow))}</code>\n"
                f"model=<code>{model_path}</code>\n"
                f"status=<code>APPLIED (Train==Serve validated)</code>"
            )
            r.xadd(os.getenv("NOTIFY_TELEGRAM_STREAM", RS.NOTIFY_TELEGRAM), {
                "type": "report",
                "text": msg,
                "ts": str(get_ny_time_millis()),
            }, maxlen=200000, approximate=True)

        else:
            r.set(f"recs:status:{bundle_id}", "PENDING", ex=ttl)
            msg = (
                "<b>Nightly meta-model V3 (LR)</b>\n"
                f"id=<code>{bundle_id}</code>\n"
                f"symbols=<code>{','.join(sorted(allow))}</code>\n"
                f"model=<code>{model_path}</code>\n"
                f"features=<code>{','.join(model_data.get('features', [])[:10])}...</code>"
            )
            r.xadd(os.getenv("NOTIFY_TELEGRAM_STREAM", RS.NOTIFY_TELEGRAM), {
                "type": "report",
                "text": msg,
                "ts": str(get_ny_time_millis()),
            }, maxlen=200000, approximate=True)

        status_data["status"] = "SUCCESS"
        status_data["bundle_id"] = bundle_id

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
