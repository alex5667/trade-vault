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
    run_dir = f"{args.out_dir}/nightly_meta_v2_{ts}"
    os.makedirs(run_dir, exist_ok=True)
    os.makedirs(args.models_dir, exist_ok=True)

    inputs_raw = f"{run_dir}/of_inputs_raw.ndjson"
    inputs_can = f"{run_dir}/of_inputs_canary.ndjson"
    trades_out = f"{run_dir}/closed_trades.ndjson"
    dataset_out = f"{run_dir}/dataset.parquet"
    model_path = f"{args.models_dir}/meta_lr_v2_{ts}.json"
    status_out = f"{run_dir}/status.json"

    status_data = {"status": "UNKNOWN", "ts": ts}

    try:
        # 1) export inputs
        subprocess.run([
            sys.executable, "-m", "tools.export_of_inputs_ndjson_v2",
            "--redis-url", redis_url,
            "--out", inputs_raw,
            "--stream", inputs_stream,
            "--field", inputs_field,
            "--state-file", os.getenv("STATE_FILE", f"{args.out_dir}/of_inputs.state"),
            "--resume",
        ], check=True, capture_output=True, text=True)

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
        if n < 100:
             # Just warn, don't exit if we want to debug, but for auto-train probably exit
             print(f"Warning: too few inputs n={n}")

        # 3) export trades
        subprocess.run([
            sys.executable, "tools/export_trade_closed_ndjson.py",
            "--since-hours", str(args.since_hours),
            "--out", trades_out,
            "--stream", trades_stream,
            "--redis-url", redis_url,
            "--max-scan", os.getenv("TRADES_MAX_SCAN", "500000"),
        ], check=True, capture_output=True, text=True)

        # 4) dataset join (v2 builder -> parquet)
        # Uses filtered canary inputs to save time/space
        subprocess.run([
            sys.executable, "-m", "tools.build_dataset_from_inputs_outcomes_v2",
            "--inputs", inputs_can,
            "--closed", trades_out,
            "--out", dataset_out,
            "--r-min", "0.0" # Binary classification usually on >0 or >0.5?
                             # In v1 bundle it used pos-th 0.
                             # In v2 builder, default is 0.5. Let's use 0.0 for "any profit" or 0.5?
                             # Existing rule usually targets profit. V2 builder default is 0.5.
        ], check=True, capture_output=True, text=True)

        # 5) train LR model (v2 trainer)
        subprocess.run([
            sys.executable, "-m", "tools.train_meta_model_lr_v2",
            "--parquet", dataset_out,
            "--out_json", model_path,
            "--threshold", str(args.meta_p_min)
        ], check=True, capture_output=True, text=True)

        # Read report from model file
        with open(model_path, encoding="utf-8") as f:
            model_data = json.load(f)
        report = model_data.get("report", {})
        auc = float(report.get("auc", 0.0))

        # 6) create bundle to enable SHADOW meta model (per symbol)
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
                # Use p_min from args
                {"op": "HSET", "key": f"{prefix}{sym}", "field": "meta_p_min", "value": f"{args.meta_p_min:.3f}"},
                {"op": "HSET", "key": f"{prefix}{sym}", "field": "meta_model_reload_sec", "value": "60"},
            ]

        # Also update global champion config (for SRE monitor consistency)
        champion_cfg = {
            "kind": "meta_lr_v1",
            "model_path": model_path,
            "run_id": ts,
            "mode": "SHADOW",
            "enforce_share": 0.05,
            "updated_ms": get_ny_time_millis(),
            "source": "nightly_train_meta_model_v2_bundle",
        }
        champion_json = json.dumps(champion_cfg, ensure_ascii=False, separators=(",", ":"))
        ops.append({"op": "SET", "key": os.getenv("ML_CFG_CHAMPION_KEY", "cfg:ml_confirm:champion"), "value": champion_json})


        bundle = {"id": bundle_id, "created_ms": get_ny_time_millis(), "ttl_sec": ttl, "who": "nightly_train_meta_model_v2_bundle", "ops": ops, "meta": {"ts": ts, "report": report}}

        r = redis.Redis.from_url(redis_url, decode_responses=True)
        r.set(f"recs:bundle:{bundle_id}", json.dumps(bundle, ensure_ascii=False, separators=(",", ":")), ex=ttl)

        # Auto-confirm logic
        auto_confirm = int(os.getenv("ML_AUTO_CONFIRM_META_MODEL", "0") or 0) == 1
        if args.auto_confirm:
            auto_confirm = True

        if auto_confirm and auc > 0.5: # Simple guard
            # Inline apply_ops utils to avoid dependency hell in tool scripts
            def audit_push(r: redis.Redis, bundle_id: str, entry: dict[str, Any], ttl: int) -> None:
                r.rpush(f"recs:audit:{bundle_id}", json.dumps(entry, ensure_ascii=False, separators=(",", ":")))
                r.expire(f"recs:audit:{bundle_id}", ttl)

            def apply_ops_inline(r: redis.Redis, bundle: dict[str, Any], ttl: int, actor: dict[str, str]) -> int:
                ops = bundle.get("ops") or []
                ts = get_ny_time_millis()
                applied = 0
                pipe = r.pipeline()
                for op in ops:
                    typ = op.get("op")
                    key = op.get("key")
                    field = op.get("field")
                    if not typ or not key:
                        continue
                    if typ == "HSET" and not field:
                        continue
                    if typ == "HSET":
                        old = r.hget(key, field)
                        old_null = 1 if old is None else 0
                        val = (op.get("value", ""))
                        pipe.hset(key, field, val)
                        audit_push(r, bundle.get("id"), {
                            "op": "HSET", "key": key, "field": field,
                            "old": "" if old is None else str(old), "old_null": old_null,
                            "new": val, "ts_ms": ts, "who": "auto_confirm", "actor": actor
                        }, ttl)
                        applied += 1
                    elif typ == "SET":
                        val = (op.get("value", ""))
                        old_val = r.get(key)
                        old_null = 1 if old_val is None else 0
                        pipe.set(key, val)
                        audit_push(r, bundle.get("id"), {
                            "op": "SET", "key": key, "field": "",
                            "old": "" if old_val is None else str(old_val),
                            "old_null": old_null,
                            "new": val,
                            "ts_ms": ts, "who": "auto_confirm", "actor": actor
                        }, ttl)
                        applied += 1
                pipe.execute()
                return applied

            # Apply
            actor = {"who": "auto-confirm", "ts": str(get_ny_time_millis())}
            n_ops = apply_ops_inline(r, bundle, ttl, actor)
            r.set(f"recs:status:{bundle_id}", "APPLIED", ex=ttl)

            # Notify APPLIED
            msg = (
                "<b>Nightly meta-model V2 (LR) - AUTO APPLIED</b>\n"
                f"id=<code>{bundle_id}</code>\n"
                f"symbols=<code>{','.join(sorted(allow))}</code>\n"
                f"model=<code>{model_path}</code>\n"
                f"auc=<code>{auc:.3f}</code> thr=<code>{float(model_data.get('threshold',0.5)):.2f}</code>\n"
                f"ops=<code>{n_ops}</code> status=<code>APPLIED</code>"
            )
            r.xadd(os.getenv("NOTIFY_TELEGRAM_STREAM", RS.NOTIFY_TELEGRAM), {
                "type": "report",
                "text": msg,
                "ts": str(get_ny_time_millis()),
            }, maxlen=200000, approximate=True)

            status_data["status"] = "SUCCESS_AUTO_APPLIED"

        else:
            # Manual confirmation flow
            r.set(f"recs:status:{bundle_id}", "PENDING", ex=ttl)

            buttons = [[
                {"text": "✅ Approve (preview)", "callback": f"recs:preview2:{bundle_id}:{sig}"},
                {"text": "❌ Reject",           "callback": f"recs:reject:{bundle_id}:{sig}"},
            ]]
            msg = (
                "<b>Nightly meta-model V2 (LR)</b>\n"
                f"id=<code>{bundle_id}</code>\n"
                f"symbols=<code>{','.join(sorted(allow))}</code>\n"
                f"model=<code>{model_path}</code>\n"
                f"auc=<code>{auc:.3f}</code> thr=<code>{float(model_data.get('threshold',0.5)):.2f}</code>\n"
                f"features=<code>{','.join(model_data.get('features', [])[:10])}...</code>"
            )
            r.xadd(os.getenv("NOTIFY_TELEGRAM_STREAM", RS.NOTIFY_TELEGRAM), {
                "type": "report",
                "text": msg,
                "buttons": json.dumps(buttons, ensure_ascii=False, separators=(",", ":")),
                "ts": str(get_ny_time_millis()),
            }, maxlen=200000, approximate=True)

            status_data["status"] = "SUCCESS_PENDING"

        status_data["status"] = "SUCCESS"
        status_data["bundle_id"] = bundle_id

    except subprocess.CalledProcessError as e:
        msg = e.stderr or str(e)
        status_data["status"] = "FAILED"
        status_data["error"] = msg[:1000]
        print(f"Failed (subprocess): {msg}")
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
