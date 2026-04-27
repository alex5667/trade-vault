"""Nightly regression test with automatic emergency disable ENFORCE on fail.

On regression mismatch:
  - Sends alert to Telegram
  - Auto-disables ENFORCE (sets meta_model_mode=SHADOW on target symbols)
  - Creates emergency bundle with Rollback button

Usage:
  python -m tools.nightly_regress_engine_replay_safe
  (reads BASELINE_INPUTS, BASELINE_OUTPUT from env)
"""

from __future__ import annotations
from utils.time_utils import get_ny_time_millis

import json
import os
import secrets
import subprocess
import sys
import time
import hmac
import hashlib
from typing import Any, Dict, List, Tuple

import redis


def now_ms() -> int:
    """Returns current timestamp in milliseconds."""
    return get_ny_time_millis()


def sign(bundle_id: str, secret: str) -> str:
    """Generate HMAC signature for bundle approval callbacks."""
    d = hmac.new(secret.encode("utf-8"), bundle_id.encode("utf-8"), hashlib.sha256).hexdigest()
    return d[:8]


def xadd_notify(r: redis.Redis, text: str, buttons: List[List[Dict[str, str]]] | None = None) -> None:
    """Send notification to Telegram stream."""
    fields = {"type": "report", "text": text, "ts": str(now_ms())}
    if buttons is not None:
        fields["buttons"] = json.dumps(buttons, ensure_ascii=False, separators=(",", ":"))
    r.xadd(os.getenv("NOTIFY_TELEGRAM_STREAM", "notify:telegram"), fields, maxlen=200000, approximate=True)


def apply_emergency_bundle(
    r: redis.Redis,
    *,
    ops: List[Dict[str, str]],
    meta: Dict[str, Any],
    who: str,
    ttl_sec: int,
    secret: str,
) -> Tuple[str, str]:
    """
    Auto-apply bundle (no approve) but keep rollback via recs_callback_worker.
    
    Process:
      - writes recs:bundle:<id>
      - writes recs:audit:<id> as list of old/new
      - sets recs:status:<id>=APPLIED
      - applies HSET operations immediately
    
    Returns:
      (bundle_id, signature)
    """
    bundle_id = secrets.token_hex(6)
    sig = sign(bundle_id, secret)

    bundle = {
        "id": bundle_id,
        "created_ms": now_ms(),
        "ttl_sec": ttl_sec,
        "who": who,
        "ops": ops,
        "meta": meta,
    }

    r.set(f"recs:bundle:{bundle_id}", json.dumps(bundle, ensure_ascii=False, separators=(",", ":")), ex=ttl_sec)
    r.set(f"recs:status:{bundle_id}", "PENDING", ex=ttl_sec)

    # audit old values + apply
    hset_ops = [op for op in ops if op.get("op") == "HSET"]
    old_vals: List[Tuple[str, str, str, str]] = []
    for op in hset_ops:
        key = op["key"]
        field = op["field"]
        newv = op["value"]
        old = r.hget(key, field)
        oldv = "" if old is None else str(old)
        old_vals.append((key, field, oldv, newv))

    pipe = r.pipeline()
    for op in hset_ops:
        pipe.hset(op["key"], op["field"], op["value"])
    pipe.execute()

    ts = now_ms()
    for key, field, oldv, newv in old_vals:
        r.rpush(
            f"recs:audit:{bundle_id}",
            json.dumps({"ts_ms": ts, "key": key, "field": field, "old": oldv, "new": newv, "who": who}, ensure_ascii=False, separators=(",", ":")),
        )
    r.expire(f"recs:audit:{bundle_id}", ttl_sec)

    r.set(f"recs:status:{bundle_id}", "APPLIED", ex=ttl_sec)
    return bundle_id, sig


def main() -> None:
    redis_url = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
    r = redis.Redis.from_url(redis_url, decode_responses=True)

    lock_key = os.getenv("REGRESS_LOCK_KEY", "lock:sre:nightly_regress_engine_replay")
    lock_ttl = int(os.getenv("REGRESS_LOCK_TTL_SEC", "7200") or 7200)
    if not r.set(lock_key, "1", nx=True, ex=lock_ttl):
        print(f"Skipping: Regression suite is already running or ran recently (lock {lock_key} active).")
        return

    baseline_inputs = os.getenv("BASELINE_INPUTS", "").strip()
    baseline_output = os.getenv("BASELINE_OUTPUT", "").strip()
    out_dir = os.getenv("OUT_DIR", "/var/lib/trade/of_reports/out").strip()
    max_mismatches = int(os.getenv("REGRESS_MAX_MISMATCHES", "0") or 0)
    fail_enforce = int(os.getenv("REGRESS_FAIL_DISABLE_ENFORCE", "1") or 1)

    if not baseline_inputs or not baseline_output:
        raise SystemExit("BASELINE_INPUTS and BASELINE_OUTPUT must be set")

    ts = time.strftime("%Y%m%d_%H%M%S")
    run_dir = f"{out_dir}/regress_safe_{ts}"
    os.makedirs(run_dir, exist_ok=True)

    cand_out = f"{run_dir}/candidate.ndjson"
    diff_out = f"{run_dir}/diff.json"
    status_out = f"{run_dir}/status.json"

    status_data = {"status": "UNKNOWN", "ts": ts}
    
    try:
        if not os.path.exists(baseline_inputs):
             status_data["status"] = "SKIPPED"
             status_data["reason"] = f"baseline_inputs_missing: {baseline_inputs}"
             print(f"Skipped: baseline inputs not found {baseline_inputs}")
             return

        # 1) engine replay on fixed baseline inputs
        subprocess.check_call([sys.executable, "-m", "tools.of_engine_replay_from_inputs", "--inputs", baseline_inputs, "--out", cand_out])

        # 2) diff vs baseline output
        # of_regress_baseline_check writes diff.json and returns non-zero if mismatches > max_mismatches
        # we call with fail=0 to always continue and read the diff
        subprocess.check_call([
            sys.executable, "-m", "tools.of_regress_baseline_check",
            "--baseline", baseline_output,
            "--candidate", cand_out,
            "--out", diff_out,
            "--fail-on-mismatch", "0",
        ])

        rep = json.loads(open(diff_out, "r", encoding="utf-8").read())
        mism = int(rep.get("mismatches", 0) or 0)
        overlap = int(rep.get("n", 0) or 0)

        # 3) Always report regress summary
        msg = (
            "<b>Gate regress (engine replay)</b>\n"
            f"overlap_n=<code>{overlap}</code> mismatches=<code>{mism}</code> max=<code>{max_mismatches}</code>\n"
            f"by_field=<code>{rep.get('mismatch_by_field',{})}</code>\n"
            f"top_scn=<code>{rep.get('mismatch_by_scenario_v4_top',[])}</code>"
        )
        xadd_notify(r, msg)

        # --- record PASS/FAIL streak for gating baseline proposals ---
        streak_key = os.getenv("REGRESS_PASS_STREAK_KEY", "sre:regress:pass_streak")
        last_status_key = os.getenv("REGRESS_LAST_STATUS_KEY", "sre:regress:last_status")
        last_ts_key = os.getenv("REGRESS_LAST_TS_KEY", "sre:regress:last_ts_ms")
        streak_ttl = int(os.getenv("REGRESS_STREAK_TTL_SEC", "1209600") or 1209600)

        passed = (mism <= max_mismatches)

        if passed:
            r.incr(streak_key)
            r.expire(streak_key, streak_ttl)
            r.set(last_status_key, "PASS", ex=streak_ttl)
            r.set(last_ts_key, str(now_ms()), ex=streak_ttl)
            status_data["status"] = "SUCCESS"
        else:
            r.set(streak_key, "0", ex=streak_ttl)
            r.set(last_status_key, "FAIL", ex=streak_ttl)
            r.set(last_ts_key, str(now_ms()), ex=streak_ttl)
            status_data["status"] = "FAILED"
            status_data["reason"] = f"mismatches={mism} > max={max_mismatches}"

        # 4) If fail -> emergency disable ENFORCE (meta model)
        if mism <= max_mismatches or fail_enforce != 1:
            return

        secret = os.getenv("RECS_HMAC_SECRET", "CHANGE_ME")
        ttl = int(os.getenv("RECS_TTL_SEC", "86400") or 86400)
        prefix = os.getenv("CFG_HASH_PREFIX", "config:orderflow:")
        symbols = os.getenv("REGRESS_TARGET_SYMBOLS", os.getenv("CANARY_SYMBOLS", "")).strip()
        syms = [s.strip().upper() for s in symbols.split(",") if s.strip()]
        if not syms:
            syms = ["BTCUSDT", "ETHUSDT"]

        ops: List[Dict[str, str]] = []
        for sym in syms:
            key = f"{prefix}{sym}"
            # emergency: force SHADOW (keep enable=1 for telemetry)
            ops.append({"op": "HSET", "key": key, "field": "meta_model_mode", "value": "SHADOW"})
            # emergency: also reset share to 0.00 (even if mode=ENFORCE somehow remains)
            ops.append({"op": "HSET", "key": key, "field": "meta_enforce_share", "value": "0.00"})

        bundle_id, sig = apply_emergency_bundle(
            r,
            ops=ops,
            meta={
                "kind": "emergency_disable_enforce",
                "reason": "regress_mismatch",
                "mismatches": mism,
                "max_mismatches": max_mismatches,
                "diff_path": diff_out,
                "candidate_path": cand_out,
                "baseline_output": baseline_output,
            },
            who="nightly_regress_engine_replay_safe",
            ttl_sec=ttl,
            secret=secret,
        )

        buttons = [[
            {"text": "↩ Rollback", "callback": f"recs:rollback:{bundle_id}:{sig}"},
        ]]

        emsg = (
            "<b>EMERGENCY</b> regress mismatch → ENFORCE disabled\n"
            f"id=<code>{bundle_id}</code>\n"
            f"symbols=<code>{','.join(syms)}</code>\n"
            f"action=<code>HSET meta_model_mode=SHADOW</code>\n"
            f"mismatches=<code>{mism}</code> max=<code>{max_mismatches}</code>"
        )
        xadd_notify(r, emsg, buttons=buttons)

        # optional: hard exit code for monitoring
        raise SystemExit(2)

    except SystemExit as e:
        msg = str(e)
        if status_data.get("status") == "UNKNOWN":
             status_data["status"] = "FAILED"
             status_data["error"] = msg
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

