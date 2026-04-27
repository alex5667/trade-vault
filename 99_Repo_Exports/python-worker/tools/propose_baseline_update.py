"""Propose baseline update: export latest OFInputs, run engine replay, diff vs current baseline.

Exports last N OFInputs for canary symbols from signals:of:inputs,
runs engine replay, compares with current baseline output,
stores bundle in Redis and sends Telegram buttons for manual confirm.

Usage:
  python -m tools.propose_baseline_update
  (reads ENV vars for streams, baseline paths, symbols)
"""

from __future__ import annotations
from utils.time_utils import get_ny_time_millis

import argparse
import json
import os
import secrets
import time
import hmac
import hashlib
import subprocess
import sys
from typing import Any, Dict, List

import redis

from common.log import setup_logger

from core.ok_fields import parse_ok_fields, get_scenario, get_ts_ms

logger = setup_logger("ProposeBaselineUpdate")


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
        Dictionary with health metrics: n, ok_rate, soft_rate, lat_p99_us, exec_p90, scenario_max_share, scenario_top
    """
    n = len(rows)
    if n == 0:
        return {"n": 0}

    ok = 0
    soft = 0
    lat = []
    ex = []
    scen = {}

    for r in rows:
        ok_i, soft_i = parse_ok_fields(r)
        ok += 1 if ok_i == 1 else 0
        soft += 1 if soft_i == 1 else 0
        try:
            lat.append(float(r.get("latency_us", 0.0) or 0.0))
        except Exception:
            pass
        try:
            ex.append(float(r.get("exec_risk_norm", 0.0) or 0.0))
        except Exception:
            pass

        sc = get_scenario(r) or "na"
        scen[sc] = scen.get(sc, 0) + 1

    ok_rate = ok / n
    soft_rate = soft / n
    lat_p99 = _pctl(lat, 0.99)
    exec_p90 = _pctl(ex, 0.90)

    # scenario share + max share (concentration proxy: if suddenly collapses to one scenario, it's suspicious)
    max_share = 0.0
    for k, v in scen.items():
        max_share = max(max_share, v / n)

    return {
        "n": n,
        "ok_rate": ok_rate,
        "soft_rate": soft_rate,
        "lat_p99_us": lat_p99,
        "exec_p90": exec_p90,
        "scenario_max_share": max_share,
        "scenario_top": sorted([(k, v / n) for k, v in scen.items()], key=lambda x: -x[1])[:6],
    }


def export_inputs(r: redis.Redis, *, stream: str, field: str, symbols: set[str], out_path: str, max_scan: int, max_write: int) -> int:
    """
    Exports OFInputs from Redis stream, filtering by symbols.
    
    Args:
        r: Redis client
        stream: Stream name (e.g. "signals:of:inputs")
        field: Field name containing JSON payload (e.g. "payload")
        symbols: Set of symbols to include (uppercase)
        out_path: Output NDJSON file path
        max_scan: Maximum messages to scan
        max_write: Maximum messages to write
        
    Returns:
        Number of messages written
    """
    scanned = 0
    written = 0
    last_id = "+"

    rows: List[dict] = []
    while scanned < max_scan and written < max_write:
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
            payload = fields.get(field)
            if not payload:
                continue
            try:
                inp = json.loads(payload) if isinstance(payload, str) else json.loads(payload.decode("utf-8"))
            except Exception:
                continue
            sym = str(inp.get("symbol", "")).upper()
            if sym and sym in symbols:
                rows.append(inp)
                written += 1
                if written >= max_write:
                    break

    rows.reverse()  # chronological order (oldest first)
    with open(out_path, "w", encoding="utf-8") as f:
        for x in rows:
            f.write(json.dumps(x, ensure_ascii=False) + "\n")
    return written


def main() -> None:
    """Main entry point: export inputs, replay, diff, propose bundle."""
    ap = argparse.ArgumentParser(description="Propose baseline update with Telegram confirm")
    ap.add_argument("--out-dir", default=os.getenv("BASELINE_DIR", "/app/of_reports_baselines"))
    ap.add_argument("--max-write", type=int, default=int(os.getenv("BASELINE_CAPTURE_MAX_WRITE", "50000") or 50000))
    ap.add_argument("--max-scan", type=int, default=int(os.getenv("BASELINE_CAPTURE_MAX_SCAN", "400000") or 400000))
    args = ap.parse_args()

    redis_url = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
    r = redis.Redis.from_url(redis_url, decode_responses=True)

    # ------------------------------------------------------------
    # Gate baseline proposal: require N consecutive PASS regress nights
    # ------------------------------------------------------------
    min_streak = int(os.getenv("BASELINE_PROPOSE_MIN_STREAK", "3") or 3)
    max_age_h = float(os.getenv("BASELINE_PROPOSE_MAX_AGE_HOURS", "30") or 30.0)

    streak_key = os.getenv("REGRESS_PASS_STREAK_KEY", "sre:regress:pass_streak")
    last_status_key = os.getenv("REGRESS_LAST_STATUS_KEY", "sre:regress:last_status")
    last_ts_key = os.getenv("REGRESS_LAST_TS_KEY", "sre:regress:last_ts_ms")

    streak = 0
    try:
        streak = int(r.get(streak_key) or "0")
    except Exception:
        streak = 0

    last_status = str(r.get(last_status_key) or "")
    last_ts = 0
    try:
        last_ts = int(r.get(last_ts_key) or "0")
    except Exception:
        last_ts = 0

    age_ok = True
    if last_ts > 0:
        age_ok = (now_ms() - last_ts) <= int(max_age_h * 3600_000)

    if not (last_status == "PASS" and age_ok and streak >= min_streak):
        if int(os.getenv("BASELINE_PROPOSE_NOTIFY_ON_SKIP", "0") or 0) == 1:
            import html
            msg = (
                "<b>Baseline proposal skipped</b>\n"
                f"need_streak=<code>{min_streak}</code> have=<code>{streak}</code>\n"
                f"last_status=<code>{html.escape(last_status, quote=False)}</code> age_ok=<code>{int(age_ok)}</code>"
            )
            r.xadd(os.getenv("NOTIFY_TELEGRAM_STREAM", "notify:telegram"), {"type": "report", "text": msg, "ts": str(now_ms())}, maxlen=200000, approximate=True)
        return

    # ------------------------------------------------------------
    # Health gate (24h metrics): latency/exec-risk/soft/ok
    # ------------------------------------------------------------
    win_h = float(os.getenv("BASELINE_PROPOSE_HEALTH_WINDOW_HOURS", "24") or 24)
    min_n = int(os.getenv("BASELINE_PROPOSE_MIN_N", "200") or 200)

    lat_cap = float(os.getenv("BASELINE_PROPOSE_LAT_P99_US_MAX", "4000") or 4000)
    exec_cap = float(os.getenv("BASELINE_PROPOSE_EXEC_P90_MAX", "0.85") or 0.85)
    soft_cap = float(os.getenv("BASELINE_PROPOSE_SOFT_RATE_MAX", "0.35") or 0.35)
    ok_floor = float(os.getenv("BASELINE_PROPOSE_OK_RATE_MIN", "0.20") or 0.20)

    metrics_stream = os.getenv("OF_GATE_METRICS_STREAM", "metrics:of_gate")
    since_ms = now_ms() - int(win_h * 3600_000)
    rows = _read_metrics_window(r, metrics_stream, since_ms, max_scan=int(os.getenv("BASELINE_PROPOSE_MAX_SCAN", "400000") or 400000))
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

    # Optional "scenario collapse" guard (proxy; if one scenario dominates too much)
    max_share_cap = float(os.getenv("BASELINE_PROPOSE_SCEN_MAX_SHARE", "0.85") or 0.85)
    if float(mh.get("scenario_max_share", 0.0)) > max_share_cap:
        health_reasons.append(f"scenario_max_share={mh.get('scenario_max_share',0.0):.2f}>{max_share_cap:.2f}")

    # Optional: require SRE stats for scenario_l1 drift check
    if int(os.getenv("BASELINE_PROPOSE_REQUIRE_SRE_STATS", "0") or 0) == 1:
        sre_key = os.getenv("SRE_PREV_KEY", "sre:of_gate:last_stats")
        raw = r.get(sre_key)
        if raw:
            try:
                d = json.loads(raw)
                # monitor stores drift in its own run; simplest: reuse its scenario_share stability:
                # If you store scenario_l1 there, read it. If not, skip.
                # For now, require that scenario_top exists and not empty.
                stats = (d.get("stats") or {})
                if not stats or int(stats.get("n", 0) or 0) < min_n:
                    health_reasons.append("sre_stats_insufficient")
                else:
                    # Check scenario_l1 drift if available in drift dict
                    drift_data = d.get("drift", {})
                    scen_l1 = float(drift_data.get("scenario_l1", 0.0) or 0.0)
                    scen_l1_max = float(os.getenv("BASELINE_PROPOSE_SCEN_L1_MAX", "0.30") or 0.30)
                    if scen_l1 > scen_l1_max:
                        health_reasons.append(f"scenario_l1={scen_l1:.2f}>{scen_l1_max:.2f}")
            except Exception:
                pass
        else:
            health_reasons.append("sre_stats_missing")

    if health_reasons:
        if int(os.getenv("BASELINE_PROPOSE_NOTIFY_ON_SKIP", "0") or 0) == 1:
            import html
            msg = (
                "<b>Baseline proposal skipped (health gate)</b>\n"
                f"reasons=<code>{html.escape(','.join(health_reasons), quote=False)}</code>\n"
                f"metrics=<code>{html.escape(str(mh), quote=False)}</code>\n"
                f"streak=<code>{streak}</code> last=<code>{html.escape(str(last_status), quote=False)}</code>"
            )
            r.xadd(os.getenv("NOTIFY_TELEGRAM_STREAM", "notify:telegram"), {"type": "report", "text": msg, "ts": str(now_ms())}, maxlen=200000, approximate=True)
        return

    stream = os.getenv("OF_INPUTS_STREAM", "signals:of:inputs")
    field = os.getenv("OF_INPUTS_STREAM_FIELD", "payload")
    symbols = {s.strip().upper() for s in os.getenv("CANARY_SYMBOLS", "BTCUSDT,ETHUSDT").split(",") if s.strip()}

    baseline_inputs = os.getenv("BASELINE_INPUTS", f"{args.out_dir}/inputs_canary.ndjson")
    baseline_output = os.getenv("BASELINE_OUTPUT", f"{args.out_dir}/baseline.ndjson")

    secret = os.getenv("RECS_HMAC_SECRET", "CHANGE_ME")
    ttl = int(os.getenv("RECS_TTL_SEC", "86400") or 86400)

    ts = time.strftime("%Y%m%d_%H%M%S")
    cand_dir = f"{args.out_dir}/candidates"
    os.makedirs(cand_dir, exist_ok=True)
    os.makedirs(f"{args.out_dir}/diffs", exist_ok=True)

    bid = secrets.token_hex(6)
    sig = sign(bid, secret)

    cand_inputs = f"{cand_dir}/inputs_{bid}_{ts}.ndjson"
    cand_output = f"{cand_dir}/output_{bid}_{ts}.ndjson"
    diff_path = f"{args.out_dir}/diffs/diff_{bid}_{ts}.json"

    # Export inputs
    logger.info("Exporting inputs from stream=%s, symbols=%s", stream, symbols)
    n = export_inputs(r, stream=stream, field=field, symbols=symbols, out_path=cand_inputs, max_scan=args.max_scan, max_write=args.max_write)
    if n < 2000:
        raise SystemExit(f"too_few_inputs_exported n={n} (minimum 2000 required)")

    # Engine replay
    logger.info("Running engine replay on %s", cand_inputs)
    subprocess.check_call([
        sys.executable, "-m", "tools.of_engine_replay_from_inputs",
        "--inputs", cand_inputs,
        "--out", cand_output
    ])

    # Diff vs current baseline output
    logger.info("Comparing candidate with baseline %s", baseline_output)
    subprocess.check_call([
        sys.executable, "-m", "tools.of_regress_baseline_check",
        "--baseline", baseline_output,
        "--candidate", cand_output,
        "--out", diff_path,
        "--fail-on-mismatch", "0",
    ])

    # Store bundle
    bundle = {
        "id": bid,
        "created_ms": now_ms(),
        "ttl_sec": ttl,
        "who": "propose_baseline_update",
        "candidate_inputs": cand_inputs,
        "candidate_output": cand_output,
        "diff_path": diff_path,
        "baseline_inputs": baseline_inputs,
        "baseline_output": baseline_output,
        "symbols": sorted(list(symbols)),
    }

    r.set(f"baseline:bundle:{bid}", json.dumps(bundle, ensure_ascii=False, separators=(",", ":")), ex=ttl)
    r.set(f"baseline:status:{bid}", "PENDING", ex=ttl)

    buttons = [[
        {"text": "👀 Preview diff", "callback": f"baseline:preview:{bid}:{sig}"},
        {"text": "❌ Reject", "callback": f"baseline:reject:{bid}:{sig}"},
    ]]

    import html
    msg = (
        "<b>Baseline update proposal</b>\n"
        f"id=<code>{html.escape(str(bid), quote=False)}</code>\n"
        f"symbols=<code>{html.escape(','.join(bundle['symbols']), quote=False)}</code>\n"
        f"inputs=<code>{html.escape(str(cand_inputs), quote=False)}</code>\n"
        f"output=<code>{html.escape(str(cand_output), quote=False)}</code>\n"
        f"diff=<code>{html.escape(str(diff_path), quote=False)}</code>"
    )
    r.xadd(
        os.getenv("NOTIFY_TELEGRAM_STREAM", "notify:telegram"),
        {
            "type": "report",
            "text": msg,
            "buttons": json.dumps(buttons, ensure_ascii=False, separators=(",", ":")),
            "ts": str(now_ms()),
        },
        maxlen=200000,
        approximate=True
    )

    logger.info("Baseline proposal created: bundle_id=%s", bid)


if __name__ == "__main__":
    main()

