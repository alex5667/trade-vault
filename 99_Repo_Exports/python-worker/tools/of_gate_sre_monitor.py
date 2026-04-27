from __future__ import annotations
from utils.time_utils import get_ny_time_millis

import argparse
import json
import os
import time
from collections import Counter
from typing import Any, Dict, List, Optional

# redis is imported lazily inside main() to avoid breaking unit tests
# that do not have a Redis server available (test isolation).
from common.redis_errors import retry_redis_operation

from core.ok_fields import parse_ok_fields, get_scenario, get_ts_ms
from tools.of_gate_metrics_contract import derive_ok_fields, is_gate_row, scenario_key
from common.of_gate_metrics_contract import validate_of_gate_row

def _now_ms() -> int:
    return get_ny_time_millis()


def _i(x: Any, d: int = 0) -> int:
    try:
        if x is None:
            return d
        return int(float(x))
    except Exception:
        return d


def _f(x: Any, d: float = 0.0) -> float:
    try:
        if x is None:
            return d
        return float(x)
    except Exception:
        return d


def pctl(xs: List[float], q: float) -> float:
    if not xs:
        return 0.0
    xs = sorted(xs)
    i = int(round((len(xs) - 1) * q))
    i = max(0, min(len(xs) - 1, i))
    return float(xs[i])


def _read_stream_window(r: redis.Redis, stream: str, start_ms: int, window_ms: int, *, max_scan: int = 600000) -> List[Dict[str, Any]]:
    end_ms = start_ms + window_ms
    rows: List[Dict[str, Any]] = []
    last_id = "+"
    scanned = 0
    while scanned < max_scan:
        try:
            batch = retry_redis_operation(
                operation=lambda: r.xrevrange(stream, max=last_id, min="-", count=2000),
                operation_name="xrevrange",
                max_retries=10,
                base_delay=1.0,
                max_delay=30.0,
                on_final_failure=lambda e: [],  # Return empty list on final failure
            )
        except Exception:
            batch = []
        if not batch:
            break
        for msg_id, fields in batch:
            scanned += 1
            if msg_id == last_id:
                continue
            last_id = msg_id
            d = dict(fields or {})
            ts = get_ts_ms(d)
            if ts <= 0:
                continue
            if ts < start_ms:
                scanned = max_scan
                break
            if ts <= end_ms:
                d["_ts_ms"] = ts
                rows.append(d)
        if len(batch) < 2000:
            break
    rows.sort(key=lambda x: int(x.get("_ts_ms", 0)))
    return rows


def _scenario_key(r: Dict[str, Any]) -> str:
    return get_scenario(r) or "na"


def _parse_missing_legs(r: Dict[str, Any]) -> List[str]:
    x = r.get("missing_legs", "")
    if not x:
        return []
    try:
        if isinstance(x, (bytes, bytearray)):
            x = x.decode("utf-8", "ignore")
        v = json.loads(str(x))
        if isinstance(v, list):
            return [str(z) for z in v[:12]]
    except Exception:
        return []
    return []


def _dist_l1(p: Dict[str, float], q: Dict[str, float]) -> float:
    keys = set(p.keys()) | set(q.keys())
    return float(sum(abs(p.get(k, 0.0) - q.get(k, 0.0)) for k in keys))


def compute_stats(rows: List[Dict[str, Any]], prev: Optional[Dict[str, Any]], *, dh_bad_th: float) -> Dict[str, Any]:
    raw_n = len(rows)
    gate_rows = [r for r in rows if is_gate_row(r)]
    
    n_total_raw = len(gate_rows)
    valid_infra: List[Dict[str, Any]] = []
    dq = Counter()
    legacy_payload = 0

    for r0 in gate_rows:
        r = r0
        if isinstance(r0, dict) and "payload" in r0 and ("ok" not in r0 or "scenario_v4" not in r0):
            try:
                inner = json.loads(r0.get("payload") or "{}")
                if isinstance(inner, dict):
                    r = {**inner, **{k: v for k, v in r0.items() if k != "payload"}}
                    legacy_payload += 1
            except Exception:
                r = r0

        _ok_val, code = validate_of_gate_row(r)
        if not _ok_val:
            dq[code] += 1
            continue
        valid_infra.append(r)

    n_total = len(valid_infra)
    no_data_total = 1 if n_total == 0 else 0

    valid_rows = [r for r in valid_infra if scenario_key(r) != "dn_veto"]
    n = len(valid_rows)
    no_data = 1 if n == 0 else 0
    dn_veto_count = n_total - n

    ok = 0
    soft = 0
    lat: List[float] = []
    ml_lat: List[float] = []
    execn: List[float] = []
    dn_usd: List[float] = []
    dn_thresh: List[float] = []
    meta_veto = 0
    book_bad = 0
    src_bad = 0
    dh_bad = 0
    miss = Counter()
    scen = Counter()
    ok_src_c = Counter()
    ok_soft_src_c = Counter()
    sample_rate = Counter()
    sample_key_mode = Counter()

    # Process ALL rows for system-wide health and infra metrics
    for r in gate_rows:
        lu = _f(r.get("latency_us", 0.0), 0.0)
        if lu > 0:
            lat.append(lu)
        mlu = _f(r.get("ml_latency_us", 0.0), 0.0)
        if mlu > 0:
            ml_lat.append(mlu)
        en = _f(r.get("exec_risk_norm", 0.0), 0.0)
        if en > 0:
            execn.append(en)
        
        du = _f(r.get("dn_usd", 0.0), 0.0)
        if du > 0:
            dn_usd.append(du)
        dt = _f(r.get("dn_tier_threshold", 0.0), 0.0)
        if dt > 0:
            dn_thresh.append(dt)

        book_bad += 1 if _i(r.get("book_health_ok", 1), 1) == 0 else 0
        src_bad += 1 if _i(r.get("source_consistency_ok", 1), 1) == 0 else 0
        dh = _f(r.get("data_health", 1.0), 1.0)
        dh_bad += 1 if dh < dh_bad_th else 0

        for m in _parse_missing_legs(r):
            miss[m] += 1

        # Sampling diagnostics (safe: bounded cardinality)
        sr = str(r.get("sample_rate", "") or "")
        if sr:
            sample_rate[sr] += 1
        sm = str(r.get("sample_key_mode", "") or "")
        if sm:
            sample_key_mode[sm] += 1

    # Process valid_rows for business logical rates
    for r in valid_rows:
        ok1, soft1, ok_src, ok_soft_src = derive_ok_fields(r)
        ok += ok1
        soft += soft1
        ok_src_c[ok_src] += 1
        ok_soft_src_c[ok_soft_src] += 1
        
        meta_veto += 1 if _i(r.get("meta_veto", 0), 0) == 1 else 0
        scen[scenario_key(r)] += 1

    ok_rate = (ok / n) if n > 0 else None
    soft_rate = (soft / n) if n > 0 else None

    scen_dist: Dict[str, float] = {}
    if n > 0:
        for k, c in scen.items():
            scen_dist[k] = float(c) / float(n)

    prev_dist = {}
    if isinstance(prev, dict):
        prev_dist = prev.get("scenario_dist", {}) if isinstance(prev.get("scenario_dist", {}), dict) else {}

    scenario_l1 = 0.0
    if prev_dist:
        try:
            prev_dist2 = {str(k): float(v) for k, v in prev_dist.items()}
            scenario_l1 = _dist_l1(scen_dist, prev_dist2)
        except Exception:
            scenario_l1 = 0.0

    out = {
        "ts_ms": _now_ms(),
        "n_raw": int(raw_n),
        "n_total_raw": int(n_total_raw),
        "n_total": int(n_total),
        "n": int(n),
        "n_invalid": int(sum(dq.values())),
        "no_data_total": int(no_data_total),
        "no_data": int(no_data),
        "legacy_payload": int(legacy_payload),
        "dq_top": [{"k": k, "n": int(v)} for k, v in dq.most_common(8)],
        "dn_veto_rate": (float(dn_veto_count / n_total) if n_total > 0 else None),
        "ok_rate": (float(ok_rate) if ok_rate is not None else None),
        "soft_rate": (float(soft_rate) if soft_rate is not None else None),
        "lat_p50_us": float(pctl(lat, 0.50)),
        "lat_p95_us": float(pctl(lat, 0.95)),
        "lat_p99_us": float(pctl(lat, 0.99)),
        "ml_lat_p50_us": float(pctl(ml_lat, 0.50)),
        "ml_lat_p95_us": float(pctl(ml_lat, 0.95)),
        "ml_lat_p99_us": float(pctl(ml_lat, 0.99)),
        "exec_p50": float(pctl(execn, 0.50)),
        "exec_p90": float(pctl(execn, 0.90)),
        "exec_p99": float(pctl(execn, 0.99)),
        "meta_veto_rate": (float(meta_veto / n) if n > 0 else None),
        "book_bad_rate": (float(book_bad / n_total) if n_total > 0 else None),
        "source_inconsistency_rate": (float(src_bad / n_total) if n_total > 0 else None),
        "data_health_bad_rate": (float(dh_bad / n_total) if n_total > 0 else None),
        "scenario_dist": scen_dist,
        "scenario_max_share": float(max(scen_dist.values())) if scen_dist else 0.0,
        "scenario_l1": float(scenario_l1),
        "dn_usd_avg": float(sum(dn_usd) / len(dn_usd)) if dn_usd else 0.0,
        "dn_thresh_avg": float(sum(dn_thresh) / len(dn_thresh)) if dn_thresh else 0.0,
        "top_missing_legs": [{"k": k, "n": int(v)} for k, v in miss.most_common(8)],
        "ok_src_top": [{"k": k, "n": int(v)} for k, v in ok_src_c.most_common(3)],
        "ok_soft_src_top": [{"k": k, "n": int(v)} for k, v in ok_soft_src_c.most_common(3)],
        "sample_rate_top": [{"k": k, "n": int(v)} for k, v in sample_rate.most_common(3)],
        "sample_key_mode_top": [{"k": k, "n": int(v)} for k, v in sample_key_mode.most_common(3)],
    }
    return out


def build_alerts(stats: Dict[str, Any], *, cfg: Dict[str, Any]) -> List[Dict[str, Any]]:
    alerts: List[Dict[str, Any]] = []
    n = _i(stats.get("n", 0), 0)
    n_total = _i(stats.get("n_total", 0), 0)
    n_total_raw = _i(stats.get("n_total_raw", n_total), n_total)
    n_invalid = _i(stats.get("n_invalid", 0), 0)
    min_n = _i(cfg.get("min_n", 200), 200)

    lat_p99_us_max = float(cfg.get("lat_p99_us_max", 25000.0))
    ml_lat_p99_us_max = float(cfg.get("ml_lat_p99_us_max", 25000.0))
    exec_p90_max = float(cfg.get("exec_p90_max", 0.90))
    src_bad_max = float(cfg.get("src_bad_max", 0.02))
    book_bad_max = float(cfg.get("book_bad_max", 0.02))
    dh_bad_max = float(cfg.get("dh_bad_max", 0.10))

    if _i(stats.get("no_data_total", 0), 0) == 1:
        alerts.append({"code": "no_data_total", "sev": "warn", "msg": f"no valid rows (raw={n_total_raw} invalid={n_invalid})"})
        return alerts
    if _i(stats.get("no_data", 0), 0) == 1:
        alerts.append({"code": "no_data", "sev": "warn", "msg": f"no eligible rows (non-dn_veto) (total_valid={n_total})"})
        return alerts

    # Infra alerts over n_total
    if n_total >= min_n:
        if _f(stats.get("lat_p99_us", 0.0), 0.0) > lat_p99_us_max:
            alerts.append({"code": "lat_p99_high", "sev": "warn", "msg": f"lat_p99_us>{lat_p99_us_max:.0f}"})
        if _f(stats.get("ml_lat_p99_us", 0.0), 0.0) > ml_lat_p99_us_max:
            alerts.append({"code": "ml_lat_p99_high", "sev": "warn", "msg": f"ml_lat_p99_us>{ml_lat_p99_us_max:.0f}"})
        if _f(stats.get("exec_p90", 0.0), 0.0) > exec_p90_max:
            alerts.append({"code": "exec_p90_high", "sev": "warn", "msg": f"exec_p90>{exec_p90_max:.3f}"})
        if _f(stats.get("source_inconsistency_rate", 0.0), 0.0) > src_bad_max:
            alerts.append({"code": "source_inconsistency_high", "sev": "warn", "msg": f"src_bad>{src_bad_max:.3f}"})
        if _f(stats.get("book_bad_rate", 0.0), 0.0) > book_bad_max:
            alerts.append({"code": "book_bad_high", "sev": "warn", "msg": f"book_bad>{book_bad_max:.3f}"})
        if _f(stats.get("data_health_bad_rate", 0.0), 0.0) > dh_bad_max:
            alerts.append({"code": "data_health_bad_high", "sev": "warn", "msg": f"dh_bad>{dh_bad_max:.3f}"})
    else:
        alerts.append({"code": "low_n_total", "sev": "warn", "msg": f"n_total={n_total} < min_n={min_n}"})
        return alerts

    # Business alerts over n (valid rows)
    if n < min_n:
        alerts.append({"code": "low_n", "sev": "warn", "msg": f"n={n} < min_n={min_n}"})
        return alerts

    ok_min = float(cfg.get("ok_min", 0.10))
    soft_max = float(cfg.get("soft_max", 0.70))
    scen_l1_max = float(cfg.get("scenario_l1_max", 0.35))
    scen_share_max = float(cfg.get("scenario_max_share_max", 0.75))

    if _f(stats.get("ok_rate", 0.0), 0.0) < ok_min:
        alerts.append({"code": "ok_rate_low", "sev": "crit", "msg": f"ok_rate<{ok_min:.3f}"})
    if _f(stats.get("soft_rate", 0.0), 0.0) > soft_max:
        alerts.append({"code": "soft_rate_high", "sev": "warn", "msg": f"soft_rate>{soft_max:.3f}"})
    if _f(stats.get("scenario_l1", 0.0), 0.0) > scen_l1_max:
        alerts.append({"code": "scenario_drift", "sev": "warn", "msg": f"scenario_l1>{scen_l1_max:.3f}"})
    if _f(stats.get("scenario_max_share", 0.0), 0.0) > scen_share_max:
        alerts.append({"code": "scenario_dominance", "sev": "warn", "msg": f"scenario_max_share>{scen_share_max:.3f}"})

    return alerts


def _fmt(stats: Dict[str, Any], alerts: List[Dict[str, Any]], *, window_min: int) -> str:
    import socket
    import os
    hostname = socket.gethostname()
    pid = os.getpid()

    def ms(us: float) -> float:
        return float(us) / 1000.0
        
    def fmt_rate(x: Any) -> str:
        if x is None:
            return "NA"
        try:
            return f"{float(x):.3f}"
        except Exception:
            return "NA"

    lines = []
    n = _i(stats.get('n',0),0)
    n_total = _i(stats.get('n_total',0),0)
    n_total_raw = _i(stats.get("n_total_raw", n_total), n_total)
    n_invalid = _i(stats.get("n_invalid", 0), 0)
    lines.append(f"OF_GATE_SRE window={window_min}m n={n} (total_valid={n_total} raw={n_total_raw} invalid={n_invalid} dn_veto={fmt_rate(stats.get('dn_veto_rate',None))}) ({hostname}:{pid})")
    lines.append(f"ok_rate={fmt_rate(stats.get('ok_rate',None))} soft_rate={fmt_rate(stats.get('soft_rate',None))} meta_veto={fmt_rate(stats.get('meta_veto_rate',None))}")
    dq_top = stats.get("dq_top", [])
    if isinstance(dq_top, list) and dq_top:
        try:
            pairs = [f"{d.get('k','')}:{int(d.get('n',0))}" for d in dq_top[:6]]
            lines.append("dq_top=" + ",".join(pairs))
        except Exception:
            pass
    lines.append(
        f"lat(ms) p50={ms(_f(stats.get('lat_p50_us',0.0),0.0)):.2f} "
        f"p95={ms(_f(stats.get('lat_p95_us',0.0),0.0)):.2f} "
        f"p99={ms(_f(stats.get('lat_p99_us',0.0),0.0)):.2f}"
    )
    lines.append(
        f"ml_lat(ms) p50={ms(_f(stats.get('ml_lat_p50_us',0.0),0.0)):.2f} "
        f"p95={ms(_f(stats.get('ml_lat_p95_us',0.0),0.0)):.2f} "
        f"p99={ms(_f(stats.get('ml_lat_p99_us',0.0),0.0)):.2f}"
    )
    lines.append(
        f"exec_norm p50={_f(stats.get('exec_p50',0.0),0.0):.3f} "
        f"p90={_f(stats.get('exec_p90',0.0),0.0):.3f} "
        f"p99={_f(stats.get('exec_p99',0.0),0.0):.3f}"
    )
    lines.append(
        f"book_bad={_f(stats.get('book_bad_rate',0.0),0.0):.3f} "
        f"src_bad={_f(stats.get('source_inconsistency_rate',0.0),0.0):.3f} "
        f"dh_bad={_f(stats.get('data_health_bad_rate',0.0),0.0):.3f}"
    )
    lines.append(
        f"scenario_l1={_f(stats.get('scenario_l1',0.0),0.0):.3f} scenario_max_share={_f(stats.get('scenario_max_share',0.0),0.0):.3f}"
    )
    lines.append(
        f"dn_avg=${_f(stats.get('dn_usd_avg',0.0),0.0):,.0f} thresh_avg=${_f(stats.get('dn_thresh_avg',0.0),0.0):,.0f}"
    )

    if alerts:
        lines.append("ALERTS:")
        for a in alerts:
            lines.append(f"- {a.get('code','')} [{a.get('sev','')}] {a.get('msg','')}")
    return "\n".join(lines)


def _notify(r: Any, stream: str, text: str, sid: Optional[str] = None) -> None:
    import html
    safe_text = html.escape(text)
    payload = {
        "type": "report",
        "subtype": "of_gate_sre",
        "ts_ms": str(_now_ms()),
        "text": safe_text
    }
    if sid:
        payload["sid"] = sid

    try:
        retry_redis_operation(
            operation=lambda: r.xadd(stream, payload, maxlen=200000, approximate=True),
            operation_name="xadd_notify",
            max_retries=1,
            base_delay=1.0,
            max_delay=10.0,
            on_final_failure=lambda e: None,  # Silently fail notification
        )
    except Exception:
        pass  # Notification failures should not crash the monitor



class AlertManager:
    """
    Manages alert state in Redis to suppress duplicates.
    State is stored in a Hash: {alert_code: last_fired_ts_ms}
    """
    def __init__(self, r: Any, key: str, cooldown_sec: int = 1800):
        self.r = r
        self.key = key
        self.cooldown_ms = cooldown_sec * 1000

    def filter(self, current_alerts: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Returns only alerts that should be fired (new or cooldown expired).
        Updates state for fired alerts.
        Clears state for alerts that are no longer active.
        """
        now = _now_ms()
        out: List[Dict[str, Any]] = []
        
        # Load current state
        try:
            raw_state = self.r.hgetall(self.key)
            state = {k: int(v) for k, v in raw_state.items()}
        except Exception:
            state = {}

        current_codes = set()
        active_map = {a.get("code"): a for a in current_alerts}

        # 1. Process current alerts
        for code, alert in active_map.items():
            current_codes.add(code)
            last_ts = state.get(code, 0)
            
            # Fire if never fired or cooldown expired
            if last_ts == 0 or (now - last_ts > self.cooldown_ms):
                out.append(alert)
                # Update state immediately (optimistic)
                state[code] = now
                try:
                    self.r.hset(self.key, code, str(now))
                except Exception:
                    pass
        
        # 2. Clear state for resolved alerts
        # (If a code is in state but not in current_codes, it's resolved)
        resolved = []
        for code in list(state.keys()):
            if code not in current_codes:
                resolved.append(code)
        
        if resolved:
            try:
                self.r.hdel(self.key, *resolved)
            except Exception:
                pass
                
        return out


def main() -> None:
    import redis  # lazy import: keeps module importable without redis for unit tests

    ap = argparse.ArgumentParser()
    ap.add_argument("--redis-url", default=os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0"))
    ap.add_argument("--metrics-stream", default=os.getenv("OF_GATE_METRICS_STREAM", "metrics:of_gate"))
    ap.add_argument("--notify-stream", default=os.getenv("NOTIFY_TELEGRAM_STREAM", "notify:telegram"))
    ap.add_argument("--state-key", default=os.getenv("SRE_OF_GATE_STATE_KEY", "sre:of_gate:last_stats"))
    ap.add_argument("--alert-state-key", default=os.getenv("SRE_OF_GATE_ALERT_STATE_KEY", "sre:of_gate:alert_state"))
    ap.add_argument("--cooldown-sec", type=int, default=int(os.getenv("SRE_OF_GATE_ALERT_COOLDOWN_SEC", "1800")))
    ap.add_argument("--window-min", type=int, default=int(os.getenv("SRE_OF_GATE_WINDOW_MIN", "60")))
    ap.add_argument("--min-n", type=int, default=int(os.getenv("SRE_OF_GATE_MIN_N", "50")))
    ap.add_argument("--always", type=int, default=int(os.getenv("SRE_OF_GATE_ALWAYS", "0")))
    ap.add_argument("--out", default=os.getenv("SRE_OF_GATE_OUT", ""))

    ap.add_argument("--ok-min", type=float, default=float(os.getenv("SRE_OF_GATE_OK_MIN", "0.10")))
    ap.add_argument("--soft-max", type=float, default=float(os.getenv("SRE_OF_GATE_SOFT_MAX", "0.70")))
    ap.add_argument("--lat-p99-us-max", type=float, default=float(os.getenv("SRE_OF_GATE_LAT_P99_US_MAX", "25000")))
    ap.add_argument("--ml-lat-p99-us-max", type=float, default=float(os.getenv("SRE_OF_GATE_ML_LAT_P99_US_MAX", "25000")))
    ap.add_argument("--exec-p90-max", type=float, default=float(os.getenv("SRE_OF_GATE_EXEC_P90_MAX", "0.90")))
    ap.add_argument("--scenario-l1-max", type=float, default=float(os.getenv("SRE_OF_GATE_SCENARIO_L1_MAX", "0.35")))
    ap.add_argument("--scenario-max-share-max", type=float, default=float(os.getenv("SRE_OF_GATE_SCENARIO_MAX_SHARE_MAX", "0.75")))
    ap.add_argument("--src-bad-max", type=float, default=float(os.getenv("SRE_OF_GATE_SRC_BAD_MAX", "0.02")))
    ap.add_argument("--book-bad-max", type=float, default=float(os.getenv("SRE_OF_GATE_BOOK_BAD_MAX", "0.02")))
    ap.add_argument("--dh-bad-max", type=float, default=float(os.getenv("SRE_OF_GATE_DH_BAD_MAX", "0.10")))
    ap.add_argument("--dh-bad-th", type=float, default=float(os.getenv("SRE_OF_GATE_DH_BAD_TH", "0.70")))

    args = ap.parse_args()

    r = redis.Redis.from_url(args.redis_url, decode_responses=True)
    window_ms = int(args.window_min) * 60_000
    start_ms = _now_ms() - window_ms
    rows = _read_stream_window(r, args.metrics_stream, start_ms, window_ms)

    prev: Optional[Dict[str, Any]] = None
    try:
        blob = retry_redis_operation(
            operation=lambda: r.get(args.state_key),
            operation_name="redis_get_state",
            max_retries=5,
            base_delay=1.0,
            max_delay=10.0,
            on_final_failure=lambda e: None,  # Return None on failure
        )
        if blob:
            prev = json.loads(blob)
    except Exception:
        prev = None

    stats = compute_stats(rows, prev, dh_bad_th=float(args.dh_bad_th))
    cfg = {
        "min_n": int(args.min_n),
        "ok_min": float(args.ok_min),
        "soft_max": float(args.soft_max),
        "lat_p99_us_max": float(args.lat_p99_us_max),
        "ml_lat_p99_us_max": float(args.ml_lat_p99_us_max),
        "exec_p90_max": float(args.exec_p90_max),
        "scenario_l1_max": float(args.scenario_l1_max),
        "scenario_max_share_max": float(args.scenario_max_share_max),
        "src_bad_max": float(args.src_bad_max),
        "book_bad_max": float(args.book_bad_max),
        "dh_bad_max": float(args.dh_bad_max),
    }
    raw_alerts = build_alerts(stats, cfg=cfg)

    # Stateful suppression for duplicate alerts
    am = AlertManager(r, args.alert_state_key, cooldown_sec=args.cooldown_sec)
    actionable_alerts = am.filter(raw_alerts)

    try:
        retry_redis_operation(
            operation=lambda: r.set(args.state_key, json.dumps(stats, ensure_ascii=False), ex=7 * 24 * 3600),
            operation_name="redis_set_state",
            max_retries=5,
            base_delay=1.0,
            max_delay=10.0,
            on_final_failure=lambda e: None,  # Silently fail state save
        )
    except Exception:
        pass

    if args.out:
        try:
            os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)
            with open(args.out, "w", encoding="utf-8") as f:
                json.dump({"stats": stats, "alerts": raw_alerts}, f, ensure_ascii=False, indent=2)
        except Exception:
            pass

    # Notify only if there are actionable (new/cooldown) alerts, OR if always=1
    if actionable_alerts or int(args.always) == 1:
        # We send the full report with ALL alerts (raw_alerts) for context,
        # but the trigger was the actionable_alerts.
        text = _fmt(stats, raw_alerts, window_min=int(args.window_min))
        
        # Deterministic SID to prevent duplicates from multiple notify workers
        # Floor timestamp to 1 minute to be stable within the same reporting cycle
        ts_now = _now_ms()
        floored_min = (ts_now // 60_000) * 60_000
        sid = f"of_gate_sre:{floored_min}"
        
        _notify(r, args.notify_stream, text, sid=sid)

    print(json.dumps({"stats": stats, "alerts": raw_alerts, "actionable": actionable_alerts}, ensure_ascii=False))


if __name__ == "__main__":
    main()
