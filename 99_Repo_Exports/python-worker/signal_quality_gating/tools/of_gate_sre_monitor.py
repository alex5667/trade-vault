from __future__ import annotations
from utils.time_utils import get_ny_time_millis

import argparse
import html
import json
import os
import socket
import time
from collections import Counter
from typing import Any, Dict, List, Optional

import redis

from common.redis_errors import retry_redis_operation


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
            ts = _i(d.get("ts_ms", d.get("ts", d.get("timestamp", 0))), 0)
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
    sv4 = str(r.get("scenario_v4", "") or "")
    if sv4:
        return sv4
    s = str(r.get("scenario", "") or "")
    return s or "na"


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
    n = len(rows)
    ok = 0
    soft = 0
    lat: List[float] = []
    ml_lat: List[float] = []
    execn: List[float] = []
    meta_veto = 0
    book_bad = 0
    src_bad = 0
    dh_bad = 0
    miss = Counter()
    scen = Counter()

    for r in rows:
        ok += 1 if _i(r.get("ok", 0), 0) == 1 else 0
        soft += 1 if _i(r.get("ok_soft", 0), 0) == 1 else 0

        lu = _f(r.get("latency_us", 0.0), 0.0)
        if lu > 0:
            lat.append(lu)
        mlu = _f(r.get("ml_latency_us", 0.0), 0.0)
        if mlu > 0:
            ml_lat.append(mlu)
        en = _f(r.get("exec_risk_norm", 0.0), 0.0)
        if en > 0:
            execn.append(en)

        meta_veto += 1 if _i(r.get("meta_veto", 0), 0) == 1 else 0
        book_bad += 1 if _i(r.get("book_health_ok", 1), 1) == 0 else 0
        src_bad += 1 if _i(r.get("source_consistency_ok", 1), 1) == 0 else 0
        dh = _f(r.get("data_health", 1.0), 1.0)
        dh_bad += 1 if dh < dh_bad_th else 0

        for m in _parse_missing_legs(r):
            miss[m] += 1
        scen[_scenario_key(r)] += 1

    ok_rate = (ok / n) if n > 0 else 0.0
    soft_rate = (soft / n) if n > 0 else 0.0

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
        "n": int(n),
        "ok_rate": float(ok_rate),
        "soft_rate": float(soft_rate),
        "lat_p50_us": float(pctl(lat, 0.50)),
        "lat_p95_us": float(pctl(lat, 0.95)),
        "lat_p99_us": float(pctl(lat, 0.99)),
        "ml_lat_p50_us": float(pctl(ml_lat, 0.50)),
        "ml_lat_p95_us": float(pctl(ml_lat, 0.95)),
        "ml_lat_p99_us": float(pctl(ml_lat, 0.99)),
        "exec_p50": float(pctl(execn, 0.50)),
        "exec_p90": float(pctl(execn, 0.90)),
        "exec_p99": float(pctl(execn, 0.99)),
        "meta_veto_rate": float(meta_veto / n) if n > 0 else 0.0,
        "book_bad_rate": float(book_bad / n) if n > 0 else 0.0,
        "source_inconsistency_rate": float(src_bad / n) if n > 0 else 0.0,
        "data_health_bad_rate": float(dh_bad / n) if n > 0 else 0.0,
        "scenario_dist": scen_dist,
        "scenario_max_share": float(max(scen_dist.values())) if scen_dist else 0.0,
        "scenario_l1": float(scenario_l1),
        "top_missing_legs": [{"k": k, "n": int(v)} for k, v in miss.most_common(8)],
    }
    return out


def build_alerts(stats: Dict[str, Any], *, cfg: Dict[str, Any]) -> List[Dict[str, Any]]:
    alerts: List[Dict[str, Any]] = []
    n = _i(stats.get("n", 0), 0)
    min_n = _i(cfg.get("min_n", 200), 200)

    if n < min_n:
        alerts.append({"code": "low_n", "sev": "warn", "msg": f"n={n} < min_n={min_n}"})
        return alerts

    ok_min = float(cfg.get("ok_min", 0.10))
    soft_max = float(cfg.get("soft_max", 0.70))
    lat_p99_us_max = float(cfg.get("lat_p99_us_max", 25000.0))
    ml_lat_p99_us_max = float(cfg.get("ml_lat_p99_us_max", 25000.0))
    exec_p90_max = float(cfg.get("exec_p90_max", 0.90))
    scen_l1_max = float(cfg.get("scenario_l1_max", 0.35))
    scen_share_max = float(cfg.get("scenario_max_share_max", 0.75))
    src_bad_max = float(cfg.get("src_bad_max", 0.02))
    book_bad_max = float(cfg.get("book_bad_max", 0.02))
    dh_bad_max = float(cfg.get("dh_bad_max", 0.10))

    if _f(stats.get("ok_rate", 0.0), 0.0) < ok_min:
        alerts.append({"code": "ok_rate_low", "sev": "crit", "msg": f"ok_rate<{ok_min:.3f}"})
    if _f(stats.get("soft_rate", 0.0), 0.0) > soft_max:
        alerts.append({"code": "soft_rate_high", "sev": "warn", "msg": f"soft_rate>{soft_max:.3f}"})
    if _f(stats.get("lat_p99_us", 0.0), 0.0) > lat_p99_us_max:
        alerts.append({"code": "lat_p99_high", "sev": "warn", "msg": f"lat_p99_us>{lat_p99_us_max:.0f}"})
    if _f(stats.get("ml_lat_p99_us", 0.0), 0.0) > ml_lat_p99_us_max:
        alerts.append({"code": "ml_lat_p99_high", "sev": "warn", "msg": f"ml_lat_p99_us>{ml_lat_p99_us_max:.0f}"})
    if _f(stats.get("exec_p90", 0.0), 0.0) > exec_p90_max:
        alerts.append({"code": "exec_p90_high", "sev": "warn", "msg": f"exec_p90>{exec_p90_max:.3f}"})
    if _f(stats.get("scenario_l1", 0.0), 0.0) > scen_l1_max:
        alerts.append({"code": "scenario_drift", "sev": "warn", "msg": f"scenario_l1>{scen_l1_max:.3f}"})
    if _f(stats.get("scenario_max_share", 0.0), 0.0) > scen_share_max:
        alerts.append({"code": "scenario_dominance", "sev": "warn", "msg": f"scenario_max_share>{scen_share_max:.3f}"})
    if _f(stats.get("source_inconsistency_rate", 0.0), 0.0) > src_bad_max:
        alerts.append({"code": "source_inconsistency_high", "sev": "warn", "msg": f"src_bad>{src_bad_max:.3f}"})
    if _f(stats.get("book_bad_rate", 0.0), 0.0) > book_bad_max:
        alerts.append({"code": "book_bad_high", "sev": "warn", "msg": f"book_bad>{book_bad_max:.3f}"})
    if _f(stats.get("data_health_bad_rate", 0.0), 0.0) > dh_bad_max:
        alerts.append({"code": "data_health_bad_high", "sev": "warn", "msg": f"dh_bad>{dh_bad_max:.3f}"})

    return alerts


def _fmt(stats: Dict[str, Any], alerts: List[Dict[str, Any]], *, window_min: int) -> str:
    hostname = socket.gethostname()
    pid = os.getpid()

    def ms(us: float) -> float:
        return float(us) / 1000.0

    lines = []
    lines.append(f"OF_GATE_SRE window={window_min}m n={_i(stats.get('n',0),0)} ({hostname}:{pid})")
    lines.append(f"ok_rate={_f(stats.get('ok_rate',0.0),0.0):.3f} soft_rate={_f(stats.get('soft_rate',0.0),0.0):.3f} meta_veto={_f(stats.get('meta_veto_rate',0.0),0.0):.3f}")
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
    if alerts:
        lines.append("ALERTS:")
        for a in alerts:
            lines.append(f"- {a.get('code','')} [{a.get('sev','')}] {a.get('msg','')}")
    return "\n".join(lines)


def _notify(r: redis.Redis, stream: str, text: str) -> None:
    safe_text = html.escape(text)
    try:
        retry_redis_operation(
            operation=lambda: r.xadd(stream, {"type": "report", "subtype": "of_gate_sre", "ts_ms": str(_now_ms()), "text": safe_text}, maxlen=200000, approximate=True),
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
    def __init__(self, r: redis.Redis, key: str, cooldown_sec: int = 1800):
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
    ap = argparse.ArgumentParser()
    ap.add_argument("--redis-url", default=os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0"))
    ap.add_argument("--metrics-stream", default=os.getenv("OF_GATE_METRICS_STREAM", "metrics:of_gate"))
    ap.add_argument("--notify-stream", default=os.getenv("NOTIFY_TELEGRAM_STREAM", "notify:telegram"))
    ap.add_argument("--state-key", default=os.getenv("SRE_OF_GATE_STATE_KEY", "sre:of_gate:last_stats"))
    ap.add_argument("--alert-state-key", default=os.getenv("SRE_OF_GATE_ALERT_STATE_KEY", "sre:of_gate:alert_state"))
    ap.add_argument("--cooldown-sec", type=int, default=int(os.getenv("SRE_OF_GATE_ALERT_COOLDOWN_SEC", "1800")))
    ap.add_argument("--window-min", type=int, default=int(os.getenv("SRE_OF_GATE_WINDOW_MIN", "60")))
    ap.add_argument("--min-n", type=int, default=int(os.getenv("SRE_OF_GATE_MIN_N", "200")))
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
        _notify(r, args.notify_stream, text)

    print(json.dumps({"stats": stats, "alerts": raw_alerts, "actionable": actionable_alerts}, ensure_ascii=False))


if __name__ == "__main__":
    main()
