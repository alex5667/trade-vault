from __future__ import annotations

import argparse
import json
import os
from collections import defaultdict
from typing import Any

import redis

from core.ok_fields import get_scenario, get_ts_ms, parse_ok_fields
from domain.evidence_keys import MetaKeys
from utils.time_utils import get_ny_time_millis
import contextlib
from core.redis_keys import RedisStreams as RS


def _now_ms() -> int:
    return get_ny_time_millis()


def _f(x: Any, d: float = 0.0) -> float:
    try:
        if x is None:
            return d
        return float(x)
    except Exception:
        return d


def _i(x: Any, d: int = 0) -> int:
    try:
        if x is None:
            return d
        return int(float(x))
    except Exception:
        return d


def pctl(xs: list[float], q: float) -> float:
    if not xs:
        return 0.0
    xs = sorted(xs)
    i = int(round((len(xs) - 1) * q))
    i = max(0, min(len(xs) - 1, i))
    return float(xs[i])


def _read_stream_window(r: redis.Redis, stream: str, start_ms: int, window_ms: int, *, max_scan: int = 800000) -> list[dict[str, Any]]:
    end_ms = start_ms + window_ms
    rows: list[dict[str, Any]] = []
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


def _group_key(d: dict[str, Any]) -> str:
    sym = (d.get("symbol", "") or "").upper() or "NA"
    scen = get_scenario(d)
    return f"{sym}|{scen}"


def compute_snapshot(rows: list[dict[str, Any]], *, dh_bad_th: float) -> dict[str, Any]:
    n = len(rows)
    lat = []
    ml_lat = []
    execn = []
    ok = 0
    soft = 0
    meta_veto = 0
    book_bad = 0
    src_bad = 0
    dh_bad = 0

    g_n = defaultdict(int)
    g_ok = defaultdict(int)
    g_soft = defaultdict(int)
    g_lat = defaultdict(list)
    g_exec = defaultdict(list)

    for d in rows:
        ok_i, soft_i = parse_ok_fields(d)
        ok += 1 if ok_i == 1 else 0
        soft += 1 if soft_i == 1 else 0
        meta_veto += 1 if _i(d.get(MetaKeys.VETO, 0), 0) == 1 else 0
        book_bad += 1 if _i(d.get("book_health_ok", 1), 1) == 0 else 0
        src_bad += 1 if _i(d.get("source_consistency_ok", 1), 1) == 0 else 0
        dh = _f(d.get("data_health", 1.0), 1.0)
        dh_bad += 1 if dh < dh_bad_th else 0

        lu = _f(d.get("latency_us", 0.0), 0.0)
        if lu > 0:
            lat.append(lu)
        mlu = _f(d.get("ml_latency_us", 0.0), 0.0)
        if mlu > 0:
            ml_lat.append(mlu)
        en = _f(d.get("exec_risk_norm", 0.0), 0.0)
        if en > 0:
            execn.append(en)

        gk = _group_key(d)
        g_n[gk] += 1
        ok_i, soft_i = parse_ok_fields(d)
        g_ok[gk] += 1 if ok_i == 1 else 0
        g_soft[gk] += 1 if soft_i == 1 else 0
        if lu > 0:
            g_lat[gk].append(lu)
        if en > 0:
            g_exec[gk].append(en)

    snap = {
        "ts_ms": _now_ms(),
        "n": int(n),
        "no_data": 1 if n == 0 else 0,
        "ok_rate": float(ok / n) if n > 0 else None,
        "soft_rate": float(soft / n) if n > 0 else None,
        "meta_veto_rate": float(meta_veto / n) if n > 0 else None,
        "book_bad_rate": float(book_bad / n) if n > 0 else None,
        "source_inconsistency_rate": float(src_bad / n) if n > 0 else None,
        "data_health_bad_rate": float(dh_bad / n) if n > 0 else None,
        "latency_us": {"p50": pctl(lat, 0.50), "p95": pctl(lat, 0.95), "p99": pctl(lat, 0.99)},
        "ml_latency_us": {"p50": pctl(ml_lat, 0.50), "p95": pctl(ml_lat, 0.95), "p99": pctl(ml_lat, 0.99)},
        "exec_risk_norm": {"p50": pctl(execn, 0.50), "p90": pctl(execn, 0.90), "p99": pctl(execn, 0.99)},
    }

    # top 200 groups by volume
    items = sorted(g_n.items(), key=lambda kv: kv[1], reverse=True)[:200]
    groups = {}
    for k, gn in items:
        groups[k] = {
            "n": int(gn),
            "ok_rate": float(g_ok[k] / gn) if gn > 0 else None,
            "soft_rate": float(g_soft[k] / gn) if gn > 0 else None,
            "lat_p99_us": float(pctl(g_lat[k], 0.99)) if g_lat[k] else 0.0,
            "exec_p90": float(pctl(g_exec[k], 0.90)) if g_exec[k] else 0.0,
        }
    snap["groups"] = groups
    return snap


def diff_snapshot(base: dict[str, Any], cur: dict[str, Any], *, topk: int = 20) -> dict[str, Any]:
    def get(d: dict[str, Any], path: list[str], default: float = 0.0) -> float:
        x: Any = d
        for p in path:
            if not isinstance(x, dict) or p not in x:
                return default
            x = x[p]
        try:
            return float(x)
        except Exception:
            return default

    core = {
        "n_base": int(base.get("n", 0) or 0),
        "n_cur": int(cur.get("n", 0) or 0),
        "delta_ok_rate": get(cur, ["ok_rate"]) - get(base, ["ok_rate"]),
        "delta_soft_rate": get(cur, ["soft_rate"]) - get(base, ["soft_rate"]),
        "delta_lat_p99_us": get(cur, ["latency_us", "p99"]) - get(base, ["latency_us", "p99"]),
        "delta_exec_p90": get(cur, ["exec_risk_norm", "p90"]) - get(base, ["exec_risk_norm", "p90"]),
        "delta_src_bad": get(cur, ["source_inconsistency_rate"]) - get(base, ["source_inconsistency_rate"]),
        "delta_book_bad": get(cur, ["book_bad_rate"]) - get(base, ["book_bad_rate"]),
        "delta_dh_bad": get(cur, ["data_health_bad_rate"]) - get(base, ["data_health_bad_rate"]),
        "delta_ml_lat_p99_us": get(cur, ["ml_latency_us", "p99"]) - get(base, ["ml_latency_us", "p99"]),
    }

    bgs = base.get("groups", {}) if isinstance(base.get("groups", {}), dict) else {}
    cgs = cur.get("groups", {}) if isinstance(cur.get("groups", {}), dict) else {}
    keys = set(bgs.keys()) | set(cgs.keys())
    diffs = []
    for k in keys:
        b = bgs.get(k, {}) if isinstance(bgs.get(k, {}), dict) else {}
        c = cgs.get(k, {}) if isinstance(cgs.get(k, {}), dict) else {}
        bn = int(b.get("n", 0) or 0)
        cn = int(c.get("n", 0) or 0)
        if bn < 50 and cn < 50:
            continue
        dok = float(c.get("ok_rate", 0.0) or 0.0) - float(b.get("ok_rate", 0.0) or 0.0)
        dlat = float(c.get("lat_p99_us", 0.0) or 0.0) - float(b.get("lat_p99_us", 0.0) or 0.0)
        dexc = float(c.get("exec_p90", 0.0) or 0.0) - float(b.get("exec_p90", 0.0) or 0.0)
        score = abs(dok) + 0.00005 * abs(dlat) + 0.5 * abs(dexc)
        diffs.append((score, k, {"dok": dok, "dlat_p99_us": dlat, "dexec_p90": dexc, "n_base": bn, "n_cur": cn}))
    diffs.sort(key=lambda x: x[0], reverse=True)
    top = [{"key": k, **v} for _, k, v in diffs[:topk]]
    return {"core": core, "topdiff": top}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--redis-url", default=os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0"))
    ap.add_argument("--stream", default=os.getenv("OF_GATE_METRICS_STREAM", "metrics:of_gate"))
    ap.add_argument("--window-hours", type=float, default=float(os.getenv("OF_GOLDEN_WINDOW_HOURS", "24")))
    ap.add_argument("--baseline", default=os.getenv("OF_GOLDEN_BASELINE", "/var/lib/trade/of_gate_golden/baseline.json"))
    ap.add_argument("--out-dir", default=os.getenv("OF_GOLDEN_OUT_DIR", "/var/lib/trade/of_gate_golden"))
    ap.add_argument("--write-baseline", type=int, default=int(os.getenv("OF_GOLDEN_WRITE_BASELINE", "0")))
    ap.add_argument("--fail-on-drift", type=int, default=int(os.getenv("OF_GOLDEN_FAIL_ON_DRIFT", "1")))
    ap.add_argument("--notify", type=int, default=int(os.getenv("OF_GOLDEN_NOTIFY", "1")))
    ap.add_argument("--notify-stream", default=os.getenv("NOTIFY_TELEGRAM_STREAM", RS.NOTIFY_TELEGRAM))

    ap.add_argument("--min-n", type=int, default=int(os.getenv("OF_GOLDEN_MIN_N", "500")))
    ap.add_argument("--max-abs-dok", type=float, default=float(os.getenv("OF_GOLDEN_MAX_ABS_DOK", "0.05")))
    ap.add_argument("--max-dlat-p99-us", type=float, default=float(os.getenv("OF_GOLDEN_MAX_DLAT_P99_US", "5000")))
    ap.add_argument("--max-dexec-p90", type=float, default=float(os.getenv("OF_GOLDEN_MAX_DEXEC_P90", "0.10")))
    ap.add_argument("--max-dsrc-bad", type=float, default=float(os.getenv("OF_GOLDEN_MAX_DSRC_BAD", "0.02")))
    ap.add_argument("--max-dbook-bad", type=float, default=float(os.getenv("OF_GOLDEN_MAX_DBOOK_BAD", "0.02")))
    ap.add_argument("--max-ddh-bad", type=float, default=float(os.getenv("OF_GOLDEN_MAX_DDH_BAD", "0.05")))
    ap.add_argument("--dh-bad-th", type=float, default=float(os.getenv("OF_GOLDEN_DH_BAD_TH", "0.70")))
    args = ap.parse_args()

    os.makedirs(args.out_dir, exist_ok=True)
    r = redis.Redis.from_url(args.redis_url, decode_responses=True)
    window_ms = int(float(args.window_hours) * 3600_000)
    start_ms = _now_ms() - window_ms
    rows = _read_stream_window(r, args.stream, start_ms, window_ms)

    cur = compute_snapshot(rows, dh_bad_th=float(args.dh_bad_th))
    cur_path = os.path.join(args.out_dir, f"candidate_{_now_ms()}.json")
    with open(cur_path, "w", encoding="utf-8") as f:
        json.dump(cur, f, ensure_ascii=False, indent=2)

    if int(args.write_baseline) == 1 or not os.path.exists(args.baseline):
        os.makedirs(os.path.dirname(args.baseline) or ".", exist_ok=True)
        with open(args.baseline, "w", encoding="utf-8") as f:
            json.dump(cur, f, ensure_ascii=False, indent=2)
        print(json.dumps({"ok": True, "mode": "baseline_written", "baseline": args.baseline, "candidate": cur_path}, ensure_ascii=False, indent=2))
        return

    with open(args.baseline, encoding="utf-8") as f:
        base = json.load(f)

    diff = diff_snapshot(base, cur, topk=20)
    diff_path = os.path.join(args.out_dir, f"diff_{_now_ms()}.json")
    with open(diff_path, "w", encoding="utf-8") as f:
        json.dump(diff, f, ensure_ascii=False, indent=2)

    core = diff.get("core", {}) if isinstance(diff.get("core", {}), dict) else {}
    n_cur = int(cur.get("n", 0) or 0)
    fail = False
    reasons = []
    if n_cur >= int(args.min_n):
        if abs(float(core.get("delta_ok_rate", 0.0) or 0.0)) > float(args.max_abs_dok):
            fail = True; reasons.append("abs(dok) too high")
        if float(core.get("delta_lat_p99_us", 0.0) or 0.0) > float(args.max_dlat_p99_us):
            fail = True; reasons.append("dlat_p99_us too high")
        if float(core.get("delta_exec_p90", 0.0) or 0.0) > float(args.max_dexec_p90):
            fail = True; reasons.append("dexec_p90 too high")
        if float(core.get("delta_src_bad", 0.0) or 0.0) > float(args.max_dsrc_bad):
            fail = True; reasons.append("dsrc_bad too high")
        if float(core.get("delta_book_bad", 0.0) or 0.0) > float(args.max_dbook_bad):
            fail = True; reasons.append("dbook_bad too high")
        if float(core.get("delta_dh_bad", 0.0) or 0.0) > float(args.max_ddh_bad):
            fail = True; reasons.append("ddh_bad too high")

    if int(args.notify) == 1 and fail:
        msg = (
            f"OF_GATE_GOLDEN drift (last {args.window_hours}h) n={n_cur}\n"
            f"dok={float(core.get('delta_ok_rate',0.0) or 0.0):+.4f} "
            f"dlat_p99_us={float(core.get('delta_lat_p99_us',0.0) or 0.0):+.0f} "
            f"dexec_p90={float(core.get('delta_exec_p90',0.0) or 0.0):+.4f} "
            f"dsrc_bad={float(core.get('delta_src_bad',0.0) or 0.0):+.4f} "
            f"dbook_bad={float(core.get('delta_book_bad',0.0) or 0.0):+.4f} "
            f"ddh_bad={float(core.get('delta_dh_bad',0.0) or 0.0):+.4f}\n"
            f"reasons: {', '.join(reasons)}\n"
            f"topdiff: {diff.get('topdiff', [])[:5]}"
        )
        import html
        safe_msg = html.escape(msg)
        with contextlib.suppress(Exception):
            r.xadd(args.notify_stream, {"type": "report", "subtype": "of_gate_golden", "ts_ms": str(_now_ms()), "text": safe_msg}, maxlen=200000, approximate=True)

    print(json.dumps({"ok": True, "candidate": cur_path, "diff": diff_path, "fail": fail, "reasons": reasons}, ensure_ascii=False, indent=2))
    if int(args.fail_on_drift) == 1 and fail:
        raise SystemExit(2)


if __name__ == "__main__":
    main()

