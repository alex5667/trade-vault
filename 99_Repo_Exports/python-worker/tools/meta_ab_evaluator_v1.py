from __future__ import annotations
from utils.time_utils import get_ny_time_millis

import argparse
import json
import os
import time
from typing import Any, Dict, List, Tuple

import redis


def now_ms() -> int:
    return get_ny_time_millis()


def safe_float(x: Any, d: float = 0.0) -> float:
    try:
        v = float(x)
        return v if v == v and abs(v) != float("inf") else d
    except Exception:
        return d


def safe_int(x: Any, d: int = 0) -> int:
    try:
        return int(float(x))
    except Exception:
        return d


def _loads_maybe_json(x: Any) -> Dict[str, Any]:
    if x is None:
        return {}
    if isinstance(x, dict):
        return x
    if isinstance(x, (bytes, bytearray)):
        try:
            x = x.decode("utf-8", "ignore")
        except Exception:
            return {}
    if isinstance(x, str):
        s = x.strip()
        if not s:
            return {}
        try:
            v = json.loads(s)
            return v if isinstance(v, dict) else {}
        except Exception:
            return {}
    return {}


def _extract_meta(fields: Dict[str, Any]) -> Dict[str, Any]:
    meta = _loads_maybe_json(fields.get("meta") or fields.get("metadata"))
    if meta:
        return meta
    payload = _loads_maybe_json(fields.get("payload"))
    if isinstance(payload, dict) and payload:
        m2 = payload.get("meta") or payload.get("metadata") or {}
        m2 = _loads_maybe_json(m2)
        if m2:
            return m2
    return {}


def _extract_of_evidence(meta: Dict[str, Any]) -> Dict[str, Any]:
    if isinstance(meta.get("of_confirm"), dict):
        oc = meta.get("of_confirm") or {}
        if isinstance(oc.get("evidence"), dict):
            return oc.get("evidence") or {}
    if isinstance(meta.get("evidence"), dict):
        return meta.get("evidence") or {}
    if isinstance(meta.get("confirm_evidence"), dict):
        return meta.get("confirm_evidence") or {}
    return {}


def _extract_r_mult(fields: Dict[str, Any], meta: Dict[str, Any]) -> float:
    rm = meta.get("r_mult", fields.get("r_mult", None))
    if rm is not None:
        return safe_float(rm, 0.0)
    pnl = safe_float(fields.get("pnl", meta.get("pnl", 0.0)), 0.0)
    risk = safe_float(fields.get("risk_usd", meta.get("risk_usd", 0.0)), 0.0)
    if risk > 0:
        return pnl / risk
    return 0.0


def read_trades_closed(r: redis.Redis, stream: str, since_ms: int, max_scan: int):
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
            ts = safe_int(fields.get("ts_ms", fields.get("ts", fields.get("timestamp", 0))), 0)
            if ts and ts < since_ms:
                scanned = max_scan
                break
            row = dict(fields)
            row["_ts_ms"] = ts
            yield row


def atomic_promote(challenger_path: str, champion_path: str) -> Tuple[bool, str]:
    try:
        challenger_path = str(challenger_path or "").strip()
        champion_path = str(champion_path or "").strip()
        if not challenger_path or not champion_path:
            return False, "missing_path"
        if not os.path.exists(challenger_path):
            return False, "challenger_missing"
        d = os.path.dirname(champion_path) or "."
        os.makedirs(d, exist_ok=True)
        ts = now_ms()
        backup = champion_path + f".bak.{ts}"
        if os.path.exists(champion_path):
            try:
                import shutil

                shutil.copy2(champion_path, backup)
            except Exception:
                pass
        tmp = champion_path + f".tmp.{ts}"
        import shutil

        shutil.copy2(challenger_path, tmp)
        os.replace(tmp, champion_path)
        return True, "ok"
    except Exception as e:
        return False, f"err:{type(e).__name__}"


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--redis-url", default=os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0"))
    ap.add_argument("--stream", default=os.getenv("TRADES_CLOSED_STREAM", "trades:closed"))
    ap.add_argument("--since-min", type=int, default=720)
    ap.add_argument("--max-scan", type=int, default=500000)
    ap.add_argument("--min-n", type=int, default=int(os.getenv("META_AB_MIN_N", "200")))
    ap.add_argument("--min-delta-mean-r", type=float, default=float(os.getenv("META_AB_MIN_DELTA_MEAN_R", "0.05")))
    ap.add_argument("--promote", action="store_true")
    args = ap.parse_args()

    r = redis.Redis.from_url(args.redis_url, decode_responses=True)
    since_ms = now_ms() - args.since_min * 60_000

    stats: Dict[str, Dict[str, Any]] = {"champion": {"r": []}, "challenger": {"r": []}, "unknown": {"r": []}}
    miss_arm = 0
    miss_meta = 0
    n_total = 0

    for x in read_trades_closed(r, args.stream, since_ms, args.max_scan):
        n_total += 1
        meta = _extract_meta(x)
        if not meta:
            miss_meta += 1
        ev = _extract_of_evidence(meta)
        arm = str(ev.get("meta_arm", meta.get("meta_arm", "")) or "").lower()
        if arm not in ("champion", "challenger"):
            arm = "unknown"
            miss_arm += 1
        rm = _extract_r_mult(x, meta)
        stats[arm]["r"].append(float(rm))

    if n_total == 0:
        return

    def summarize(arr: List[float]) -> Dict[str, Any]:
        if not arr:
            return {"n": 0}
        arr2 = sorted(arr)
        n = len(arr2)
        win = sum(1 for v in arr2 if v > 0)
        mean = sum(arr2) / n
        med = arr2[n // 2]
        p90 = arr2[int(0.90 * (n - 1))]
        return {"n": n, "win_rate": win / max(1, n), "mean_r": mean, "median_r": med, "p90_r": p90}

    champ = summarize(stats["champion"]["r"])
    chal = summarize(stats["challenger"]["r"])

    winner = "none"
    delta_mean_r = 0.0
    if champ.get("n", 0) >= args.min_n and chal.get("n", 0) >= args.min_n:
        delta_mean_r = float(chal.get("mean_r", 0.0) - champ.get("mean_r", 0.0))
        winner = "challenger" if delta_mean_r >= float(args.min_delta_mean_r) else "champion"

    report = {
        "ts_ms": now_ms(),
        "since_min": int(args.since_min),
        "n_total": n_total,
        "missing_meta_n": int(miss_meta),
        "missing_arm_n": int(miss_arm),
        "champion": champ,
        "challenger": chal,
        "winner": winner,
        "delta_mean_r": delta_mean_r,
        "min_n": int(args.min_n),
        "min_delta_mean_r": float(args.min_delta_mean_r),
    }

    r.set("meta_ab:last_report", json.dumps(report, separators=(",", ":")))
    r.set("meta_ab:last_ts_ms", str(report["ts_ms"]))
    try:
        r.xadd(
            os.getenv("META_AB_METRICS_STREAM", "metrics:meta_ab"),
            {"ts_ms": str(report["ts_ms"]), "json": json.dumps(report)},
            maxlen=200000,
            approximate=True,
        )
    except Exception:
        pass

    if args.promote and winner == "challenger":
        champion_path = os.getenv("META_MODEL_PATH", "").strip()
        challenger_path = os.getenv("META_MODEL_CHALLENGER_PATH", "").strip()
        ok, reason = atomic_promote(challenger_path, champion_path)
        report["promote"] = {"ok": ok, "reason": reason}
        r.set("meta_ab:last_report", json.dumps(report, separators=(",", ":")))
        r.set("meta_ab:last_ts_ms", str(report["ts_ms"]))


if __name__ == "__main__":
    main()
