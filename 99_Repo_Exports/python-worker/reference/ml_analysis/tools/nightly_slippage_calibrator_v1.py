#!/usr/bin/env python3
"""nightly_slippage_calibrator_v1.py

Nightly calibrator for slippage decomposition impact coefficient.

Model:
  expected_slippage_decomp_bps = spread_bps + k_bps * impact_proxy

We fit per-symbol, per-exec_regime_bucket coefficient k_bps using DB view:
  v_exec_slippage_eval

Target:
  impact_target_bps = max(0, realized_slip_worse_bps - spread_bps)
  ratio = impact_target_bps / max(eps, impact_proxy)
  k_bps = median(ratio) (robust) with optional EMA smoothing vs previous k.

Writes (unless dry-run):
  cfg:slippage_decomp_impact_coeff_bps:{sym}:{bucket} = <k_bps>

State keys (best-effort, for exporters/alerts):
  state:slippage_calibrator:last_ok_ts_ms
  state:slippage_calibrator:last_dur_ms
  state:slippage_calibrator:last_ok

Outputs:
  - status JSON:  SLIPPAGE_CAL_STATUS_PATH
  - report JSON:  SLIPPAGE_CAL_REPORT_PATH

Exit codes:
  0 OK (or skipped with reason written to status)
  1 failure
  2 soft-fail (missing deps / missing DSN)
"""

from __future__ import annotations
from utils.time_utils import get_ny_time_millis

import argparse
import json
import math
import os
import re
import time
from datetime import datetime
from typing import Any, Dict, List, Optional, Sequence, Tuple


def _now_ms() -> int:
    return get_ny_time_millis()


def _env_int(name: str, default: str) -> int:
    try:
        return int(str(os.getenv(name, default)).strip())
    except Exception:
        return int(default)


def _env_float(name: str, default: str) -> float:
    try:
        return float(str(os.getenv(name, default)).strip())
    except Exception:
        return float(default)


def _parse_list(raw: str) -> List[str]:
    s = str(raw or "").strip()
    if not s:
        return []
    out: List[str] = []
    for p in s.replace(";", ",").split(","):
        x = p.strip().upper()
        if x and x not in out:
            out.append(x)
    return out


def _safe_ident(name: str, default: str) -> str:
    s = str(name or "").strip()
    if not s:
        return default
    # allow schema-qualified view names: public.v_exec_slippage_eval
    if not re.fullmatch(r"[A-Za-z0-9_\.]+", s):
        return default
    return s


def _write_json_atomic(path: str, obj: Dict[str, Any]) -> None:
    try:
        p = str(path or "").strip()
        if not p:
            return
        d = os.path.dirname(os.path.abspath(p))
        if d:
            os.makedirs(d, exist_ok=True)
        tmp = p + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(obj, f, ensure_ascii=False, indent=2, sort_keys=True)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, p)
    except Exception:
        return


def _median(xs: Sequence[float]) -> float:
    if not xs:
        return float("nan")
    ys = sorted(float(x) for x in xs)
    n = len(ys)
    mid = n // 2
    if n % 2 == 1:
        return float(ys[mid])
    return float((ys[mid - 1] + ys[mid]) / 2.0)


def _percentile(xs: Sequence[float], q: float) -> float:
    if not xs:
        return float("nan")
    if q <= 0:
        return float(min(xs))
    if q >= 1:
        return float(max(xs))
    ys = sorted(float(x) for x in xs)
    pos = q * (len(ys) - 1)
    lo = int(math.floor(pos))
    hi = int(math.ceil(pos))
    if lo == hi:
        return float(ys[lo])
    w = pos - lo
    return float(ys[lo] * (1 - w) + ys[hi] * w)


def _connect_redis(url: str):
    u = str(url or "").strip()
    if not u:
        return None
    try:
        import redis  # type: ignore

        return redis.Redis.from_url(u, decode_responses=True)
    except Exception:
        return None


def _pg_connect(dsn: str):
    try:
        import psycopg2  # type: ignore

        return psycopg2.connect(dsn)
    except Exception:
        return None


def _select_symbols(cur, *, view: str, lookback_days: int, max_syms: int) -> List[str]:
    q = f"""
      select sym
             sum(size_usd) as usd
             count(*) as n
      from {view}
      where ts > now() - (%s * interval '1 day')
      group by sym
      order by usd desc nulls last, n desc
      limit %s
    """
    cur.execute(q, (int(lookback_days), int(max_syms)))
    out: List[str] = []
    for row in cur.fetchall() or []:
        try:
            s = str(row[0] or "").strip().upper()
            if s and s not in out:
                out.append(s)
        except Exception:
            continue
    return out


def _fetch_rows(cur, *, view: str, sym: str, bucket: str, lookback_days: int, min_impact_proxy: float) -> List[Tuple[float, float, float]]:
    q = f"""
      select spread_bps, impact_proxy, realized_slip_worse_bps
      from {view}
      where ts > now() - (%s * interval '1 day')
        and sym = %s
        and exec_regime_bucket = %s
        and impact_proxy > %s
    """
    cur.execute(q, (int(lookback_days), str(sym), str(bucket), float(min_impact_proxy)))
    rows = cur.fetchall() or []
    out: List[Tuple[float, float, float]] = []
    for r in rows:
        try:
            spread = float(r[0] or 0.0)
            imp = float(r[1] or 0.0)
            realized = float(r[2] or 0.0)
            out.append((spread, imp, realized))
        except Exception:
            continue
    return out


def _fit_k_bps(
    rows: Sequence[Tuple[float, float, float]]
    *
    cap_k_bps: float
    trim_q: float
) -> Dict[str, Any]:
    ratios: List[float] = []
    for spread, imp, realized in rows:
        if not math.isfinite(spread) or not math.isfinite(imp) or not math.isfinite(realized):
            continue
        if imp <= 0:
            continue
        impact_target = realized - max(0.0, spread)
        if impact_target <= 0:
            continue
        r = impact_target / imp
        if not math.isfinite(r):
            continue
        if r < 0:
            continue
        if r > cap_k_bps:
            r = cap_k_bps
        ratios.append(float(r))

    res: Dict[str, Any] = {
        "n_raw": int(len(rows))
        "n_used": int(len(ratios))
        "k_median_bps": None
        "k_p10_bps": None
        "k_p90_bps": None
        "k_trim_mean_bps": None
    }
    if not ratios:
        return res

    k_med = _median(ratios)
    p10 = _percentile(ratios, 0.10)
    p90 = _percentile(ratios, 0.90)

    # Optional trimming to reduce tail noise (keep within [q, 1-q])
    if 0.0 < trim_q < 0.5:
        lo = _percentile(ratios, trim_q)
        hi = _percentile(ratios, 1.0 - trim_q)
        trimmed = [x for x in ratios if x >= lo and x <= hi]
    else:
        trimmed = list(ratios)

    k_trim = float(sum(trimmed) / max(1, len(trimmed)))

    res["k_median_bps"] = float(k_med)
    res["k_p10_bps"] = float(p10)
    res["k_p90_bps"] = float(p90)
    res["k_trim_mean_bps"] = float(k_trim)
    return res


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser()

    ap.add_argument("--dsn", default=os.getenv("ANALYTICS_DB_DSN") or (os.getenv("ANALYTICS_DB_DSN") or os.getenv("DATABASE_URL")) or "")
    ap.add_argument("--redis-url", default=os.getenv("REDIS_URL") or os.getenv("CRYPTO_NOTIFY_REDIS_URL") or "")

    ap.add_argument("--view", default=os.getenv("SLIPPAGE_CAL_VIEW", "v_exec_slippage_eval"))
    ap.add_argument("--lookback-days", type=int, default=_env_int("SLIPPAGE_CAL_LOOKBACK_DAYS", "7"))

    ap.add_argument("--symbols", default=os.getenv("SLIPPAGE_CALIBRATOR_SYMBOLS", ""))
    ap.add_argument("--max-syms", type=int, default=_env_int("SLIPPAGE_CALIBRATOR_MAX_SYMS", "10"))

    ap.add_argument(
        "--buckets"
        default=os.getenv("SLIPPAGE_CALIBRATOR_BUCKETS", "NORMAL,LOW_LIQ,HIGH_VOL,HIGH_VOL_LOW_LIQ")
    )

    ap.add_argument("--min-n", type=int, default=_env_int("SLIPPAGE_CALIBRATOR_MIN_N", "120"))
    ap.add_argument("--min-impact-proxy", type=float, default=_env_float("SLIPPAGE_CALIBRATOR_MIN_IMPACT_PROXY", "1e-9"))

    ap.add_argument("--cap-k-bps", type=float, default=_env_float("SLIPPAGE_CALIBRATOR_CAP_K_BPS", "200"))
    ap.add_argument("--trim-q", type=float, default=_env_float("SLIPPAGE_CALIBRATOR_TRIM_Q", "0.10"))

    # EMA smoothing: alpha applied to new fit, (1-alpha) to previous Redis value
    ap.add_argument("--ema-alpha", type=float, default=_env_float("SLIPPAGE_CALIBRATOR_EMA_ALPHA", "0.30"))

    ap.add_argument("--dry-run", type=int, default=_env_int("SLIPPAGE_CALIBRATOR_DRY_RUN", "0"))
    ap.add_argument("--once", action="store_true", default=True)

    ap.add_argument(
        "--status-path"
        default=os.getenv(
            "SLIPPAGE_CAL_STATUS_PATH"
            "/var/lib/trade/of_reports/out/enforce/stats/slippage_calibrator_status.json"
        )
    )
    ap.add_argument(
        "--report-path"
        default=os.getenv(
            "SLIPPAGE_CAL_REPORT_PATH"
            "/var/lib/trade/of_reports/out/enforce/stats/slippage_calibrator_report.json"
        )
    )

    args = ap.parse_args(argv)

    stamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    t0 = time.time()

    status: Dict[str, Any] = {
        "ts_ms": _now_ms()
        "stamp": stamp
        "ok": False
        "dry_run": bool(int(args.dry_run) == 1)
        "lookback_days": int(args.lookback_days)
        "min_n": int(args.min_n)
        "view": str(args.view)
        "error": ""
    }

    dsn = str(args.dsn or "").strip()
    if not dsn:
        status["error"] = "missing_dsn"
        _write_json_atomic(str(args.status_path), status)
        return 2

    view = _safe_ident(str(args.view), "v_exec_slippage_eval")

    # Connect DB
    conn = _pg_connect(dsn)
    if conn is None:
        status["error"] = "psycopg2_missing_or_connect_failed"
        _write_json_atomic(str(args.status_path), status)
        return 2

    # Connect Redis (optional for dry-run)
    r = _connect_redis(str(args.redis_url))
    if r is None and int(args.dry_run) != 1:
        status["error"] = "redis_unavailable"
        _write_json_atomic(str(args.status_path), status)
        try:
            conn.close()
        except Exception:
            pass
        return 1

    buckets = _parse_list(str(args.buckets))
    if not buckets:
        buckets = ["NORMAL", "LOW_LIQ", "HIGH_VOL", "HIGH_VOL_LOW_LIQ"]

    # Determine symbols
    symbols = _parse_list(str(args.symbols))

    try:
        cur = conn.cursor()

        if not symbols:
            symbols = _select_symbols(cur, view=view, lookback_days=int(args.lookback_days), max_syms=int(args.max_syms))

        status["symbols"] = symbols
        status["buckets"] = buckets

        if not symbols:
            status["ok"] = True
            status["error"] = "no_symbols"
            _write_json_atomic(str(args.status_path), status)
            try:
                cur.close()
                conn.close()
            except Exception:
                pass
            return 0

        report: Dict[str, Any] = {
            "ts_ms": status["ts_ms"]
            "stamp": stamp
            "lookback_days": int(args.lookback_days)
            "min_n": int(args.min_n)
            "cap_k_bps": float(args.cap_k_bps)
            "trim_q": float(args.trim_q)
            "ema_alpha": float(args.ema_alpha)
            "dry_run": bool(int(args.dry_run) == 1)
            "items": []
        }

        written = 0
        skipped = 0

        for sym in symbols:
            for b in buckets:
                rows = _fetch_rows(
                    cur
                    view=view
                    sym=sym
                    bucket=b
                    lookback_days=int(args.lookback_days)
                    min_impact_proxy=float(args.min_impact_proxy)
                )

                fit = _fit_k_bps(rows, cap_k_bps=float(args.cap_k_bps), trim_q=float(args.trim_q))

                item: Dict[str, Any] = {
                    "sym": sym
                    "bucket": b
                    **fit
                }

                k_med = fit.get("k_median_bps")
                if k_med is None or not isinstance(k_med, (int, float)):
                    skipped += 1
                    item["reason"] = "no_fit"
                    report["items"].append(item)
                    continue

                if int(fit.get("n_used") or 0) < int(args.min_n):
                    skipped += 1
                    item["reason"] = f"insufficient_n:{fit.get('n_used')}"
                    report["items"].append(item)
                    continue

                # EMA smoothing vs existing k (if any)
                prev = None
                key = f"cfg:slippage_decomp_impact_coeff_bps:{sym}:{b}"
                if r is not None:
                    try:
                        pv = r.get(key)
                        if pv not in (None, "", "na"):
                            prev = float(pv)
                    except Exception:
                        prev = None

                alpha = float(args.ema_alpha)
                if alpha < 0.0:
                    alpha = 0.0
                if alpha > 1.0:
                    alpha = 1.0

                k_new = float(k_med)
                if prev is not None and math.isfinite(prev) and alpha > 0:
                    k_new = float(alpha * k_new + (1.0 - alpha) * float(prev))

                # clamp to [0, cap]
                if k_new < 0:
                    k_new = 0.0
                if k_new > float(args.cap_k_bps):
                    k_new = float(args.cap_k_bps)

                item["prev_k_bps"] = prev
                item["k_bps"] = k_new
                item["key"] = key

                if int(args.dry_run) != 1 and r is not None:
                    try:
                        r.set(key, f"{k_new:.6f}")
                        written += 1
                    except Exception as e:
                        item["write_error"] = str(e)[:200]
                        report["items"].append(item)
                        continue

                report["items"].append(item)

        report["written"] = int(written)
        report["skipped"] = int(skipped)

        status["ok"] = True
        status["written"] = int(written)
        status["skipped"] = int(skipped)

        dur_ms = int((time.time() - t0) * 1000)
        status["dur_ms"] = dur_ms

        _write_json_atomic(str(args.report_path), report)
        _write_json_atomic(str(args.status_path), status)

        # Export state keys for exporters/alerts
        if r is not None:
            try:
                pipe = r.pipeline(transaction=False)
                pipe.set("state:slippage_calibrator:last_ok_ts_ms", str(int(status["ts_ms"])))
                pipe.set("state:slippage_calibrator:last_dur_ms", str(int(dur_ms)))
                pipe.set("state:slippage_calibrator:last_ok", "1")
                pipe.execute()
            except Exception:
                pass

        try:
            cur.close()
            conn.close()
        except Exception:
            pass

        return 0

    except Exception as e:
        status["error"] = str(e)[:300]
        status["dur_ms"] = int((time.time() - t0) * 1000)
        _write_json_atomic(str(args.status_path), status)
        if r is not None:
            try:
                r.set("state:slippage_calibrator:last_ok", "0", ex=24 * 3600)
            except Exception:
                pass
        try:
            conn.close()
        except Exception:
            pass
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
