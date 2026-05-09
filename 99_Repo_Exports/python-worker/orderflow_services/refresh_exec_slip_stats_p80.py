from __future__ import annotations

#!/usr/bin/env python3
"""refresh_exec_slip_stats_p80.py

P80: Refresh materialized view mv_exec_slippage_eval_1h_stats to accelerate promoter/rollback/freezer.

ENV:
  ANALYTICS_DB_DSN or DATABASE_URL (required)
  EXEC_SLIP_STATS_MV (default: mv_exec_slippage_eval_1h_stats)
  EXEC_SLIP_STATS_REFRESH_TIMEOUT_S (default: 300)

  EXEC_SLIP_STATS_STATUS_PATH
    default: /var/lib/trade/of_reports/out/enforce/stats/exec_slip_stats_refresh_status.json

Optional Redis state (if REDIS_URL/CRYPTO_NOTIFY_REDIS_URL provided):
  state:exec_slip_stats_refresher:last_ok_ts_ms
  state:exec_slip_stats_refresher:last_dur_ms
  state:exec_slip_stats_refresher:last_ok,
""",
import json
import os
import sys
import time

from utils.time_utils import get_ny_time_millis

try:
    import psycopg2  # type: ignore
except Exception:
    psycopg2 = None


def _env_int(name: str, default: str) -> int:
    try:
        return int(os.getenv(name, default).strip())
    except Exception:
        return default


def _now_ms() -> int:
    return get_ny_time_millis()


def _write_status(path: str, obj: dict) -> None:
    try:
        p = (path or "").strip()
        if not p:
            return
        d = os.path.dirname(p)
        if d:
            os.makedirs(d, exist_ok=True)
        tmp = p + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(obj, f, ensure_ascii=False, separators=(",", ":"))
        os.replace(tmp, p)
    except Exception:
        return


def _connect_redis():
    rurl = os.getenv("REDIS_URL") or os.getenv("CRYPTO_NOTIFY_REDIS_URL") or ""
    if not str(rurl).strip():
        return None
    try:
        import redis  # type: ignore

        return redis.Redis.from_url(rurl, decode_responses=True)
    except Exception:
        return None


def main() -> int:
    status_path = os.getenv(
        "EXEC_SLIP_STATS_STATUS_PATH",
        "/var/lib/trade/of_reports/out/enforce/stats/exec_slip_stats_refresh_status.json",
    )

    if psycopg2 is None:
        _write_status(
            status_path,
            {
                "ok": False,
                "ts_ms": _now_ms(),
                "mv": os.getenv("EXEC_SLIP_STATS_MV", "mv_exec_slippage_eval_1h_stats"),
                "error": "psycopg2_missing",
            }
        )
        print("FATAL: psycopg2 not installed", file=sys.stderr)
        return 2

    dsn = os.getenv("ANALYTICS_DB_DSN") or (os.getenv("ANALYTICS_DB_DSN") or os.getenv("DATABASE_URL")) or ""
    if not dsn:
        _write_status(
            status_path,
            {
                "ok": False,
                "ts_ms": _now_ms(),
                "mv": os.getenv("EXEC_SLIP_STATS_MV", "mv_exec_slippage_eval_1h_stats"),
                "error": "missing_dsn",
            }
        )
        print("FATAL: missing ANALYTICS_DB_DSN/DATABASE_URL", file=sys.stderr)
        return 2

    mv = os.getenv("EXEC_SLIP_STATS_MV", "mv_exec_slippage_eval_1h_stats").strip() or "mv_exec_slippage_eval_1h_stats"
    timeout_s = _env_int("EXEC_SLIP_STATS_REFRESH_TIMEOUT_S", "300")

    t0 = time.time()
    ok = True
    err = ""
    try:
        conn = psycopg2.connect(dsn)
        conn.autocommit = True
        cur = conn.cursor()
        try:
            cur.execute(f"refresh materialized view concurrently {mv}")
        except Exception:
            cur.execute(f"refresh materialized view {mv}")
        cur.close()
        conn.close()
    except Exception as e:
        ok = False
        err = str(e)

    dur_ms = int((time.time() - t0) * 1000)
    ts_ms = _now_ms()

    _write_status(
        status_path,
        {
            "ok": bool(ok),
            "ts_ms": ts_ms,
            "mv": mv,
            "dur_ms": dur_ms,
            "error": err[:200] if err else "",
        }
    )

    r = _connect_redis()
    if r is not None:
        try:
            pipe = r.pipeline(transaction=False)
            pipe.set("state:exec_slip_stats_refresher:last_ok_ts_ms", str(ts_ms))
            pipe.set("state:exec_slip_stats_refresher:last_dur_ms", str(dur_ms))
            pipe.set("state:exec_slip_stats_refresher:last_ok", "1" if ok else "0")
            pipe.execute()
        except Exception:
            pass

    if not ok:
        print(f"refresh_exec_slip_stats_p80 failed: {err}", file=sys.stderr)
        return 1

    if (dur_ms / 1000.0) > float(timeout_s):
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
