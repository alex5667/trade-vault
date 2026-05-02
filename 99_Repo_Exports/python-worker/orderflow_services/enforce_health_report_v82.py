from __future__ import annotations
"""P82: Unified health report for enforce-bucket automation.

Writes a single JSON file suitable for dashboards and SRE on-call.

The report includes:
  - Redis stream staleness (metrics:of_gate)
  - DB view/MV freshness (v_exec_slippage_eval or mv_exec_slippage_eval_1h_stats)
  - Status files freshness (promoter/refresher/freezer)
  - Current enforce bucket cfg (global + per-symbol)
  - apply_blocked decision (soft/hard) and reasons

ENV:
  ENFORCE_HEALTH_REPORT_PATH
    default: /var/lib/trade/of_reports/out/enforce/health/enforce_health_report_v82.json

  ENFORCE_HEALTH_SYMBOLS=BTCUSDT,ETHUSDT (optional)

  REDIS_URL / CRYPTO_NOTIFY_REDIS_URL
  ANALYTICS_DB_DSN

  ENFORCE_REDIS_STREAM, ENFORCE_DB_VIEW_NAME, ENFORCE_DB_VIEW_TS_COL (see gates module)
""",
import json
import os
import time
from typing import Any, Dict, List, Optional


def _now_s() -> int:
    return int(time.time())


def _env_str(name: str, default: str) -> str:
    v = os.getenv(name)
    return str(v).strip() if v is not None and str(v).strip() else str(default)


def _env_list(name: str, default: str = "") -> List[str]:
    raw = str(os.getenv(name, default) or "").strip()
    if not raw:
        return []
    out: List[str] = []
    for x in raw.replace(";", ",").split(","):
        x = x.strip()
        if x:
            out.append(x)
    return out


def _write_json(path: str, obj: Dict[str, Any]) -> None:
    p = str(path or "").strip()
    if not p:
        return
    d = os.path.dirname(p)
    if d:
        os.makedirs(d, exist_ok=True)
    tmp = p + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, separators=(",", ":"))
    os.replace(tmp, p)


def _redis_from_env():
    import redis  # type: ignore

    url = os.getenv("CRYPTO_NOTIFY_REDIS_URL") or os.getenv("REDIS_URL") or ""
    url = str(url).strip()
    if not url:
        return None
    return redis.Redis.from_url(url, decode_responses=True)


def _db_from_env():
    import psycopg2  # type: ignore

    dsn = os.getenv("ANALYTICS_DB_DSN") or (os.getenv("ANALYTICS_DB_DSN") or os.getenv("DATABASE_URL")) or ""
    dsn = str(dsn).strip()
    if not dsn:
        return None
    return psycopg2.connect(dsn)


def _get_cfg_buckets(r: Any, key: str) -> Optional[str]:
    try:
        v = r.get(key)
        if v is None:
            return None
        return str(v)
    except Exception:
        return None


def build_report() -> Dict[str, Any]:
    from orderflow_services.enforce_health_gates_v82 import run_staleness_gates

    status_files = {
        "promoter": _env_str(
            "PROMOTE_STATUS_PATH",
            "/var/lib/trade/of_reports/out/enforce/promoter/enforce_bucket_promoter_status.json",
        ),
        "refresher": _env_str(
            "EXEC_SLIP_STATS_STATUS_PATH",
            "/var/lib/trade/of_reports/out/enforce/stats/exec_slip_stats_refresh_status.json",
        ),
        "freezer": _env_str(
            "ENFORCE_FREEZER_STATUS_PATH",
            "/var/lib/trade/of_reports/out/enforce/freezer/enforce_bucket_slo_freezer_status.json",
        ),
    }

    r = _redis_from_env()
    db = _db_from_env()

    gates = run_staleness_gates(redis_client=r, db_conn=db, status_files=status_files)

    cfg: Dict[str, Any] = {
        "global": {
            "slippage_decomp_enforce_buckets": None,
            "taker_flow_gate_enforce_buckets": None,
        },
        "per_symbol": {},
    }

    syms = _env_list("ENFORCE_HEALTH_SYMBOLS", "")
    if r is not None:
        cfg["global"]["slippage_decomp_enforce_buckets"] = _get_cfg_buckets(r, "cfg:slippage_decomp_enforce_buckets")
        cfg["global"]["taker_flow_gate_enforce_buckets"] = _get_cfg_buckets(r, "cfg:taker_flow_gate_enforce_buckets")
        for s in syms:
            cfg["per_symbol"][s] = {
                "slippage_decomp_enforce_buckets": _get_cfg_buckets(
                    r, f"cfg:slippage_decomp_enforce_buckets:{s}"
                ),
                "taker_flow_gate_enforce_buckets": _get_cfg_buckets(
                    r, f"cfg:taker_flow_gate_enforce_buckets:{s}"
                ),
            }

    out: Dict[str, Any] = {
        "ts_s": _now_s(),
        "apply_blocked": bool(gates.get("blocked")),
        "severity": str(gates.get("severity")),
        "reasons": list(gates.get("reasons") or []),
        "checks": gates.get("checks") or {},
        "cfg": cfg,
    }

    try:
        if db is not None:
            db.close()
    except Exception:
        pass

    return out


def main() -> int:
    path = _env_str(
        "ENFORCE_HEALTH_REPORT_PATH",
        "/var/lib/trade/of_reports/out/enforce/health/enforce_health_report_v82.json",
    )
    rep = build_report()
    _write_json(path, rep)
    # return 2 on hard block, 0 otherwise (tool semantics)
    if rep.get("apply_blocked") and rep.get("severity") == "hard":
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
