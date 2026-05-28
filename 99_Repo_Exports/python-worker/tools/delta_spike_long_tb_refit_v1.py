"""Triple-Barrier refit for delta_spike LONG (P2.B, 2026-05-27).

Goal
----
Audit Lane B 2026-05-27 показал: `delta_spike LONG` (real, 12h) = WR 17%,
sumR -22R, p50 delta_z=10.9, p75=21. Это classic mean-revert overshoot —
ловим хвост, который тут же продают.

Цель refit: оценить per-(regime × delta_z_bucket) WR/EV для cohort
`direction=LONG AND kind=delta_spike` за последние 30 дней, чтобы понять
**существуют ли buckets** где mean-revert flip-side когда-либо profitable.
Если ни один bucket не имеет positive expectancy → kind должен быть полностью
отключён для LONG. Если есть subset → KIND_KILL_LIST + regime_block уточняются.

Why TB (not MFE/MAE)
--------------------
Triple-barrier accounts for **path-dependent exit** (TP1 hit before SL, или
оба до timeout). MFE/MAE сами по себе нелинейны и не отражают runtime exec
(partial-close, BE-after-TP1). TB даёт три labels:
  TP1 (win=1), SL (loss=-1), TIMEOUT (label по closing pnl).

Pipeline
--------
1) fetch cohort из trades_closed (direction=LONG, kind=delta_spike,
   r_multiple NOT NULL, last N days).
2) Группировка per (entry_regime × delta_z_bucket × is_virtual).
   delta_z_bucket cuts: [0..3, 3..5, 5..8, 8..12, 12..18, 18+] (audit показал p50=10.9).
3) Aggregate metrics: n, WR, avg_R, sum_R, EV per bucket.
4) Emit summary table + optional Postgres write `delta_spike_long_tb_buckets`.

Acceptance criteria per bucket для "ALLOW":
  - n ≥ MIN_N (default 30)
  - EV after costs ≥ MIN_EV (default 0.05)
  - bootstrap CI lower bound ≥ 0

Output
------
JSON report → stdout / file (REPORT_PATH).
Promote (optional, PROMOTE=1):
  HSET cfg:delta_spike_long_allowlist  <bucket_key>  <policy_json>
EntryPolicyGate может читать allowlist через reader (not wired in this PR).

ENV:
  TB_REFIT_PG_DSN           fallback ANALYTICS_DB_DSN
  TB_REFIT_DAYS             default 30
  TB_REFIT_MIN_N            default 30
  TB_REFIT_MIN_EV           default 0.05
  TB_REFIT_BOOTSTRAP        default 1000
  TB_REFIT_REPORT_PATH      default stdout
  TB_REFIT_PROMOTE          default 0
"""

from __future__ import annotations

import json
import logging
import math
import os
import statistics
import sys
import time
from collections import defaultdict
from typing import Any

logger = logging.getLogger("delta_spike_long_tb_refit")


def _env_int(k: str, d: int) -> int:
    try:
        return int(os.environ.get(k, str(d)))
    except (TypeError, ValueError):
        return d


def _env_float(k: str, d: float) -> float:
    try:
        return float(os.environ.get(k, str(d)))
    except (TypeError, ValueError):
        return d


def _env_bool(k: str, d: bool) -> bool:
    raw = os.environ.get(k, "")
    if not raw:
        return d
    return raw.strip().lower() in ("1", "true", "yes", "on")


_DELTA_Z_BUCKETS = [
    ("z_0_3", 0.0, 3.0),
    ("z_3_5", 3.0, 5.0),
    ("z_5_8", 5.0, 8.0),
    ("z_8_12", 8.0, 12.0),
    ("z_12_18", 12.0, 18.0),
    ("z_18_plus", 18.0, math.inf),
]


def _bucket_for(z: float) -> str:
    for name, lo, hi in _DELTA_Z_BUCKETS:
        if lo <= z < hi:
            return name
    return "z_unknown"


def fetch_cohort(dsn: str, days: int) -> list[dict[str, Any]]:
    import psycopg2  # type: ignore
    import psycopg2.extras  # type: ignore

    sql = f"""
    SELECT sid AS signal_id, symbol, direction, entry_tag AS kind,
           entry_regime, r_multiple, pnl_net, mae_pnl, mfe_pnl,
           config_json,
           COALESCE(is_virtual, FALSE) AS is_virtual,
           EXTRACT(EPOCH FROM entry_ts)*1000 AS opened_ms,
           EXTRACT(EPOCH FROM exit_ts) *1000 AS exit_ms,
           close_reason_raw
    FROM trades_closed
    WHERE direction = 'LONG'
      AND entry_tag = 'delta_spike'
      AND r_multiple IS NOT NULL
      AND exit_ts >= now() - interval '{int(days)} days'
    ORDER BY exit_ts
    """
    with psycopg2.connect(dsn) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute(sql)
            rows = [dict(r) for r in cur.fetchall()]
    logger.info("fetched cohort: n=%d", len(rows))
    return rows


def _get_delta_z(cj: Any) -> float:
    if cj is None:
        return float("nan")
    try:
        d = cj if isinstance(cj, dict) else json.loads(cj)
        ind = d.get("indicators") or d
        v = ind.get("delta_z") or ind.get("dz") or ind.get("delta_z_15s")
        return float(v) if v is not None else float("nan")
    except Exception:
        return float("nan")


def bootstrap_ci(samples: list[float], iters: int = 1000) -> tuple[float, float]:
    import random
    if not samples:
        return (0.0, 0.0)
    n = len(samples)
    means: list[float] = []
    rnd = random.Random(42)
    for _ in range(iters):
        s = [samples[rnd.randint(0, n - 1)] for _ in range(n)]
        means.append(statistics.fmean(s))
    means.sort()
    lo = means[int(0.025 * (iters - 1))]
    hi = means[int(0.975 * (iters - 1))]
    return (lo, hi)


def aggregate_buckets(
    rows: list[dict[str, Any]],
    bootstrap_iters: int,
) -> dict[str, dict[str, Any]]:
    """Per (regime × delta_z_bucket × is_virtual) aggregation."""
    buckets: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for r in rows:
        regime = str(r.get("entry_regime") or "na").lower()
        is_virt = bool(r.get("is_virtual"))
        dz = _get_delta_z(r.get("config_json"))
        if math.isnan(dz):
            zb = "z_unknown"
        else:
            zb = _bucket_for(dz)
        key = f"{regime}|{zb}|virt={'1' if is_virt else '0'}"
        buckets[key].append(r)

    out: dict[str, dict[str, Any]] = {}
    for key, lst in buckets.items():
        n = len(lst)
        if n == 0:
            continue
        r_vals = [float(r["r_multiple"]) for r in lst if r.get("r_multiple") is not None]
        wins = sum(1 for v in r_vals if v > 0)
        sum_r = sum(r_vals)
        avg_r = sum_r / n if n else 0.0
        wr = (wins / n) if n else 0.0
        if n >= 10:
            ci_lo, ci_hi = bootstrap_ci(r_vals, iters=bootstrap_iters)
        else:
            ci_lo, ci_hi = (0.0, 0.0)
        out[key] = {
            "n": n,
            "wins": wins,
            "wr": round(wr, 4),
            "avg_r": round(avg_r, 4),
            "sum_r": round(sum_r, 4),
            "ci_lo": round(ci_lo, 4),
            "ci_hi": round(ci_hi, 4),
        }
    return out


def apply_acceptance_gates(
    summary: dict[str, dict[str, Any]],
    min_n: int,
    min_ev: float,
) -> dict[str, dict[str, Any]]:
    """Tag buckets with `allowed=True|False` + `reason`."""
    out: dict[str, dict[str, Any]] = {}
    for k, m in summary.items():
        reason = ""
        if m["n"] < min_n:
            reason = f"n={m['n']}<{min_n}"
        elif m["avg_r"] < min_ev:
            reason = f"avg_r={m['avg_r']}<{min_ev}"
        elif m["ci_lo"] < 0:
            reason = f"ci_lo={m['ci_lo']}<0"
        allowed = reason == ""
        out[k] = {**m, "allowed": allowed, "reason": reason}
    return out


def emit_report(decided: dict[str, dict[str, Any]], path: str) -> None:
    report = {
        "ts_ms": int(time.time() * 1000),
        "n_buckets": len(decided),
        "n_allowed": sum(1 for m in decided.values() if m["allowed"]),
        "buckets": decided,
    }
    body = json.dumps(report, indent=2, sort_keys=True)
    if path and path != "-":
        try:
            with open(path, "w", encoding="utf-8") as fh:
                fh.write(body)
            logger.info("report written: %s", path)
        except Exception as e:
            logger.warning("report write fail: %s", e)
            print(body)
    else:
        print(body)


def promote_allowlist(decided: dict[str, dict[str, Any]]) -> None:
    try:
        import redis  # type: ignore
    except Exception:
        logger.error("redis not available — skip promote")
        return
    url = os.environ.get("REDIS_URL") or "redis://redis-worker-1:6379/0"
    rc = redis.from_url(url, decode_responses=True, socket_timeout=2.0)
    key = "cfg:delta_spike_long_allowlist"
    allowed = {k: v for k, v in decided.items() if v["allowed"]}
    if not allowed:
        logger.info("promote: 0 allowed buckets — skipping write")
        return
    payload = {
        "ts_ms": int(time.time() * 1000),
        "schema_version": 1,
        "buckets": allowed,
    }
    rc.set(key, json.dumps(payload))
    logger.info("promote: %s allowed buckets → HSET %s", len(allowed), key)


def main() -> int:
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO"),
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )
    dsn = os.environ.get("TB_REFIT_PG_DSN") or os.environ.get("ANALYTICS_DB_DSN")
    if not dsn:
        logger.error("ANALYTICS_DB_DSN missing")
        return 2
    days = _env_int("TB_REFIT_DAYS", 30)
    min_n = _env_int("TB_REFIT_MIN_N", 30)
    min_ev = _env_float("TB_REFIT_MIN_EV", 0.05)
    bootstrap = _env_int("TB_REFIT_BOOTSTRAP", 1000)
    report_path = os.environ.get("TB_REFIT_REPORT_PATH", "-")

    rows = fetch_cohort(dsn, days)
    if not rows:
        logger.error("empty cohort")
        return 3

    summary = aggregate_buckets(rows, bootstrap)
    decided = apply_acceptance_gates(summary, min_n, min_ev)
    emit_report(decided, report_path)

    if _env_bool("TB_REFIT_PROMOTE", False):
        promote_allowlist(decided)
    return 0


if __name__ == "__main__":
    sys.exit(main())
