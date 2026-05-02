#!/usr/bin/env python3
from __future__ import annotations
"""P71 Policy Effectiveness Report Worker.

Computes 24h policy effectiveness deltas vs baseline (policy_effective_mode=ok)
from already-computed Signal Quality by policy mode KPIs in settings:dynamic_cfg.

Writes:
  - settings:dynamic_cfg:
      policy_effectiveness_last_ts_ms
      policy_effectiveness_baseline_ok_present
      policy_effectiveness_share_24h_{mode}
      policy_effectiveness_expectancy_r_delta_24h_{mode}
      policy_effectiveness_precision_top5p_delta_24h_{mode}
      policy_effectiveness_ece_delta_24h_{mode}
      policy_effectiveness_input_last_ts_ms
      policy_effectiveness_total_n_24h
  - reports:policy_effectiveness:p71:last_json (SET)
  - reports:policy_effectiveness:p71:last_csv  (SET)

This worker is intentionally lightweight and low-cardinality.
Input freshness is monitored separately by P70 signal quality policy-mode alerts.
""",
from utils.time_utils import get_ny_time_millis

import argparse
import json
import os
import time
from dataclasses import dataclass
from typing import Any, Dict, List

import redis


POLICY_MODES: List[str] = ["ok", "warn", "block", "unknown"]


def _now_ms() -> int:
    return get_ny_time_millis()


def _to_int(v: Any, default: int = 0) -> int:
    try:
        if v is None:
            return default
        if isinstance(v, (bytes, bytearray)):
            v = v.decode("utf-8", errors="replace")
        return int(float(str(v).strip()))
    except Exception:
        return default


def _to_float(v: Any, default: float = 0.0) -> float:
    try:
        if v is None:
            return default
        if isinstance(v, (bytes, bytearray)):
            v = v.decode("utf-8", errors="replace")
        s = str(v).strip()
        if s == "" or s.lower() in {"nan", "none", "null"}:
            return default
        return float(s)
    except Exception:
        return default


@dataclass(frozen=True)
class ModeKPI:
    mode: str
    n: int
    expectancy_r: float
    precision_top5p: float
    ece: float


def _redis() -> redis.Redis:
    url = os.environ.get("REDIS_URL", "redis://localhost:6379/0")
    return redis.from_url(url, decode_responses=False)


def _hgetall_decoded(r: redis.Redis, key: str) -> Dict[str, str]:
    raw = r.hgetall(key)
    out: Dict[str, str] = {}
    for k, v in raw.items():
        kk = k.decode("utf-8", errors="replace") if isinstance(k, (bytes, bytearray)) else str(k)
        vv = v.decode("utf-8", errors="replace") if isinstance(v, (bytes, bytearray)) else str(v)
        out[kk] = vv
    return out


def _build_mode_kpis(cfg: Dict[str, str]) -> Dict[str, ModeKPI]:
    out: Dict[str, ModeKPI] = {}
    for mode in POLICY_MODES:
        n = _to_int(cfg.get(f"signal_quality_n_24h_policy_{mode}"), 0)
        expectancy_r = _to_float(cfg.get(f"signal_quality_expectancy_r_24h_policy_{mode}"), 0.0)
        precision_top5p = _to_float(cfg.get(f"signal_quality_precision_top5p_24h_policy_{mode}"), 0.0)
        ece = _to_float(cfg.get(f"signal_quality_ece_24h_policy_{mode}"), 0.0)
        out[mode] = ModeKPI(mode=mode, n=n, expectancy_r=expectancy_r, precision_top5p=precision_top5p, ece=ece)
    return out


def _as_csv(rows: List[Dict[str, Any]]) -> str:
    cols = [
        "mode",
        "n",
        "share_24h",
        "expectancy_r_24h",
        "precision_top5p_24h",
        "ece_24h",
        "expectancy_r_delta_24h",
        "precision_top5p_delta_24h",
        "ece_delta_24h",
    ]
    lines = [",".join(cols)]
    for r in rows:
        line: List[str] = []
        for c in cols:
            v = r.get(c)
            if v is None:
                line.append("")
            elif isinstance(v, float):
                line.append(f"{v:.6f}")
            else:
                line.append(str(v))
        lines.append(",".join(line))
    return "\n".join(lines) + "\n"


def run_once() -> int:
    dyn_cfg_key = os.environ.get("DYN_CFG_KEY", "settings:dynamic_cfg")
    baseline_min_n = _to_int(os.environ.get("POLICY_EFF_BASELINE_MIN_N", "40"), 40)

    r = _redis()

    try:
        cfg = _hgetall_decoded(r, dyn_cfg_key)
    except Exception as e:
        print(f"policy_effectiveness: cannot read {dyn_cfg_key}: {e}")
        return 2

    last_in_ts_ms = _to_int(cfg.get("signal_quality_policy_mode_last_ts_ms"), 0)
    mk = _build_mode_kpis(cfg)

    total_n = sum(v.n for v in mk.values())
    ok = mk.get("ok", ModeKPI("ok", 0, 0.0, 0.0, 0.0))
    baseline_ok_present = 1 if (ok.n >= baseline_min_n and ok.n > 0) else 0

    rows: List[Dict[str, Any]] = []
    for mode in POLICY_MODES:
        m = mk[mode]
        share = (m.n / total_n) if total_n > 0 else 0.0
        if baseline_ok_present:
            d_exp = m.expectancy_r - ok.expectancy_r
            d_prec = m.precision_top5p - ok.precision_top5p
            d_ece = m.ece - ok.ece
        else:
            d_exp = 0.0
            d_prec = 0.0
            d_ece = 0.0
        rows.append(
            {
                "mode": mode,
                "n": int(m.n),
                "share_24h": float(share),
                "expectancy_r_24h": float(m.expectancy_r),
                "precision_top5p_24h": float(m.precision_top5p),
                "ece_24h": float(m.ece),
                "expectancy_r_delta_24h": float(d_exp),
                "precision_top5p_delta_24h": float(d_prec),
                "ece_delta_24h": float(d_ece),
            }
        )

    now_ms = _now_ms()
    hset_map: Dict[str, Any] = {
        "policy_effectiveness_last_ts_ms": str(now_ms),
        "policy_effectiveness_baseline_ok_present": str(int(baseline_ok_present)),
        "policy_effectiveness_input_last_ts_ms": str(int(last_in_ts_ms)),
        "policy_effectiveness_total_n_24h": str(int(total_n)),
    }
    for row in rows:
        mode = row["mode"]
        hset_map[f"policy_effectiveness_share_24h_{mode}"] = f"{row['share_24h']:.6f}"
        hset_map[f"policy_effectiveness_expectancy_r_delta_24h_{mode}"] = f"{row['expectancy_r_delta_24h']:.6f}"
        hset_map[f"policy_effectiveness_precision_top5p_delta_24h_{mode}"] = f"{row['precision_top5p_delta_24h']:.6f}"
        hset_map[f"policy_effectiveness_ece_delta_24h_{mode}"] = f"{row['ece_delta_24h']:.6f}"

    try:
        r.hset(dyn_cfg_key, mapping=hset_map)
    except Exception as e:
        print(f"policy_effectiveness: cannot write {dyn_cfg_key}: {e}")
        return 2

    report = {
        "ts_ms": now_ms,
        "lookback_h": 24,
        "source": "settings:dynamic_cfg (signal_quality_*_policy_*)",
        "input_last_ts_ms": int(last_in_ts_ms),
        "baseline": {
            "mode": "ok",
            "min_n": int(baseline_min_n),
            "present": int(baseline_ok_present),
            "n": int(ok.n),
            "expectancy_r": float(ok.expectancy_r),
            "precision_top5p": float(ok.precision_top5p),
            "ece": float(ok.ece),
        },
        "total_n": int(total_n),
        "rows": rows,
    }
    report_json = json.dumps(report, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
    report_csv = _as_csv(rows)

    try:
        r.set("reports:policy_effectiveness:p71:last_json", report_json)
        r.set("reports:policy_effectiveness:p71:last_csv", report_csv)
    except Exception as e:
        print(f"policy_effectiveness: cannot write reports:* keys: {e}")
        return 2

    print(
        "policy_effectiveness: ok "
        f"baseline_ok_present={baseline_ok_present} "
        f"total_n={total_n} "
        f"input_last_ts_ms={last_in_ts_ms}"
    )
    return 0


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--once", action="store_true", help="Run once and exit")
    ap.add_argument(
        "--interval-sec",
        type=int,
        default=_to_int(os.environ.get("POLICY_EFF_INTERVAL_SEC", "600"), 600),
        help="Loop interval (seconds) when not --once",
    )
    args = ap.parse_args()

    if args.once:
        return run_once()

    interval = max(30, int(args.interval_sec))
    while True:
        rc = run_once()
        if rc != 0:
            time.sleep(min(10, interval))
        else:
            time.sleep(interval)


if __name__ == "__main__":
    raise SystemExit(main())
