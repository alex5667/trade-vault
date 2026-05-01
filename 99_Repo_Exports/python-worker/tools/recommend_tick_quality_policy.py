#!/usr/bin/env python3
from __future__ import annotations
"""Recommend tick-quality policy & alert thresholds from Step13 smoke JSON.

Step 14 goal
  - Convert the Step13 smoke report into actionable knobs and alert thresholds.

Outputs
  - Recommended env vars:
      CRYPTO_OF_UNKNOWN_SIDE_POLICY
      TICK_SIDE_QUARANTINE_SAMPLE (if quarantine)
      CRYPTO_OF_MAX_TS_SKEW_MS
  - Prometheus alert rules YAML bundle.

Expected input schema (from tools.smoke_tick_side_quality)
  {
    "ticks": {
      "n": 123,
      "by_side_conf": {"explicit": 10, "maker": 5, "unknown": 1, "missing": 0},
      "by_ts_source": {"payload": 10, "stream_id": 6, "now": 7},
      "abs_event_stream_skew": {"p99_ms": 1500.0, ...},
      ...
    },
    "quarantine_count": 0,
    ...
  }

Usage
  python -m tools.recommend_tick_quality_policy --smoke /tmp/smoke.json
  python -m tools.recommend_tick_quality_policy --smoke /tmp/smoke.json --format env
  python -m tools.recommend_tick_quality_policy --smoke /tmp/smoke.json --format yaml --out /tmp/tick_quality_alerts.yml

Notes
  - Heuristics are conservative. For low sample sizes, it recommends "ignore_delta".
  - It does NOT modify any running services; it only prints recommendations.
"""


import argparse
import json
import os
import sys
from typing import Any, Dict, Optional, Tuple


def _clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))


def _safe_div(num: float, den: float) -> float:
    if den <= 0:
        return 0.0
    return float(num) / float(den)


def _load_json(path: str) -> Dict[str, Any]:
    if path == "-":
        return json.loads(sys.stdin.read())
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _get_ticks(smoke: Dict[str, Any]) -> Dict[str, Any]:
    t = smoke.get("ticks")
    if isinstance(t, dict) and "n" in t:
        return t
    # Defensive: allow nested structure
    t2 = None
    if isinstance(t, dict):
        t2 = t.get("ticks")
    if isinstance(t2, dict) and "n" in t2:
        return t2
    raise ValueError("Invalid smoke JSON: missing top-level 'ticks' summary")


def _extract_shares(ticks: Dict[str, Any]) -> Dict[str, float]:
    n = float(ticks.get("n") or 0)
    by_sc = ticks.get("by_side_conf") or {}
    by_ts = ticks.get("by_ts_source") or {}

    def _g(d: Any, k: str) -> float:
        try:
            return float(d.get(k) or 0)
        except Exception:
            return 0.0

    unknown_sc = _g(by_sc, "unknown") + _g(by_sc, "missing")
    maker_sc = _g(by_sc, "maker")
    explicit_sc = _g(by_sc, "explicit")

    # ts_source: treat 'now' as wall-clock
    wall_ts = _g(by_ts, "wall") + _g(by_ts, "now") + _g(by_ts, "missing")
    payload_ts = _g(by_ts, "payload")
    stream_id_ts = _g(by_ts, "stream_id") + _g(by_ts, "msg_id")

    return {
        "unknown_side_share": _safe_div(unknown_sc, n),
        "maker_side_share": _safe_div(maker_sc, n),
        "explicit_side_share": _safe_div(explicit_sc, n),
        "wall_ts_share": _safe_div(wall_ts, n),
        "payload_ts_share": _safe_div(payload_ts, n),
        "stream_id_ts_share": _safe_div(stream_id_ts, n),
    }


def _extract_p99_skew_ms(ticks: Dict[str, Any]) -> float:
    skew = ticks.get("abs_event_stream_skew") or {}
    try:
        return float(skew.get("p99_ms") or 0.0)
    except Exception:
        return 0.0


def _recommend_unknown_side_policy(n: int, unknown_share: float) -> Tuple[str, Optional[float], str]:
    """Return (policy, quarantine_sample, rationale)."""
    if n < 1000:
        return ("ignore_delta", None, "low_sample_n<1000")
    if unknown_share <= 0.02:
        return ("ignore_delta", None, "unknown_share<=0.02")
    # Prefer quarantine over drop (safer; keeps visibility)
    if unknown_share <= 0.10:
        sample = float(_clamp(unknown_share * 0.10, 0.01, 0.03))
        return ("quarantine", sample, "0.02<unknown_share<=0.10")
    # High unknown: quarantine with larger sample; drop only after root-cause fix
    sample = float(_clamp(unknown_share * 0.10, 0.02, 0.05))
    return ("quarantine", sample, "unknown_share>0.10")


def _recommend_max_ts_skew_ms(p99_skew_ms: float) -> int:
    # Conservative: 2x p99, clamped.
    if p99_skew_ms <= 0:
        return int(os.getenv("CRYPTO_OF_MAX_TS_SKEW_MS", "60000"))
    return int(_clamp(p99_skew_ms * 2.0, 2000.0, 60000.0))


def _recommend_alert_thresholds(unknown_share: float, wall_ts_share: float, max_ts_skew_ms: int) -> Dict[str, float]:
    # Unknown-side: tighten around observed levels, but with safe floors.
    warn_unknown = float(_clamp(max(0.03, unknown_share * 1.5), 0.03, 0.30))
    crit_unknown = float(_clamp(max(0.07, unknown_share * 2.0), 0.07, 0.40))

    # Wall-clock ts_source indicates upstream not providing event time.
    warn_wall = float(_clamp(max(0.005, wall_ts_share * 2.0), 0.005, 0.20))
    crit_wall = float(_clamp(max(0.02, wall_ts_share * 4.0), 0.02, 0.30))

    # Hard drops: ratio of hard drops to ticks_read_total.
    hard_drop_warn = 0.001  # 0.1%
    hard_drop_crit = 0.005  # 0.5%

    # Dedup drops: duplicates should be rare; spikes indicate upstream replay/dup bug.
    dedup_warn = 0.01  # 1%

    # Unknown-side drops (if policy drop/quarantine): keep small.
    unknown_drop_warn = 0.01  # 1%

    return {
        "unknown_warn": warn_unknown,
        "unknown_crit": crit_unknown,
        "wall_warn": warn_wall,
        "wall_crit": crit_wall,
        "hard_drop_warn": hard_drop_warn,
        "hard_drop_crit": hard_drop_crit,
        "dedup_warn": dedup_warn,
        "unknown_drop_warn": unknown_drop_warn,
        "max_ts_skew_ms": float(max_ts_skew_ms),
    }


def _render_prometheus_rules(th: Dict[str, float]) -> str:
    # Render as plain YAML string (no external YAML dependency).
    # PromQL uses clamp_min to avoid division by zero.
    u_warn = th["unknown_warn"]
    u_crit = th["unknown_crit"]
    w_warn = th["wall_warn"]
    w_crit = th["wall_crit"]
    hd_warn = th["hard_drop_warn"]
    hd_crit = th["hard_drop_crit"]
    dd_warn = th["dedup_warn"]
    ud_warn = th["unknown_drop_warn"]

    # wall ts_source is derived from ticks_ts_source_total{ts_source=~"now|wall|missing"}
    lines = [
        "groups:",
        "- name: tick-quality",
        "  rules:",
        "  - alert: TickUnknownSideHigh",
        f"    expr: (sum by(symbol) (rate(ticks_unknown_side_policy_total[5m])) / clamp_min(sum by(symbol) (rate(ticks_processed_total[5m])), 1)) > {u_warn:.6f}",
        "    for: 10m",
        "    labels:",
        "      severity: warning",
        "    annotations:",
        "      summary: Unknown-side share is elevated",
        "      description: UNKNOWN side share > threshold. Check upstream side_conf and side-quarantine stream.",
        "  - alert: TickUnknownSideCritical",
        f"    expr: (sum by(symbol) (rate(ticks_unknown_side_policy_total[5m])) / clamp_min(sum by(symbol) (rate(ticks_processed_total[5m])), 1)) > {u_crit:.6f}",
        "    for: 10m",
        "    labels:",
        "      severity: critical",
        "    annotations:",
        "      summary: Unknown-side share is critical",
        "      description: UNKNOWN side share > critical threshold. Consider CRYPTO_OF_UNKNOWN_SIDE_POLICY=quarantine and fix upstream side inference.",
        "  - alert: TickWallTsSourceHigh",
        f"    expr: (sum by(symbol) (rate(ticks_ts_source_total{{ts_source=~\"now|wall|missing\"}}[5m])) / clamp_min(sum by(symbol) (rate(ticks_processed_total[5m])), 1)) > {w_warn:.6f}",
        "    for: 10m",
        "    labels:",
        "      severity: warning",
        "    annotations:",
        "      summary: wall-clock ts_source is elevated",
        "      description: Too many ticks rely on wall-clock for event_ts_ms; enforce payload ts or stream-id derived ts.",
        "  - alert: TickWallTsSourceCritical",
        f"    expr: (sum by(symbol) (rate(ticks_ts_source_total{{ts_source=~\"now|wall|missing\"}}[5m])) / clamp_min(sum by(symbol) (rate(ticks_processed_total[5m])), 1)) > {w_crit:.6f}",
        "    for: 10m",
        "    labels:",
        "      severity: critical",
        "    annotations:",
        "      summary: wall-clock ts_source is critical",
        "      description: Timestamp source degraded (wall-clock). Fix upstream event time propagation.",
        "  - alert: TickTimeHardDropsHigh",
        f"    expr: (sum by(symbol) (rate(tick_time_hard_drop_total[5m])) / clamp_min(sum by(symbol) (rate(ticks_read_total[5m])), 1)) > {hd_warn:.6f}",
        "    for: 5m",
        "    labels:",
        "      severity: warning",
        "    annotations:",
        "      summary: Tick time hard drops are elevated",
        "      description: Hard drops due to bad time (future/past/reorder_hard). Check tick_time policy and upstream timestamps.",
        "  - alert: TickTimeHardDropsCritical",
        f"    expr: (sum by(symbol) (rate(tick_time_hard_drop_total[5m])) / clamp_min(sum by(symbol) (rate(ticks_read_total[5m])), 1)) > {hd_crit:.6f}",
        "    for: 5m",
        "    labels:",
        "      severity: critical",
        "    annotations:",
        "      summary: Tick time hard drops are critical",
        "      description: Hard drops exceed critical ratio; likely bad upstream time or clock skew. Investigate immediately.",
        "  - alert: TickTimeQuarantineActive",
        "    expr: max by(symbol) (tick_time_quarantine_active) > 0",
        "    for: 10m",
        "    labels:",
        "      severity: warning",
        "    annotations:",
        "      summary: Bad time quarantine active",
        "      description: Quarantine is active for a prolonged period. Expect signal suppression; inspect stream:tick_time:quarantine.",
        "  - alert: TickDedupDropHigh",
        f"    expr: (sum by(symbol) (rate(tick_dedup_drop_total[5m])) / clamp_min(sum by(symbol) (rate(ticks_read_total[5m])), 1)) > {dd_warn:.6f}",
        "    for: 10m",
        "    labels:",
        "      severity: warning",
        "    annotations:",
        "      summary: Duplicate tick drops are elevated",
        "      description: Dedup drops > threshold. Upstream may be replaying/duplicating ticks; check uid computation and source.",
        "  - alert: TickDroppedUnknownSideHigh",
        f"    expr: (sum by(symbol) (rate(ticks_dropped_total{{reason=~\"unknown_side_.*\"}}[5m])) / clamp_min(sum by(symbol) (rate(ticks_read_total[5m])), 1)) > {ud_warn:.6f}",
        "    for: 10m",
        "    labels:",
        "      severity: warning",
        "    annotations:",
        "      summary: Unknown-side drops are elevated",
        "      description: Dropping/quarantining too many UNKNOWN-side ticks. Fix upstream side inference or relax policy.",
    ]
    return "\n".join(lines) + "\n"


def compute_recommendation(smoke: Dict[str, Any]) -> Dict[str, Any]:
    ticks = _get_ticks(smoke)
    n = int(ticks.get("n") or 0)
    shares = _extract_shares(ticks)
    unknown_share = float(shares["unknown_side_share"])
    wall_ts_share = float(shares["wall_ts_share"])
    p99_skew_ms = float(_extract_p99_skew_ms(ticks))

    policy, q_sample, policy_rationale = _recommend_unknown_side_policy(n, unknown_share)
    max_ts_skew_ms = _recommend_max_ts_skew_ms(p99_skew_ms)
    th = _recommend_alert_thresholds(unknown_share, wall_ts_share, max_ts_skew_ms)

    env: Dict[str, Any] = {
        "CRYPTO_OF_UNKNOWN_SIDE_POLICY": policy,
        "CRYPTO_OF_MAX_TS_SKEW_MS": int(max_ts_skew_ms),
    }
    if policy == "quarantine" and q_sample is not None:
        env["TICK_SIDE_QUARANTINE_SAMPLE"] = float(q_sample)
        env["TICK_SIDE_QUARANTINE_STREAM"] = "stream:tick_side:quarantine"
        env["TICK_SIDE_QUARANTINE_MAXLEN"] = 20000

    return {
        "inputs": {
            "n": n,
            "unknown_side_share": unknown_share,
            "wall_ts_share": wall_ts_share,
            "p99_event_stream_skew_ms": p99_skew_ms,
        },
        "recommendations": {
            "env": env,
            "policy_rationale": policy_rationale,
            "alert_thresholds": th,
        },
        "prometheus_rules_yaml": _render_prometheus_rules(th),
    }


def _print_env(env: Dict[str, Any]) -> None:
    for k in sorted(env.keys()):
        v = env[k]
        if isinstance(v, float):
            sys.stdout.write(f"{k}={v:.6f}\n")
        else:
            sys.stdout.write(f"{k}={v}\n")


def main(argv: Optional[list[str]] = None) -> int:
    ap = argparse.ArgumentParser(description="Recommend tick-quality policy & Prometheus alerts from smoke JSON")
    ap.add_argument("--smoke", required=True, help="Path to Step13 smoke JSON (or '-' for stdin)")
    ap.add_argument("--format", choices=["pretty", "json", "env", "yaml"], default="pretty", help="Output format")
    ap.add_argument("--out", default="", help="Write YAML rules to this path (only with --format yaml or pretty)")
    args = ap.parse_args(argv)

    try:
        smoke = _load_json(str(args.smoke))
        rec = compute_recommendation(smoke)
    except Exception as e:
        sys.stderr.write(f"ERROR: {e}\n")
        return 2

    yaml_rules = str(rec.get("prometheus_rules_yaml") or "")

    if args.out:
        try:
            with open(str(args.out), "w", encoding="utf-8") as f:
                f.write(yaml_rules)
        except Exception as e:
            sys.stderr.write(f"ERROR writing --out: {e}\n")
            return 3

    if args.format == "json":
        sys.stdout.write(json.dumps(rec, ensure_ascii=False))
        sys.stdout.write("\n")
        return 0

    if args.format == "env":
        env = rec.get("recommendations", {}).get("env") or {}
        if not isinstance(env, dict):
            env = {}
        _print_env(env)
        return 0

    if args.format == "yaml":
        sys.stdout.write(yaml_rules)
        return 0

    # pretty
    inputs = rec.get("inputs") or {}
    env = rec.get("recommendations", {}).get("env") or {}
    th = rec.get("recommendations", {}).get("alert_thresholds") or {}
    sys.stdout.write("# Inputs\n")
    sys.stdout.write(json.dumps(inputs, ensure_ascii=False, indent=2))
    sys.stdout.write("\n\n# Recommended ENV\n")
    if isinstance(env, dict):
        _print_env(env)
    else:
        sys.stdout.write("(missing)\n")
    sys.stdout.write("\n# Recommended alert thresholds\n")
    sys.stdout.write(json.dumps(th, ensure_ascii=False, indent=2))
    sys.stdout.write("\n\n# Prometheus rules YAML\n")
    sys.stdout.write(yaml_rules)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
