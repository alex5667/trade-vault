from __future__ import annotations
from core.redis_keys import RedisStreams as RS

"""
tools/build_v12_of_dataset.py
==============================
Offline dataset builder for v12_of model retraining.

Extends build_edge_stack_dataset_from_redis.py:
  - Enforces v12_of feature schema (214 numeric keys)
  - Back-fills Group MC temporal features from signals:of:inputs ts_ms (deterministic)
  - Warns on Group MD missing keys (require go-worker data)
  - Outputs JSONL ready for train_edge_stack_v1_oof

Usage:
  python -m ml_analysis.tools.build_v12_of_dataset \\
    --redis_url redis://localhost:6379/0 \\
    --out_jsonl ./v12_of_train.jsonl \\
    --out_report_json ./v12_of_report.json \\
    --lookback_days 30

Required: signals:of:inputs (feature snapshots) + trades:closed (labels).
Optional: archive_dir for data beyond Redis retention window.
"""


import argparse
import json
import math
import os
from typing import Any

# -- Import base builder utilities -----------------------------------------------
try:
    from ml_analysis.tools.build_edge_stack_dataset_from_redis import (
        CloseRow,
        SignalRow,
        _as_float,
        _as_int,
        _as_str,
        _filter_by_time,
        _now_ms,
        _read_archive_items,
        _safe_json_loads,
        _xrevrange_recent,
        parse_replay_signal,
        parse_trade_closed,
    )
except ImportError:
    raise ImportError(
        "build_v12_of_dataset requires build_edge_stack_dataset_from_redis in PYTHONPATH.\n"
        "Run from: python-worker/ directory."
    )

# -- v12_of schema ----------------------------------------------------------------
from core.ml_feature_schema_v12_of import V12_OF_NUMERIC_KEYS
from core.v12_of_features import (
    _is_session_overlap,
    _next_funding_ts_ms,
)
import contextlib

V12_OF_KEYS_SET = frozenset(V12_OF_NUMERIC_KEYS)
V12_OF_KEY_COUNT = len(V12_OF_NUMERIC_KEYS)

# Group MC keys that can always be recomputed from ts_ms (Train==Serve ✓)
_RECOMPUTABLE_MC = frozenset([
    "minutes_to_funding",
    "session_overlap_flag",
])

# Group MD keys that require go-worker (may be 0.0 in old snapshots)
_GOWORKER_MD = frozenset([
    "eth_btc_corr_5m",
    "perp_spot_basis_bps",
    "stable_coin_flow_delta",
])


# ---------------------------------------------------------------------------
# Feature row vectorisation
# ---------------------------------------------------------------------------

def vectorise_v12_of(
    indicators: dict[str, Any],
    *,
    ts_ms: int,
    backfill_mc: bool = True,
) -> dict[str, float]:
    """
    Produce a {key: float} dict for all V12_OF_NUMERIC_KEYS.

    - Missing keys default to 0.0 (fail-open).
    - Group MC keys (minutes_to_funding, session_overlap_flag) are recomputed
      from ts_ms when backfill_mc=True, ensuring Train==Serve for temporal features.
    """
    row: dict[str, float] = {}

    for k in V12_OF_NUMERIC_KEYS:
        raw = indicators.get(k, 0.0)
        try:
            row[k] = float(raw) if raw is not None else 0.0
        except Exception:
            row[k] = 0.0

    # Back-fill deterministic MC features from ts_ms (always possible offline)
    if backfill_mc and ts_ms > 0:
        try:
            next_fund = _next_funding_ts_ms(ts_ms)
            row["minutes_to_funding"] = float(max(0.0, next_fund - ts_ms) / 60_000.0)
        except Exception:
            pass
        with contextlib.suppress(Exception):
            row["session_overlap_flag"] = _is_session_overlap(ts_ms)

    return row


def _nan_count(row: dict[str, float]) -> int:
    return sum(1 for v in row.values() if v is None or (isinstance(v, float) and math.isnan(v)))


def _zero_count(row: dict[str, float], keys: frozenset) -> int:
    return sum(1 for k in keys if row.get(k, 0.0) == 0.0)


# ---------------------------------------------------------------------------
# Dataset builder
# ---------------------------------------------------------------------------

def build_dataset(
    *,
    redis_url: str,
    signal_stream: str = RS.OF_INPUTS,
    outcome_stream: str = "trades:closed",
    signal_count: int = 500_000,
    outcome_count: int = 200_000,
    out_jsonl: str = "v12_of_train.jsonl",
    out_report_json: str | None = None,
    out_quarantine_jsonl: str | None = None,
    lookback_days: int = 30,
    y_min_r: float = 0.0,
    join_window_ms: int = 5_000,
    archive_signal_dir: str | None = None,
    archive_outcome_dir: str | None = None,
) -> dict[str, Any]:
    """
    Core dataset building pipeline.

    Returns a stats dict.
    """
    import redis as _redis

    rdb = _redis.from_url(redis_url, decode_responses=False)
    t0 = _now_ms()

    print(f"[v12_of] Connecting to Redis: {redis_url}")
    print(f"[v12_of] Schema: {V12_OF_KEY_COUNT} numeric keys")

    # ------------------------------------------------------------------
    # 1. Load signals (feature snapshots)
    # ------------------------------------------------------------------
    print(f"[v12_of] Reading signal stream: {signal_stream} (count={signal_count})")
    raw_signals = _xrevrange_recent(rdb, signal_stream, count=signal_count)
    if archive_signal_dir:
        arc_items, arc_stats = _read_archive_items(
            archive_signal_dir,
            start_ms=None, end_ms=None,
            lookback_days=lookback_days,
            max_records=signal_count,
        )
        raw_signals.extend(arc_items)
        print(f"[v12_of] Archive signals: {arc_stats}")

    print(f"[v12_of] Raw signal entries: {len(raw_signals)}")

    # Parse + index by sid
    signals: dict[str, SignalRow] = {}
    parse_errors = 0
    for _msg_id, fields in raw_signals:
        try:
            row = parse_replay_signal(fields)
            if row is None:
                parse_errors += 1
                continue
            # Keep newest by ts_ms per sid
            existing = signals.get(row.sid)
            if existing is None or row.ts_ms > existing.ts_ms:
                signals[row.sid] = row
        except Exception:
            parse_errors += 1
    print(f"[v12_of] Parsed signals: {len(signals)}, errors: {parse_errors}")

    # ------------------------------------------------------------------
    # 2. Load outcomes (trades:closed)
    # ------------------------------------------------------------------
    print(f"[v12_of] Reading outcome stream: {outcome_stream} (count={outcome_count})")
    raw_outcomes = _xrevrange_recent(rdb, outcome_stream, count=outcome_count)
    if archive_outcome_dir:
        arc_items2, arc_stats2 = _read_archive_items(
            archive_outcome_dir,
            start_ms=None, end_ms=None,
            lookback_days=lookback_days,
            max_records=outcome_count,
        )
        raw_outcomes.extend(arc_items2)
        print(f"[v12_of] Archive outcomes: {arc_stats2}")
    print(f"[v12_of] Raw outcome entries: {len(raw_outcomes)}")

    outcomes: dict[str, CloseRow] = {}
    outcome_errors = 0
    for _msg_id, fields in raw_outcomes:
        try:
            row = parse_trade_closed(fields)
            if row is None:
                outcome_errors += 1
                continue
            existing = outcomes.get(row.sid)
            if existing is None or row.close_ts_ms > existing.close_ts_ms:
                outcomes[row.sid] = row
        except Exception:
            outcome_errors += 1
    print(f"[v12_of] Parsed outcomes: {len(outcomes)}, errors: {outcome_errors}")

    # ------------------------------------------------------------------
    # 3. Join on SID
    # ------------------------------------------------------------------
    joined: list[dict[str, Any]] = []
    missing_outcome = 0
    zero_risk = 0
    md_zero_rows = 0

    quarantine_fh = None
    if out_quarantine_jsonl:
        os.makedirs(os.path.dirname(os.path.abspath(out_quarantine_jsonl)) or ".", exist_ok=True)
        quarantine_fh = open(out_quarantine_jsonl, "w", encoding="utf-8")

    def _quarantine(reason: str, data: Any) -> None:
        if quarantine_fh is not None:
            quarantine_fh.write(json.dumps({"reason": reason, "data": data}, ensure_ascii=False, separators=(",", ":")))
            quarantine_fh.write("\n")

    for sid, sig in signals.items():
        outcome = outcomes.get(sid)
        if outcome is None:
            missing_outcome += 1
            _quarantine("no_outcome", {"sid": sid, "symbol": sig.symbol, "ts_ms": sig.ts_ms})
            continue

        if outcome.risk_usd <= 0.0:
            zero_risk += 1
            _quarantine("zero_risk_usd", {"sid": sid})
            continue

        r_mult = outcome.pnl / outcome.risk_usd
        y = int(r_mult >= y_min_r)

        # Vectorise v12_of features
        feat_row = vectorise_v12_of(
            sig.indicators,
            ts_ms=sig.ts_ms,
            backfill_mc=True,
        )

        # Stats: Group MD zero fraction (data quality signal)
        md_zero = _zero_count(feat_row, _GOWORKER_MD)
        if md_zero == len(_GOWORKER_MD):
            md_zero_rows += 1

        joined.append({
            "ts_ms": sig.ts_ms,
            "close_ts_ms": outcome.close_ts_ms,
            "sid": sid,
            "symbol": sig.symbol,
            "direction": sig.direction,
            "scenario": sig.scenario,
            "features": feat_row,
            "pnl": outcome.pnl,
            "risk_usd": outcome.risk_usd,
            "r_mult": round(r_mult, 6),
            "y": y,
        })

    if quarantine_fh is not None:
        quarantine_fh.close()

    # Sort deterministically
    joined.sort(key=lambda x: (int(x["ts_ms"]), str(x["sid"])))
    print(f"[v12_of] Joined rows: {len(joined)} (missing_outcome={missing_outcome}, zero_risk={zero_risk})")
    print(f"[v12_of] Rows with ALL Group MD zeros: {md_zero_rows}/{len(joined)} "
          f"({'go-worker not yet deployed?' if md_zero_rows > len(joined) * 0.9 else 'ok'})")

    # ------------------------------------------------------------------
    # 4. Write JSONL
    # ------------------------------------------------------------------
    os.makedirs(os.path.dirname(os.path.abspath(out_jsonl)) or ".", exist_ok=True)
    with open(out_jsonl, "w", encoding="utf-8") as fh:
        for row in joined:
            fh.write(json.dumps(row, ensure_ascii=False, separators=(",", ":")))
            fh.write("\n")
    print(f"[v12_of] Written: {out_jsonl}  ({len(joined)} rows)")

    # ------------------------------------------------------------------
    # 5. Report
    # ------------------------------------------------------------------
    y_pos = sum(1 for r in joined if r["y"] == 1)
    stats: dict[str, Any] = {
        "schema_ver": "v12_of",
        "numeric_key_count": V12_OF_KEY_COUNT,
        "ts_built_ms": _now_ms(),
        "elapsed_ms": _now_ms() - t0,
        "n_signals": len(signals),
        "n_outcomes": len(outcomes),
        "n_rows": len(joined),
        "n_positive": y_pos,
        "n_negative": len(joined) - y_pos,
        "positive_rate": round(y_pos / max(1, len(joined)), 4),
        "y_min_r": y_min_r,
        "missing_outcome": missing_outcome,
        "zero_risk": zero_risk,
        "md_zero_rows": md_zero_rows,
        "parse_errors_signals": parse_errors,
        "parse_errors_outcomes": outcome_errors,
        "out_jsonl": out_jsonl,
    }

    if out_report_json:
        os.makedirs(os.path.dirname(os.path.abspath(out_report_json)) or ".", exist_ok=True)
        with open(out_report_json, "w", encoding="utf-8") as fh:
            json.dump(stats, fh, indent=2, ensure_ascii=False)
        print(f"[v12_of] Report: {out_report_json}")

    return stats


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Build v12_of training dataset from Redis streams")
    p.add_argument("--redis_url", default=os.getenv("REDIS_URL", "redis://localhost:6379/0"))
    p.add_argument("--signal_stream", default=RS.OF_INPUTS)
    p.add_argument("--outcome_stream", default="trades:closed")
    p.add_argument("--signal_count", type=int, default=500_000)
    p.add_argument("--outcome_count", type=int, default=200_000)
    p.add_argument("--out_jsonl", default="v12_of_train.jsonl")
    p.add_argument("--out_report_json", default=None)
    p.add_argument("--out_quarantine_jsonl", default=None)
    p.add_argument("--lookback_days", type=int, default=30)
    p.add_argument("--y_min_r", type=float, default=0.0,
                   help="Min R-multiple to be labelled y=1 (default 0.0 = any profitable close)")
    p.add_argument("--archive_signal_dir", default=None,
                   help="Directory with NDJSON/gz archives for signals beyond Redis retention")
    p.add_argument("--archive_outcome_dir", default=None,
                   help="Directory with NDJSON/gz archives for outcomes beyond Redis retention")
    return p.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    stats = build_dataset(
        redis_url=args.redis_url,
        signal_stream=args.signal_stream,
        outcome_stream=args.outcome_stream,
        signal_count=args.signal_count,
        outcome_count=args.outcome_count,
        out_jsonl=args.out_jsonl,
        out_report_json=args.out_report_json,
        out_quarantine_jsonl=args.out_quarantine_jsonl,
        lookback_days=args.lookback_days,
        y_min_r=args.y_min_r,
        archive_signal_dir=args.archive_signal_dir,
        archive_outcome_dir=args.archive_outcome_dir,
    )
    print("\n=== Build Stats ===")
    print(json.dumps(stats, indent=2, ensure_ascii=False))
