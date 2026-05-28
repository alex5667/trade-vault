#!/usr/bin/env python3
"""build_dataset_v5_tb_v15of.py — Triple-Barrier Labels × v15_of signal_snapshots join.

Purpose:
  Joins `signal_snapshots` (Postgres, v15_of features) with pre-computed
  Triple-Barrier labels (NDJSON from label_triple_barrier_from_redis_ticks_v10.py
  or any other TBL labeler that outputs {sid, label, barrier_hit, …}).

  The output NDJSON can be fed to `train_v15_lgbm --source=tbl` for a
  path-based label instead of the realized `r_multiple >= threshold` label.
  This avoids look-ahead bias from trade exits and captures path quality
  (MFE / drawdown) rather than final PnL.

Pipeline:
  [label_triple_barrier_from_redis_ticks_v10.py]  →  tbl_labels.ndjson
                                                         │
  [signal_snapshots in Postgres]  ───────────────────────┤
                                                         ↓
  [build_dataset_v5_tb_v15of.py]  →  ml_dataset_tb_v15of.ndjson
                                                         │
  [train_v15_lgbm --source=tbl]  ────────────────────────┘

TBL label schema (input NDJSON, one JSON per line):
  {
    "sid":        "<raw signal id>",
    "label":      0 | 1,              # 1 = TP1 hit before SL
    "outcome":    "tp1" | "sl" | "timeout" | "tp2",
    "hit_tp1":    true | false,
    "hit_tp2":    true | false,
    "hit_sl":     true | false,
    "barrier_ms": <ms elapsed to first barrier hit>,
    "entry_price": float,
    "exit_price":  float,
    "mfe_bps":     float,             # optional
    "mae_bps":     float,             # optional
  }

Output NDJSON schema (one JSON per line):
  {
    "sid":     "<normalized sid>",
    "ts_ms":   int,
    "symbol":  str,
    "regime":  str,
    "hit":     0 | 1,               # from TBL label
    "r":       0.0,                 # placeholder (not available from TBL)
    "tbl_outcome": str,             # "tp1" | "sl" | "timeout" | "tp2"
    "tbl_barrier_ms": int | null,
    "tbl_mfe_bps": float | null,
    "tbl_mae_bps": float | null,
    "features": { … v15_of feature dict … }
  }

Usage:
  python -m tools.build_dataset_v5_tb_v15of \\
    --pg-dsn "postgresql://..." \\
    --tbl-labels /tmp/tbl_labels.ndjson \\
    --out /tmp/ml_dataset_tb_v15of.ndjson \\
    --lookback-days 30 \\
    --tbl-outcome tp1   # which outcome to use as label (default: tp1)

ENV:
  ANALYTICS_DB_DSN   — Postgres DSN (overridden by --pg-dsn)
  PG_DSN             — fallback DSN
"""
from __future__ import annotations

import argparse
import json
import logging
import math
import os
import sys
import time
from typing import Any

log = logging.getLogger("build_dataset_v5_tb_v15of")
if not log.handlers:
    h = logging.StreamHandler(sys.stdout)
    h.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s %(message)s"))
    log.addHandler(h)
log.setLevel(os.getenv("LOG_LEVEL", "INFO").upper())


# ── helpers ──────────────────────────────────────────────────────────────────

def _safe_float(x: Any, default: float = 0.0) -> float:
    try:
        v = float(x)
        return v if math.isfinite(v) else default
    except Exception:
        return default


def norm_sid(raw: str | None) -> str | None:
    """Normalise raw SID to canonical form (strip prefix up to 3-part split).

    Matches norm_sid() in train_v15_lgbm.py.
    """
    if not raw:
        return None
    parts = str(raw).split(":")
    if len(parts) < 3:
        return None
    return ":".join(parts[-2:])


def _load_features(ind_raw: Any) -> dict[str, float]:
    """Parse indicators JSON blob from signal_snapshots.indicators column."""
    if ind_raw is None:
        return {}
    if isinstance(ind_raw, str):
        try:
            ind_raw = json.loads(ind_raw)
        except Exception:
            return {}
    if not isinstance(ind_raw, dict):
        return {}
    out: dict[str, float] = {}
    for k, v in ind_raw.items():
        if isinstance(v, bool):
            out[k] = 1.0 if v else 0.0
        elif isinstance(v, (int, float)):
            f = float(v)
            if math.isfinite(f):
                out[k] = f
        elif isinstance(v, str):
            fv = _safe_float(v, float("nan"))
            if math.isfinite(fv):
                out[k] = fv
    return out


# ── loaders ──────────────────────────────────────────────────────────────────

def load_tbl_labels(path: str, outcome_col: str = "tp1") -> dict[str, dict]:
    """Read TBL label NDJSON → {norm_sid: label_dict}.

    label_dict keys: label (0|1), outcome, hit_tp1, hit_tp2, hit_sl,
                     barrier_ms, entry_price, exit_price, mfe_bps, mae_bps.
    """
    labels: dict[str, dict] = {}
    n_parsed = 0
    n_bad_sid = 0
    with open(path, encoding="utf-8") as f:
        for lineno, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError as e:
                log.warning("tbl labels line %d: JSON parse error: %s", lineno, e)
                continue

            raw_sid = obj.get("sid") or obj.get("signal_id")
            sid = norm_sid(raw_sid)
            if not sid:
                n_bad_sid += 1
                continue

            # Resolve label: prefer explicit `label` field, else derive from outcome_col
            label_val = obj.get("label")
            if label_val is None:
                # Derive: hit_<outcome_col> == True → 1
                hit_key = f"hit_{outcome_col}"
                label_val = 1 if obj.get(hit_key) else 0
            else:
                label_val = int(bool(label_val))

            labels[sid] = {
                "label": label_val,
                "outcome": obj.get("outcome", ""),
                "barrier_ms": obj.get("barrier_ms"),
                "mfe_bps": _safe_float(obj.get("mfe_bps"), float("nan")),
                "mae_bps": _safe_float(obj.get("mae_bps"), float("nan")),
            }
            n_parsed += 1

    log.info("TBL labels loaded: %d valid, %d bad-sid (from %s)", n_parsed, n_bad_sid, path)
    return labels


def load_signal_snapshots_pg(
    pg_dsn: str,
    lookback_days: int,
) -> dict[str, dict]:
    """Read signal_snapshots from Postgres → {norm_sid: snapshot_dict}.

    Returns only rows within the last lookback_days UTC days.
    """
    import psycopg2

    snapshots: dict[str, dict] = {}
    cutoff_ms = int((time.time() - lookback_days * 86400) * 1000)

    sql = """
        SELECT sid, ts_ms, symbol, regime, indicators
        FROM signal_snapshots
        WHERE ts_ms >= %s
        ORDER BY ts_ms ASC
    """

    conn = None
    try:
        conn = psycopg2.connect(pg_dsn)
        with conn.cursor() as cur:
            cur.execute(sql, (cutoff_ms,))
            for row in cur:
                sid_raw, ts_ms, symbol, regime, indicators = row
                sid = norm_sid(sid_raw)
                if not sid:
                    continue
                feats = _load_features(indicators)
                if not feats:
                    continue
                snapshots[sid] = {
                    "sid": sid,
                    "ts_ms": int(ts_ms or 0),
                    "symbol": str(symbol or ""),
                    "regime": str(regime or "na"),
                    "features": feats,
                }
        log.info("signal_snapshots loaded: %d records (lookback=%dd)", len(snapshots), lookback_days)
    finally:
        if conn is not None:
            conn.close()

    return snapshots


def join_and_write(
    snapshots: dict[str, dict],
    tbl_labels: dict[str, dict],
    out_path: str,
) -> tuple[int, int]:
    """Join snapshots × tbl_labels on norm_sid, write output NDJSON.

    Returns (n_joined, n_unmatched).
    """
    n_joined = 0
    n_unmatched_snap = 0

    os.makedirs(os.path.dirname(os.path.abspath(out_path)), exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        for sid, snap in snapshots.items():
            lbl = tbl_labels.get(sid)
            if lbl is None:
                n_unmatched_snap += 1
                continue

            mfe = lbl["mfe_bps"]
            mae = lbl["mae_bps"]
            record = {
                "sid": sid,
                "ts_ms": snap["ts_ms"],
                "symbol": snap["symbol"],
                "regime": snap["regime"],
                "hit": lbl["label"],
                "r": 0.0,                         # TBL path doesn't have r_multiple
                "tbl_outcome": lbl["outcome"],
                "tbl_barrier_ms": lbl["barrier_ms"],
                "tbl_mfe_bps": None if (isinstance(mfe, float) and math.isnan(mfe)) else mfe,
                "tbl_mae_bps": None if (isinstance(mae, float) and math.isnan(mae)) else mae,
                "features": snap["features"],
            }
            f.write(json.dumps(record, separators=(",", ":")) + "\n")
            n_joined += 1

    log.info(
        "Join complete: %d matched, %d snapshots unmatched in TBL labels → %s",
        n_joined, n_unmatched_snap, out_path,
    )
    return n_joined, n_unmatched_snap


def load_tbl_dataset(path: str) -> list[dict]:
    """Read joined dataset NDJSON → list of record dicts (for train_v15_lgbm)."""
    records: list[dict] = []
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                records.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    return records


# ── main ─────────────────────────────────────────────────────────────────────

def main() -> int:
    ap = argparse.ArgumentParser(
        description="Join signal_snapshots (PG) × TBL labels (NDJSON) for v15_lgbm training."
    )
    ap.add_argument("--pg-dsn", default=os.getenv("ANALYTICS_DB_DSN") or os.getenv("PG_DSN", ""),
                    help="Postgres DSN for signal_snapshots")
    ap.add_argument("--tbl-labels", required=True,
                    help="Path to TBL labels NDJSON (from label_triple_barrier_from_redis_ticks_v10)")
    ap.add_argument("--out", required=True,
                    help="Output path for joined NDJSON dataset")
    ap.add_argument("--lookback-days", type=int, default=30,
                    help="How many days of signal_snapshots to load (default 30)")
    ap.add_argument("--tbl-outcome", default="tp1",
                    choices=["tp1", "tp2", "sl"],
                    help="Which TBL outcome to use as hit=1 label (default: tp1)")
    args = ap.parse_args()

    if not args.pg_dsn:
        log.error("--pg-dsn or ANALYTICS_DB_DSN/PG_DSN env is required")
        return 2

    t0 = time.time()

    tbl_labels = load_tbl_labels(args.tbl_labels, outcome_col=args.tbl_outcome)
    if not tbl_labels:
        log.error("No TBL labels loaded from %s — aborting", args.tbl_labels)
        return 2

    snapshots = load_signal_snapshots_pg(args.pg_dsn, lookback_days=args.lookback_days)
    if not snapshots:
        log.error("No signal_snapshots loaded from Postgres — aborting")
        return 2

    n_joined, n_unmatched = join_and_write(snapshots, tbl_labels, args.out)

    if n_joined == 0:
        log.error("Join produced 0 rows — check SID format alignment between labels and snapshots")
        return 2

    match_rate = n_joined / max(len(snapshots), 1) * 100
    log.info(
        "Done in %.1fs — %d joined / %d snapshots = %.1f%% match rate",
        time.time() - t0, n_joined, len(snapshots), match_rate,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
