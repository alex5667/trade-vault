from __future__ import annotations

import argparse
import json
from collections.abc import Iterable
from typing import Any

import pandas as pd

from common.ml_labeling import compute_y_and_r_from_closed


def _read_ndjson(path: str) -> Iterable[dict[str, Any]]:
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            yield json.loads(line)


def _loads_maybe_json(v: Any) -> Any:
    if isinstance(v, (dict, list)):
        return v
    if v is None:
        return {}
    if isinstance(v, bytes):
        v = v.decode("utf-8", "ignore")
    if isinstance(v, str):
        s = v.strip()
        if not s:
            return {}
        try:
            return json.loads(s)
        except Exception:
            return {}
    return {}


def _norm_sid(sid: str) -> str:
    """ Normalize SID for matching: SYMBOL:SECONDS_TS. """
    if not sid:
        return ""
    if sid.startswith("crypto-of:"):
        sid = sid[len("crypto-of:") :]

    if ":" in sid:
        parts = sid.rsplit(":", 1)
        if len(parts) == 2:
            try:
                # TS might be ms or s, convert to s
                ts = int(float(parts[1]))
                if ts > 100_000_000_000: # ms
                    ts = ts // 1000
                return f"{parts[0].upper()}:{ts}"
            except Exception:
                pass
    return sid.strip().upper()


def _get_payload(obj: dict[str, Any]) -> dict[str, Any]:
    # tolerate stream export formats: {"payload":"{...}"} or already expanded
    if "payload" in obj and isinstance(obj["payload"], str) and obj["payload"].lstrip().startswith("{"):
        try:
            p = json.loads(obj["payload"])
            return p if isinstance(p, dict) else obj
        except Exception:
            return obj
    return obj


def _scrub_empty_dicts(d: Any) -> Any:
    """Recursively remove empty dicts to satisfy PyArrow Parquet writer."""
    if not isinstance(d, dict):
        return d
    out: dict[str, Any] = {}
    for k, v in d.items():
        if isinstance(v, dict):
            if not v:
                continue
            child = _scrub_empty_dicts(v)
            if not child:
                continue
            out[k] = child
        else:
            out[k] = v
    return out


def _pick_closed(existing: dict[str, Any], candidate: dict[str, Any]) -> dict[str, Any]:
    """Prefer richer POSITION_CLOSED over sparse CLOSE, otherwise keep latest by exit_ts_ms."""
    cur_et = (existing.get("event_type") or "").upper()
    new_et = (candidate.get("event_type") or "").upper()

    if cur_et == "POSITION_CLOSED" and new_et == "CLOSE":
        return existing
    if cur_et == "CLOSE" and new_et == "POSITION_CLOSED":
        return candidate

    # otherwise, prefer the one with later exit_ts_ms/ts_ms (best effort)
    cur_ts = int(existing.get("exit_ts_ms") or existing.get("ts_ms") or 0)
    new_ts = int(candidate.get("exit_ts_ms") or candidate.get("ts_ms") or 0)
    return candidate if new_ts >= cur_ts else existing


def _load_tb_labels(path: str) -> dict[str, dict[str, Any]]:
    """Load labels:tb export (NDJSON) into {sid -> payload} map.

    Accepts formats:
      - {"sid": "...", "primary": {...}, "meta": {...}}
      - {"payload": "{...json...}"} (stream export)
      - {"payload": {...}}
    """
    tb: dict[str, dict[str, Any]] = {}
    for obj in _read_ndjson(path):
        o = _get_payload(obj)
        payload = o
        if "payload" in o:
            payload2 = _loads_maybe_json(o.get("payload"))
            if isinstance(payload2, dict) and payload2:
                payload = payload2
        sid = _norm_sid((payload.get("sid", "") or ""))
        if not sid:
            continue
        # Prefer the latest record in file order
        tb[sid] = payload
    return tb


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--inputs", required=True, help="ndjson captured from signals:of:inputs")
    ap.add_argument("--closed", required=True, help="ndjson exported from events:trades (POSITION_CLOSED)")
    ap.add_argument("--out", required=True, help="output parquet path")

    # Closed-label threshold (legacy / baseline)
    ap.add_argument("--r-min", type=float, default=0.5, help="label y=1 if r_mult>=r_min (closed-label baseline)")

    # Triple-barrier labels export (optional)
    ap.add_argument("--tb-labels", default="", help="ndjson exported from labels:tb (optional)")
    ap.add_argument(
        "--label-source",
        default="closed",
        choices=["closed", "tb_primary", "tb_util"],
        help="which label to use for y/r_mult: closed | tb_primary (primary.y_edge/r_mult) | tb_util (util_r threshold)",
    )
    ap.add_argument("--tb-util-min-r", type=float, default=0.0, help="for label-source=tb_util: y=1 if util_r>=tb_util_min_r")

    args = ap.parse_args()

    tb_by_sid: dict[str, dict[str, Any]] = {}
    if str(args.tb_labels or "").strip():
        tb_by_sid = _load_tb_labels(str(args.tb_labels))

    # index closed by sid (exact and fuzzy)
    closed: dict[str, dict[str, Any]] = {}
    closed_fuzzy: dict[str, list[dict[str, Any]]] = {} # Changed to list for multiple matches
    for obj in _read_ndjson(args.closed):
        o = _get_payload(obj)
        sid_raw = (o.get("sid") or "")
        sid = _norm_sid(sid_raw)
        if not sid:
            continue
        # Index by both raw normalized and rounded (redundant but safe)
        # Prefer later event if multiple for same sid (exact match)
        if sid in closed:
            closed[sid] = _pick_closed(closed[sid], o)
        else:
            closed[sid] = o

        # Extract symbol for fallback
        sym = sid.split(":")[0] if ":" in sid else (o.get("symbol") or "").upper()
        if sym:
            closed_fuzzy.setdefault(sym, []).append(o)

    print(f"Loaded {len(closed)} closed trades ({len(closed_fuzzy)} fuzzy keys)")

    rows: list[dict[str, Any]] = []
    miss = 0

    for obj in _read_ndjson(args.inputs):
        o = _get_payload(obj)
        sid_raw = (o.get("sid") or "")
        sid = _norm_sid(sid_raw)
        if not sid:
            continue

        c = closed.get(sid)
        miss_closed = 0
        if not c:
            # Try symbol-only fuzzy match as last resort (if enabled by some flag or just to see)
            sym = sid.split(":")[0] if ":" in sid else sid
            possibles = closed_fuzzy.get(sym, [])
            if possibles:
                # Find best by timestamp?
                ts_ms = o.get("ts_ms") or o.get("ts")
                if ts_ms:
                    # find closest trade
                    best_c = None
                    min_dt = float("inf")
                    for cand in possibles:
                        cts = int(cand.get("exit_ts_ms") or cand.get("ts_ms") or 0)
                        dt = abs(int(ts_ms) - cts)
                        if dt < min_dt:
                            min_dt = dt
                            best_c = cand
                    if best_c and min_dt < 300_000: # 5 min window for fuzzy
                        c = best_c
                        if miss < 50:
                            print(f"DEBUG: SYMBOL-ONLY FUZZY MATCH! input_sid={sid} match_sid={c.get('sid')} dt_ms={min_dt}")

        if not c:
            miss_closed = 1

        y_closed, r_mult_closed, src_closed = compute_y_and_r_from_closed(c, r_min=float(args.r_min)) if c else (0, 0.0, "none")

        # -----------------------
        # TB override (optional)
        # -----------------------
        r_mult = float(r_mult_closed)
        y = int(y_closed)
        label_source = src_closed

        tb = tb_by_sid.get(sid)
        tb_primary = tb.get("primary", {}) if isinstance(tb, dict) else {}
        tb_meta = tb.get("meta", {}) if isinstance(tb, dict) else {}

        if tb and str(args.label_source) in ("tb_primary", "tb_util"):
            if str(args.label_source) == "tb_primary" and isinstance(tb_primary, dict) and tb_primary:
                # primary: use y_edge (binary) + r_mult at hit
                y = int(tb_primary.get("y_edge", 0) or 0)
                r_mult = float(tb_primary.get("r_mult", 0.0) or 0.0)
                label_source = "tb_primary"
            elif str(args.label_source) == "tb_util" and isinstance(tb_meta, dict) and tb_meta:
                util_r = float(tb_meta.get("util_r", 0.0) or 0.0)
                y = 1 if util_r >= float(args.tb_util_min_r) else 0
                r_mult = float(util_r)
                label_source = "tb_util"

        if label_source == "closed" and miss_closed:
            miss += 1
            continue

        ts_ms = int(o.get("ts_ms") or o.get("ts") or 0)
        if 0 < ts_ms < 10_000_000_000:
            ts_ms *= 1000

        row: dict[str, Any] = {
            "sid": sid,
            "ts_ms": int(ts_ms),
            "symbol": (o.get("symbol") or ""),
            "direction": (o.get("direction") or ""),
            "scenario_v4": str(o.get("scenario_v4") or o.get("scenario") or ""),
            "indicators": _scrub_empty_dicts(o),  # full payload; training expects indicators.*
        }

        # Flatten sub-dicts into 'indicators' for easier feature extraction
        # This fixes missing features which are nested inside these sub-dicts
        for sub_key in ["evidence", "indicators", "legs", "meta_context", "score_breakdown"]:
            if sub_key in row["indicators"] and isinstance(row["indicators"][sub_key], dict):
                for k, v in row["indicators"][sub_key].items():
                    if k not in row["indicators"]:
                       row["indicators"][k] = v

        # Explicitly default critical features if missing (upstream snapshot issue)
        # This converts "Column missing" error -> "High missing rate" quality check
        for crit in ["qimb_wmean", "ofi_ml_norm", "mp_mid_bps", "obi_dw"]:
            if crit not in row["indicators"]:
                row["indicators"][crit] = 0.0

        row.update({
            # baseline label
            "r_mult_closed": float(r_mult_closed),
            "y_closed": int(y_closed),

            # active label
            "label_source": str(label_source),
            "r_mult": float(r_mult),
            "y": int(y),

            "closed_event_type": (c.get("event_type") or "") if c else "",
        })

        # TB diagnostics (optional columns)
        if tb and isinstance(tb_primary, dict) and tb_primary:
            row["tb_primary_label"] = (tb_primary.get("label", "") or "")
            row["tb_primary_hit_ms"] = int(tb_primary.get("hit_ms", 0) or 0)
            row["tb_primary_ret_bps"] = float(tb_primary.get("ret_bps", 0.0) or 0.0)
            row["tb_primary_r_mult"] = float(tb_primary.get("r_mult", 0.0) or 0.0)
            row["tb_primary_y_edge"] = int(tb_primary.get("y_edge", 0) or 0)

        if tb and isinstance(tb_meta, dict) and tb_meta:
            row["tb_util_r"] = float(tb_meta.get("util_r", 0.0) or 0.0)
            row["tb_exec_cost_r"] = float(tb_meta.get("exec_cost_r", 0.0) or 0.0)

        # Optional: include closed-side pnl/risk if present (helps debugging).
        if c:
            for k in ("pnl", "pnl_net", "risk_usd", "reason", "reason_raw"):
                if k in c:
                    row[f"closed_{k}"] = c.get(k)

        rows.append(row)

    df = pd.DataFrame(rows)
    df.to_parquet(args.out, index=False)

    summary = {
        "inputs_rows": int(len(rows) + miss),
        "joined_rows": int(len(rows)),
        "missing_closed": int(miss),
        "label_r_min": float(args.r_min),
        "pos_rate": float(df["y"].mean()) if len(df) else 0.0,
        "label_src_counts": df["label_source"].value_counts().to_dict() if len(df) else {},
    }
    with open(args.out + ".json", "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)


if __name__ == "__main__":
    main()
