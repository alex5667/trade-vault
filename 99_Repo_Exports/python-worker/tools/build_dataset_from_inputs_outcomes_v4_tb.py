from __future__ import annotations

import argparse
import json
from collections.abc import Iterable
from typing import Any

import pandas as pd


def _read_ndjson(path: str) -> Iterable[dict[str, Any]]:
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            yield json.loads(line)


def _get_payload(obj: dict[str, Any]) -> dict[str, Any]:
    if "payload" in obj and isinstance(obj["payload"], str) and obj["payload"].strip().startswith("{"):
        try:
            return json.loads(obj["payload"])
        except Exception:
            return obj
    return obj


def _norm_sid(sid: str) -> str:
    if not sid:
        return ""
    if sid.startswith("crypto-of:"):
        return sid[len("crypto-of:") :]
    return sid


# Top-level fields that are metadata (extracted into named columns), not features.
# Everything else in the payload flows into the `indicators` column so ML training
# can consume all features (including og_* and future v15+ keys) without code changes.
_META_KEYS: frozenset[str] = frozenset({
    "v", "sid", "ts_ms", "ts", "symbol", "direction",
    "scenario", "scenario_v4", "regime", "regime_group",
})


def _build_indicators(payload: dict[str, Any]) -> dict[str, Any]:
    """Extract feature dict from OFInputsV{1,2,3,...} flat payload.

    Preserves the legacy nested-indicators path if the payload has a real
    `indicators` sub-dict (some producers wrap differently); otherwise treats
    the whole payload as indicators (excluding _META_KEYS).
    """
    nested = payload.get("indicators")
    if isinstance(nested, dict) and nested:
        src = nested
    else:
        src = {k: v for k, v in payload.items() if k not in _META_KEYS}

    return {
        k: (json.dumps(v) if isinstance(v, (dict, list)) else v)
        for k, v in src.items()
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--inputs", required=True)
    ap.add_argument("--tb-labels", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument(
        "--y-label-col",
        choices=("y_edge", "y_edge_cost_aware"),
        default="y_edge",
        help="Which TB label column to copy into the canonical `y` column "
             "(default: y_edge for backward-compat; use y_edge_cost_aware "
             "after running labeler with TB_FEES_BPS_ONE_SIDE>0).",
    )
    ap.add_argument(
        "--out-format",
        choices=("parquet", "jsonl"),
        default="parquet",
        help="Output format. `parquet` (default) requires pyarrow/fastparquet. "
             "Use `jsonl` for environments without arrow (tests, ad-hoc analysis).",
    )
    args = ap.parse_args()

    tb: dict[str, dict[str, Any]] = {}
    for obj in _read_ndjson(args.tb_labels):
        sid = _norm_sid((obj.get("sid", "") or ""))
        if sid:
            tb[sid] = obj

    rows: list[dict[str, Any]] = []
    miss = 0
    for obj in _read_ndjson(args.inputs):
        o = _get_payload(obj)
        sid = _norm_sid((o.get("sid", "") or ""))
        if not sid:
            continue
        t = tb.get(sid)
        if not t:
            miss += 1
            continue

        # Both labels carried through; downstream training picks one via --y-label-col.
        y_legacy = int(t.get("y_edge", 0) or 0)
        y_cost_aware = int(t.get("y_edge_cost_aware", 0) or 0)
        y_active = y_cost_aware if args.y_label_col == "y_edge_cost_aware" else y_legacy

        rows.append({
            "sid": sid,
            "ts_ms": int(o.get("ts_ms", o.get("ts", 0)) or 0),
            "symbol": (o.get("symbol", "") or ""),
            "direction": (o.get("direction", "") or ""),
            "scenario_v4": (o.get("scenario_v4", o.get("scenario", "")) or ""),
            "indicators": _build_indicators(o),
            # Active label (selected via --y-label-col)
            "y_edge": int(y_active),
            # Both labels preserved for offline drift / flip analysis
            "y_edge_legacy": y_legacy,
            "y_edge_cost_aware": y_cost_aware,
            "tb_outcome": (t.get("tb_outcome", "") or ""),
            "mae_bps": float(t.get("mae_bps", 0.0) or 0.0),
            "mfe_bps": float(t.get("mfe_bps", 0.0) or 0.0),
            "adverse_proxy": float(t.get("adverse_proxy", 0.0) or 0.0),
            "mae_r": float(t.get("mae_r", 0.0) or 0.0),
            "mfe_r": float(t.get("mfe_r", 0.0) or 0.0),
            # v14_of cost-aware columns (0.0 / 0 when labeler ran without cost — backward-compat)
            "cost_bps": float(t.get("cost_bps", 0.0) or 0.0),
            "realized_close_bps": float(t.get("realized_close_bps", 0.0) or 0.0),
            "edge_after_cost_bps": float(t.get("edge_after_cost_bps", 0.0) or 0.0),
        })

    df = pd.DataFrame(rows)
    if args.out_format == "jsonl":
        with open(args.out, "w", encoding="utf-8") as f:
            for r in rows:
                f.write(json.dumps(r, ensure_ascii=False, default=str) + "\n")
    else:
        df.to_parquet(args.out, index=False)

    # Diagnostic summary: pos rates for both labels + flip rate.
    pos_rate = float(df["y_edge"].mean()) if len(df) else 0.0
    pos_rate_legacy = float(df["y_edge_legacy"].mean()) if len(df) else 0.0
    pos_rate_cost = float(df["y_edge_cost_aware"].mean()) if len(df) else 0.0
    flip_rate = (
        float((df["y_edge_legacy"] != df["y_edge_cost_aware"]).mean())
        if len(df) else 0.0
    )

    summary = {
        "inputs_rows": len(rows) + miss,
        "joined_rows": len(rows),
        "missing_tb": miss,
        "y_label_col": args.y_label_col,
        "pos_rate": pos_rate,
        "pos_rate_legacy": pos_rate_legacy,
        "pos_rate_cost_aware": pos_rate_cost,
        "label_flip_rate": flip_rate,  # fraction where legacy ≠ cost-aware
    }
    with open(args.out + ".json", "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)


if __name__ == "__main__":
    main()
