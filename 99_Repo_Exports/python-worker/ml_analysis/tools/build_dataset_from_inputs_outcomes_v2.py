from __future__ import annotations

import argparse
import json
import os
from typing import Any, Dict, Iterable, List, Optional

import pandas as pd

from common.ml_labeling import compute_y_and_r_from_closed


# ---------------------------------------------------------------------------
# Feature Registry (опциональный импорт, не падает если PYTHONPATH не содержит tick_flow_full)
# ---------------------------------------------------------------------------
try:
    from core.feature_registry import get_schema_info as _get_schema_info  # type: ignore
    _REGISTRY_AVAILABLE = True
except ImportError:
    _REGISTRY_AVAILABLE = False
    _get_schema_info = None  # type: ignore


# Offline-only derived features (F/G/H) for ablation without touching runtime pipeline.
try:
    from ml_analysis.common.derived_fgh import derive_fgh_rows  # type: ignore
    _DERIVE_FGH_AVAILABLE = True
except Exception:
    _DERIVE_FGH_AVAILABLE = False
    derive_fgh_rows = None  # type: ignore


# ---------------------------------------------------------------------------
# Centralized schema choices (avoid drift across tools)
# ---------------------------------------------------------------------------
try:
    from tools.schema_choices_v1 import schema_choices as _schema_choices, normalize_schema_ver as _norm_schema_ver  # type: ignore
except Exception:  # pragma: no cover
    from ml_analysis.tools.schema_choices_v1 import schema_choices as _schema_choices, normalize_schema_ver as _norm_schema_ver  # type: ignore


def _read_ndjson(path: str) -> Iterable[Dict[str, Any]]:
    with open(path, "r", encoding="utf-8") as f:
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
            except:
                pass
    return sid.strip().upper()


def _get_payload(obj: Dict[str, Any]) -> Dict[str, Any]:
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
    out: Dict[str, Any] = {}
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


def _pick_closed(existing: Dict[str, Any], candidate: Dict[str, Any]) -> Dict[str, Any]:
    """Prefer richer POSITION_CLOSED over sparse CLOSE, otherwise keep latest by exit_ts_ms."""
    cur_et = str(existing.get("event_type") or "").upper()
    new_et = str(candidate.get("event_type") or "").upper()

    if cur_et == "POSITION_CLOSED" and new_et == "CLOSE":
        return existing
    if cur_et == "CLOSE" and new_et == "POSITION_CLOSED":
        return candidate

    # otherwise, prefer the one with later exit_ts_ms/ts_ms (best effort)
    cur_ts = int(existing.get("exit_ts_ms") or existing.get("ts_ms") or 0)
    new_ts = int(candidate.get("exit_ts_ms") or candidate.get("ts_ms") or 0)
    return candidate if new_ts >= cur_ts else existing


def _load_tb_labels(path: str) -> Dict[str, Dict[str, Any]]:
    """Load labels:tb export (NDJSON) into {sid -> payload} map.

    Accepts formats:
      - {"sid": "...", "primary": {...}, "meta": {...}}
      - {"payload": "{...json...}"} (stream export)
      - {"payload": {...}}
    """
    tb: Dict[str, Dict[str, Any]] = {}
    for obj in _read_ndjson(path):
        o = _get_payload(obj)
        payload = o
        if "payload" in o:
            payload2 = _loads_maybe_json(o.get("payload"))
            if isinstance(payload2, dict) and payload2:
                payload = payload2
        sid = _norm_sid(str(payload.get("sid", "") or ""))
        if not sid:
            continue
        # Prefer the latest record in file order
        tb[sid] = payload
    return tb


def _emit_wide_cols(
    rows: List[Dict[str, Any]],
    *,
    schema_ver: str,
    out_meta: Optional[str],
    drop_indicators: bool,
) -> List[Dict[str, Any]]:
    """Добавляет к каждому row-у плоские колонки по схеме Feature Registry.

    Использует vectorize() из MLFeatureSchemaV4OF (или FeatureSchemaInfo) для
    детерминированного вектора фич. Заменяет ':' на '_' в именах колонок, чтобы
    они были безопасны для Parquet.

    Args:
        rows: список row-объектов (dict) из основного цикла.
        schema_ver: версия схемы («v3», «v4_of», …).
        out_meta: путь к .meta.json (None — не сохранять).
        drop_indicators: удалить сырой blob 'indicators' из row.

    Returns:
        rows с добавленными плоскими колонками.
    """
    if not _REGISTRY_AVAILABLE:
        print("[WARN] Feature Registry недоступен (PYTHONPATH не содержит tick_flow_full). "
              "--emit-wide-cols=1 игнорируется.")
        return rows

    schema_info = _get_schema_info(schema_ver)
    col_names = schema_info.column_names  # безопасные имена col (: → _)
    feat_names = schema_info.feature_names  # оригинальные имена (n:key, b:key, ...)

    # Для vectorize нам нужна сама схема (train==serve порядок фич).
    # Поддерживаем v4_of..v7_of (+ stable варианты).
    _schema_obj = None
    try:
        v = _norm_schema_ver(str(schema_ver))
        if v in ("v4_of", "v4"):
            from core.ml_feature_schema_v4_of import MLFeatureSchemaV4OF  # type: ignore
            _schema_obj = MLFeatureSchemaV4OF()
        elif v in ("v5_of", "v5"):
            from core.ml_feature_schema_v5_of import MLFeatureSchemaV5OF  # type: ignore
            _schema_obj = MLFeatureSchemaV5OF()
        elif v in ("v5_of_stable", "v5_stable"):
            from core.ml_feature_schema_v5_of import MLFeatureSchemaV5OFStable  # type: ignore
            _schema_obj = MLFeatureSchemaV5OFStable()
        elif v in ("v6_of", "v6"):
            from core.ml_feature_schema_v6_of import MLFeatureSchemaV6OF  # type: ignore
            _schema_obj = MLFeatureSchemaV6OF()
        elif v in ("v6_of_stable", "v6_stable"):
            from core.ml_feature_schema_v6_of import MLFeatureSchemaV6OFStable  # type: ignore
            _schema_obj = MLFeatureSchemaV6OFStable()
        elif v in ("v7_of", "v7"):
            from core.ml_feature_schema_v7_of import MLFeatureSchemaV7OF  # type: ignore
            _schema_obj = MLFeatureSchemaV7OF()
        elif v in ("v7_of_stable", "v7_stable"):
            from core.ml_feature_schema_v7_of import MLFeatureSchemaV7OFStable  # type: ignore
            _schema_obj = MLFeatureSchemaV7OFStable()
    except Exception:
        _schema_obj = None

    def _vectorize_row(row: Dict[str, Any]) -> Optional[List[float]]:
        """Вызов vectorize() или fallback через feature_names."""
        if _schema_obj is not None:
            try:
                ind = row.get("indicators") or {}
                if isinstance(ind, str):
                    ind = json.loads(ind) if ind.strip().startswith("{") else {}
                cancel_spike_veto = bool(ind.get("cancel_spike_veto", False))
                return _schema_obj.vectorize(
                    ts_ms=int(row.get("ts_ms") or 0),
                    direction=str(row.get("direction") or ""),
                    scenario=str(row.get("scenario_v4") or row.get("scenario") or ""),
                    indicators=ind,
                    cancel_spike_veto=cancel_spike_veto,
                )
            except Exception:
                pass
        return None

    out_rows: List[Dict[str, Any]] = []
    for row in rows:
        vec = _vectorize_row(row)
        new_row = dict(row)
        if vec is not None and len(vec) == len(col_names):
            for col, val in zip(col_names, vec):
                new_row[col] = float(val)
        if drop_indicators:
            new_row.pop("indicators", None)
        out_rows.append(new_row)

    # записать .meta.json сайдкар
    if out_meta:
        column_map = dict(zip(feat_names, col_names))
        meta = {
            "ver": schema_ver,
            "schema_hash": schema_info.schema_hash,
            "n_features": len(feat_names),
            "feature_names": list(feat_names),
            "column_names": list(col_names),
            "column_map": column_map,
        }
        os.makedirs(os.path.dirname(os.path.abspath(out_meta)) or ".", exist_ok=True)
        with open(out_meta, "w", encoding="utf-8") as f:
            json.dump(meta, f, ensure_ascii=False, indent=2)
        print(f"[feature_registry] meta.json → {out_meta}  (schema_hash={schema_info.schema_hash[:16]}…)")

    return out_rows


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--inputs", required=True, help="ndjson captured from signals:of:inputs")
    ap.add_argument("--closed", required=True, help="ndjson exported from events:trades (POSITION_CLOSED)")
    ap.add_argument("--out", required=True, help="output path (format depends on --out-format)")
    ap.add_argument(
        "--out-format",
        default=os.environ.get("ML_DATASET_OUT_FORMAT", "parquet"),
        choices=["parquet", "csv", "jsonl"],
        help="output format. Default: parquet. If parquet engine is unavailable, use csv/jsonl.",
    )

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

    # Feature Registry: wide cols (опциональные флаги, по умолчанию не меняют пайплайн)
    ap.add_argument(
        "--schema-ver",
        default=os.environ.get("ML_FEATURE_SCHEMA_VER", "v3"),
        choices=_schema_choices(include_empty=False),
        help="версия Feature Registry для --emit-wide-cols (default: v3). Use v5_of_stable for stable baseline.",
    )
    ap.add_argument(
        "--emit-wide-cols",
        type=int,
        default=0,
        help="1 = добавить плоские колонки по схеме Feature Registry (default: 0)",
    )
    ap.add_argument(
        "--drop-indicators",
        type=int,
        default=0,
        help="1 = удалить blob 'indicators' когда emit-wide-cols=1 (default: 0)",
    )
    ap.add_argument(
        "--out-meta",
        default="",
        help="путь к .meta.json с schema_hash и column_map (default: <out>.meta.json при emit-wide-cols=1)",
    )

    # Offline-only derived features (F/G/H) for ablation; does NOT touch runtime pipeline.
    ap.add_argument("--derive-fgh", type=int, default=int(os.environ.get("DERIVE_FGH", "0")))
    ap.add_argument("--fgh-leader-symbol", default=os.environ.get("FGH_LEADER_SYMBOL", "BTCUSDT"))
    ap.add_argument(
        "--fgh-leader-max-lag-ms",
        type=int,
        default=int(os.environ.get("FGH_LEADER_MAX_LAG_MS", "2000")),
    )
    ap.add_argument(
        "--fgh-vel-z-alpha",
        type=float,
        default=float(os.environ.get("FGH_VEL_Z_ALPHA", "0.06")),
    )
    ap.add_argument(
        "--fgh-store-debug-flags",
        type=int,
        default=int(os.environ.get("FGH_STORE_DEBUG_FLAGS", "0")),
    )

    args = ap.parse_args()

    tb_by_sid: Dict[str, Dict[str, Any]] = {}
    if str(args.tb_labels or "").strip():
        tb_by_sid = _load_tb_labels(str(args.tb_labels))

    # index closed by sid (exact and fuzzy)
    closed: Dict[str, Dict[str, Any]] = {}
    closed_fuzzy: Dict[str, List[Dict[str, Any]]] = {} # Changed to list for multiple matches
    for obj in _read_ndjson(args.closed):
        o = _get_payload(obj)
        sid_raw = str(o.get("sid") or "")
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
        sym = sid.split(":")[0] if ":" in sid else str(o.get("symbol") or "").upper()
        if sym:
            closed_fuzzy.setdefault(sym, []).append(o)

    print(f"Loaded {len(closed)} closed trades ({len(closed_fuzzy)} fuzzy keys)")

    rows: List[Dict[str, Any]] = []
    miss = 0

    for obj in _read_ndjson(args.inputs):
        o = _get_payload(obj)
        sid_raw = str(o.get("sid") or "")
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
        
        y_closed, r_mult_closed, _ = compute_y_and_r_from_closed(c, r_min=float(args.r_min)) if c else (0, 0.0, "none")

        # -----------------------
        # TB override (optional)
        # -----------------------
        r_mult = float(r_mult_closed)
        y = int(y_closed)
        label_source = "closed"

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

        row: Dict[str, Any] = {
            "sid": sid,
            "ts_ms": int(ts_ms),
            "symbol": str(o.get("symbol") or ""),
            "direction": str(o.get("direction") or ""),
            "scenario_v4": str(o.get("scenario_v4") or o.get("scenario") or ""),
            "indicators": _scrub_empty_dicts(o),  # full payload; training expects indicators.*
            
            # baseline label
            "r_mult_closed": float(r_mult_closed),
            "y_closed": int(y_closed),

            # active label
            "label_source": str(label_source),
            "r_mult": float(r_mult),
            "y": int(y),
            
            "closed_event_type": str(c.get("event_type") or "") if c else "",
        }

        # TB diagnostics (optional columns)
        if tb and isinstance(tb_primary, dict) and tb_primary:
            row["tb_primary_label"] = str(tb_primary.get("label", "") or "")
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

    if int(getattr(args, "derive_fgh", 0) or 0) == 1:
        if not _DERIVE_FGH_AVAILABLE or derive_fgh_rows is None:
            print("[WARN] derive-fgh requested but ml_analysis.common.derived_fgh is unavailable")
        else:
            rep = derive_fgh_rows(
                rows,
                leader_symbol=str(getattr(args, "fgh_leader_symbol", "BTCUSDT") or "BTCUSDT"),
                leader_max_lag_ms=int(getattr(args, "fgh_leader_max_lag_ms", 2000) or 2000),
                vel_z_alpha=float(getattr(args, "fgh_vel_z_alpha", 0.06) or 0.06),
                store_debug_flags=int(getattr(args, "fgh_store_debug_flags", 0) or 0) == 1,
            )
            print(f"[derive_fgh] ok stats={rep.get('stats', {}) if isinstance(rep, dict) else {}}")

    # Feature Registry: wide cols — добавляем если запрошено
    if int(args.emit_wide_cols) == 1:
        # определить путь к .meta.json
        meta_path: Optional[str] = str(args.out_meta).strip() or f"{args.out}.meta.json"
        rows = _emit_wide_cols(
            rows,
            schema_ver=str(args.schema_ver),
            out_meta=meta_path,
            drop_indicators=bool(int(args.drop_indicators)),
        )

    df = pd.DataFrame(rows)
    out_fmt = str(getattr(args, "out_format", "parquet") or "parquet").strip().lower()
    if out_fmt == "parquet":
        try:
            df.to_parquet(args.out, index=False)
        except ImportError as e:
            raise SystemExit(
                "Parquet engine is not available (pyarrow/fastparquet). "
                "Install dependency or rerun with --out-format=csv/jsonl. "
                f"Original error: {e}"
            )
    elif out_fmt == "csv":
        df.to_csv(args.out, index=False)
    elif out_fmt == "jsonl":
        with open(args.out, "w", encoding="utf-8") as f:
            for rec in df.to_dict(orient="records"):
                f.write(json.dumps(rec, ensure_ascii=False) + "\n")
    else:
        raise SystemExit(f"Unsupported --out-format={out_fmt}")

    summary: Dict[str, Any] = {
        "inputs_rows": int(len(rows) + miss),
        "joined_rows": int(len(rows)),
        "missing_closed": int(miss),
        "label_r_min": float(args.r_min),
        "pos_rate": float(df["y"].mean()) if len(df) else 0.0,
        "label_src_counts": df["label_source"].value_counts().to_dict() if len(df) else {},
        "emit_wide_cols": int(args.emit_wide_cols),
        "schema_ver": str(args.schema_ver),
        "out_format": str(out_fmt),
    }
    # добавляем schema_hash в summary когда wide cols активны
    if int(args.emit_wide_cols) == 1 and _REGISTRY_AVAILABLE:
        try:
            _si = _get_schema_info(str(args.schema_ver))
            summary["schema_hash"] = _si.schema_hash
            summary["n_features"] = len(_si.feature_names)
        except Exception:
            pass
    with open(args.out + ".json", "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)


if __name__ == "__main__":
    main()
