from __future__ import annotations

"""Nightly feature drift report (PSI + KS) for key online features.

Purpose
-------
Add an *explainable* batch-drift layer on top of online EMA/z-score drift. The
report intentionally focuses on a small Tier-1 feature set so Prometheus labels
and operator workflows stay manageable.

Outputs
-------
- report JSON with per-feature drift metrics and low-cardinality summary
- CSV with the same per-feature rows for analyst inspection
- optional Redis hash summary (metrics:feature_drift_batch:last)

Optional integration hooks
--------------------------
- `denylist_suggested`: strong candidate for feature-denylist AB loop
- `shadow_disable_suggested`: strong candidate for temporary shadow-disable

Data assumptions
----------------
Works with:
- CSV / parquet wide datasets (preferred)
- JSONL/NDJSON with either top-level numeric fields or `indicators.{feature}`

This module is intentionally independent from train bundles so it can run on any
stable baseline/current window pair.
"""

import argparse
import csv
import fnmatch
import json
import math
import os
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, List, Mapping, MutableMapping, Optional, Sequence, Tuple

import numpy as np

from services.nightly.feature_drift_ks import ks_report
from services.nightly.feature_drift_psi import psi_report

try:
    import pandas as pd  # type: ignore
except Exception:  # pragma: no cover - pandas is optional at import time
    pd = None  # type: ignore

try:
    import redis  # type: ignore
except Exception:  # pragma: no cover
    redis = None  # type: ignore


_TIER1_PATTERNS: Tuple[str, ...] = (
    "ofi_norm",
    "dw_obi",
    "depth_slope_*",
    "spread_bps",
    "liq_resiliency_*",
    "tca_*",
    "funding_*",
    "basis_bps",
    "open_interest",
    "delta_oi_*",
    "oi_notional_usd",
)


@dataclass(frozen=True)
class FeatureDriftRow:
    feature: str
    n_ref: int
    n_cur: int
    psi: float
    ks_stat: float
    ks_pvalue: float
    missing_rate_delta: float
    zero_rate_delta: float
    clip_rate_delta: float
    missing_rate_ref: float
    missing_rate_cur: float
    zero_rate_ref: float
    zero_rate_cur: float
    clip_rate_ref: float
    clip_rate_cur: float
    flag_warn: int
    flag_crit: int
    denylist_suggested: int
    shadow_disable_suggested: int
    reasons: List[str]


@dataclass(frozen=True)
class FeatureDriftSummary:
    status: str
    features_total: int
    features_evaluated: int
    warn_n: int
    crit_n: int
    denylist_suggest_n: int
    shadow_disable_suggest_n: int
    worst_feature: str
    worst_psi: float
    worst_ks_stat: float


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def _now_ms() -> int:
    return int(time.time() * 1000)


def _safe_float(x: Any, d: float = 0.0) -> float:
    try:
        if x is None:
            return float(d)
        v = float(x)
        if not math.isfinite(v):
            return float(d)
        return float(v)
    except Exception:
        return float(d)


def _safe_int(x: Any, d: int = 0) -> int:
    try:
        if x is None:
            return int(d)
        return int(float(x))
    except Exception:
        return int(d)


def _json_default(x: Any) -> Any:
    if isinstance(x, (np.integer,)):
        return int(x)
    if isinstance(x, (np.floating,)):
        v = float(x)
        if math.isfinite(v):
            return v
        return None
    raise TypeError(f"unsupported type: {type(x)!r}")


def _match_any(name: str, patterns: Sequence[str]) -> bool:
    return any(fnmatch.fnmatch(name, p) for p in patterns)


def _candidate_feature_names(columns: Iterable[str], patterns: Sequence[str]) -> List[str]:
    out: List[str] = []
    for c in columns:
        if _match_any(str(c), patterns):
            out.append(str(c))
    return sorted(set(out))


def _iter_jsonl(path: str) -> Iterator[Dict[str, Any]]:
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            s = line.strip()
            if not s:
                continue
            try:
                obj = json.loads(s)
            except Exception:
                continue
            if isinstance(obj, dict):
                yield obj


def _maybe_nested_feature(row: Mapping[str, Any], feature: str) -> Any:
    if feature in row:
        return row.get(feature)
    ind = row.get("indicators")
    if isinstance(ind, Mapping) and feature in ind:
        return ind.get(feature)
    return None


def _gather_from_jsonl(path: str, selected_features: Sequence[str]) -> Dict[str, List[float | None]]:
    out: Dict[str, List[float | None]] = {f: [] for f in selected_features}
    for row in _iter_jsonl(path):
        for f in selected_features:
            v = _maybe_nested_feature(row, f)
            try:
                if v is None:
                    out[f].append(None)
                else:
                    out[f].append(float(v))
            except Exception:
                out[f].append(None)
    return out


def _infer_jsonl_features(path: str, patterns: Sequence[str], max_rows: int = 1024) -> List[str]:
    seen: set[str] = set()
    n = 0
    for row in _iter_jsonl(path):
        n += 1
        for k in row.keys():
            if isinstance(k, str) and _match_any(k, patterns):
                seen.add(k)
        ind = row.get("indicators")
        if isinstance(ind, Mapping):
            for k in ind.keys():
                if isinstance(k, str) and _match_any(k, patterns):
                    seen.add(k)
        if n >= max_rows:
            break
    return sorted(seen)


def _load_wide_table(path: str) -> "pd.DataFrame":
    if pd is None:
        raise RuntimeError("pandas is required for csv/parquet feature drift inputs")
    p = str(path).lower()
    if p.endswith(".parquet") or p.endswith(".pq"):
        return pd.read_parquet(path)
    if p.endswith(".csv"):
        return pd.read_csv(path)
    raise RuntimeError(f"unsupported tabular format: {path}")


def _load_feature_vectors(path: str, selected_features: Sequence[str], patterns: Sequence[str]) -> Tuple[Dict[str, List[float | None]], List[str]]:
    p = str(path).lower()
    if p.endswith(".jsonl") or p.endswith(".ndjson"):
        feats = list(selected_features) if selected_features else _infer_jsonl_features(path, patterns)
        return _gather_from_jsonl(path, feats), feats

    df = _load_wide_table(path)
    feats = list(selected_features) if selected_features else _candidate_feature_names(df.columns, patterns)
    out: Dict[str, List[float | None]] = {}
    for f in feats:
        if f not in df.columns:
            out[f] = [None] * int(len(df))
        else:
            vals = []
            for x in df[f].tolist():
                try:
                    vals.append(None if x is None else float(x))
                except Exception:
                    vals.append(None)
            out[f] = vals
    return out, feats


def _score_feature(
    feature: str,
    ref_vals: Sequence[float | None],
    cur_vals: Sequence[float | None],
    *,
    psi_warn: float,
    psi_crit: float,
    ks_warn: float,
    ks_crit: float,
    ks_pvalue_max: float,
    missing_delta_warn: float,
    zero_delta_warn: float,
    clip_delta_warn: float,
    min_samples: int,
    protect_patterns: Sequence[str],
) -> FeatureDriftRow:
    psi_res = psi_report(ref_vals, cur_vals)
    ks_res = ks_report(ref_vals, cur_vals)

    reasons: List[str] = []
    warn = 0
    crit = 0

    if min(psi_res.n_ref, psi_res.n_cur) < int(min_samples):
        reasons.append(f"insufficient_samples<{int(min_samples)}")

    if psi_res.psi >= float(psi_warn):
        warn = 1
        reasons.append(f"psi>={float(psi_warn):.3f}")
    if psi_res.psi >= float(psi_crit):
        crit = 1
        reasons.append(f"psi>={float(psi_crit):.3f}")

    if ks_res.ks_stat >= float(ks_warn) and ks_res.ks_pvalue <= float(ks_pvalue_max):
        warn = 1
        reasons.append(f"ks_warn(stat>={float(ks_warn):.3f},p<={float(ks_pvalue_max):.3f})")
    if ks_res.ks_stat >= float(ks_crit) and ks_res.ks_pvalue <= float(ks_pvalue_max):
        crit = 1
        reasons.append(f"ks_crit(stat>={float(ks_crit):.3f},p<={float(ks_pvalue_max):.3f})")

    if abs(psi_res.missing_rate_delta) >= float(missing_delta_warn):
        warn = 1
        reasons.append(f"missing_delta>={float(missing_delta_warn):.3f}")
    if abs(psi_res.zero_rate_delta) >= float(zero_delta_warn):
        warn = 1
        reasons.append(f"zero_delta>={float(zero_delta_warn):.3f}")
    if abs(psi_res.clip_rate_delta) >= float(clip_delta_warn):
        warn = 1
        reasons.append(f"clip_delta>={float(clip_delta_warn):.3f}")

    shadow_disable = 1 if (crit == 1 or abs(psi_res.missing_rate_delta) >= 2.0 * float(missing_delta_warn)) else 0
    protected = _match_any(feature, protect_patterns)
    denylist_suggested = 1 if (crit == 1 and shadow_disable == 1 and not protected) else 0

    return FeatureDriftRow(
        feature=str(feature),
        n_ref=int(psi_res.n_ref),
        n_cur=int(psi_res.n_cur),
        psi=float(psi_res.psi),
        ks_stat=float(ks_res.ks_stat),
        ks_pvalue=float(ks_res.ks_pvalue),
        missing_rate_delta=float(psi_res.missing_rate_delta),
        zero_rate_delta=float(psi_res.zero_rate_delta),
        clip_rate_delta=float(psi_res.clip_rate_delta),
        missing_rate_ref=float(psi_res.missing_rate_ref),
        missing_rate_cur=float(psi_res.missing_rate_cur),
        zero_rate_ref=float(psi_res.zero_rate_ref),
        zero_rate_cur=float(psi_res.zero_rate_cur),
        clip_rate_ref=float(psi_res.clip_rate_ref),
        clip_rate_cur=float(psi_res.clip_rate_cur),
        flag_warn=int(warn),
        flag_crit=int(crit),
        denylist_suggested=int(denylist_suggested),
        shadow_disable_suggested=int(shadow_disable),
        reasons=sorted(set(reasons)),
    )


def build_feature_drift_report(
    *,
    reference_path: str,
    current_path: str,
    features_csv: str = "",
    tier1_only: int = 1,
    extra_patterns_csv: str = "",
    protect_patterns_csv: str = "",
    psi_warn: float = 0.10,
    psi_crit: float = 0.25,
    ks_warn: float = 0.12,
    ks_crit: float = 0.20,
    ks_pvalue_max: float = 0.05,
    missing_delta_warn: float = 0.05,
    zero_delta_warn: float = 0.10,
    clip_delta_warn: float = 0.05,
    min_samples: int = 64,
) -> Dict[str, Any]:
    selected_features = [s.strip() for s in str(features_csv or "").split(",") if s.strip()]
    patterns = list(_TIER1_PATTERNS if int(tier1_only) == 1 else ())
    patterns.extend([s.strip() for s in str(extra_patterns_csv or "").split(",") if s.strip()])
    if not selected_features and not patterns:
        patterns = list(_TIER1_PATTERNS)

    ref_vectors, ref_feats = _load_feature_vectors(reference_path, selected_features, patterns)
    cur_vectors, cur_feats = _load_feature_vectors(current_path, selected_features, patterns)

    if selected_features:
        features = sorted(set(selected_features))
    else:
        features = sorted(set(ref_feats) | set(cur_feats))

    protect_patterns = [s.strip() for s in str(protect_patterns_csv or "").split(",") if s.strip()]

    rows: List[FeatureDriftRow] = []
    for f in features:
        row = _score_feature(
            f,
            ref_vectors.get(f, []),
            cur_vectors.get(f, []),
            psi_warn=float(psi_warn),
            psi_crit=float(psi_crit),
            ks_warn=float(ks_warn),
            ks_crit=float(ks_crit),
            ks_pvalue_max=float(ks_pvalue_max),
            missing_delta_warn=float(missing_delta_warn),
            zero_delta_warn=float(zero_delta_warn),
            clip_delta_warn=float(clip_delta_warn),
            min_samples=int(min_samples),
            protect_patterns=protect_patterns,
        )
        rows.append(row)

    rows.sort(key=lambda r: (int(r.flag_crit), float(r.psi), float(r.ks_stat)), reverse=True)

    warn_n = sum(int(r.flag_warn) for r in rows)
    crit_n = sum(int(r.flag_crit) for r in rows)
    deny_n = sum(int(r.denylist_suggested) for r in rows)
    shadow_n = sum(int(r.shadow_disable_suggested) for r in rows)
    worst = rows[0] if rows else None

    status = "ok"
    if crit_n > 0:
        status = "crit"
    elif warn_n > 0:
        status = "warn"

    summary = FeatureDriftSummary(
        status=status,
        features_total=int(len(features)),
        features_evaluated=int(len(rows)),
        warn_n=int(warn_n),
        crit_n=int(crit_n),
        denylist_suggest_n=int(deny_n),
        shadow_disable_suggest_n=int(shadow_n),
        worst_feature=str(worst.feature if worst else ""),
        worst_psi=float(worst.psi if worst else 0.0),
        worst_ks_stat=float(worst.ks_stat if worst else 0.0),
    )

    return {
        "tool": "feature_drift_report_v1",
        "ts_ms": _now_ms(),
        "reference_path": str(reference_path),
        "current_path": str(current_path),
        "patterns": list(patterns),
        "summary": asdict(summary),
        "features": [asdict(r) for r in rows],
        "top_features": [asdict(r) for r in rows[: min(20, len(rows))]],
    }


# ---------------------------------------------------------------------------
# Redis / IO helpers
# ---------------------------------------------------------------------------

def _write_json(path: str, obj: Any) -> None:
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2, sort_keys=True, default=_json_default)


def _write_csv(path: str, rows: Sequence[Mapping[str, Any]]) -> None:
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    cols: List[str] = []
    for r in rows:
        for k in r.keys():
            if k not in cols:
                cols.append(str(k))
    with open(path, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        for r in rows:
            rec = dict(r)
            if isinstance(rec.get("reasons"), list):
                rec["reasons"] = "|".join(str(x) for x in rec["reasons"])
            w.writerow(rec)


def _write_metrics_hash(redis_url: str, metrics_key: str, report_json: str, rep: Mapping[str, Any]) -> None:
    if not redis_url or redis is None:
        return
    try:
        r = redis.Redis.from_url(redis_url, decode_responses=True)
        s = dict(rep.get("summary") or {})
        mapping = {
            "status": str(s.get("status", "")),
            "updated_ts_ms": int(rep.get("ts_ms", 0) or 0),
            "features_total": int(s.get("features_total", 0) or 0),
            "features_evaluated": int(s.get("features_evaluated", 0) or 0),
            "warn_n": int(s.get("warn_n", 0) or 0),
            "crit_n": int(s.get("crit_n", 0) or 0),
            "denylist_suggest_n": int(s.get("denylist_suggest_n", 0) or 0),
            "shadow_disable_suggest_n": int(s.get("shadow_disable_suggest_n", 0) or 0),
            "worst_feature": str(s.get("worst_feature", "")),
            "worst_psi": float(s.get("worst_psi", 0.0) or 0.0),
            "worst_ks_stat": float(s.get("worst_ks_stat", 0.0) or 0.0),
            "report_json": str(report_json),
            "reference_path": str(rep.get("reference_path", "")),
            "current_path": str(rep.get("current_path", "")),
        }
        r.hset(metrics_key, mapping={str(k): str(v) for k, v in mapping.items()})
    except Exception:
        return


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main(argv: Optional[Sequence[str]] = None) -> int:
    ap = argparse.ArgumentParser(description="Nightly feature drift batch report (PSI/KS)")
    ap.add_argument("--reference_path", required=True)
    ap.add_argument("--current_path", required=True)
    ap.add_argument("--out_json", required=True)
    ap.add_argument("--out_csv", default="")
    ap.add_argument("--redis_url", default=os.getenv("REDIS_URL", ""))
    ap.add_argument("--metrics_key", default=os.getenv("FEATURE_DRIFT_BATCH_METRICS_KEY", "metrics:feature_drift_batch:last"))

    ap.add_argument("--features_csv", default=os.getenv("FEATURE_DRIFT_BATCH_FEATURES_CSV", ""))
    ap.add_argument("--tier1_only", type=int, default=_safe_int(os.getenv("FEATURE_DRIFT_BATCH_TIER1_ONLY", "1"), 1))
    ap.add_argument("--extra_patterns_csv", default=os.getenv("FEATURE_DRIFT_BATCH_EXTRA_PATTERNS_CSV", ""))
    ap.add_argument("--protect_patterns_csv", default=os.getenv("FEATURE_DRIFT_BATCH_PROTECT_PATTERNS_CSV", ""))

    ap.add_argument("--psi_warn", type=float, default=_safe_float(os.getenv("FEATURE_DRIFT_BATCH_PSI_WARN", "0.10"), 0.10))
    ap.add_argument("--psi_crit", type=float, default=_safe_float(os.getenv("FEATURE_DRIFT_BATCH_PSI_CRIT", "0.25"), 0.25))
    ap.add_argument("--ks_warn", type=float, default=_safe_float(os.getenv("FEATURE_DRIFT_BATCH_KS_WARN", "0.12"), 0.12))
    ap.add_argument("--ks_crit", type=float, default=_safe_float(os.getenv("FEATURE_DRIFT_BATCH_KS_CRIT", "0.20"), 0.20))
    ap.add_argument("--ks_pvalue_max", type=float, default=_safe_float(os.getenv("FEATURE_DRIFT_BATCH_KS_PVALUE_MAX", "0.05"), 0.05))
    ap.add_argument("--missing_delta_warn", type=float, default=_safe_float(os.getenv("FEATURE_DRIFT_BATCH_MISSING_DELTA_WARN", "0.05"), 0.05))
    ap.add_argument("--zero_delta_warn", type=float, default=_safe_float(os.getenv("FEATURE_DRIFT_BATCH_ZERO_DELTA_WARN", "0.10"), 0.10))
    ap.add_argument("--clip_delta_warn", type=float, default=_safe_float(os.getenv("FEATURE_DRIFT_BATCH_CLIP_DELTA_WARN", "0.05"), 0.05))
    ap.add_argument("--min_samples", type=int, default=_safe_int(os.getenv("FEATURE_DRIFT_BATCH_MIN_SAMPLES", "64"), 64))

    args = ap.parse_args(list(argv) if argv is not None else None)

    try:
        rep = build_feature_drift_report(
            reference_path=str(args.reference_path),
            current_path=str(args.current_path),
            features_csv=str(args.features_csv),
            tier1_only=int(args.tier1_only),
            extra_patterns_csv=str(args.extra_patterns_csv),
            protect_patterns_csv=str(args.protect_patterns_csv),
            psi_warn=float(args.psi_warn),
            psi_crit=float(args.psi_crit),
            ks_warn=float(args.ks_warn),
            ks_crit=float(args.ks_crit),
            ks_pvalue_max=float(args.ks_pvalue_max),
            missing_delta_warn=float(args.missing_delta_warn),
            zero_delta_warn=float(args.zero_delta_warn),
            clip_delta_warn=float(args.clip_delta_warn),
            min_samples=int(args.min_samples),
        )
        _write_json(str(args.out_json), rep)
        out_csv = str(args.out_csv or "").strip() or (str(args.out_json) + ".csv")
        _write_csv(out_csv, rep.get("features") or [])
        _write_metrics_hash(str(args.redis_url), str(args.metrics_key), str(args.out_json), rep)
        return 0
    except Exception as e:
        err = {
            "tool": "feature_drift_report_v1",
            "ts_ms": _now_ms(),
            "status": "error",
            "error": str(e),
            "reference_path": str(args.reference_path),
            "current_path": str(args.current_path),
        }
        try:
            _write_json(str(args.out_json), err)
        except Exception:
            pass
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
