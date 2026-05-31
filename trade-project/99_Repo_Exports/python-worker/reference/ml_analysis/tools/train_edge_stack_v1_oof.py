from __future__ import annotations

"""Train edge_stack_v1 with strict out-of-fold stacking (OOF).

This tool trains a two-base-model stack:
  - base_lr: LogisticRegression
  - base_gbdt: HistGradientBoostingClassifier
  - meta: LogisticRegression on OOF (p_lr_oof, p_gbdt_oof)

Outputs a joblib "dict-pack" compatible with MLConfirmGate._decide_edge_stack_v1.

Design goals:
  - deterministic (fixed sorting by ts_ms, fixed random_state)
  - leakage-safe (purged + embargoed time-split)
  - train==serve feature engineering (feature_transforms + robust_scaler dict-pack)

Example:
  # Ensure this folder is on PYTHONPATH (e.g. export PYTHONPATH=./ml_analysis)
  python3 -m tools.train_edge_stack_v1_oof \
    --data_jsonl /path/to/dataset.jsonl \
    --out_model /path/to/edge_stack_v1.joblib \
    --feature_cols_json /path/to/feature_cols.json \
    --n_splits 5 --purge_ms 300000 --embargo_ms 120000
"""


import argparse
import json
import math
import os
import time
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from typing import Any

try:
    import numpy as np
except Exception as e:  # pragma: no cover
    raise SystemExit(f"numpy is required: {e}")

try:
    import joblib  # type: ignore
except Exception as e:  # pragma: no cover
    raise SystemExit(f"joblib is required: {e}")

# sklearn is intentionally optional for import-time; we error with a clean message at runtime
try:
    from sklearn.ensemble import HistGradientBoostingClassifier  # type: ignore
    from sklearn.linear_model import LogisticRegression  # type: ignore
except Exception:
    LogisticRegression = None  # type: ignore
    HistGradientBoostingClassifier = None  # type: ignore


# ---------------------------------------------------------------------------
# Centralized schema choices (avoid drift across tools)
# ---------------------------------------------------------------------------
try:
    from tools.schema_choices_v1 import normalize_schema_ver as _norm_schema_ver
    from tools.schema_choices_v1 import schema_choices as _schema_choices  # type: ignore
except Exception:  # pragma: no cover
    from ml_analysis.tools.schema_choices_v1 import normalize_schema_ver as _norm_schema_ver
    from ml_analysis.tools.schema_choices_v1 import schema_choices as _schema_choices  # type: ignore

# Prefer the project's feature engineering for train==serve consistency.
try:
    from core.feature_engineering import (
        RobustScalerPack,
        apply_transform,
        bucketize,
        derive_regime_label,
        derive_session_label,
    )
except Exception:  # pragma: no cover
    RobustScalerPack = None  # type: ignore

    def apply_transform(x: float, spec: Any) -> float:  # type: ignore
        return float(x)

    def bucketize(x: float, edges: Sequence[float]) -> int:  # type: ignore
        # simple fallback
        for i, e in enumerate(edges):
            if x <= float(e):
                return i
        return len(edges)

    def derive_regime_label(v: Any, fallback_score: float | None, cfg: dict[str, Any]) -> str:  # type: ignore
        return (v or "") or "unknown"

    def derive_session_label(ts_ms: int, cfg: dict[str, Any]) -> str:  # type: ignore
        return "unknown"


try:
    from services.ml_calibration import brier_score, ece_score, fit_platt_logit, logloss
except Exception:  # pragma: no cover
    brier_score = ece_score = logloss = fit_platt_logit = None  # type: ignore


def _sha256_16(items: Sequence[str]) -> str:
    """16-символьный SHA-256 от упорядоченного списка строк — короткий хэш для логов.

    Используется для feature_cols_hash в артефакте модели (pinning metadata).
    Позволяет алертить 'column drift' при несовпадении hash-ей.
    """
    import hashlib

    payload = "\n".join([str(x) for x in items]).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()[:16]


def _f(x: Any, d: Any = 0.0) -> float | None:
    """Convert x to float; return d on failure/non-finite.

    d=None is allowed (returns None), useful for optional fallback_score args.
    """
    try:
        if x is None:
            return None if d is None else d
        v = float(x)
        if not math.isfinite(v):
            return None if d is None else d
        return float(v)
    except Exception:
        return None if d is None else d


def _median(xs: Sequence[float]) -> float:
    ys = sorted(float(x) for x in xs)
    n = len(ys)
    if n == 0:
        return 0.0
    m = n // 2
    if n % 2 == 1:
        return float(ys[m])
    return float(0.5 * (ys[m - 1] + ys[m]))


def _mad(xs: Sequence[float], center: float) -> float:
    dev = [abs(float(x) - float(center)) for x in xs]
    return _median(dev)


@dataclass
class PurgedEmbargoTimeSeriesSplit:
    """Time-ordered CV split with purge + embargo.

    Walk-forward splits:
      - validation is a contiguous time slice
      - training uses ONLY the past (ts < val_start_ts)
      - purge removes the last purge_ms of training before val_start
      - embargo removes the first embargo_ms after val_end from being used in later folds

    This is sufficient to produce OOF predictions without time leakage.
    """

    n_splits: int = 5
    purge_ms: int = 0
    embargo_ms: int = 0
    min_train: int = 200

    def split(self, ts_ms: Sequence[int]) -> Iterable[tuple[np.ndarray, np.ndarray]]:
        if self.n_splits < 2:
            raise ValueError("n_splits must be >= 2")
        order = np.argsort(np.asarray(ts_ms, dtype=np.int64), kind="mergesort")
        n = int(len(order))
        if n == 0:
            return
        fold_sizes = [n // self.n_splits] * self.n_splits
        for i in range(n % self.n_splits):
            fold_sizes[i] += 1

        start = 0
        for k, fs in enumerate(fold_sizes):
            end = start + fs
            val_idx = order[start:end]
            if len(val_idx) == 0:
                start = end
                continue

            val_start_ts = int(np.min(np.asarray(ts_ms, dtype=np.int64)[val_idx]))
            val_end_ts = int(np.max(np.asarray(ts_ms, dtype=np.int64)[val_idx]))

            # train = strictly before val_start - purge
            cut_ts = val_start_ts - int(self.purge_ms)
            train_mask = np.asarray(ts_ms, dtype=np.int64) < cut_ts
            train_idx = np.where(train_mask)[0]

            # embargo affects only future folds; still, to be safe, drop samples in (val_end, val_end+embargo]
            if int(self.embargo_ms) > 0:
                embargo_end = val_end_ts + int(self.embargo_ms)
                emb_mask = (np.asarray(ts_ms, dtype=np.int64) > val_end_ts) & (np.asarray(ts_ms, dtype=np.int64) <= embargo_end)
                if np.any(emb_mask):
                    train_idx = np.asarray([i for i in train_idx if not bool(emb_mask[i])], dtype=np.int64)

            if len(train_idx) < int(self.min_train):
                # too small training set -> skip fold (keeps determinism)
                start = end
                continue

            yield np.asarray(train_idx, dtype=np.int64), np.asarray(val_idx, dtype=np.int64)
            start = end


def _scenario_norm(s: Any) -> str:
    ss = (s or "").strip().lower()
    if not ss:
        return "other"
    return ss


def _bucket_from_scenario(s: Any) -> str:
    # Keep in sync with gate-side bucket mapping.
    ss = _scenario_norm(s)
    if ss in ("trend", "range", "news"):
        return ss
    return "other"


def _spread_bucket_label(spread_bps: float, edges: Sequence[float]) -> str:
    es = [float(e) for e in edges] if edges else [2.0, 5.0, 10.0, 20.0]
    x = float(spread_bps)
    if x <= es[0]:
        return f"le{int(es[0])}"
    for a, b in zip(es[:-1], es[1:]):
        if float(a) < x <= float(b):
            return f"{int(a)}_{int(b)}"
    return f"gt{int(es[-1])}"


def _load_jsonl(path: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with open(path, encoding="utf-8") as f:
        for line in f:
            s = line.strip()
            if not s:
                continue
            try:
                d = json.loads(s)
            except Exception:
                continue
            if isinstance(d, dict):
                rows.append(d)
    return rows


def _get_indicators(row: dict[str, Any]) -> dict[str, Any]:
    ind = row.get("indicators")
    if isinstance(ind, dict):
        return ind
    # fallback: treat row itself as indicators-like
    return row


def _get_ts_ms(row: dict[str, Any], i: int) -> int:
    for k in ("ts_ms", "ts", "t_ms", "t"):
        if k in row:
            try:
                return int(row[k])
            except Exception:
                pass
    # fallback: stable monotonic ts
    return int(i)


def _get_direction(row: dict[str, Any]) -> str:
    return str(row.get("direction") or row.get("side") or "").upper() or "BUY"


def _get_scenario(row: dict[str, Any]) -> str:
    return str(row.get("scenario") or row.get("sc") or "").lower() or "other"


def _get_label(row: dict[str, Any]) -> int | None:
    for k in ("y", "label", "target"):
        if k in row:
            try:
                return 1 if int(row[k]) == 1 else 0
            except Exception:
                return None
    return None


def _collect_base_feature_names(feature_cols: Sequence[str]) -> list[str]:
    out: list[str] = []
    for col in feature_cols:
        if col.startswith("f_"):
            out.append(col[2:])
        elif col.startswith("mul_"):
            pair = col[4:]
            if "__" in pair:
                a, b = pair.split("__", 1)
                out.append(a)
                out.append(b)
    # unique, stable order
    seen = set()
    uniq: list[str] = []
    for x in out:
        if x not in seen:
            seen.add(x)
            uniq.append(x)
    return uniq


def _fit_robust_scaler(rows: Sequence[dict[str, Any]], feature_names: Sequence[str]) -> dict[str, dict[str, float]]:
    params: dict[str, dict[str, float]] = {}
    for name in feature_names:
        xs: list[float] = []
        for r in rows:
            ind = _get_indicators(r)
            xs.append(_f(ind.get(name, 0.0), 0.0))
        c = _median(xs)
        s = _mad(xs, c)
        if not math.isfinite(s) or s <= 1e-12:
            s = 1.0
        params[str(name)] = {"center": float(c), "scale": float(s)}
    return params


def _make_num_getter(
    *,
    indicators: dict[str, Any],
    transforms: dict[str, Any],
    scaler_params: dict[str, dict[str, float]] | None,
) -> Any:
    cache: dict[str, float] = {}

    def num(name: str) -> float:
        if name in cache:
            return cache[name]
        x = _f(indicators.get(name, 0.0), 0.0)
        if transforms and name in transforms:
            x = float(apply_transform(float(x), transforms.get(name)))
        if scaler_params and name in scaler_params:
            c = float(scaler_params[name].get("center", 0.0))
            s = float(scaler_params[name].get("scale", 1.0))
            if not math.isfinite(s) or s <= 1e-12:
                s = 1.0
            x = (float(x) - c) / s
        cache[name] = float(x)
        return cache[name]

    return num


def build_feature_row(
    *,
    feature_cols: Sequence[str],
    indicators: dict[str, Any],
    direction: str,
    scenario: str,
    ts_ms: int,
    feature_transforms: dict[str, Any] | None = None,
    robust_scaler_params: dict[str, dict[str, float]] | None = None,
    session_cfg: dict[str, Any] | None = None,
    spread_bucket_edges: Sequence[float] | None = None,
    liq_cfg: dict[str, Any] | None = None,
) -> list[float]:
    tf = feature_transforms if isinstance(feature_transforms, dict) else {}
    sc = session_cfg if isinstance(session_cfg, dict) else {}
    lc = liq_cfg if isinstance(liq_cfg, dict) else {}

    d = (direction or "").upper()
    s = _scenario_norm(scenario)

    # derived categorical features
    session_label = str(derive_session_label(int(ts_ms or 0), cfg=sc))

    edges = list(spread_bucket_edges) if isinstance(spread_bucket_edges, (list, tuple)) and spread_bucket_edges else [2.0, 5.0, 10.0, 20.0]
    spread_bps_raw = _f(indicators.get("spread_bps", 0.0), 0.0)
    spread_bucket_idx = int(bucketize(float(spread_bps_raw), [float(x) for x in edges]))
    spread_bucket_lbl = _spread_bucket_label(float(spread_bps_raw), edges)

    liq_label = str(derive_regime_label(indicators.get("liq_regime"), fallback_score=_f(indicators.get("liq_score"), None), cfg=lc)).lower()
    vol_label = str(derive_regime_label(indicators.get("vol_regime"), fallback_score=_f(indicators.get("vol_score"), None), cfg=lc)).lower()

    # UTC hour/day-of-week and scenario bucket (Commit 8)
    tm = time.gmtime(float(int(ts_ms or 0)) / 1000.0)
    utc_hour = int(getattr(tm, "tm_hour", 0))
    utc_dow = int(getattr(tm, "tm_wday", 0))
    bucket = _bucket_from_scenario(s)

    num = _make_num_getter(indicators=indicators, transforms=tf, scaler_params=robust_scaler_params)

    row: list[float] = []
    for col in feature_cols:
        if col.startswith("f_"):
            key = col[2:]
            row.append(float(num(key)))
        elif col.startswith("mul_"):
            pair = col[4:]
            if "__" in pair:
                a, b = pair.split("__", 1)
                row.append(float(num(a) * num(b)))
            else:
                row.append(0.0)
        elif col.startswith("direction_"):
            val = col[len("direction_"):].upper()
            row.append(1.0 if val == d else 0.0)
        elif col.startswith("scenario_v4_"):
            val = col[len("scenario_v4_"):].lower()
            row.append(1.0 if val == s else 0.0)
        elif col.startswith("bucket:"):
            val = col[len("bucket:"):].lower()
            row.append(1.0 if val == bucket else 0.0)
        elif col.startswith("hour:"):
            try:
                hh = int(col[len("hour:"):])
            except Exception:
                hh = -1
            row.append(1.0 if hh == utc_hour else 0.0)
        elif col.startswith("dow:"):
            try:
                dd = int(col[len("dow:"):])
            except Exception:
                dd = -1
            row.append(1.0 if dd == utc_dow else 0.0)
        elif col.startswith("session_"):
            val = col[len("session_"):].lower()
            row.append(1.0 if val == str(session_label).lower() else 0.0)
        elif col.startswith("spread_bucket_"):
            val = col[len("spread_bucket_"):].lower()
            ok = (val == str(spread_bucket_idx)) or (val == f"b{spread_bucket_idx}") or (val == spread_bucket_lbl)
            row.append(1.0 if ok else 0.0)
        elif col.startswith("liq_regime_"):
            val = col[len("liq_regime_"):].lower()
            row.append(1.0 if val == liq_label else 0.0)
        elif col.startswith("vol_regime_"):
            val = col[len("vol_regime_"):].lower()
            row.append(1.0 if val == vol_label else 0.0)
        else:
            row.append(0.0)
    return row


def _precision_at_top_k(probs: Sequence[float], y: Sequence[int], frac: float = 0.05) -> float:
    n = len(probs)
    if n == 0:
        return 0.0
    k = max(1, int(round(float(frac) * float(n))))
    idx = np.argsort(np.asarray(probs, dtype=np.float64))[::-1][:k]
    hits = 0
    for i in idx:
        hits += 1 if int(y[int(i)]) == 1 else 0
    return float(hits) / float(k)


def main(argv: Sequence[str] | None = None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--data_jsonl", required=True)
    ap.add_argument("--out_model", required=True)

    ap.add_argument("--feature_cols", default="")
    ap.add_argument("--feature_cols_json", default="")
    # Feature Registry: детерминированный feature_cols без feature_cols.json
    ap.add_argument(
        "--feature_schema_ver",
        default=os.environ.get("FEATURE_SCHEMA_VER", os.environ.get("ML_FEATURE_SCHEMA_VER", "")),
        choices=_schema_choices(include_empty=True),
        help="Если задан, берёт feature_cols из Feature Registry (детерминированно). "
             "Заменяет feature_cols.json — исключает column drift.",
    )
    ap.add_argument(
        "--dataset_report_json",
        default="",
        help="Опциональный report.json от builder-а; проверяет feature_cols_hash/schema_hash.",
    )
    ap.add_argument(
        "--scenario_prefix",
        default="bucket:",
        help="Префикс сценарного кодирования (bucket: — рекомендуется).",
    )
    ap.add_argument(
        "--include_time_onehot",
        type=int,
        default=1,
        help="Включить hour:/dow: one-hots в registry-derived feature_cols.",
    )
    ap.add_argument(
        "--require_feature_registry",
        type=int,
        default=int(os.environ.get("EDGE_STACK_REQUIRE_REGISTRY", "0")),
        help="Упасть, если registry/meta-валидация недоступна.",
    )
    ap.add_argument(
        "--strict_registry_match",
        type=int,
        default=1,
        help="Если заданы оба --feature_cols_json и --feature_schema_ver, "
             "требовать exact match списков колонок.",
    )
    ap.add_argument("--run_id", default="")

    ap.add_argument("--n_splits", type=int, default=5)
    ap.add_argument("--purge_ms", type=int, default=0)
    ap.add_argument("--embargo_ms", type=int, default=0)
    ap.add_argument("--min_train", type=int, default=200)

    ap.add_argument("--p_min", type=float, default=0.55)
    ap.add_argument("--p_min_by_bucket_json", default="")

    ap.add_argument("--with_robust_scaler", type=int, default=1)
    ap.add_argument("--feature_transforms_json", default="")

    # Strict feature schema: reject scenario_v4_* one-hots (use bucket:/hour:/dow: instead).
    # Activated via --strict_feature_cols=1 or EDGE_STACK_STRICT_FEATURE_COLS=1 env var.
    ap.add_argument(
        "--strict_feature_cols",
        type=int,
        default=0,
        help="Reject scenario_v4_* one-hots; use bucket:/hour:/dow: low-cardinality encoding instead",
    )

    # Base model knobs
    ap.add_argument("--lr_C", type=float, default=1.0)
    ap.add_argument("--lr_class_weight", default="balanced")

    ap.add_argument("--gbdt_max_depth", type=int, default=3)
    ap.add_argument("--gbdt_learning_rate", type=float, default=0.05)
    ap.add_argument("--gbdt_max_iter", type=int, default=400)

    # meta
    ap.add_argument("--meta_C", type=float, default=1.0)

    ap.add_argument("--calibrate", type=int, default=1)

    args = ap.parse_args(list(argv) if argv is not None else None)

    if LogisticRegression is None or HistGradientBoostingClassifier is None:
        raise SystemExit("scikit-learn is required (sklearn.linear_model.LogisticRegression, sklearn.ensemble.HistGradientBoostingClassifier)")

    rows = _load_jsonl(args.data_jsonl)
    if not rows:
        raise SystemExit("dataset is empty or unreadable")

    # Resolve feature_cols: Registry-first, fallback to legacy sources
    feature_cols: list[str] = []
    registry_meta: dict[str, Any] | None = None
    schema_ver = _norm_schema_ver(str(getattr(args, "feature_schema_ver", "") or "").strip())

    if schema_ver:
        # Registry-derived: детерминированный список из Feature Registry
        try:
            from core.feature_registry import get_edge_stack_feature_spec  # type: ignore

            spec = get_edge_stack_feature_spec(
                schema_ver=schema_ver,
                scenario_prefix=str(getattr(args, "scenario_prefix", "bucket:") or "bucket:"),
                include_direction=True,
                include_scenario=True,
                include_time_onehot=int(getattr(args, "include_time_onehot", 1) or 0) == 1,
                strict_feature_cols=bool(
                    os.environ.get("EDGE_STACK_STRICT_FEATURE_COLS", "0") in ("1", "true", "yes")
                ) or (int(getattr(args, "strict_feature_cols", 0) or 0) == 1),
                forbid_scenario_v4_onehot=True,
                max_numeric=int(os.environ.get("MAX_NUMERIC", "128")),
            )
            feature_cols = list(spec.feature_cols)
            registry_meta = spec.to_dict()
        except Exception as e:
            if int(getattr(args, "require_feature_registry", 0) or 0) == 1:
                raise SystemExit(f"feature_registry_unavailable: {e}")
            # Mягкий fallback на legacy sources
            schema_ver = ""

    if not feature_cols:
        # Legacy fallback: --feature_cols_json → CSV → dataset row
        if args.feature_cols_json:
            feature_cols = json.loads(open(args.feature_cols_json, encoding="utf-8").read())
        elif args.feature_cols:
            feature_cols = [c.strip() for c in str(args.feature_cols).split(",") if c.strip()]
        else:
            # Try infer from first row if provided
            fc = rows[0].get("feature_cols")
            if isinstance(fc, list) and fc:
                feature_cols = [str(x) for x in fc]

    if not feature_cols:
        raise SystemExit(
            "feature_cols is required: use --feature_schema_ver, --feature_cols, --feature_cols_json "
            "or include feature_cols in first dataset row"
        )

    # Strict match: если заданы оба --feature_cols_json и --feature_schema_ver, сверяем точно
    if schema_ver and args.feature_cols_json and int(getattr(args, "strict_registry_match", 1) or 0) == 1:
        try:
            legacy_cols = json.loads(open(args.feature_cols_json, encoding="utf-8").read())
            if list(legacy_cols) != list(feature_cols):
                raise SystemExit(
                    "feature_cols_mismatch: feature_cols_json не совпадает с registry-derived feature_cols; "
                    "пересоздайте feature_cols.json из registry или уберите --feature_cols_json"
                )
        except SystemExit:
            raise
        except Exception as e:
            raise SystemExit(f"feature_cols_mismatch: {e}")

    # Commit 12: reject scenario_v4_* if strict mode is on.
    # Activated via --strict_feature_cols=1 OR EDGE_STACK_STRICT_FEATURE_COLS=1 env var.
    _strict_env = os.environ.get("EDGE_STACK_STRICT_FEATURE_COLS", os.environ.get("ML_STRICT_FEATURE_COLS", "0") or "0").strip().lower()
    strict_cols = (int(getattr(args, "strict_feature_cols", 0) or 0) == 1) or (_strict_env in ("1", "true", "yes"))
    if strict_cols:
        bad = [c for c in feature_cols if str(c).startswith("scenario_v4_")]
        if bad:
            raise SystemExit(
                f"strict_feature_cols: scenario_v4_* is not allowed (found={bad[:5]}); "
                "use bucket:/hour:/dow: one-hots instead"
            )

    # Опциональная валидация feature_registry по dataset_report_json (hash-check)
    if getattr(args, "dataset_report_json", ""):
        try:
            rep = json.loads(open(args.dataset_report_json, encoding="utf-8").read())
            fr = rep.get("feature_registry") if isinstance(rep, dict) else None
            if isinstance(fr, dict):
                expected_hash = (fr.get("feature_cols_hash") or "").strip()
                got_hash = _sha256_16([str(x) for x in feature_cols])
                if expected_hash and expected_hash != got_hash:
                    raise SystemExit(
                        f"feature_cols_hash_mismatch: dataset report expected={expected_hash} "
                        f"got={got_hash} — column drift detected"
                    )
                # schema_hash сверяем только если registry_meta доступен
                if (
                    schema_ver
                    and registry_meta
                    and (fr.get("schema_hash") or "").strip()
                    and (fr.get("schema_hash") or "").strip() != (registry_meta.get("schema_hash") or "")
                ):
                    raise SystemExit(
                        f"schema_hash_mismatch: expected={fr.get('schema_hash')} "
                        f"got={registry_meta.get('schema_hash')}"
                    )
            else:
                # dataset_report_json не содержит feature_registry секции
                if schema_ver and int(getattr(args, "require_feature_registry", 0) or 0) == 1:
                    raise SystemExit("dataset_report_missing_feature_registry")
        except SystemExit:
            raise
        except Exception as e:
            if schema_ver and int(getattr(args, "require_feature_registry", 0) or 0) == 1:
                raise SystemExit(f"dataset_report_read_failed: {e}")

    feature_transforms: dict[str, Any] = {}
    if args.feature_transforms_json:
        try:
            feature_transforms = json.loads(open(args.feature_transforms_json, encoding="utf-8").read())
        except Exception:
            feature_transforms = {}

    # Collect usable examples
    ex: list[dict[str, Any]] = []
    ts: list[int] = []
    y: list[int] = []
    direction: list[str] = []
    scenario: list[str] = []

    for i, r in enumerate(rows):
        yy = _get_label(r)
        if yy is None:
            continue
        ex.append(r)
        ts.append(_get_ts_ms(r, i))
        y.append(int(yy)),
        direction.append(_get_direction(r)),
        scenario.append(_get_scenario(r)),

    if len(ex) < 100:
        raise SystemExit(f"not enough labeled rows: {len(ex)}")

    # Robust scaler params over base numeric features (f_ and mul_ inputs)
    scaler_params: dict[str, dict[str, float]] | None = None,
    if int(args.with_robust_scaler) == 1:
        base_names = _collect_base_feature_names(feature_cols),
        scaler_params = _fit_robust_scaler(ex, base_names),

    # Build X
    X = np.zeros((len(ex), len(feature_cols)), dtype=np.float32),
    for i, r in enumerate(ex):
        ind = _get_indicators(r),
        X[i, :] = np.asarray(
            build_feature_row(
                feature_cols=feature_cols,
                indicators=ind,
                direction=direction[i],
                scenario=scenario[i],
                ts_ms=ts[i],
                feature_transforms=feature_transforms,
                robust_scaler_params=scaler_params,
                # optional knobs can be provided later via cfg/model-pack
                session_cfg=None,
                spread_bucket_edges=None,
                liq_cfg=None,
            ),
            dtype=np.float32,
        )

    y_arr = np.asarray(y, dtype=np.int64)

    splitter = PurgedEmbargoTimeSeriesSplit(
        n_splits=int(args.n_splits),
        purge_ms=int(args.purge_ms),
        embargo_ms=int(args.embargo_ms),
        min_train=int(args.min_train),
    )

    oof_lr = np.full((len(ex),), np.nan, dtype=np.float64)
    oof_gbdt = np.full((len(ex),), np.nan, dtype=np.float64)

    fold_n = 0
    for tr_idx, va_idx in splitter.split(ts):
        fold_n += 1
        X_tr = X[tr_idx]
        y_tr = y_arr[tr_idx]
        X_va = X[va_idx]

        lr = LogisticRegression(
            C=float(args.lr_C),
            max_iter=500,
            solver="lbfgs",
            class_weight=(None if args.lr_class_weight == "none" else args.lr_class_weight),
            random_state=42,
        )
        gbdt = HistGradientBoostingClassifier(
            max_depth=int(args.gbdt_max_depth),
            learning_rate=float(args.gbdt_learning_rate),
            max_iter=int(args.gbdt_max_iter),
            random_state=42,
        )
        lr.fit(X_tr, y_tr)
        gbdt.fit(X_tr, y_tr)

        oof_lr[va_idx] = lr.predict_proba(X_va)[:, 1]
        oof_gbdt[va_idx] = gbdt.predict_proba(X_va)[:, 1]

    if fold_n == 0:
        raise SystemExit("no usable folds produced (check n_splits/min_train/purge/embargo)")

    # Keep only rows with filled OOF
    mask = np.isfinite(oof_lr) & np.isfinite(oof_gbdt)
    if int(np.sum(mask)) < 200:
        raise SystemExit(f"not enough OOF points: {int(np.sum(mask))}")

    Z = np.stack([oof_lr[mask], oof_gbdt[mask]], axis=1).astype(np.float32)
    y_z = y_arr[mask]

    meta = LogisticRegression(
        C=float(args.meta_C),
        max_iter=500,
        solver="lbfgs",
        class_weight=(None if args.lr_class_weight == "none" else args.lr_class_weight),
        random_state=42,
    )
    meta.fit(Z, y_z)

    # Train final base models on all available data
    base_lr = LogisticRegression(
        C=float(args.lr_C),
        max_iter=500,
        solver="lbfgs",
        class_weight=(None if args.lr_class_weight == "none" else args.lr_class_weight),
        random_state=42,
    )
    base_gbdt = HistGradientBoostingClassifier(
        max_depth=int(args.gbdt_max_depth),
        learning_rate=float(args.gbdt_learning_rate),
        max_iter=int(args.gbdt_max_iter),
        random_state=42,
    )
    base_lr.fit(X, y_arr)
    base_gbdt.fit(X, y_arr)

    # Optional calibration (Platt scaling on logit(p_meta))
    calibrator_dict: dict[str, Any] | None = None
    if int(args.calibrate) == 1 and fit_platt_logit is not None:
        p_meta_oof = meta.predict_proba(Z)[:, 1]
        cal = fit_platt_logit([float(x) for x in p_meta_oof], [int(x) for x in y_z])
        calibrator_dict = cal.to_dict()

    # Report
    report: dict[str, Any] = {
        "n_total": int(len(ex)),
        "n_oof": int(np.sum(mask)),
        "pos_rate": float(np.mean(y_arr)) if len(y_arr) else 0.0,
        "p_min": float(args.p_min),
        "p_min_by_bucket": {},
        "oof": {},
        "trained_at": int(time.time()),
        "run_id": str(args.run_id or ""),
    }

    # Registry / schema pinning metadata — предотвращает column drift при промоции модели
    report["feature_cols_hash"] = _sha256_16([str(x) for x in feature_cols])
    if schema_ver:
        report["feature_schema_ver"] = str(schema_ver)
    if registry_meta is not None:
        report["feature_registry"] = registry_meta

    if args.p_min_by_bucket_json:
        try:
            report["p_min_by_bucket"] = json.loads(open(args.p_min_by_bucket_json, encoding="utf-8").read())
        except Exception:
            report["p_min_by_bucket"] = {}

    if logloss is not None and brier_score is not None and ece_score is not None:
        p_lr_oof = oof_lr[mask]
        p_gbdt_oof = oof_gbdt[mask]
        p_meta_oof = meta.predict_proba(Z)[:, 1]
        report["oof"] = {
            "lr": {
                "logloss": float(logloss([float(x) for x in p_lr_oof], [int(x) for x in y_z])),
                "brier": float(brier_score([float(x) for x in p_lr_oof], [int(x) for x in y_z])),
                "precision_top5pct": float(_precision_at_top_k(p_lr_oof, y_z, 0.05)),
            },
            "gbdt": {
                "logloss": float(logloss([float(x) for x in p_gbdt_oof], [int(x) for x in y_z])),
                "brier": float(brier_score([float(x) for x in p_gbdt_oof], [int(x) for x in y_z])),
                "precision_top5pct": float(_precision_at_top_k(p_gbdt_oof, y_z, 0.05)),
            },
            "meta": {
                "logloss": float(logloss([float(x) for x in p_meta_oof], [int(x) for x in y_z])),
                "brier": float(brier_score([float(x) for x in p_meta_oof], [int(x) for x in y_z])),
                "precision_top5pct": float(_precision_at_top_k(p_meta_oof, y_z, 0.05)),
            }
        }
        ece, bins = ece_score([float(x) for x in p_meta_oof], [int(x) for x in y_z])
        report["oof"]["meta"]["ece"] = float(ece)
        report["oof"]["meta"]["ece_bins"] = bins[:10]

    out_pack: dict[str, Any] = {
        "schema_version": 1,
        "kind": "edge_stack_v1",
        "feature_cols": [str(x) for x in feature_cols],
        # Pinning metadata: позволяет детектировать column drift при загрузке
        "feature_cols_hash": _sha256_16([str(x) for x in feature_cols]),
        "feature_schema_ver": (schema_ver or ""),
        "feature_registry": registry_meta or {},
        "feature_transforms": feature_transforms,
        "robust_scaler": scaler_params or {},
        "lr": base_lr,
        "gbdt": base_gbdt,
        "meta": meta,
        "report": report,
    }

    if calibrator_dict is not None:
        out_pack["suggested_calibrator"] = calibrator_dict

    if args.run_id:
        out_pack["run_id"] = str(args.run_id)

    # Persist
    os.makedirs(os.path.dirname(os.path.abspath(args.out_model)) or ".", exist_ok=True)
    joblib.dump(out_pack, args.out_model)

    # Print minimal report JSON to stdout (useful for CI logs)
    print(json.dumps(report, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
