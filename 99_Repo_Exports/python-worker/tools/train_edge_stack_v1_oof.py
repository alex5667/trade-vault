#!/usr/bin/env python3
from __future__ import annotations

"""
train_edge_stack_v1_oof.py

Train OOF stacking model (LR + GBDT -> meta LR) with Platt calibration for MLConfirmGate kind=edge_stack_v1.

Ключевые особенности:
  - OOF (Out-of-Fold) предсказания для base моделей (LR, GBDT) с time-series split (purge/embargo)
  - Meta-LR обучается только на OOF предсказаниях (без leakage)
  - PlattLogitCalibrator обучается на OOF meta-prob (без leakage)
  - Экспорт model.joblib (dict pack) + calibrator.json

Input:
  NDJSON dataset с полями:
    - ts_ms: timestamp в миллисекундах
    - direction: LONG/SHORT
    - scenario: scenario_v4 (trend/range/reversal/etc)
    - indicators: dict с фичами (delta_z, ofi_z, obi, spread_bps, etc)
    - y: binary label (0/1)

Output:
  model.joblib: dict-pack с lr/gbdt/meta моделями
  calibrator.json: PlattLogitCalibrator параметры (a, b, eps)
"""

import argparse
import json
import math
import os
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from typing import Any

import joblib
import numpy as np
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import RobustScaler

from utils.time_utils import get_ny_time_millis

try:
    import xgboost as xgb
except ImportError:
    xgb = None

# Dynamic GPU detection — no hard dependency on CUDA
_XGBOOST_DEVICE = "cpu"
try:
    import torch as _torch
    if _torch.cuda.is_available():
        _XGBOOST_DEVICE = "cuda"
except Exception:
    try:
        import cupy as _cp
        if _cp.cuda.is_available():
            _XGBOOST_DEVICE = "cuda"
    except Exception:
        pass

# ваш детерминированный Platt scaling (без sklearn)
from services.ml_calibration import fit_platt_logit


# -------------------------
# Time-series OOF splitter (Purged/Embargo для предотвращения leakage)
# -------------------------
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

    def split(self, ts_ms: Sequence[int]) -> Iterable[tuple[list[int], list[int]]]:
        n = len(ts_ms)
        if n == 0 or self.n_splits <= 0:
            return []

        idx = list(range(n))
        idx.sort(key=lambda i: int(ts_ms[i]))
        ts_sorted = [int(ts_ms[i]) for i in idx]

        fold_sizes = [n // self.n_splits] * self.n_splits
        for i in range(n % self.n_splits):
            fold_sizes[i] += 1

        # contiguous folds on sorted time
        starts: list[int] = []
        s = 0
        for fs in fold_sizes:
            starts.append(s)
            s += fs

        for k in range(self.n_splits):
            v_start = starts[k]
            v_end = starts[k] + fold_sizes[k]
            if v_start >= v_end:
                continue

            val_sorted = idx[v_start:v_end]
            val_start_ts = ts_sorted[v_start]
            val_end_ts = ts_sorted[v_end - 1]

            # walk-forward: train only from the past, with purge before val
            cutoff = int(val_start_ts) - int(self.purge_ms)
            train_sorted = [idx[i] for i in range(0, v_start) if ts_sorted[i] <= cutoff]

            # embargo: drop train samples too close after val_end (not applicable in pure walk-forward),
            # kept here for compatibility (no-op unless you later extend to symmetric CV)
            if self.embargo_ms and train_sorted:
                pass

            if len(train_sorted) < int(self.min_train):
                continue

            yield (train_sorted, val_sorted)


# -------------------------
# Feature row builder (копия логики из ml_confirm_gate._build_feature_row)
# -------------------------
def _f(x: Any, d: float = 0.0) -> float:
    """Безопасное преобразование в float."""
    try:
        if x is None:
            return d
        v = float(x)
        if not math.isfinite(v):
            return d
        return v
    except Exception:
        return d


def _scenario_norm(s: str) -> str:
    """Нормализация scenario."""
    return (s or "").strip().lower()


def _bucket_from_scenario(scenario: str) -> str:
    """Определение bucket из scenario."""
    s = _scenario_norm(scenario)
    if "trend" in s:
        return "trend"
    if "range" in s:
        return "range"
    return "other"


def build_feature_row(
    *,
    feature_cols: list[str],
    indicators: dict[str, Any],
    direction: str,
    scenario: str,
    ts_ms: int,
) -> tuple[list[float], list[str]]:
    """
    Строит feature row из indicators.
    
    ВАЖНО: здесь вы должны повторить ТО ЖЕ преобразование,
    что и в gate через transforms/scaler.
    Для "первого прохода" оставляем identity, а scaling/transforms
    внедряется через P0 fix (pack содержит robust_scaler/feature_transforms,
    а gate применяет их в проде).
    
    Args:
        feature_cols: список названий фич
        indicators: dict с индикаторами
        direction: LONG/SHORT
        scenario: scenario_v4
        ts_ms: timestamp в миллисекундах
    
    Returns:
        (row, missing) - feature vector и список отсутствующих критических фич
    """
    # Минимум: критические (как в gate)
    missing: list[str] = []
    for k in ("spread_bps", "expected_slippage_bps"):
        if k not in indicators:
            missing.append(k)
    if "exec_risk_norm" not in indicators:
        # допускаем 0, но фиксируем missing
        missing.append("exec_risk_norm")

    d = (direction or "").upper()
    s = _scenario_norm(scenario)
    bucket = _bucket_from_scenario(scenario)

    # ВАЖНО: здесь вы должны повторить ТО ЖЕ преобразование,
    # что и в gate через transforms/scaler.
    # Для "первого прохода" оставляем identity, а scaling/transforms
    # внедряется через P0 fix (pack содержит robust_scaler/feature_transforms,
    # а gate применяет их в проде).
    cache: dict[str, float] = {}

    def num(name: str) -> float:
        """Получить числовую фичу с кэшированием."""
        if name in cache:
            return cache[name]
        v = _f(indicators.get(name, 0.0), 0.0)
        cache[name] = float(v)
        return cache[name]

    row: list[float] = []
    for col in feature_cols:
        if col.startswith("f_"):
            # числовая фича: f_delta_z -> indicators["delta_z"]
            row.append(num(col[2:]))
        elif col.startswith("mul_"):
            # interaction term: mul_a__b -> a*b (после per-feature transform/scale)
            pair = col[4:]
            if "__" in pair:
                a, b = pair.split("__", 1)
                row.append(num(a) * num(b))
            else:
                row.append(0.0)
        elif col.startswith("direction_"):
            # one-hot: direction_LONG, direction_SHORT
            val = col[len("direction_"):].upper()
            row.append(1.0 if val == d else 0.0)
        elif col.startswith("scenario_v4_"):
            # one-hot: scenario_v4_trend, scenario_v4_range, scenario_v4_other
            val = col[len("scenario_v4_"):].lower()
            row.append(1.0 if val == s else 0.0)
        else:
            # session_/spread_bucket_/liq_regime_/vol_regime_ —
            # лучше включить после того как вы утвердите генераторы в одном месте.
            row.append(0.0)

    return row, missing


def load_jsonl(path: str) -> list[dict[str, Any]]:
    """Загружает NDJSON файл."""
    rows = []
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            rows.append(json.loads(line))
    return rows


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Train OOF stacking model (LR + GBDT -> meta LR) with Platt calibration"
    )
    # Support both old and new argument names for compatibility
    ap.add_argument("--data_jsonl", dest="inp", default="", help="dataset jsonl/ndjson (new format)")
    ap.add_argument("--in", dest="inp_old", default="", help="dataset jsonl/ndjson (legacy format)")
    ap.add_argument("--out_model", required=True, help="path to joblib pack")
    ap.add_argument("--out_calib", default="", help="path to calibrator json (optional)")
    ap.add_argument("--feature_cols_json", default="", help="path to feature_cols.json (optional, will use default if not provided)")
    ap.add_argument(
        "--feature_schema_ver",
        default="",
        help=(
            "Optional registered schema version to load feature_cols from "
            "core.feature_registry.get_edge_stack_feature_spec (e.g. v15_of). "
            "When set, takes precedence over the legacy default 13-col list "
            "and is incompatible with --feature_cols_json (fail-fast)."
        ),
    )
    ap.add_argument("--n_splits", type=int, default=5, help="number of OOF folds")
    ap.add_argument("--purge_ms", type=int, default=5*60_000, help="purge window in ms (default: 5 min)")
    ap.add_argument("--embargo_ms", type=int, default=5*60_000, help="embargo window in ms (default: 5 min)")
    ap.add_argument("--min_train", type=int, default=200, help="minimum training samples per fold")
    ap.add_argument("--lr_c", type=float, default=1.0, help="LR C parameter")
    ap.add_argument("--gbdt_max_depth", type=int, default=3, help="GBDT max_depth")
    ap.add_argument("--gbdt_lr", type=float, default=0.06, help="GBDT learning_rate")
    ap.add_argument("--gbdt_max_iter", type=int, default=250, help="GBDT max_iter")
    ap.add_argument("--gbdt_l2", type=float, default=0.1, help="GBDT l2_regularization")
    ap.add_argument("--calib_l2", type=float, default=1e-3, help="Platt calibration L2 regularization")
    ap.add_argument("--calib_max_iter", type=int, default=50, help="Platt calibration max iterations")
    ap.add_argument("--calibrate", type=int, default=1, help="enable calibration (1) or disable (0)")
    args = ap.parse_args()

    # Determine input file (prefer new format, fallback to old)
    inp_file = args.inp or args.inp_old
    if not inp_file:
        raise ValueError("Either --data_jsonl or --in must be provided")

    # Загрузка данных
    data = load_jsonl(inp_file)
    if not data:
        raise ValueError(f"No data loaded from {inp_file}")

    # ── feature_cols resolution ────────────────────────────────────────────
    # Priority: --feature_schema_ver  >  --feature_cols_json  >  legacy default 13-col.
    # Fail-fast guards (per audit P0):
    #   1. --feature_schema_ver и --feature_cols_json одновременно — ошибка
    #      (две конфликтующие истины).
    #   2. REQUIRE_FEATURE_COLS_JSON=1 в env запрещает legacy default —
    #      обучение должно явно зафиксировать схему.
    #   3. --feature_schema_ver требует registered schema (через
    #      core.feature_registry.get_edge_stack_feature_spec).
    feature_schema_ver = (args.feature_schema_ver or "").strip()
    require_explicit = os.getenv("REQUIRE_FEATURE_COLS_JSON", "0") == "1"

    if feature_schema_ver and args.feature_cols_json:
        raise ValueError(
            "--feature_schema_ver and --feature_cols_json are mutually exclusive: "
            f"got schema_ver={feature_schema_ver!r} and cols_json={args.feature_cols_json!r}"
        )

    if feature_schema_ver:
        try:
            from core.feature_registry import get_edge_stack_feature_spec  # type: ignore
        except Exception as e:
            raise RuntimeError(
                f"--feature_schema_ver={feature_schema_ver!r} requested but "
                f"core.feature_registry.get_edge_stack_feature_spec is unavailable: {e}"
            )
        spec = get_edge_stack_feature_spec(
            feature_schema_ver,
            strict_feature_cols=True,
            forbid_scenario_v4_onehot=True,
        )
        feature_cols = list(spec.feature_cols)
        if not feature_cols:
            raise ValueError(
                f"get_edge_stack_feature_spec({feature_schema_ver!r}) returned empty feature_cols"
            )
    elif args.feature_cols_json:
        try:
            with open(args.feature_cols_json, encoding="utf-8") as f:
                feature_cols = json.load(f)
            if not isinstance(feature_cols, list):
                raise ValueError(f"feature_cols_json must contain a JSON array, got {type(feature_cols)}")
        except Exception as e:
            raise ValueError(f"Failed to load feature_cols from {args.feature_cols_json}: {e}")
    else:
        if require_explicit:
            raise ValueError(
                "REQUIRE_FEATURE_COLS_JSON=1: legacy default 13-col list disabled. "
                "Pass --feature_schema_ver=<ver> (registry) or --feature_cols_json=<path>."
            )
        # ---- feature_cols: зафиксируйте один раз и не меняйте без version bump
        # ВАЖНО: эти фичи должны совпадать с теми, что используются в ml_confirm_gate
        feature_cols = [
            "f_delta_z", "f_ofi_z", "f_obi_z", "f_spread_bps", "f_expected_slippage_bps",
            "f_exec_risk_norm", "f_liq_score",
            "mul_delta_z__liq_score",
            "direction_LONG", "direction_SHORT",
            # scenario one-hot (пример)
            "scenario_v4_trend", "scenario_v4_range", "scenario_v4_other",
        ]

    # Извлечение timestamps и labels
    ts = np.array([int(r.get("ts_ms", 0)) for r in data], dtype=np.int64)
    y = np.array([int(r.get("y", 0)) for r in data], dtype=np.int64)

    # Проверка на наличие данных
    if len(ts) == 0:
        raise ValueError("No timestamps found in data")
    if len(y) == 0:
        raise ValueError("No labels found in data")

    # Построение feature matrix
    X_list: list[list[float]] = []
    miss_n = 0
    for r in data:
        indicators = r.get("indicators") or {}
        row, missing = build_feature_row(
            feature_cols=feature_cols,
            indicators=indicators,
            direction=(r.get("direction", "")),
            scenario=(r.get("scenario", "")),
            ts_ms=int(r.get("ts_ms", 0)),
        )
        if missing:
            miss_n += 1
        X_list.append(row)

    X = np.asarray(X_list, dtype=np.float32)

    # Проверка на NaN/Inf
    if not np.all(np.isfinite(X)):
        raise ValueError("X contains NaN or Inf values")

    # OOF splitting
    splitter = PurgedEmbargoTimeSeriesSplit(
        n_splits=args.n_splits, purge_ms=args.purge_ms, embargo_ms=args.embargo_ms, min_train=args.min_train
    )

    lr = Pipeline([
        ("scaler", RobustScaler(with_centering=True, with_scaling=True, quantile_range=(25.0, 75.0))),
        ("lr", LogisticRegression(
            C=args.lr_c,
            penalty="l2",
            solver="lbfgs",
            max_iter=200,
            class_weight="balanced",
            n_jobs=None,
            random_state=42,  # для детерминизма
        ))
    ])

    if xgb is not None:
        # Use XGBoost on GPU if available, fallback to CPU transparently
        gbdt = xgb.XGBClassifier(
            n_estimators=args.gbdt_max_iter,
            max_depth=args.gbdt_max_depth,
            learning_rate=args.gbdt_lr,
            tree_method="hist",
            device=_XGBOOST_DEVICE,
            eval_metric="logloss",
            random_state=42,
        )
    else:
        gbdt = HistGradientBoostingClassifier(
            max_depth=args.gbdt_max_depth,
            learning_rate=args.gbdt_lr,
            max_iter=args.gbdt_max_iter,
            l2_regularization=args.gbdt_l2,
            random_state=42,  # для детерминизма
        )
    meta = LogisticRegression(
        C=args.lr_c, penalty="l2", solver="lbfgs", max_iter=200, class_weight="balanced",
        random_state=42,  # для детерминизма
    )

    # OOF buffers
    p_lr_oof = np.full(len(X), np.nan, dtype=np.float64)
    p_gbdt_oof = np.full(len(X), np.nan, dtype=np.float64)

    print(f"Starting OOF training with {args.n_splits} folds...")
    # Convert ts to list for splitter
    ts_list = ts.tolist() if hasattr(ts, 'tolist') else list(ts)
    for fold_idx, (tr_idx, va_idx) in enumerate(splitter.split(ts_list)):
        print(f"Fold {fold_idx + 1}/{args.n_splits}: train={len(tr_idx)}, val={len(va_idx)}")
        # Convert list indices to numpy arrays for indexing
        tr_idx_arr = np.asarray(tr_idx, dtype=np.int64)
        va_idx_arr = np.asarray(va_idx, dtype=np.int64)
        Xtr, ytr = X[tr_idx_arr], y[tr_idx_arr]
        Xva = X[va_idx_arr]

        lr_fold = Pipeline([
            ("scaler", RobustScaler(with_centering=True, with_scaling=True, quantile_range=(25.0, 75.0))),
            ("lr", LogisticRegression(
                C=lr.named_steps["lr"].C, penalty="l2", solver="lbfgs",
                max_iter=lr.named_steps["lr"].max_iter, class_weight="balanced",
                random_state=42,
            ))
        ]).fit(Xtr, ytr)

        if xgb is not None:
            gbdt_fold = xgb.XGBClassifier(
                n_estimators=args.gbdt_max_iter,
                max_depth=args.gbdt_max_depth,
                learning_rate=args.gbdt_lr,
                tree_method="hist",
                device=_XGBOOST_DEVICE,
                eval_metric="logloss",
                random_state=42,
            ).fit(Xtr, ytr)
        else:
            gbdt_fold = HistGradientBoostingClassifier(
                max_depth=gbdt.max_depth,
                learning_rate=gbdt.learning_rate,
                max_iter=gbdt.max_iter,
                l2_regularization=gbdt.l2_regularization,
                random_state=42,
            ).fit(Xtr, ytr)

        # OOF предсказания на validation fold
        p_lr_oof[va_idx_arr] = lr_fold.predict_proba(Xva)[:, 1]
        p_gbdt_oof[va_idx_arr] = gbdt_fold.predict_proba(Xva)[:, 1]

    # Проверка на NaN/Inf в OOF предсказаниях
    ok = np.isfinite(p_lr_oof) & np.isfinite(p_gbdt_oof)
    if not ok.any():
        raise ValueError("All OOF predictions are NaN or Inf")

    Z_oof = np.vstack([p_lr_oof[ok], p_gbdt_oof[ok]]).T.astype(np.float32)
    y_oof = y[ok].astype(np.int64)

    print(f"OOF predictions: {ok.sum()}/{len(X)} valid")

    # meta на OOF
    meta.fit(Z_oof, y_oof)
    p_meta_oof = meta.predict_proba(Z_oof)[:, 1].astype(np.float64)

    # Platt calibrator на OOF meta prob (без leakage) - только если включена калибровка
    cal = None
    if args.calibrate:
        cal = fit_platt_logit(
            p_meta_oof.tolist(), y_oof.tolist(), l2=args.calib_l2, max_iter=args.calib_max_iter
        )
        print(f"Calibrator: a={cal.a:.6f}, b={cal.b:.6f}")

    # финальные base модели на всём датасете
    print("Training final models on full dataset...")
    lr.fit(X, y)
    gbdt.fit(X, y)

    pack = {
        "schema_version": 1,
        "kind": "edge_stack_v1",
        "created_ms": get_ny_time_millis(),
        "feature_cols": feature_cols,
        # Stamp the registry schema version when --feature_schema_ver was used,
        # so ml_confirm gate can publish runtime schema_ver label and the
        # train/runtime hash-mismatch alert has ground truth.
        "feature_schema_ver": feature_schema_ver or "",
        "lr": lr,
        "gbdt": gbdt,
        "meta": meta,
        # IMPORTANT: после P0 fix можно добавить:
        # "feature_transforms": {...},
        # "robust_scaler": {...}  (параметры median/MAD)
        # "session_cfg": {...}, "spread_bucket_edges": [...], "liq_cfg": {...}
    }

    # Добавляем калибровщик в pack, если он есть
    if cal is not None:
        pack["suggested_calibrator"] = {
            "type": "platt_logit",
            "a": float(cal.a),
            "b": float(cal.b),
            "eps": float(cal.eps),
        }

    # Сохранение модели
    os.makedirs(os.path.dirname(args.out_model) or ".", exist_ok=True)
    joblib.dump(pack, args.out_model)
    print(f"Model saved to {args.out_model}")

    # Сохранение калибровщика (если указан путь и калибровка включена)
    if args.out_calib and cal is not None:
        os.makedirs(os.path.dirname(args.out_calib) or ".", exist_ok=True)
        with open(args.out_calib, "w", encoding="utf-8") as f:
            json.dump({"a": cal.a, "b": cal.b, "eps": cal.eps}, f, ensure_ascii=False, indent=2)
        print(f"Calibrator saved to {args.out_calib}")

    # Статистика
    stats = {
        "status": "ok",
        "n": int(len(X)),
        "miss_any": int(miss_n),
        "oof_n": int(ok.sum()),
    }
    if cal is not None:
        stats["cal_a"] = float(cal.a)
        stats["cal_b"] = float(cal.b)
        stats["cal_eps"] = float(cal.eps)
    print(json.dumps(stats, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()

