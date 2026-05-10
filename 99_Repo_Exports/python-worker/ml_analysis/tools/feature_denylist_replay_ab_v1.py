from __future__ import annotations

"""Replay/AB gate for feature-denylist proposals.

Goal
- Given a denylist proposal manifest (pending_ab), run a minimal deterministic AB check:
  A) full feature set (baseline)
  B) full minus proposed denylist_after (stable)

- Train the same model on the same time-split and compare quality.
- Write an AB report (json + md) and update manifest status:
    pending_ab -> ab_done (gate_pass=1)
    pending_ab -> ab_failed (gate_pass=0)

Design constraints
- Deterministic (seed + time-based split + fixed purge gap)
- Low-latency / nightly friendly (caps for max rows)
- Non-invasive (does not apply patches)

Exit codes
- 0: gate passed
- 2: gate failed or error

"""


import argparse
import json
import math
import os
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import numpy as np

UTC = UTC


def _utc_now_iso() -> str:
    return datetime.now(tz=UTC).isoformat()


def _read_json(p: Path) -> dict[str, Any]:
    return json.loads(p.read_text(encoding="utf-8"))


def _write_json(p: Path, obj: dict[str, Any]) -> None:
    p.write_text(json.dumps(obj, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _sigmoid(x: np.ndarray) -> np.ndarray:
    return 1.0 / (1.0 + np.exp(-x))


@dataclass(frozen=True)
class Split:
    train_idx: np.ndarray
    val_idx: np.ndarray


def time_split(ts_ms: np.ndarray, val_frac: float, purge_ms: int) -> Split:
    """Time-based split with a purge gap (leakage guard).

    - Sort is assumed done upstream.
    - Validation is the most recent val_frac.
    - Purge removes samples from train that are within purge_ms before val start.

    """
    n = int(len(ts_ms))
    if n < 10:
        return Split(train_idx=np.array([], dtype=np.int64), val_idx=np.array([], dtype=np.int64))

    cut = int(math.floor((1.0 - float(val_frac)) * n))
    cut = max(1, min(n - 1, cut))

    val_start_ts = int(ts_ms[cut])
    purge_start_ts = val_start_ts - int(purge_ms)

    idx = np.arange(n, dtype=np.int64)
    train_mask = ts_ms < purge_start_ts
    val_mask = ts_ms >= val_start_ts

    return Split(train_idx=idx[train_mask], val_idx=idx[val_mask])


def _ensure_sklearn() -> None:
    try:
        import sklearn  # noqa: F401
    except Exception as e:
        raise SystemExit(f"sklearn is required for this tool: {e}")


def _fit_model(kind: str, X: np.ndarray, y: np.ndarray, seed: int):
    _ensure_sklearn()
    if kind == "lr":
        from sklearn.linear_model import LogisticRegression

        # Deterministic settings; keep it simple and stable.
        m = LogisticRegression(
            max_iter=400,
            solver="lbfgs",
            n_jobs=1,
        )
        m.fit(X, y)
        return m

    if kind == "gbdt":
        from sklearn.ensemble import HistGradientBoostingClassifier

        m = HistGradientBoostingClassifier(
            learning_rate=0.06,
            max_depth=3,
            max_leaf_nodes=31,
            min_samples_leaf=80,
            l2_regularization=1e-4,
            random_state=int(seed),
        )
        m.fit(X, y)
        return m

    raise SystemExit(f"unknown model kind: {kind}")


def _predict_proba(model, X: np.ndarray) -> np.ndarray:
    if hasattr(model, "predict_proba"):
        p = model.predict_proba(X)
        return p[:, 1].astype(np.float64)
    if hasattr(model, "decision_function"):
        s = model.decision_function(X)
        return _sigmoid(np.asarray(s, dtype=np.float64))
    raise SystemExit("model has no predict_proba/decision_function")


def _auc(y: np.ndarray, p: np.ndarray) -> float | None:
    _ensure_sklearn()
    from sklearn.metrics import roc_auc_score

    if len(np.unique(y)) < 2:
        return None
    return float(roc_auc_score(y, p))


def _brier(y: np.ndarray, p: np.ndarray) -> float:
    y = y.astype(np.float64)
    return float(np.mean((p - y) ** 2))


def _logloss(y: np.ndarray, p: np.ndarray) -> float | None:
    _ensure_sklearn()
    from sklearn.metrics import log_loss

    eps = 1e-12
    p2 = np.clip(p, eps, 1.0 - eps)
    try:
        return float(log_loss(y, p2, labels=[0, 1]))
    except Exception:
        return None


def _mcc(y: np.ndarray, p: np.ndarray, thr: float = 0.5) -> float | None:
    _ensure_sklearn()
    from sklearn.metrics import matthews_corrcoef

    if len(np.unique(y)) < 2:
        return None
    yhat = (p >= float(thr)).astype(np.int8)
    return float(matthews_corrcoef(y, yhat))


def _utc_hour(ts_ms: int) -> int:
    # epoch ms -> hour UTC
    return int(datetime.fromtimestamp(float(ts_ms) / 1000.0, tz=UTC).hour)


def _load_df(data_path: str):
    import pandas as pd

    if data_path.endswith(".parquet"):
        return pd.read_parquet(data_path)
    if data_path.endswith(".csv"):
        return pd.read_csv(data_path)
    if data_path.endswith(".ndjson") or data_path.endswith(".jsonl"):
        rows: list[dict[str, Any]] = []
        with open(data_path, encoding="utf-8") as f:
            for line in f:
                s = line.strip()
                if not s:
                    continue
                rows.append(json.loads(s))
        return pd.DataFrame(rows)
    raise SystemExit("Unsupported data format (use .parquet/.csv/.ndjson/.jsonl)")


def _expand_indicators_if_needed(df, feature_names: Sequence[str], column_names: Sequence[str]):
    import pandas as pd

    if not any(c not in df.columns for c in column_names):
        return df

    if "indicators" not in df.columns:
        missing = [c for c in column_names if c not in df.columns]
        raise SystemExit(f"dataset missing feature columns and no indicators to expand: {missing[:8]} (n={len(missing)})")

    ind_df = pd.json_normalize(df["indicators"].tolist()).fillna(0.0)

    have = [f for f in feature_names if f in ind_df.columns]
    ind_df = ind_df.reindex(columns=have).fillna(0.0)

    rename = {feature_names[i]: column_names[i] for i in range(len(feature_names)) if feature_names[i] in ind_df.columns}
    ind_df = ind_df.rename(columns=rename)

    for c in column_names:
        if c not in ind_df.columns:
            ind_df[c] = 0.0

    out = pd.concat([df.drop(columns=["indicators"], errors="ignore"), ind_df[list(column_names)]], axis=1)
    return out


def _schema_from_registry(schema_ver: str) -> tuple[list[str], list[str]] | None:
    try:
        from core.feature_registry import FeatureRegistry  # type: ignore

        s = FeatureRegistry().get_schema_info(str(schema_ver))
        return list(s.feature_names), list(s.column_names)
    except Exception:
        return None


def _filter_by_denylist(
    feature_names: Sequence[str],
    column_names: Sequence[str],
    deny_num: Sequence[str],
    deny_bool: Sequence[str],
) -> tuple[list[str], list[str]]:
    dnum = set(map(str, deny_num or []))
    dbool = set(map(str, deny_bool or []))

    out_fn: list[str] = []
    out_cn: list[str] = []
    for fn, cn in zip(feature_names, column_names):
        s = str(fn)
        if s.startswith("n:"):
            k = s.split(":", 1)[1]
            if k in dnum:
                continue
        if s.startswith("b:"):
            k = s.split(":", 1)[1]
            if k in dbool:
                continue
        out_fn.append(str(fn))
        out_cn.append(str(cn))
    return out_fn, out_cn


def _group_auc(y: np.ndarray, p: np.ndarray, mask: np.ndarray) -> float | None:
    y2 = y[mask]
    p2 = p[mask]
    if len(y2) < 10:
        return None
    return _auc(y2, p2)


def _group_metrics(
    y: np.ndarray,
    p: np.ndarray,
    regimes: np.ndarray,
    hours: np.ndarray,
    min_group_rows: int,
) -> dict[str, Any]:
    out: dict[str, Any] = {"regime": {}, "hour": {}}

    # regime
    for g in sorted({str(x) for x in regimes.tolist()}):
        m = regimes.astype(str) == g
        n = int(np.sum(m))
        if n < int(min_group_rows):
            out["regime"][g] = {"n": n, "auc": None, "brier": None}
            continue
        out["regime"][g] = {"n": n, "auc": _group_auc(y, p, m), "brier": float(np.mean((p[m] - y[m]) ** 2))}

    # hour 0..23
    for h in range(24):
        m = hours.astype(int) == int(h)
        n = int(np.sum(m))
        if n < int(min_group_rows):
            out["hour"][str(h)] = {"n": n, "auc": None, "brier": None}
            continue
        out["hour"][str(h)] = {"n": n, "auc": _group_auc(y, p, m), "brier": float(np.mean((p[m] - y[m]) ** 2))}

    return out


def _worst_auc_drop(groups_a: dict[str, Any], groups_b: dict[str, Any]) -> float:
    worst = 0.0
    for k, ga in groups_a.items():
        gb = groups_b.get(k) or {}
        auc_a = ga.get("auc")
        auc_b = gb.get("auc")
        if auc_a is None or auc_b is None:
            continue
        drop = float(auc_a) - float(auc_b)
        if drop > worst:
            worst = drop
    return float(worst)


def main(argv: Sequence[str] | None = None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--manifest", required=True)
    ap.add_argument("--out_dir", default="")

    ap.add_argument("--model", default="", choices=["", "gbdt", "lr"], help="default: from fs summary.json or gbdt")
    ap.add_argument("--label_col", default="label")
    ap.add_argument("--ts_col", default="ts_ms")
    ap.add_argument("--regime_col", default="scenario_v4")

    ap.add_argument("--val_frac", type=float, default=float(os.getenv("FEATURE_DENYLIST_AB_VAL_FRAC", "0.2")))
    ap.add_argument("--purge_ms", type=int, default=int(os.getenv("FEATURE_DENYLIST_AB_PURGE_MS", "300000")))
    ap.add_argument("--max_val_rows", type=int, default=int(os.getenv("FEATURE_DENYLIST_AB_MAX_VAL_ROWS", "250000")))
    ap.add_argument("--seed", type=int, default=int(os.getenv("FEATURE_DENYLIST_AB_SEED", "7")))
    ap.add_argument("--min_group_rows", type=int, default=int(os.getenv("FEATURE_DENYLIST_AB_MIN_GROUP_ROWS", "1500")))

    # gate thresholds (fail-closed)
    ap.add_argument("--auc_drop_max", type=float, default=float(os.getenv("FEATURE_DENYLIST_AB_AUC_DROP_MAX", "0.0025")))
    ap.add_argument("--brier_increase_max", type=float, default=float(os.getenv("FEATURE_DENYLIST_AB_BRIER_INC_MAX", "0.00025")))
    ap.add_argument("--mcc_drop_max", type=float, default=float(os.getenv("FEATURE_DENYLIST_AB_MCC_DROP_MAX", "0.01")))
    ap.add_argument(
        "--worst_group_auc_drop_max",
        type=float,
        default=float(os.getenv("FEATURE_DENYLIST_AB_WORST_GROUP_AUC_DROP_MAX", "0.02")),
    )

    args = ap.parse_args(list(argv) if argv is not None else None)

    mp = Path(args.manifest).expanduser().resolve()
    if not mp.exists():
        print(f"manifest not found: {mp}")
        return 2

    m = _read_json(mp)
    if not isinstance(m, dict) or (m.get("kind") != "feature_denylist_proposal"):
        print("bad manifest format/kind")
        return 2

    if (m.get("status")) not in ("pending_ab", "ab_failed", "ab_done"):
        print(f"manifest status not eligible for AB: {m.get('status')}")
        return 2

    inputs = m.get("inputs") or {}
    fs_run_dir = (inputs.get("fs_run_dir") or "")
    stab_path = (inputs.get("stability_table") or "")

    fs_dir = Path(fs_run_dir).expanduser().resolve() if fs_run_dir else (mp.parent.parent if mp.parent.name == "proposals" else mp.parent)
    if not fs_dir.exists():
        print(f"fs_run_dir not found: {fs_dir}")
        return 2

    # Find summary.json
    summary_path = fs_dir / "summary.json"
    if not summary_path.exists() and stab_path:
        sp = Path(stab_path).expanduser().resolve()
        if sp.exists() and (sp.parent / "summary.json").exists():
            summary_path = sp.parent / "summary.json"

    if not summary_path.exists():
        print(f"summary.json not found in fs_run_dir: {fs_dir}")
        return 2

    fs_sum = _read_json(summary_path)
    data_path = (fs_sum.get("data_path") or "")
    meta_json = (fs_sum.get("meta_json") or "")

    if not data_path:
        print("summary.json missing data_path")
        return 2

    # Output directory
    out_dir = Path(args.out_dir).expanduser().resolve() if str(args.out_dir).strip() else (fs_dir / "proposals" / "ab_runs")
    out_dir.mkdir(parents=True, exist_ok=True)

    proposal_hash = (m.get("proposal_hash") or "")
    tag = proposal_hash[:12] if proposal_hash else mp.stem.replace("denylist_proposal_", "")

    # Baseline schema lists: prefer registry (v5_of), otherwise meta.json
    schema_full = _schema_from_registry("v5_of")

    if schema_full is None:
        if not meta_json:
            print("no FeatureRegistry(v5_of) and summary.json has empty meta_json")
            return 2
        mj = _read_json(Path(meta_json).expanduser().resolve())
        feature_names_full = list(mj.get("feature_names") or [])
        column_names_full = list(mj.get("column_names") or [])
        schema_ver = str(mj.get("ver") or fs_sum.get("schema_ver") or "")
    else:
        feature_names_full, column_names_full = schema_full
        schema_ver = "v5_of"

    if not feature_names_full or not column_names_full or len(feature_names_full) != len(column_names_full):
        print("bad feature_names/column_names for baseline schema")
        return 2

    deny = m.get("denylist_after") or {}
    deny_num = list(deny.get("deny_num") or [])
    deny_bool = list(deny.get("deny_bool") or [])

    feature_names_stable, column_names_stable = _filter_by_denylist(
        feature_names_full,
        column_names_full,
        deny_num=deny_num,
        deny_bool=deny_bool,
    )

    # Load dataset
    df = _load_df(data_path)

    # regime fallback (some datasets name it 'scenario')
    if str(args.regime_col) not in df.columns and "scenario" in df.columns:
        args.regime_col = "scenario"

    # Expand indicators to wide columns if needed
    df = _expand_indicators_if_needed(df, feature_names_full, column_names_full)

    # Column checks
    base_cols = [str(args.label_col), str(args.ts_col), str(args.regime_col)]
    need_cols = base_cols + list(column_names_full)
    miss = [c for c in need_cols if c not in df.columns]
    if miss:
        print(f"dataset missing required columns: {miss[:8]} (n={len(miss)})")
        return 2

    # Deterministic sort
    sort_cols = [str(args.ts_col)]
    if "sid" in df.columns:
        sort_cols.append("sid")
    df = df.sort_values(by=sort_cols).reset_index(drop=True)

    # Clean
    df[str(args.label_col)] = df[str(args.label_col)].astype(int)
    df[str(args.ts_col)] = df[str(args.ts_col)].astype("int64")

    # Fill NaN/Inf for features
    df[column_names_full] = df[column_names_full].replace([np.inf, -np.inf], np.nan).fillna(0.0)

    ts = df[str(args.ts_col)].to_numpy(dtype=np.int64)
    y = df[str(args.label_col)].to_numpy(dtype=np.int8)

    split = time_split(ts, val_frac=float(args.val_frac), purge_ms=int(args.purge_ms))
    # Nightly-friendly lower bounds; fail-closed but still update manifest/report.
    min_train = 1000
    min_val = 500
    if len(split.train_idx) < min_train or len(split.val_idx) < min_val:
        # Update manifest so exporter shows a clear AB-fail reason.
        m2 = dict(m)
        m2["status"] = "ab_failed"
        m2["ab_finished_utc"] = _utc_now_iso()
        m2["ab"] = {
            "ts_utc": _utc_now_iso(),
            "gate_pass": 0,
            "reasons": [f"not_enough_data_after_split train={len(split.train_idx)} val={len(split.val_idx)}"],
        }
        _write_json(mp, m2)
        print(f"not enough data after split: train={len(split.train_idx)} val={len(split.val_idx)}")
        return 2

    # Build matrices
    X_full = df[column_names_full].to_numpy(dtype=np.float64)
    X_stable = df[column_names_stable].to_numpy(dtype=np.float64)

    Xf_tr, yf_tr = X_full[split.train_idx], y[split.train_idx]
    Xs_tr, ys_tr = X_stable[split.train_idx], y[split.train_idx]

    Xf_va, yf_va = X_full[split.val_idx], y[split.val_idx]
    Xs_va, ys_va = X_stable[split.val_idx], y[split.val_idx]

    # Optional cap for faster nightly
    max_val = int(args.max_val_rows)
    if max_val > 0 and len(yf_va) > max_val:
        rng = np.random.default_rng(int(args.seed))
        idx = rng.choice(len(yf_va), size=max_val, replace=False)
        Xf_va, yf_va = Xf_va[idx], yf_va[idx]
        Xs_va, ys_va = Xs_va[idx], ys_va[idx]
        ts_va = ts[split.val_idx][idx]
        regimes_va = df.loc[split.val_idx, str(args.regime_col)].to_numpy()[idx]
    else:
        ts_va = ts[split.val_idx]
        regimes_va = df.loc[split.val_idx, str(args.regime_col)].to_numpy()

    hours_va = np.array([_utc_hour(int(t)) for t in ts_va], dtype=np.int16)

    # Decide model kind
    model_kind = str(args.model).strip() or (fs_sum.get("model") or "").strip() or "gbdt"
    if model_kind not in ("gbdt", "lr"):
        model_kind = "gbdt"

    # Fit + eval A
    mf = _fit_model(model_kind, Xf_tr, yf_tr, seed=int(args.seed))
    pf = _predict_proba(mf, Xf_va)

    # Fit + eval B
    ms = _fit_model(model_kind, Xs_tr, ys_tr, seed=int(args.seed))
    ps = _predict_proba(ms, Xs_va)

    auc_f = _auc(yf_va, pf)
    auc_s = _auc(ys_va, ps)
    brier_f = _brier(yf_va, pf)
    brier_s = _brier(ys_va, ps)

    ll_f = _logloss(yf_va, pf)
    ll_s = _logloss(ys_va, ps)

    mcc_f = _mcc(yf_va, pf)
    mcc_s = _mcc(ys_va, ps)

    groups_f = _group_metrics(yf_va, pf, regimes_va, hours_va, min_group_rows=int(args.min_group_rows))
    groups_s = _group_metrics(ys_va, ps, regimes_va, hours_va, min_group_rows=int(args.min_group_rows))

    worst_regime_auc_drop = _worst_auc_drop(groups_f["regime"], groups_s["regime"])
    worst_hour_auc_drop = _worst_auc_drop(groups_f["hour"], groups_s["hour"])

    # deltas (positive is worse for stable)
    auc_drop = (float(auc_f) - float(auc_s)) if (auc_f is not None and auc_s is not None) else None
    brier_inc = float(brier_s) - float(brier_f)
    mcc_drop = (float(mcc_f) - float(mcc_s)) if (mcc_f is not None and mcc_s is not None) else None

    gate = {
        "auc_drop_max": float(args.auc_drop_max),
        "brier_increase_max": float(args.brier_increase_max),
        "mcc_drop_max": float(args.mcc_drop_max),
        "worst_group_auc_drop_max": float(args.worst_group_auc_drop_max),
        "min_group_rows": int(args.min_group_rows),
    }

    gate_pass = True
    reasons: list[str] = []

    if auc_drop is None:
        gate_pass = False
        reasons.append("auc_undefined")
    else:
        if float(auc_drop) > float(args.auc_drop_max):
            gate_pass = False
            reasons.append(f"auc_drop={float(auc_drop):.6f} > {float(args.auc_drop_max):.6f}")

    if float(brier_inc) > float(args.brier_increase_max):
        gate_pass = False
        reasons.append(f"brier_inc={float(brier_inc):.8f} > {float(args.brier_increase_max):.8f}")

    if mcc_drop is None:
        # non-fatal; MCC can be undefined for small/degenerate y
        pass
    else:
        if float(mcc_drop) > float(args.mcc_drop_max):
            gate_pass = False
            reasons.append(f"mcc_drop={float(mcc_drop):.6f} > {float(args.mcc_drop_max):.6f}")

    if float(worst_regime_auc_drop) > float(args.worst_group_auc_drop_max):
        gate_pass = False
        reasons.append(f"worst_regime_auc_drop={float(worst_regime_auc_drop):.6f} > {float(args.worst_group_auc_drop_max):.6f}")

    if float(worst_hour_auc_drop) > float(args.worst_group_auc_drop_max):
        gate_pass = False
        reasons.append(f"worst_hour_auc_drop={float(worst_hour_auc_drop):.6f} > {float(args.worst_group_auc_drop_max):.6f}")

    report = {
        "kind": "feature_denylist_ab_report_v1",
        "ts_utc": _utc_now_iso(),
        "proposal_hash": proposal_hash,
        "schema_baseline": schema_ver,
        "model": model_kind,
        "data_path": str(data_path),
        "meta_json": str(meta_json),
        "split": {
            "val_frac": float(args.val_frac),
            "purge_ms": int(args.purge_ms),
            "n_total": int(len(df)),
            "n_train": int(len(split.train_idx)),
            "n_val": int(len(split.val_idx)),
            "n_val_used": int(len(yf_va)),
        },
        "features": {
            "n_full": int(len(feature_names_full)),
            "n_stable": int(len(feature_names_stable)),
            "deny_num": deny_num,
            "deny_bool": deny_bool,
        },
        "metrics": {
            "full": {
                "auc": auc_f,
                "brier": float(brier_f),
                "logloss": ll_f,
                "mcc@0.5": mcc_f,
            },
            "stable": {
                "auc": auc_s,
                "brier": float(brier_s),
                "logloss": ll_s,
                "mcc@0.5": mcc_s,
            },
            "delta": {
                "auc_drop": auc_drop,
                "brier_increase": float(brier_inc),
                "mcc_drop": mcc_drop,
                "worst_regime_auc_drop": float(worst_regime_auc_drop),
                "worst_hour_auc_drop": float(worst_hour_auc_drop),
            },
            "by_group": {
                "full": groups_f,
                "stable": groups_s,
            },
        },
        "gate": {"pass": int(gate_pass), "reasons": reasons, "thresholds": gate},
    }

    rep_json = out_dir / f"ab_report_{tag}.json"
    rep_md = out_dir / f"ab_report_{tag}.md"

    _write_json(rep_json, report)

    # markdown summary (human quick scan)
    with open(rep_md, "w", encoding="utf-8") as f:
        f.write("# Feature denylist AB report\n\n")
        f.write(f"proposal_hash: `{proposal_hash}`\n\n")
        f.write(f"model: **{model_kind}**\n\n")
        f.write(f"gate: **{'PASS' if gate_pass else 'FAIL'}**\n\n")
        if reasons:
            f.write("reasons:\n")
            for r in reasons[:20]:
                f.write(f"- {r}\n")
            f.write("\n")
        f.write("## Metrics (val)\n\n")
        f.write("|variant|auc|brier|logloss|mcc@0.5|\n|---|---:|---:|---:|---:|\n")
        f.write(
            f"|full|{auc_f if auc_f is not None else 'NA'}|{brier_f:.6f}|{ll_f if ll_f is not None else 'NA'}|{mcc_f if mcc_f is not None else 'NA'}|\n"
        )
        f.write(
            f"|stable|{auc_s if auc_s is not None else 'NA'}|{brier_s:.6f}|{ll_s if ll_s is not None else 'NA'}|{mcc_s if mcc_s is not None else 'NA'}|\n"
        )
        f.write("\n")
        f.write("## Deltas (stable vs full)\n\n")
        f.write("|metric|value|threshold|\n|---|---:|---:|\n")
        f.write(f"|auc_drop|{auc_drop if auc_drop is not None else 'NA'}|{gate['auc_drop_max']}|\n")
        f.write(f"|brier_increase|{brier_inc:.8f}|{gate['brier_increase_max']}|\n")
        f.write(f"|mcc_drop|{mcc_drop if mcc_drop is not None else 'NA'}|{gate['mcc_drop_max']}|\n")
        f.write(f"|worst_regime_auc_drop|{worst_regime_auc_drop:.6f}|{gate['worst_group_auc_drop_max']}|\n")
        f.write(f"|worst_hour_auc_drop|{worst_hour_auc_drop:.6f}|{gate['worst_group_auc_drop_max']}|\n")

    # Update manifest
    m2 = dict(m)
    m2["status"] = "ab_done" if gate_pass else "ab_failed"
    m2["ab_finished_utc"] = _utc_now_iso()
    m2["ab"] = {
        "ts_utc": report["ts_utc"],
        "gate_pass": int(gate_pass),
        "reasons": reasons,
        "report_json": str(rep_json),
        "report_md": str(rep_md),
        "model": model_kind,
        "metrics": report["metrics"],
        "thresholds": gate,
    }

    _write_json(mp, m2)

    # Print compact line for logs
    print(
        json.dumps(
            {
                "gate_pass": int(gate_pass),
                "status": m2["status"],
                "proposal_hash": proposal_hash,
                "report_json": str(rep_json),
                "auc_drop": auc_drop,
                "brier_increase": float(brier_inc),
            },
            ensure_ascii=False,
        )
    )

    return 0 if gate_pass else 2


if __name__ == "__main__":
    raise SystemExit(main())
