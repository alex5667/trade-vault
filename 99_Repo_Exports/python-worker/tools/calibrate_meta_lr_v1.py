"""calibrate_meta_lr_v1.py

Калибрует MetaModelLR Platt-логит калибратором на dataset.jsonl.

Запуск (dry-run):
    cd python-worker
    python -m tools.calibrate_meta_lr_v1

Применить (записать в Redis):
    python -m tools.calibrate_meta_lr_v1 --apply

ENV:
    REDIS_URL       = redis://redis-worker-1:6379/0
    CHAMPION_KEY    = cfg:ml_confirm:champion
    CAL_DATASET     = /app/calibration/dataset.jsonl
    CAL_MODEL_PATH  = (если пусто — читает из CHAMPION_KEY.model_path)
    LABEL_COL       = y  (y / y_closed)
    VAL_FRAC        = 0.30
"""

from __future__ import annotations

import argparse
import json
import os
import random
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from core.meta_model_lr import MetaModelLR
from services.ml_calibration import (
    brier_score,
    ece_score,
    fit_platt_logit,
)

_REDIS_URL = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
_CHAMPION_KEY = os.getenv("CHAMPION_KEY", "cfg:ml_confirm:champion")
_DATASET = os.getenv("CAL_DATASET", "/app/calibration/dataset.jsonl")
_LABEL_COL = os.getenv("LABEL_COL", "y")
_VAL_FRAC = float(os.getenv("VAL_FRAC", "0.30") or 0.30)


def _ts() -> str:
    return time.strftime("%Y%m%d_%H%M%S", time.gmtime())


def _auc(probs: list[float], labels: list[int]) -> float:
    pos = [p for p, y in zip(probs, labels) if y == 1]
    neg = [p for p, y in zip(probs, labels) if y == 0]
    if not pos or not neg:
        return 0.5
    hits = sum(1 for a in pos for b in neg if a > b)
    ties = sum(1 for a in pos for b in neg if a == b)
    return (hits + 0.5 * ties) / (len(pos) * len(neg))


def _wr_by_bucket(probs: list[float], labels: list[int]) -> str:
    buckets = [
        (0.00, 0.10, "<0.10"),
        (0.10, 0.20, "0.10-0.20"),
        (0.20, 0.30, "0.20-0.30"),
        (0.30, 0.50, "0.30-0.50"),
        (0.50, 1.01, ">0.50"),
    ]
    lines = []
    for lo, hi, lbl in buckets:
        pairs = [(p, y) for p, y in zip(probs, labels) if lo <= p < hi]
        if pairs:
            wr = sum(y for _, y in pairs) / len(pairs)
            avg_p = sum(p for p, _ in pairs) / len(pairs)
            lines.append(f"  {lbl}: n={len(pairs):4d}, WR={wr:.1%}, avg_p={avg_p:.3f}")
    return "\n".join(lines)


def load_model(model_path: str, redis_url: str, champion_key: str) -> tuple[MetaModelLR, str]:
    if not model_path:
        try:
            import redis as _redis
            r = _redis.Redis.from_url(redis_url, decode_responses=True)
            raw = r.get(champion_key)
            if not raw:
                raise RuntimeError(f"No champion config at {champion_key}")
            cfg = json.loads(str(raw))
            model_path = cfg.get("model_path") or ""
        except Exception as exc:
            raise RuntimeError(f"Cannot read champion config from Redis: {exc}") from exc
    if not model_path or not os.path.exists(model_path):
        raise FileNotFoundError(f"Model not found: {model_path}")
    return MetaModelLR.load(model_path), model_path


def load_dataset(
    dataset_path: str, label_col: str
) -> tuple[list[dict], list[int]]:
    if not os.path.exists(dataset_path):
        raise FileNotFoundError(f"Dataset not found: {dataset_path}")
    indicators: list[dict] = []
    labels: list[int] = []
    with open(dataset_path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            rec = json.loads(line)
            ind = rec.get("indicators", {})
            if not isinstance(ind, dict):
                try:
                    ind = json.loads(ind)
                except Exception:
                    ind = {}
            y_raw = rec.get(label_col, rec.get("y", 0))
            try:
                y = int(y_raw)
            except Exception:
                y = 0
            indicators.append(ind)
            labels.append(y)
    return indicators, labels


def run(args: argparse.Namespace) -> int:
    print(f"[calibrate_meta_lr_v1] start  apply={args.apply}  dataset={args.dataset}")

    # ── load model ──────────────────────────────────────────────────────────
    model, model_path = load_model(args.model_path, args.redis_url, args.champion_key)
    print(f"Model loaded: {model_path}")
    print(f"  features={len(model.features)}  intercept={model.intercept:.4f}  threshold={model.threshold}")

    # ── load dataset and run inference ───────────────────────────────────────
    indicators, labels = load_dataset(args.dataset, args.label_col)
    probs = [model.predict_proba(ind) for ind in indicators]
    n = len(probs)
    pos_n = sum(labels)
    print(f"\nDataset: n={n}  wins={pos_n}  WR={pos_n/n:.1%}")
    print(f"p_raw range: {min(probs):.4f} – {max(probs):.4f}")
    print(f"p_raw mean={sum(probs)/n:.4f}  median={sorted(probs)[n//2]:.4f}")
    auc_raw = _auc(probs, labels)
    print(f"AUC (raw): {auc_raw:.4f}  {'⚠ inverted (<0.5)' if auc_raw < 0.5 else ''}")
    print("\nWR by p_raw bucket:")
    print(_wr_by_bucket(probs, labels))

    # ── train / val split ────────────────────────────────────────────────────
    rng = random.Random(42)
    idx = list(range(n))
    rng.shuffle(idx)
    k = max(2, int(n * (1.0 - _VAL_FRAC)))
    tr_p = [probs[i] for i in idx[:k]]
    tr_y = [labels[i] for i in idx[:k]]
    val_p = [probs[i] for i in idx[k:]]
    val_y = [labels[i] for i in idx[k:]]
    print(f"\nSplit: train={len(tr_p)}  val={len(val_p)}")

    # ── fit PlattLogit ────────────────────────────────────────────────────────
    print("Fitting PlattLogit calibrator …")
    calibrator = fit_platt_logit(tr_p, tr_y)
    print(f"  a={calibrator.a:.4f}  b={calibrator.b:.4f}")
    if calibrator.a < 0:
        print("  ⚠ a < 0: model discrimination is inverted — calibrator corrects direction")

    # ── evaluate on val ──────────────────────────────────────────────────────
    cal_val_p = [calibrator.apply_one(p) for p in val_p]
    brier_raw = brier_score(val_p, val_y)
    brier_cal = brier_score(cal_val_p, val_y)
    ece_raw, _ = ece_score(val_p, val_y, n_bins=10)
    ece_cal, _ = ece_score(cal_val_p, val_y, n_bins=10)
    auc_cal = _auc(cal_val_p, val_y)

    # baseline Brier (constant base-rate prediction)
    base_wr = sum(val_y) / len(val_y)
    brier_base = brier_score([base_wr] * len(val_y), val_y)

    print(f"\nVal set (n={len(val_p)}):")
    print(f"  Brier:  {brier_raw:.4f} → {brier_cal:.4f}  (baseline={brier_base:.4f})")
    print(f"  ECE:    {ece_raw:.4f} → {ece_cal:.4f}")
    print(f"  AUC:    {_auc(val_p, val_y):.4f} → {auc_cal:.4f}")

    # gate check
    if brier_cal > brier_base + 0.01:
        print(f"\n✗ Calibrator Brier ({brier_cal:.4f}) worse than baseline ({brier_base:.4f}) — skip apply")
        return 1
    if ece_cal > 0.15:
        print(f"\n✗ ECE_cal={ece_cal:.4f} > 0.15 — calibration quality insufficient")
        return 1

    # show live signal mapping
    print("\nMapping live p_raw → p_cal:")
    for p_raw in [0.05, 0.10, 0.15, 0.20, 0.25, 0.30]:
        p_cal = calibrator.apply_one(p_raw)
        print(f"  p_raw={p_raw:.2f} → p_cal={p_cal:.4f}")

    if not args.apply:
        print("\n[dry-run] Pass --apply to write to Redis")
        return 0

    # ── write to Redis ────────────────────────────────────────────────────────
    ts = _ts()
    run_id = f"cal_meta_lr_v1_{ts}"
    cal_dict: dict = calibrator.to_dict()
    cal_dict.update(
        {
            "schema_version": 1,
            "run_id": run_id,
            "source": "calibrate_meta_lr_v1",
            "created_ms": int(time.time() * 1000),
            "model_path": model_path,
            "n_total": n,
            "n_train": len(tr_p),
            "n_val": len(val_p),
            "train_report": {
                "brier_raw": round(brier_raw, 6),
                "brier_cal": round(brier_cal, 6),
                "ece_raw": round(ece_raw, 6),
                "ece_cal": round(ece_cal, 6),
                "auc_cal": round(auc_cal, 6),
                "base_wr": round(base_wr, 4),
            },
        }
    )

    try:
        import redis as _redis
        r = _redis.Redis.from_url(args.redis_url, decode_responses=True)

        # 1) Update champion config — embed calibrator inline + enable calibrate_p_edge
        raw = r.get(args.champion_key)
        if not raw:
            print("✗ Champion key missing in Redis")
            return 1
        cfg = json.loads(str(raw))
        cfg["calibrator"] = cal_dict
        cfg["calibrate_p_edge"] = True
        cfg["calib_updated_ms"] = int(time.time() * 1000)
        r.set(
            args.champion_key,
            json.dumps(cfg, ensure_ascii=False, separators=(",", ":")),
        )
        print(f"\n✓ Champion config updated: {args.champion_key}")
        print(f"  calibrator embedded  calibrate_p_edge=True  run_id={run_id}")

        # 2) Optional: also write standalone calibrator key
        if args.cal_key:
            r.set(
                args.cal_key,
                json.dumps(cal_dict, ensure_ascii=False, separators=(",", ":")),
            )
            print(f"✓ Standalone calibrator key: {args.cal_key}")

    except Exception as exc:
        print(f"✗ Redis write failed: {exc}")
        return 1

    print(f"\nDone. run_id={run_id}")
    print("Gate will reload calibrator within cache TTL (~30s).")
    return 0


def _build_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(description="Calibrate MetaModelLR with PlattLogit")
    ap.add_argument("--redis-url", default=_REDIS_URL)
    ap.add_argument("--champion-key", default=_CHAMPION_KEY)
    ap.add_argument("--cal-key", default=os.getenv("CAL_KEY", "cfg:ml_confirm:v14_of:calibrator"))
    ap.add_argument("--dataset", default=_DATASET)
    ap.add_argument("--model-path", default=os.getenv("CAL_MODEL_PATH", ""))
    ap.add_argument("--label-col", default=_LABEL_COL)
    ap.add_argument("--apply", action="store_true")
    return ap


if __name__ == "__main__":
    ap = _build_parser()
    args = ap.parse_args()
    sys.exit(run(args))
