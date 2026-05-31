#!/usr/bin/env python3
"""replay_validate_per_regime_blend.py — P2.7: per-regime ensemble replay harness.

Loads a saved v15_lgbm joblib pack (with per_regime sub-models) and a holdout
dataset (NDJSON, same schema as build_dataset_v5_tb_v15of output), then runs
blend_predictions for every sample and emits per-regime validation metrics.

Output JSON schema:
  {
    "run_ts_ms": int,
    "model_path": str,
    "dataset_path": str,
    "n_samples": int,
    "regimes": {
      "<regime>": {
        "n": int,
        "n_pos": int,
        "wr": float,
        "avg_r": float,
        "auc": float,
        "brier": float,
        "ece": float,
        "blend_source": "regime_model" | "global_only",
        "weight_regime_mean": float,
      },
      ...
    },
    "global": { "n": int, "wr": float, "avg_r": float, "auc": float, "brier": float, "ece": float },
    "gates": [{"name": str, "ok": bool, "value": float, "threshold": float}, ...],
    "all_gates_ok": bool,
  }

Usage:
  python -m tools.replay_validate_per_regime_blend \\
    --model /var/lib/trade/ml_models/scorer_v15_lgbm/scorer_v15_lgbm.joblib \\
    --dataset /tmp/ml_dataset_tb_v15of.ndjson \\
    --out /tmp/replay_report.json \\
    [--min-samples-per-regime 20]
    [--min-auc 0.52]
    [--max-ece 0.10]
"""
from __future__ import annotations

import argparse
import json
import logging
import math
import os
import sys
import time
from collections import defaultdict
from typing import Any

log = logging.getLogger("replay_validate_per_regime_blend")
if not log.handlers:
    h = logging.StreamHandler(sys.stdout)
    h.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s %(message)s"))
    log.addHandler(h)
log.setLevel(os.getenv("LOG_LEVEL", "INFO").upper())

_GATE_MIN_AUC: float = 0.52
_GATE_MAX_ECE: float = 0.10


# ── metrics helpers ───────────────────────────────────────────────────────────

def _ece(labels: list[int], probs: list[float], n_bins: int = 10) -> float:
    """Expected Calibration Error (equal-width bins)."""
    if not labels:
        return float("nan")
    n = len(labels)
    bins: dict[int, list] = defaultdict(lambda: [[], []])
    for y, p in zip(labels, probs):
        b = min(int(p * n_bins), n_bins - 1)
        bins[b][0].append(y)
        bins[b][1].append(p)
    ece = 0.0
    for ys, ps in bins.values():
        if not ys:
            continue
        ece += len(ys) / n * abs(sum(ys) / len(ys) - sum(ps) / len(ps))
    return ece


def _brier(labels: list[int], probs: list[float]) -> float:
    if not labels:
        return float("nan")
    return sum((p - y) ** 2 for y, p in zip(labels, probs)) / len(labels)


def _auc_approx(labels: list[int], scores: list[float]) -> float:
    """Mann-Whitney U approximation of ROC-AUC (no sklearn required).

    Sort ascending; for each positive count negatives already seen (lower score).
    """
    if not labels or len(set(labels)) < 2:
        return float("nan")
    n_pos = sum(labels)
    n_neg = len(labels) - n_pos
    if n_pos == 0 or n_neg == 0:
        return float("nan")
    pairs = sorted(zip(scores, labels), key=lambda x: x[0])
    rank_sum = 0
    cum_neg = 0
    for _, y in pairs:
        if y == 0:
            cum_neg += 1
        else:
            rank_sum += cum_neg
    return rank_sum / (n_pos * n_neg)


# ── loading ───────────────────────────────────────────────────────────────────

def load_model(path: str) -> dict[str, Any]:
    import joblib
    pack = joblib.load(path)
    assert "model" in pack and "feature_cols" in pack, "not a v15_lgbm pack"
    return pack


def load_dataset(path: str) -> list[dict]:
    records = []
    with open(path, encoding="utf-8") as f:
        for lineno, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            try:
                records.append(json.loads(line))
            except json.JSONDecodeError as e:
                log.warning("line %d JSON error: %s", lineno, e)
    return records


# ── replay ────────────────────────────────────────────────────────────────────

def replay(
    pack: dict[str, Any],
    records: list[dict],
) -> dict[str, Any]:
    import numpy as np

    model = pack["model"]
    calibrator = pack.get("calibrator")
    feature_cols: list[str] = pack["feature_cols"]
    per_regime: dict[str, Any] = pack.get("per_regime") or {}

    # sort by time for determinism
    records = sorted(records, key=lambda r: int(r.get("ts_ms") or 0))

    global_labels: list[int] = []
    global_probs: list[float] = []
    global_r: list[float] = []

    by_regime: dict[str, dict] = defaultdict(lambda: {
        "labels": [], "probs": [], "r": [], "weight_regime": [],
    })

    for rec in records:
        feats = rec.get("features")
        if not isinstance(feats, dict) or not feats:
            continue
        label = int(rec.get("hit", 0))
        r_val = float(rec.get("r", 0.0) or 0.0)
        regime = (rec.get("regime") or "na").lower()

        X_row = [float(feats.get(k, 0.0)) for k in feature_cols]
        X_arr = np.array([X_row])

        # Global prediction
        raw_p = float(model.predict_proba(X_arr)[0][1])
        if calibrator is not None:
            try:
                global_p = float(calibrator.predict([raw_p])[0])
            except Exception:
                global_p = raw_p
        else:
            global_p = raw_p

        # Blend with per-regime model if available
        sub = per_regime.get(regime)
        if sub:
            p_sub = float(sub["model"].predict_proba(X_arr)[0][1])
            try:
                p_sub_cal = float(sub["calibrator"].predict([p_sub])[0])
            except Exception:
                p_sub_cal = p_sub
            auc_sub = sub["oof_auc"]
            n_sub = sub["n"]
            quality = max(0.0, (auc_sub - 0.50) * 2.0)
            sample_factor = min(1.0, n_sub / 200.0)
            w_regime = quality * sample_factor
            blended = (1.0 - w_regime) * global_p + w_regime * p_sub_cal
            blend_source = "regime_model"
        else:
            blended = global_p
            w_regime = 0.0
            blend_source = "global_only"

        global_labels.append(label)
        global_probs.append(blended)
        global_r.append(r_val)

        rg_data = by_regime[regime]
        rg_data["labels"].append(label)
        rg_data["probs"].append(blended)
        rg_data["r"].append(r_val)
        rg_data["weight_regime"].append(w_regime)
        rg_data.setdefault("blend_source", blend_source)

    # Global metrics
    def _metrics(labels, probs, r_list):
        n = len(labels)
        if n == 0:
            return {"n": 0, "n_pos": 0, "wr": float("nan"), "avg_r": float("nan"),
                    "auc": float("nan"), "brier": float("nan"), "ece": float("nan")}
        n_pos = sum(labels)
        wr = n_pos / n if n > 0 else float("nan")
        avg_r = sum(r_list) / n if r_list else float("nan")
        return {
            "n": n, "n_pos": n_pos,
            "wr": round(wr, 4),
            "avg_r": round(avg_r, 4),
            "auc": round(_auc_approx(labels, probs), 4),
            "brier": round(_brier(labels, probs), 4),
            "ece": round(_ece(labels, probs), 4),
        }

    global_m = _metrics(global_labels, global_probs, global_r)

    regimes_out: dict[str, dict] = {}
    for rg, data in by_regime.items():
        m = _metrics(data["labels"], data["probs"], data["r"])
        m["blend_source"] = data.get("blend_source", "global_only")
        ws = data.get("weight_regime", [])
        m["weight_regime_mean"] = round(sum(ws) / len(ws), 4) if ws else 0.0
        regimes_out[rg] = m

    return {"global": global_m, "regimes": regimes_out}


# ── gates ─────────────────────────────────────────────────────────────────────

def evaluate_replay_gates(
    result: dict[str, Any],
    min_auc: float,
    max_ece: float,
    min_samples_per_regime: int,
) -> tuple[list[dict], bool]:
    gates = []

    def add(name, ok, val, thr):
        gates.append({"name": name, "ok": bool(ok), "value": val, "threshold": thr})

    g = result["global"]
    add("global_auc_min", not math.isnan(g["auc"]) and g["auc"] >= min_auc, g["auc"], min_auc)
    add("global_ece_max", not math.isnan(g["ece"]) and g["ece"] <= max_ece, g["ece"], max_ece)

    for rg, rm in result["regimes"].items():
        if rm["n"] < min_samples_per_regime:
            continue
        add(f"regime_{rg}_auc_min",
            not math.isnan(rm["auc"]) and rm["auc"] >= min_auc,
            rm["auc"], min_auc)
        add(f"regime_{rg}_ece_max",
            not math.isnan(rm["ece"]) and rm["ece"] <= max_ece,
            rm["ece"], max_ece)

    all_ok = all(g_["ok"] for g_ in gates)
    return gates, all_ok


# ── main ──────────────────────────────────────────────────────────────────────

def main() -> int:
    ap = argparse.ArgumentParser(description="Per-regime ensemble replay harness (P2.7).")
    ap.add_argument("--model", required=True, help="Path to v15_lgbm joblib pack")
    ap.add_argument("--dataset", required=True, help="Path to holdout NDJSON dataset")
    ap.add_argument("--out", default="", help="Output JSON report path (stdout if empty)")
    ap.add_argument("--min-samples-per-regime", type=int, default=20)
    ap.add_argument("--min-auc", type=float, default=_GATE_MIN_AUC)
    ap.add_argument("--max-ece", type=float, default=_GATE_MAX_ECE)
    args = ap.parse_args()

    t0 = time.time()
    log.info("Loading model from %s", args.model)
    try:
        pack = load_model(args.model)
    except Exception as e:
        log.error("Failed to load model: %s", e)
        return 2

    log.info("Loading dataset from %s", args.dataset)
    records = load_dataset(args.dataset)
    if not records:
        log.error("No records loaded from %s", args.dataset)
        return 2

    log.info("Replaying %d records ...", len(records))
    result = replay(pack, records)

    gates, all_ok = evaluate_replay_gates(
        result, args.min_auc, args.max_ece, args.min_samples_per_regime,
    )

    report = {
        "run_ts_ms": int(time.time() * 1000),
        "model_path": args.model,
        "dataset_path": args.dataset,
        "n_samples": result["global"]["n"],
        "regimes": result["regimes"],
        "global": result["global"],
        "gates": gates,
        "all_gates_ok": all_ok,
        "elapsed_s": round(time.time() - t0, 2),
    }

    report_json = json.dumps(report, indent=2)

    if args.out:
        os.makedirs(os.path.dirname(os.path.abspath(args.out)), exist_ok=True)
        with open(args.out, "w") as f:
            f.write(report_json + "\n")
        log.info("Report written → %s", args.out)
    else:
        print(report_json)

    status = "PASS" if all_ok else "FAIL"
    log.info("Replay validation: %s (%d gates, %d samples, %.1fs)",
             status, len(gates), result["global"]["n"], time.time() - t0)

    return 0 if all_ok else 1


if __name__ == "__main__":
    sys.exit(main())
