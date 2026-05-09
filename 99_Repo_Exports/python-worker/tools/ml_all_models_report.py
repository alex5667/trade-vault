#!/usr/bin/env python3
from __future__ import annotations
from core.redis_keys import RedisStreams as RS

"""
ML All Models Report — Generates comprehensive Telegram reports for every ML model.

Reads Redis metrics, model artifacts, and training summaries to produce
model-specific reports with domain-appropriate metrics for each model type.

Models covered:
  1. ML Scorer V2 (Regression) — MAE, R², Spearman, Top5%
  2. ML Scorer V3 (Binary Classification) — ROC-AUC, LogLoss, Brier, Top5%
  3. Edge Stack V1 (Stacking Ensemble) — Brier, ECE, Joined, Pos Rate
  4. Meta-Model LR (Logistic Regression) — AUC, LogLoss, Brier, Pos Rate
  5. ML Confirm Gate — mode, kind, p_min, model age
  6. Confidence Calibration — ECE, Brier (raw vs calibrated)
  7. News Agent ML — tighten-only, model age
  8. Feature Drift — PSI, KS, drifted features count

Usage:
  python3 -m tools.ml_all_models_report [--redis-url ...] [--send-telegram 1]
"""

import argparse
import json
import logging
import math
import os
from datetime import UTC, datetime
from typing import Any

from utils.time_utils import get_ny_time_millis
import contextlib

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(name)s %(levelname)s %(message)s",
)
logger = logging.getLogger("ml_all_models_report")

try:
    import redis as _redis
except ImportError:
    _redis = None  # type: ignore

try:
    import joblib
except ImportError:
    joblib = None  # type: ignore


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

class DummyModel:
    """Mock for old joblib artifacts that stored a __main__.DummyModel reference"""
    pass

def _f(x: Any, default: float = 0.0) -> float:
    try:
        v = float(x) if x is not None else default
        return v if math.isfinite(v) else default
    except Exception:
        return default


def _fmt(v: float, fmt: str = ".4f") -> str:
    if math.isfinite(v) and v != -1.0:
        return f"{v:{fmt}}"
    return "N/A"


def _fmt_pct(v: float) -> str:
    if math.isfinite(v) and v != -1.0:
        return f"{v:.1%}"
    return "N/A"


def _age_str(ms: int) -> str:
    if ms <= 0:
        return "N/A"
    age_s = (get_ny_time_millis() - ms) / 1000.0
    if age_s < 3600:
        return f"{age_s / 60:.0f}m"
    if age_s < 86400:
        return f"{age_s / 3600:.1f}h"
    return f"{age_s / 86400:.1f}d"


def _ts_str(ms: int) -> str:
    if ms <= 0:
        return "N/A"
    try:
        return datetime.fromtimestamp(ms / 1000.0, tz=UTC).strftime("%Y-%m-%d %H:%M UTC")
    except Exception:
        return "N/A"


def _status_emoji(ok: bool) -> str:
    return "✅" if ok else "❌"


def _bar(value: float, lo: float, hi: float, width: int = 10) -> str:
    """Simple progress bar. Green zone = (lo, hi)."""
    if not math.isfinite(value):
        return "░" * width
    ratio = max(0.0, min(1.0, (value - lo) / max(hi - lo, 1e-9)))
    filled = int(ratio * width)
    return "█" * filled + "░" * (width - filled)


# ---------------------------------------------------------------------------
# Redis helpers
# ---------------------------------------------------------------------------

def _connect(url: str):
    if _redis is None:
        raise RuntimeError("redis-py required")
    r = _redis.from_url(url, decode_responses=True)
    r.ping()
    return r


def _hgetall_safe(r, key: str) -> dict[str, str]:
    try:
        return r.hgetall(key) or {}
    except Exception:
        return {}


def _get_safe(r, key: str) -> str | None:
    try:
        return r.get(key)
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Model 1: ML Scorer V2
# ---------------------------------------------------------------------------

def _report_scorer_v2(r, model_dir: str) -> str:
    lines: list[str] = []
    lines.append("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    lines.append("🎯 <b>Model 1: ML Scorer V2 (Regression)</b>")
    lines.append("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

    prod_path = os.path.join(model_dir, "scorer_v2.joblib")
    cand_path = os.path.join(model_dir, "scorer_v2.candidate.joblib")

    prod_exists = os.path.exists(prod_path)
    cand_exists = os.path.exists(cand_path)

    lines.append(f"  <b>Status:</b> {'🟢 Production' if prod_exists else '🔴 No production model'}")

    # Load production model metrics
    pack = None
    if prod_exists and joblib is not None:
        with contextlib.suppress(Exception):
            pack = joblib.load(prod_path)

    if pack and isinstance(pack, dict):
        m = pack.get("metrics", {})
        n = pack.get("n_samples", 0)
        trained_ms = pack.get("trained_at_ms", 0)

        lines.append("  <b>Algorithm:</b> LightGBM Regression")
        lines.append("  <b>Target:</b> R-multiple (winsorized ±3σ MAD)")
        lines.append(f"  <b>Features:</b> {len(pack.get('feature_names', []))} features")
        lines.append(f"  <b>Samples:</b> <code>{n}</code>")
        lines.append(f"  <b>Trained:</b> {_ts_str(trained_ms)} ({_age_str(trained_ms)} ago)")
        lines.append("")
        lines.append(f"  📊 <b>OOF Metrics ({m.get('folds', '?')} folds, Purged+Embargo)</b>")
        mae = _f(m.get("mae_oof"), -1)
        r2 = _f(m.get("r2_oof"), -1)
        spearman = _f(m.get("spearman_oof"), -1)
        top5 = _f(m.get("top5_hit_rate"), -1)
        y_mean = _f(m.get("y_mean"), 0)
        y_std = _f(m.get("y_std"), 0)

        lines.append(f"  • MAE:        <code>{_fmt(mae)}</code> R  {_status_emoji(0 < mae < 50)}")
        lines.append(f"  • R²:         <code>{_fmt(r2)}</code>     {_status_emoji(r2 > 0)}")
        lines.append(f"  • Spearman:   <code>{_fmt(spearman)}</code>     {_status_emoji(spearman > 0.05)}")
        lines.append(f"  • Top5% hit:  <code>{_fmt_pct(top5)}</code>     {_status_emoji(top5 > 0.5)}")
        lines.append(f"  • Target μ:   <code>{_fmt(y_mean)}</code> R")
        lines.append(f"  • Target σ:   <code>{_fmt(y_std)}</code> R")

        cal = pack.get("calibrator")
        lines.append(f"  • Calibrator: {'Isotonic ✅' if cal is not None else '⚠️ None (sigmoid fallback)'}")

        # Guard rails assessment
        lines.append("")
        lines.append("  🛡️ <b>Guard Rails</b>")
        lines.append(f"  • MAE < 50:     {_status_emoji(0 < mae < 50)}")
        lines.append(f"  • Spearman > 0: {_status_emoji(spearman > 0)}")
        lines.append(f"  • min_samples:  {_status_emoji(n >= 2000)} ({n}/2000)")
    else:
        lines.append("  ⚠️ Could not load model pack")

    if cand_exists:
        lines.append(f"\n  ⏳ <b>Candidate pending:</b> {os.path.basename(cand_path)}")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Model 2: ML Scorer V3
# ---------------------------------------------------------------------------

def _report_scorer_v3(r, model_dir: str) -> str:
    lines: list[str] = []
    lines.append("")
    lines.append("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    lines.append("🏷️ <b>Model 2: ML Scorer V3 (Binary Classification)</b>")
    lines.append("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

    prod_path = os.path.join(model_dir, "scorer_v3.joblib")
    cand_path = os.path.join(model_dir, "scorer_v3.candidate.joblib")

    # Try candidate if no prod
    load_path = prod_path if os.path.exists(prod_path) else cand_path
    label = "Production" if os.path.exists(prod_path) else "Candidate"
    exists = os.path.exists(load_path)

    lines.append(f"  <b>Status:</b> {'🟢' if os.path.exists(prod_path) else '🟡'} {label}")

    pack = None
    if exists and joblib is not None:
        with contextlib.suppress(Exception):
            pack = joblib.load(load_path)

    if pack and isinstance(pack, dict):
        m = pack.get("metrics", {})
        n = pack.get("n_samples", 0)
        trained_ms = pack.get("trained_at_ms", 0)

        lines.append("  <b>Algorithm:</b> LightGBM Binary Classification")
        lines.append("  <b>Target:</b> P(R ≥ 0.3) — Hit TP label")
        lines.append("  <b>Balancing:</b> RandomUnderSampler (50/50)")
        lines.append("  <b>num_leaves:</b> 15 (conservative)")
        lines.append(f"  <b>Samples:</b> <code>{n}</code>")
        lines.append(f"  <b>Trained:</b> {_ts_str(trained_ms)} ({_age_str(trained_ms)} ago)")
        lines.append("")
        lines.append(f"  📊 <b>OOF Metrics ({m.get('folds', '?')} folds)</b>")
        roc = _f(m.get("roc_auc_oof"), -1)
        ll = _f(m.get("logloss_oof"), -1)
        brier = _f(m.get("brier_oof"), -1)
        top5 = _f(m.get("top5_hit_rate"), -1)
        y_mean = _f(m.get("y_mean"), -1)

        lines.append(f"  • ROC-AUC:    <code>{_fmt(roc)}</code>     {_status_emoji(roc > 0.52)}")
        lines.append(f"  • LogLoss:    <code>{_fmt(ll)}</code>     {_status_emoji(0 < ll < 0.69)}")
        lines.append(f"  • Brier:      <code>{_fmt(brier)}</code>     {_status_emoji(0 < brier < 0.25)}")
        lines.append(f"  • Top5% hit:  <code>{_fmt_pct(top5)}</code>")
        lines.append(f"  • Prior P(1): <code>{_fmt_pct(y_mean)}</code>")

        lines.append("")
        lines.append("  🛡️ <b>Guard Rails</b>")
        lines.append(f"  • ROC-AUC ≥ 0.50: {_status_emoji(roc >= 0.50)}")
        lines.append(f"  • min_samples:    {_status_emoji(n >= 2000)} ({n}/2000)")
    else:
        lines.append("  ⚠️ No model artifact found")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Model 3: Edge Stack V1
# ---------------------------------------------------------------------------

def _report_edge_stack(r, base_dir: str, metrics_key: str, cfg_key: str) -> str:
    lines: list[str] = []
    lines.append("")
    lines.append("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    lines.append("🏗️ <b>Model 3: Edge Stack V1 (Stacking Ensemble)</b>")
    lines.append("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

    m = _hgetall_safe(r, metrics_key)
    cfg = _hgetall_safe(r, cfg_key)

    champion_path = os.path.join(base_dir, "champions", "edge_stack_v1_champion.joblib")
    candidate_path = os.path.join(base_dir, "champions", "edge_stack_v1_candidate.joblib")

    has_champion = os.path.exists(champion_path)
    has_candidate = os.path.exists(candidate_path)

    status_label = "🟢 Champion deployed" if has_champion else ("🟡 Candidate only" if has_candidate else "🔴 No model")
    lines.append(f"  <b>Status:</b> {status_label}")
    lines.append("  <b>Algorithm:</b> LR(C=0.01) + GBDT(d=3,lr=0.05,i=400) → Meta-LR (2-level stack)")
    lines.append("  <b>Target:</b> P(R ≥ y_min_r) binary")

    if m:
        run_id = m.get("run_id") or "?"
        joined = _f(m.get("joined"), 0)
        pos_rate = _f(m.get("pos_rate"), -1)
        brier = _f(m.get("oof_meta_brier"), -1)
        ece = _f(m.get("oof_meta_ece"), -1)
        schema = m.get("feature_schema_ver") or "?"
        train_ok = m.get("train_ok") or "?"
        promote = m.get("promote_applied") or "0"
        status = m.get("status") or "?"
        updated = int(_f(m.get("updated_ts_ms"), 0))

        lines.append(f"  <b>Schema:</b> <code>{schema}</code>")
        lines.append(f"  <b>Run ID:</b> <code>{run_id}</code>")
        lines.append(f"  <b>Updated:</b> {_ts_str(updated)} ({_age_str(updated)} ago)")
        lines.append("")
        lines.append("  📊 <b>Training Metrics</b>")
        lines.append(f"  • Joined:    <code>{int(joined)}</code>  {_status_emoji(joined >= 2000)}")
        lines.append(f"  • Pos rate:  <code>{_fmt(pos_rate)}</code>  {_status_emoji(0.05 <= pos_rate <= 0.60)}")
        lines.append(f"  • Brier:     <code>{_fmt(brier)}</code>  {_status_emoji(0 < brier <= 0.30)}")
        lines.append(f"  • ECE:       <code>{_fmt(ece)}</code>  {_status_emoji(0 <= ece <= 0.08)}")

        lines.append("")
        lines.append("  🛡️ <b>Guard Rails</b>")
        lines.append(f"  • Joined ≥ 2000:     {_status_emoji(joined >= 2000)}")
        lines.append(f"  • Pos rate ∈ [5-60%]: {_status_emoji(0.05 <= pos_rate <= 0.60)}")
        lines.append(f"  • Brier ≤ 0.30:      {_status_emoji(0 < brier <= 0.30)}")
        lines.append(f"  • ECE ≤ 0.08:        {_status_emoji(0 <= ece <= 0.08)}")
        lines.append(f"  • Train OK:          {_status_emoji(str(train_ok) == '1')}")
        lines.append(f"  • Promoted:          {_status_emoji(str(promote) == '1')}")
    else:
        lines.append("  ⚠️ No training metrics in Redis")

    # CFG pointers
    if cfg:
        lines.append("")
        lines.append(f"  📋 <b>Runtime Config ({cfg_key})</b>")
        champion_ver = cfg.get("model_ver") or "?"
        challenger_ver = cfg.get("challenger_ver") or "?"
        lines.append(f"  • Champion:   <code>{champion_ver}</code>")
        lines.append(f"  • Challenger: <code>{challenger_ver}</code>")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Model 4: Meta-Model LR
# ---------------------------------------------------------------------------

def _report_meta_lr(r, model_dir: str) -> str:
    lines: list[str] = []
    lines.append("")
    lines.append("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    lines.append("📐 <b>Model 4: Meta-Model LR (Logistic Regression)</b>")
    lines.append("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

    # Try to load both v8 and v9 artifacts
    for schema_label, filename in [("v8", "meta_model_lr_v8.json"), ("v9", "meta_model_lr_v9.json")]:
        path = os.path.join(model_dir, filename)
        if not os.path.exists(path):
            lines.append(f"\n  <b>{schema_label}:</b> ⚠️ Not found ({filename})")
            continue

        try:
            with open(path, encoding="utf-8") as f:
                data = json.load(f)
        except Exception:
            lines.append(f"\n  <b>{schema_label}:</b> ⚠️ Load error")
            continue

        ts = data.get("training_summary", {})
        cv_mean = ts.get("cv_mean", {})
        full = ts.get("train_full", {})
        n_rows = ts.get("n_rows", 0)
        pos_rate = _f(ts.get("pos_rate"), -1)
        y_col = ts.get("y_col") or "?"
        created_ms = int(_f(ts.get("created_ms"), 0))

        schema_name = data.get("schema_name") or "?"
        n_features = len(data.get("features", []))

        auc_cv = _f(cv_mean.get("auc"), -1) if cv_mean else -1.0
        ll_cv = _f(cv_mean.get("logloss"), -1) if cv_mean else -1.0
        brier_cv = _f(cv_mean.get("brier"), -1) if cv_mean else -1.0

        lines.append(f"\n  📊 <b>Schema {schema_label}: <code>{schema_name}</code></b>")
        lines.append(f"  • Features:  <code>{n_features}</code>")
        lines.append(f"  • Target:    <code>{y_col}</code>")
        lines.append(f"  • Samples:   <code>{n_rows}</code>")
        lines.append(f"  • Pos rate:  <code>{_fmt_pct(pos_rate)}</code>")
        lines.append(f"  • Trained:   {_ts_str(created_ms)} ({_age_str(created_ms)} ago)")
        lines.append("")
        lines.append("  <b>CV Mean (Purged+Embargo)</b>")
        lines.append(f"  • AUC:       <code>{_fmt(auc_cv)}</code>  {_status_emoji(auc_cv > 0.52)}")
        lines.append(f"  • LogLoss:   <code>{_fmt(ll_cv)}</code>  {_status_emoji(0 < ll_cv < 0.69)}")
        lines.append(f"  • Brier:     <code>{_fmt(brier_cv)}</code>  {_status_emoji(0 < brier_cv < 0.25)}")

        # Full train (sanity only)
        auc_full = _f(full.get("auc"), -1) if full else -1.0
        lines.append(f"  <b>Full train AUC:</b> <code>{_fmt(auc_full)}</code> (sanity)")

    # Champion/Challenger status from Redis
    champion_path_r = _get_safe(r, "meta_model:champion_path")
    challenger_path_r = _get_safe(r, "meta_model:challenger_path")
    if champion_path_r or challenger_path_r:
        lines.append("")
        lines.append("  📋 <b>A/B Status</b>")
        if champion_path_r:
            lines.append(f"  • Champion:   <code>{champion_path_r}</code>")
        if challenger_path_r:
            lines.append(f"  • Challenger: <code>{challenger_path_r}</code>")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Model 5: ML Confirm Gate (runtime status)
# ---------------------------------------------------------------------------

def _report_ml_confirm_gate(r) -> str:
    lines: list[str] = []
    lines.append("")
    lines.append("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    lines.append("🚦 <b>Model 5: ML Confirm Gate (Runtime)</b>")
    lines.append("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

    cfg = _hgetall_safe(r, "cfg:ml_confirm")

    mode = os.getenv("ML_CONFIRM_MODE") or cfg.get("mode") or "?"
    kind = cfg.get("model_kind") or cfg.get("kind") or "?"
    champion_ver = cfg.get("model_ver") or "?"
    challenger_ver = cfg.get("challenger_ver") or "?"
    model_path = cfg.get("model_path") or "?"

    lines.append(f"  <b>Mode:</b>       <code>{mode}</code> {'🟢' if mode == 'ENFORCE' else '🟡' if mode == 'SHADOW' else '⚪'}")
    lines.append(f"  <b>Kind:</b>       <code>{kind}</code>")
    lines.append(f"  <b>Champion:</b>   <code>{champion_ver}</code>")
    lines.append(f"  <b>Challenger:</b> <code>{challenger_ver}</code>")

    if model_path and model_path != "?":
        exists = os.path.exists(model_path)
        lines.append(f"  <b>Model file:</b> {'✅ exists' if exists else '❌ MISSING!'}")
        if exists:
            try:
                mtime_ms = int(os.path.getmtime(model_path) * 1000)
                lines.append(f"  <b>Model age:</b>  {_age_str(mtime_ms)}")
            except Exception:
                pass

    # Supported model kinds
    lines.append("")
    lines.append("  📋 <b>Supported Kinds</b>")
    lines.append("  • edge_stack_v1 — 2-level stack (LR+GBDT→Meta)")
    lines.append("  • util_mh_v1 — Fast Linear Utility MH")
    lines.append("  • meta_lr — MetaModel Logistic Regression")
    lines.append("  • edge_stack_mh_v1 — Multi-horizon Edge Stack")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Model 6: Confidence Calibration
# ---------------------------------------------------------------------------

def _report_confidence_cal(r, cal_dir: str) -> str:
    lines: list[str] = []
    lines.append("")
    lines.append("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    lines.append("🎚️ <b>Model 6: Confidence Calibration</b>")
    lines.append("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

    for label, filename in [("V1", "confidence_calibration.json"), ("V2", "confidence_calibration_v2.json")]:
        path = os.path.join(cal_dir, filename)
        if not os.path.exists(path):
            lines.append(f"\n  <b>{label}:</b> ⚠️ Not found")
            continue

        try:
            with open(path, encoding="utf-8") as f:
                data = json.load(f)
        except Exception:
            lines.append(f"\n  <b>{label}:</b> ⚠️ Load error")
            continue

        # Try to extract global metrics
        buckets = data.get("buckets", {})
        g = buckets.get("global", {})
        metrics = g.get("metrics", {})

        if metrics:
            raw = metrics.get("raw", {})
            cal = metrics.get("cal", {})
            raw_ece = _f(raw.get("ece"), -1)
            cal_ece = _f(cal.get("ece"), -1)
            raw_brier = _f(raw.get("brier"), -1)
            cal_brier = _f(cal.get("brier"), -1)

            lines.append(f"\n  📊 <b>Calibration {label} — Global</b>")
            lines.append(f"  • ECE raw:     <code>{_fmt(raw_ece)}</code>")
            lines.append(f"  • ECE cal:     <code>{_fmt(cal_ece)}</code>  {_status_emoji(cal_ece < raw_ece)}")
            lines.append(f"  • Brier raw:   <code>{_fmt(raw_brier)}</code>")
            lines.append(f"  • Brier cal:   <code>{_fmt(cal_brier)}</code>  {_status_emoji(cal_brier < raw_brier)}")

            if cal_ece >= 0 and raw_ece > 0:
                improvement = ((raw_ece - cal_ece) / raw_ece) * 100
                lines.append(f"  • ECE Δ:       <code>{improvement:+.1f}%</code>")
        else:
            lines.append(f"\n  <b>{label}:</b> ✅ Loaded (no global metrics in JSON)")

        # Method
        method = data.get("method") or data.get("calibration_method") or "?"
        created = data.get("created_ms", data.get("trained_ms", 0))
        lines.append(f"  • Method:      <code>{method}</code>")
        if created:
            lines.append(f"  • Created:     {_ts_str(int(created))} ({_age_str(int(created))} ago)")

        n_buckets = len(buckets) - (1 if "global" in buckets else 0)
        if n_buckets > 0:
            lines.append(f"  • Buckets:     <code>{n_buckets}</code> regime-specific calibrators")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Model 7: Feature Drift
# ---------------------------------------------------------------------------

def _report_feature_drift(r) -> str:
    lines: list[str] = []
    lines.append("")
    lines.append("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    lines.append("📉 <b>Feature Drift & Governance</b>")
    lines.append("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

    m = _hgetall_safe(r, "metrics:feature_drift_batch:last")
    if m:
        updated = int(_f(m.get("updated_ts_ms", m.get("ts_ms")), 0))
        n_features = int(_f(m.get("n_features"), 0))
        n_drifted = int(_f(m.get("n_drifted_psi"), 0))
        max_psi = _f(m.get("max_psi"), -1)
        max_ks = _f(m.get("max_ks"), -1)
        max_psi_feat = m.get("max_psi_feature") or "?"

        lines.append(f"  <b>Batch Report:</b> {_ts_str(updated)} ({_age_str(updated)} ago)")
        lines.append(f"  • Features monitored: <code>{n_features}</code>")
        lines.append(f"  • PSI drifted:        <code>{n_drifted}</code>  {_status_emoji(n_drifted == 0)}")
        lines.append(f"  • Max PSI:            <code>{_fmt(max_psi)}</code>  {_status_emoji(max_psi < 0.10)}")
        lines.append(f"  • Max KS:             <code>{_fmt(max_ks)}</code>  {_status_emoji(max_ks < 0.15)}")
        if max_psi_feat and max_psi_feat != "?":
            lines.append(f"  • Worst feature:      <code>{max_psi_feat}</code>")

        lines.append("")
        lines.append("  🛡️ <b>Thresholds</b>")
        lines.append("  • PSI alert: > 0.10")
        lines.append("  • KS alert:  > 0.15")
    else:
        lines.append("  ⚠️ No drift batch metrics in Redis")

    # Meta drift guard
    meta_drift = _hgetall_safe(r, "metrics:meta_drift_guard:last")
    if meta_drift:
        drift_status = meta_drift.get("status") or "?"
        frozen_count = int(_f(meta_drift.get("frozen_count"), 0))
        lines.append("\n  📋 <b>Meta Drift Guard</b>")
        lines.append(f"  • Status:  <code>{drift_status}</code>")
        lines.append(f"  • Frozen models: <code>{frozen_count}</code>")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Model 8: Edge Stack V13 (Candidate)
# ---------------------------------------------------------------------------

def _report_edge_stack_v13(r, base_dir: str) -> str:
    lines: list[str] = []
    lines.append("")
    lines.append("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    lines.append("🧪 <b>Model 3b: Edge Stack V13_OF (Candidate)</b>")
    lines.append("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

    m = _hgetall_safe(r, "metrics:edge_stack_train_v13:last")

    if m:
        run_id = m.get("run_id") or "?"
        joined = _f(m.get("joined"), 0)
        pos_rate = _f(m.get("pos_rate"), -1)
        brier = _f(m.get("oof_meta_brier"), -1)
        ece = _f(m.get("oof_meta_ece"), -1)
        schema = m.get("feature_schema_ver") or "v13_of"
        status = m.get("status") or "?"
        updated = int(_f(m.get("updated_ts_ms"), 0))

        lines.append(f"  <b>Schema:</b>  <code>{schema}</code> (242 features)")
        lines.append(f"  <b>Run ID:</b>  <code>{run_id}</code>")
        lines.append(f"  <b>Status:</b>  <code>{status}</code>")
        lines.append(f"  <b>Updated:</b> {_ts_str(updated)} ({_age_str(updated)} ago)")
        lines.append("")
        lines.append("  📊 <b>Metrics</b>")
        lines.append(f"  • Joined:  <code>{int(joined)}</code>  {_status_emoji(joined >= 2000)}")
        lines.append(f"  • Pos rate:<code>{_fmt(pos_rate)}</code>  {_status_emoji(0.05 <= pos_rate <= 0.60)}")
        lines.append(f"  • Brier:   <code>{_fmt(brier)}</code>  {_status_emoji(0 < brier <= 0.30)}")
        lines.append(f"  • ECE:     <code>{_fmt(ece)}</code>  {_status_emoji(0 <= ece <= 0.08)}")
    else:
        lines.append("  ⚠️ No V13 training metrics in Redis")
        champion = os.path.join(base_dir, "champions", "edge_stack_v1_champion.joblib")
        if os.path.exists(champion):
            lines.append("  📁 Champion file exists at isolated path")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Summary / Aggregate
# ---------------------------------------------------------------------------

def _report_summary_header() -> str:
    now = datetime.now(UTC).strftime("%Y-%m-%d %H:%M UTC")
    return (
        f"🧠 <b>Trade Scanner — ML Models Report</b>\n"
        f"📅 <code>{now}</code>\n"
    )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def build_full_report(r, *, scorer_dir: str, edge_dir: str, edge_v13_dir: str,
                      meta_dir: str, cal_dir: str) -> str:
    """Build the complete multi-model report."""
    parts: list[str] = []

    parts.append(_report_summary_header())

    # 1. Scorer V2
    parts.append(_report_scorer_v2(r, scorer_dir))

    # 2. Scorer V3
    parts.append(_report_scorer_v3(r, scorer_dir))

    # 3. Edge Stack V1 (primary)
    parts.append(_report_edge_stack(r, edge_dir, "metrics:edge_stack_train:last", "cfg:ml_confirm"))

    # 3b. Edge Stack V13 (candidate)
    parts.append(_report_edge_stack_v13(r, edge_v13_dir))

    # 4. Meta-Model LR
    parts.append(_report_meta_lr(r, meta_dir))

    # 5. ML Confirm Gate (runtime)
    parts.append(_report_ml_confirm_gate(r))

    # 6. Confidence Calibration
    parts.append(_report_confidence_cal(r, cal_dir))

    # 7. Feature Drift
    parts.append(_report_feature_drift(r))

    return "\n".join(parts)


def main() -> int:
    ap = argparse.ArgumentParser(description="ML All Models Report")
    ap.add_argument("--redis-url", default=os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0"))
    ap.add_argument("--send-telegram", type=int, default=1, help="1=send report to Telegram")
    ap.add_argument("--notify-stream", default=os.getenv("NOTIFY_STREAM", RS.NOTIFY_TELEGRAM))

    # Model directories
    ap.add_argument("--scorer-dir", default=os.getenv("ML_SCORER_DIR",
                    "/var/lib/trade/ml_models/scorer_v2"))
    ap.add_argument("--edge-dir", default=os.getenv("EDGE_STACK_V1_DIR",
                    "/var/lib/trade/ml_models/edge_stack_v1"))
    ap.add_argument("--edge-v13-dir", default=os.getenv("EDGE_STACK_V13_DIR",
                    "/var/lib/trade/ml_models/edge_stack_v13_of"))
    ap.add_argument("--meta-dir", default=os.getenv("META_MODEL_OUT_DIR",
                    "/var/lib/trade/ml_models/meta_model"))
    ap.add_argument("--cal-dir", default=os.getenv("CONF_CAL_OUT_DIR",
                    "/var/lib/trade/of_calibrators"))

    args = ap.parse_args()

    try:
        r = _connect(args.redis_url)
    except Exception as e:
        logger.error("Redis connection failed: %s", e)
        return 1

    report = build_full_report(
        r,
        scorer_dir=args.scorer_dir,
        edge_dir=args.edge_dir,
        edge_v13_dir=args.edge_v13_dir,
        meta_dir=args.meta_dir,
        cal_dir=args.cal_dir,
    )

    # Always print to stdout
    print(report)

    # Optionally send to Telegram
    if args.send_telegram:
        # Telegram has a 4096 char limit per message; split if needed
        MAX_MSG = 4000
        parts = []
        current = ""
        for line in report.split("\n"):
            if len(current) + len(line) + 1 > MAX_MSG:
                parts.append(current)
                current = line
            else:
                current = current + "\n" + line if current else line
        if current:
            parts.append(current)

        notified = 0
        for i, part in enumerate(parts):
            is_first = (i == 0)
            fields = {
                "type": "report",
                "text": part,
                "parse_mode": "HTML",
                "source": "ml_all_models_report",
            }
            try:
                r.xadd(args.notify_stream, fields, maxlen=50_000)
                notified += 1
            except Exception as e:
                logger.error("Failed to send part %d to Telegram: %s", i, e)

        logger.info("Sent %d message(s) to %s", notified, args.notify_stream)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
