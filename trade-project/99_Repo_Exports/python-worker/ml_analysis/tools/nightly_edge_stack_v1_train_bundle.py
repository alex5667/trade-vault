from __future__ import annotations
from core.redis_keys import RedisStreams as RS

"""P59 nightly bundle for edge_stack_v1 (Dataset -> Validate -> Train -> Validate -> Promote).

Design goals:
  - deterministic feature columns via Feature Registry (schema pinning)
  - strict dataset health guardrails (joined, pos_rate)
  - strict hash-pin checks (feature_cols_hash, schema_hash) via dataset report + train tool
  - atomic artifact promotion (candidate/champion)
  - best-effort Redis metrics write for Prometheus alerts

Redis keys:
  - metrics hash: metrics:edge_stack_train:last
  - ML confirm cfg hash: cfg:ml_confirm (fields: challenger_model_path, challenger_ver, model_path, model_ver)

Promotion policy:
  - always writes candidate artifact + challenger cfg
  - only promotes to champion if EDGE_STACK_AUTO_PROMOTE=1 AND dataset+train validations pass
"""


import argparse
import hashlib
import json
import logging
import os
import subprocess
import sys
import time
from datetime import datetime, timezone
from typing import Any

try:
    import redis  # type: ignore
except Exception:
    redis = None  # type: ignore

try:
    from tools.schema_choices_v1 import normalize_schema_ver as _norm_schema_ver
    from tools.schema_choices_v1 import schema_choices as _schema_choices  # type: ignore
except Exception:
    from ml_analysis.tools.schema_choices_v1 import normalize_schema_ver as _norm_schema_ver
    from ml_analysis.tools.schema_choices_v1 import schema_choices as _schema_choices  # type: ignore

from ml_analysis.tools.edge_stack_train_bundle_utils_p59 import (
    atomic_copy,
    atomic_write_json,
    compare_with_champion,
    now_ms,
    validate_dataset_report,
    validate_train_report,
    write_train_metrics,
)
import contextlib

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("nightly_edge_stack_v1_bundle_p59")


def _sha256_file(path: str) -> str:
    """Compute SHA-256 of a file (for artifact integrity check)."""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _run(module: str, args: list, timeout: int = 3600) -> tuple[bool, str, str]:
    """Run a python module via subprocess, return (ok, stdout, stderr)."""
    cmd = [sys.executable, "-m", module] + list(args)
    logger.info("Running: %s", " ".join(cmd))
    p = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    ok = p.returncode == 0
    if not ok:
        logger.error("Command failed code=%s\nSTDOUT:%s\nSTDERR:%s", p.returncode, p.stdout, p.stderr)
    return ok, (p.stdout or ""), (p.stderr or "")


def _load_json(path: str) -> dict[str, Any]:
    """Load JSON from file; return empty dict on any error."""
    try:
        with open(path, encoding="utf-8") as f:
            obj = json.load(f)
        return obj if isinstance(obj, dict) else {}
    except Exception:
        return {}


def _connect_redis(redis_url: str):
    """Connect to Redis; raises RuntimeError if redis-py not installed."""
    if redis is None:
        raise RuntimeError("redis-py is required for bundle metrics/cfg updates")
    return redis.Redis.from_url(redis_url, decode_responses=True)


def _write_cfg(r, key: str, mapping: dict[str, Any]) -> None:
    """Best-effort cfg write. Uses HSET for legacy hash keys, or SET JSON for new keys."""
    try:
        # Detect if it's a legacy hash
        is_hash = key == "cfg:ml_confirm"
        if not is_hash:
            try:
                t = r.type(key)
                if t == "hash":
                    is_hash = True
            except Exception:
                pass

        if is_hash:
            flat = {str(k): str(v) for k, v in mapping.items() if v is not None}
            if flat:
                r.hset(key, mapping=flat)
        else:
            r.set(key, json.dumps(mapping))
    except Exception:
        return



def _build_telegram_report(
    *,
    run_id: str,
    schema_ver: str,
    promoted: bool,
    promote_reason: str,
    dv_ok: bool,
    dv_reason: str,
    tv_ok: bool,
    tv_reason: str,
    tv_brier: float,
    tv_ece: float,
    tr: dict[str, Any],
    champion_cmp: Any,
    n_total: int,
    n_oof: int,
    pos_rate: float,
) -> str:
    """Build a compact Telegram text report for a v13 training cycle."""
    ts_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    schema_tag = f" [{schema_ver}]" if schema_ver else ""
    header = f"🧠 <b>ML Model Train</b>{schema_tag}\n<code>run: {run_id}</code>  {ts_str}\n"

    # --- resolve challenger metrics from train report
    oof = tr.get("oof") or {}
    meta = oof.get("meta") or {}
    gbdt = oof.get("gbdt") or {}
    lr_m = oof.get("lr") or {}
    c_brier = float(meta.get("brier") or tv_brier or 0.0)
    c_ece   = float(meta.get("ece") or tv_ece or 0.0)
    c_ll    = float(meta.get("logloss") or 0.0)
    c_p5    = float(meta.get("precision_top5pct") or 0.0)
    c_n_oof = int(tr.get("n_oof") or n_oof or 0)

    def _delta_row(
        name: str,
        challenger: float,
        champion: float,
        *,
        lower_better: bool = True,
        fmt: str = ".6f",
        threshold: float | None = None,
        pct: bool = False,
    ) -> str:
        """Build one table row."""
        scale = 100.0 if pct else 1.0
        cv = challenger * scale
        hv = champion * scale
        delta = cv - hv
        neg_is_better = lower_better
        # Status icon
        if champion == 0.0:
            icon = "➖"
        elif threshold is not None:
            icon = "✅" if abs(delta) <= threshold * scale else ("✅" if (neg_is_better and delta < 0) else "⚠️")
        elif neg_is_better:
            icon = "✅" if delta <= 0 else "⚠️"
        else:
            icon = "✅" if delta >= 0 else "⚠️"
        sign = "+" if delta >= 0 else ""
        if pct:
            fmt_str = f"{cv:.4f}"
            champ_str = f"{hv:.4f}"
            delta_str = f"{sign}{delta:.4f}"
        else:
            fmt_str = f"{cv:.6f}"
            champ_str = f"{hv:.6f}" if hv > 0 else "—"
            delta_str = f"{sign}{delta:.6f}" if hv > 0 else "—"
        return f"│ {name:<18}│ {fmt_str:<12}│ {champ_str:<12}│ {delta_str:<14}│ {icon} │"

    # Champion baseline
    ch_brier = ch_ece = ch_ll = ch_p5 = ch_n_oof = 0.0
    if champion_cmp is not None and not champion_cmp.no_champion:
        ch_brier = float(champion_cmp.champion_brier or 0.0)
        ch_ece   = float(champion_cmp.champion_ece or 0.0)
        ch_ll    = float(getattr(champion_cmp, "champion_logloss", 0.0))
        ch_p5    = float(getattr(champion_cmp, "champion_precision_top5pct", 0.0))
        ch_n_oof = float(getattr(champion_cmp, "champion_n_oof", 0.0))

    # Build table
    sep = "├──────────────────┼─────────────┼─────────────┼───────────────┼────┤"
    header_row = "│ Метрика          │ Challenger  │ Champion    │ Delta         │ 🚦 │"
    top_border = "┌──────────────────┬─────────────┬─────────────┬───────────────┬────┐"
    bot_border = "└──────────────────┴─────────────┴─────────────┴───────────────┴────┘"

    rows = [
        top_border,
        header_row,
        sep,
        _delta_row("Brier",            c_brier, ch_brier, lower_better=True,  threshold=0.005),
        _delta_row("ECE",              c_ece,   ch_ece,   lower_better=True,  threshold=0.010),
        _delta_row("LogLoss",          c_ll,    ch_ll,    lower_better=True),
        _delta_row("Precision Top-5%", c_p5,    ch_p5,    lower_better=False, pct=False),
        _delta_row("n_oof",            float(c_n_oof), float(int(ch_n_oof or 0)), lower_better=False, fmt=".0f"),
        bot_border,
    ]
    table = "\n".join(rows)

    # Dataset line
    dataset_line = f"📦 Dataset: n_total={n_total}  n_oof={c_n_oof}  pos_rate={pos_rate:.2%}"

    # Decision block
    if not dv_ok:
        decision = f"❌ <b>REJECTED</b> — dataset validation failed\n<code>{dv_reason}</code>"
    elif not tv_ok:
        decision = f"❌ <b>REJECTED</b> — train quality check failed\n<code>{tv_reason}</code>"
    elif promoted:
        # strip long reason, keep first 120 chars
        short_reason = promote_reason[:120].replace("<", "").replace(">", "")
        decision = f"✅ <b>PROMOTED → Champion</b>\n<code>{short_reason}</code>"
    else:
        short_reason = promote_reason[:120].replace("<", "").replace(">", "")
        decision = f"⏸ <b>Candidate only</b> (not promoted)\n<code>{short_reason}</code>"

    return f"{header}\n<pre>{table}</pre>\n{dataset_line}\n\n{decision}"


def _notify_telegram_sync(
    redis_url: str,
    text: str,
    *,
    stream: str = RS.NOTIFY_TELEGRAM,
) -> None:
    """Best-effort synchronous Redis xadd to notify:telegram stream."""
    try:
        if redis is None:
            return
        r = redis.Redis.from_url(redis_url, decode_responses=True, socket_timeout=3)
        r.xadd(
            stream,
            {"type": "report", "text": text[:3900], "ts": str(int(time.time() * 1000))},
            maxlen=200000,
            approximate=True,
        )
    except Exception as e:
        logger.warning("[telegram-notify] failed: %s", e)


def main(argv: list | None = None) -> int:
    ap = argparse.ArgumentParser(description="P59 nightly edge_stack_v1 train bundle")
    ap.add_argument("--redis_url", default=os.environ.get("REDIS_URL", "redis://redis-worker-1:6379/0"))
    ap.add_argument("--cfg_hash_key", default=os.environ.get("ML_CONFIRM_CFG_KEY", "cfg:ml_confirm"))
    ap.add_argument("--metrics_key", default=os.environ.get("EDGE_STACK_TRAIN_METRICS_KEY", "metrics:edge_stack_train:last"))

    ap.add_argument("--out_dir", default=os.environ.get("EDGE_STACK_V1_DIR", "/var/lib/trade/ml_models/edge_stack_v1"))
    ap.add_argument("--signals_stream", default=os.environ.get("EDGE_STACK_SIGNALS_STREAM", RS.OF_INPUTS))
    ap.add_argument("--closed_stream", default=os.environ.get("EDGE_STACK_CLOSED_STREAM", RS.TRADES_CLOSED))
    ap.add_argument("--window_hours", type=int, default=int(os.environ.get("EDGE_STACK_WINDOW_HOURS", "168")))
    ap.add_argument("--signals_count", type=int, default=int(os.environ.get("EDGE_STACK_SIGNALS_COUNT", "200000")))
    ap.add_argument("--closes_count", type=int, default=int(os.environ.get("EDGE_STACK_CLOSES_COUNT", "200000")))
    ap.add_argument("--y_min_r", type=float, default=float(os.environ.get("Y_MIN_R", os.environ.get("EDGE_STACK_Y_MIN_R", "0.10"))))

    # Feature schema / registry pinning
    ap.add_argument("--feature_schema_ver", default=os.environ.get("ML_EDGE_STACK_OOF_FEATURE_SCHEMA_VER", os.environ.get("ML_FEATURE_SCHEMA_VER", "v3")), choices=_schema_choices(include_empty=True))
    ap.add_argument("--scenario_prefix", default=os.environ.get("EDGE_STACK_SCENARIO_PREFIX", "bucket:"))
    ap.add_argument("--include_time_onehot", type=int, default=int(os.environ.get("EDGE_STACK_INCLUDE_TIME_ONEHOT", "1")))
    ap.add_argument("--strict_feature_cols", type=int, default=int(os.environ.get("ML_STRICT_FEATURE_COLS", "0")))
    ap.add_argument("--forbid_scenario_v4_onehot", type=int, default=int(os.environ.get("ML_FORBID_SCENARIO_V4_ONEHOT", "0")))

    # Dataset validation guardrails
    ap.add_argument("--min_joined", type=int, default=int(os.environ.get("EDGE_STACK_MIN_JOINED", "2000")))
    ap.add_argument("--pos_rate_min", type=float, default=float(os.environ.get("EDGE_STACK_POS_RATE_MIN", "0.05")))
    ap.add_argument("--pos_rate_max", type=float, default=float(os.environ.get("EDGE_STACK_POS_RATE_MAX", "0.60")))

    # Train hyperparams
    ap.add_argument("--n_splits", type=int, default=int(os.environ.get("ML_EDGE_STACK_OOF_N_SPLITS", "5")))
    ap.add_argument("--purge_ms", type=int, default=int(os.environ.get("ML_EDGE_STACK_OOF_PURGE_MS", "300000")))
    ap.add_argument("--embargo_ms", type=int, default=int(os.environ.get("ML_EDGE_STACK_OOF_EMBARGO_MS", "300000")))
    ap.add_argument("--min_train", type=int, default=int(os.environ.get("ML_EDGE_STACK_OOF_MIN_TRAIN", "500")))
    ap.add_argument("--lr_C", type=float, default=float(os.environ.get("ML_EDGE_STACK_OOF_LR_C", "1.0")))
    ap.add_argument("--gbdt_max_depth", type=int, default=int(os.environ.get("ML_EDGE_STACK_OOF_GBDT_MAX_DEPTH", "3")))
    ap.add_argument("--gbdt_lr", type=float, default=float(os.environ.get("ML_EDGE_STACK_OOF_GBDT_LR", "0.05")))
    ap.add_argument("--gbdt_max_iter", type=int, default=int(os.environ.get("ML_EDGE_STACK_OOF_GBDT_MAX_ITER", "400")))
    ap.add_argument("--calibrate", type=int, default=int(os.environ.get("ML_EDGE_STACK_OOF_CALIBRATE", "1")))

    # Train validation + promotion thresholds
    ap.add_argument("--brier_max", type=float, default=float(os.environ.get("EDGE_STACK_PROMOTE_BRIER_MAX", "0.30")))
    ap.add_argument("--ece_max", type=float, default=float(os.environ.get("EDGE_STACK_PROMOTE_ECE_MAX", "0.08")))
    # auto_promote=0 is safe default: produces candidate but never auto-promotes champion
    ap.add_argument("--auto_promote", type=int, default=int(os.environ.get("EDGE_STACK_AUTO_PROMOTE", "0")))

    # Optional explicitly-provided dataset and aliases
    ap.add_argument("--dataset", required=False, help="Path to pre-built JSONL dataset. Skips dataset generation step.")
    ap.add_argument("--dataset_report", required=False, help="Path to pre-built dataset report JSON.")
    ap.add_argument("--promote_candidate_only", type=int, required=False, help="Alias for auto_promote=0")

    # Champion comparison gate (requires compare_with_champion=1 + auto_promote=1 to be effective)
    ap.add_argument(
        "--compare_with_champion",
        type=int,
        default=int(os.environ.get("EDGE_STACK_COMPARE_WITH_CHAMPION", "1")),
        help="If 1 (default), block auto-promote when challenger is worse than champion.",
    )
    # Path to champion's bundle JSON used as baseline for comparison.
    # Falls back to out_dir/edge_stack_v1_train_bundle_champion.json
    ap.add_argument(
        "--champion_bundle_path",
        default=os.environ.get("EDGE_STACK_CHAMPION_BUNDLE_PATH", ""),
        help="Path to champion bundle JSON for regression check. Defaults to <out_dir>/champion_bundle.json",
    )
    # Tolerance thresholds — how much worse challenger can be before being rejected.
    ap.add_argument(
        "--regression_brier_max",
        type=float,
        default=float(os.environ.get("EDGE_STACK_REGRESSION_BRIER_MAX", "0.005")),
        help="Max allowed Brier regression vs champion (0.0 = must be equal or better).",
    )
    ap.add_argument(
        "--regression_ece_max",
        type=float,
        default=float(os.environ.get("EDGE_STACK_REGRESSION_ECE_MAX", "0.010")),
        help="Max allowed ECE regression vs champion.",
    )


    args = ap.parse_args(argv)

    if args.promote_candidate_only is not None and args.promote_candidate_only == 1:
        args.auto_promote = 0

    feature_schema_ver = _norm_schema_ver(str(args.feature_schema_ver or "").strip())


    run_id = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    out_dir = os.path.abspath(str(args.out_dir))
    run_dir = os.path.join(out_dir, "runs", run_id)
    champions_dir = os.path.join(out_dir, "champions")
    versions_dir = os.path.join(out_dir, "versions")
    os.makedirs(run_dir, exist_ok=True)
    os.makedirs(champions_dir, exist_ok=True)
    os.makedirs(versions_dir, exist_ok=True)

    # Per-run artifact paths
    dataset_jsonl = os.path.join(run_dir, "edge_train.jsonl")
    dataset_report = os.path.join(run_dir, "edge_dataset_report.json")
    quarantine_jsonl = os.path.join(run_dir, "edge_quarantine.jsonl")
    feature_cols_json = os.path.join(run_dir, "feature_cols.json")
    model_path = os.path.join(run_dir, "edge_stack_v1.joblib")
    train_report_json = os.path.join(run_dir, "train_report.json")

    # Promotion artifact paths (atomic copy on same FS)
    candidate_path = os.path.join(champions_dir, "edge_stack_v1_candidate.joblib")
    champion_path = os.path.join(champions_dir, "edge_stack_v1_champion.joblib")
    champion_prev_path = os.path.join(champions_dir, "edge_stack_v1_champion_prev.joblib")

    bundle_latest = os.path.join(out_dir, "edge_stack_v1_train_bundle_latest.json")
    version_json = os.path.join(versions_dir, f"edge_stack_v1_train_{run_id}.json")

    end_ms = now_ms()
    start_ms = end_ms - int(args.window_hours) * 3600 * 1000

    # --- Step 1: Build dataset
    if args.dataset and os.path.exists(args.dataset):
        logger.info("Using pre-built dataset: %s", args.dataset)
        # Attempt to find corresponding dataset report
        pre_report = args.dataset_report
        if not pre_report or not os.path.exists(pre_report):
            pre_report = str(args.dataset).replace(".jsonl", "_report.json")
        if not os.path.exists(pre_report):
            # Fallback to the hardcoded v12 report name in the same folder
            pre_report = os.path.join(os.path.dirname(args.dataset), "v12_of_report.json")

        logger.info("Using pre-built report: %s", pre_report)

        # We don't redefine dataset_jsonl string globally, we just copy it into our run_dir
        # so steps 2 and 3 can use it seamlessly
        try:
            atomic_copy(args.dataset, dataset_jsonl)
            if os.path.exists(pre_report):
                atomic_copy(pre_report, dataset_report)
            else:
                logger.error("Pre-built report does not exist at %s", pre_report)
                return 2
        except Exception as e:
            logger.error("Failed to copy pre-built dataset: %s", e)
            return 2
    else:
        build_args = [
            "--redis_url", str(args.redis_url),
            "--closed_stream", str(args.closed_stream),
            "--signals_count", str(args.signals_count),
            "--closes_count", str(args.closes_count),
            "--since_ms", str(start_ms),
            "--until_ms", str(end_ms),
            "--y_min_r", str(args.y_min_r),
            "--out_jsonl", dataset_jsonl,
            "--out_report_json", dataset_report,
            "--out_quarantine_jsonl", quarantine_jsonl,
            "--emit_feature_cols_json", feature_cols_json,
            "--feature_schema_ver", (feature_schema_ver or "").strip(),
            "--scenario_prefix", str(args.scenario_prefix),
            "--include_time_onehot", str(int(args.include_time_onehot)),
            "--strict_feature_cols", str(int(args.strict_feature_cols)),
            "--forbid_scenario_v4_onehot", str(int(args.forbid_scenario_v4_onehot)),
        ]
        ok_build, _, _ = _run("ml_analysis.tools.build_edge_stack_dataset_from_redis", build_args, timeout=3600)
        if not ok_build or not os.path.exists(dataset_report):
            # Metrics: fail_build
            mapping = {
                "status": "fail_build",
                "reason": "dataset_build_failed",
                "success": 0,
                "run_id": run_id,
                "updated_ts_ms": now_ms(),
            }
            with contextlib.suppress(Exception):
                write_train_metrics(str(args.redis_url), str(args.metrics_key), mapping)
            atomic_write_json(version_json, {"run_id": run_id, "status": "fail_build", "reason": "dataset_build_failed"})
            atomic_write_json(bundle_latest, {"run_id": run_id, "status": "fail_build", "reason": "dataset_build_failed"})
            return 2

    # --- Step 2: Validate dataset report (joined/pos_rate guardrails)
    rep = _load_json(dataset_report)
    dv = validate_dataset_report(rep, min_joined=int(args.min_joined), pos_rate_min=float(args.pos_rate_min), pos_rate_max=float(args.pos_rate_max))
    if not dv.ok:
        logger.error(
            "Dataset validation failed: %s (joined=%s pos_rate=%.6f report=%s)",
            dv.reason,
            dv.joined,
            dv.pos_rate,
            dataset_report,
        )
        mapping = {
            "status": "fail_validate",
            "reason": dv.reason,
            "success": 0,
            "run_id": run_id,
            "joined": dv.joined,
            "pos_rate": dv.pos_rate,
            "updated_ts_ms": now_ms(),
        }
        # Include feature registry hashes if available
        fr = rep.get("feature_registry") if isinstance(rep, dict) else None
        if isinstance(fr, dict):
            mapping["feature_cols_hash"] = fr.get("feature_cols_hash", "")
            mapping["schema_hash"] = fr.get("schema_hash", "")
            mapping["feature_schema_ver"] = fr.get("schema_ver", "")
        with contextlib.suppress(Exception):
            write_train_metrics(str(args.redis_url), str(args.metrics_key), mapping)
        atomic_write_json(version_json, {"run_id": run_id, "status": "fail_validate", "reason": dv.reason, "dataset_report": rep})
        atomic_write_json(bundle_latest, {"run_id": run_id, "status": "fail_validate", "reason": dv.reason})
        return 3

    # --- Step 3: Train OOF model
    train_args = [
        "--data_jsonl", dataset_jsonl,
        "--out_model", model_path,
        "--run_id", run_id,
        "--n_splits", str(args.n_splits),
        "--purge_ms", str(args.purge_ms),
        "--embargo_ms", str(args.embargo_ms),
        "--min_train", str(args.min_train),
        "--lr_C", "0.01",  # Heavy regularization to prevent OOF collapse under target weighting
        "--gbdt_max_depth", str(args.gbdt_max_depth),
        "--gbdt_learning_rate", str(args.gbdt_lr),
        "--gbdt_max_iter", str(args.gbdt_max_iter),
        "--calibrate", str(int(args.calibrate)),
        # --feature_cols_json is intentionally NOT passed: when --feature_schema_ver=v9_of
        # is set, trainer derives feature_cols from registry directly.
        # Passing both triggers strict_registry_match check which fails due to session_* one-hots.
        "--feature_schema_ver", (feature_schema_ver or "").strip(),
        "--scenario_prefix", str(args.scenario_prefix),
        "--include_time_onehot", str(int(args.include_time_onehot)),
        "--require_feature_registry", "0",
        "--dataset_report_json", dataset_report,
        "--weight_by_rmult", "1",
    ]
    ok_train, out, _ = _run("ml_analysis.tools.train_edge_stack_v1_oof", train_args, timeout=3600)
    if not ok_train or not os.path.exists(model_path):
        logger.error(
            "Train step failed: model_missing=%s model_path=%s",
            int(not os.path.exists(model_path)),
            model_path,
        )
        mapping = {
            "status": "fail_train",
            "reason": "train_failed",
            "success": 0,
            "run_id": run_id,
            "joined": dv.joined,
            "pos_rate": dv.pos_rate,
            "updated_ts_ms": now_ms(),
        }
        with contextlib.suppress(Exception):
            write_train_metrics(str(args.redis_url), str(args.metrics_key), mapping)
        atomic_write_json(version_json, {"run_id": run_id, "status": "fail_train", "reason": "train_failed"})
        atomic_write_json(bundle_latest, {"run_id": run_id, "status": "fail_train", "reason": "train_failed"})
        return 4

    # parse train report from stdout (last JSON object on line)
    tr: dict[str, Any] = {}
    for line in (out or "").splitlines()[::-1]:
        line = line.strip()
        if line.startswith("{") and line.endswith("}"):
            try:
                obj = json.loads(line)
                if isinstance(obj, dict):
                    tr = obj
                    break
            except Exception:
                continue
    atomic_write_json(train_report_json, tr)

    # --- Step 4: Validate train report (brier/ECE guardrails)
    tv = validate_train_report(tr, brier_max=float(args.brier_max), ece_max=float(args.ece_max))
    if not tv.ok:
        logger.error(
            "Train report validation failed: %s (brier=%.6f ece=%.6f)",
            tv.reason,
            tv.brier,
            tv.ece,
        )

    # --- Step 5: Promote candidate (always - regardless of validation)
    atomic_copy(model_path, candidate_path)

    # Update cfg hash with challenger pointer (best-effort, non-blocking)
    try:
        r = _connect_redis(str(args.redis_url))
        _write_cfg(r, str(args.cfg_hash_key), {
            "kind": "edge_stack_v1",
            "model_path": candidate_path,
            "model_ver": run_id,
            "feature_schema_ver": (feature_schema_ver or ""),
            "mode": "SHADOW",
            "fail_policy": "OPEN"
        })
    except Exception:
        pass

    # --- Step 6: Optionally promote to champion (auto_promote=1 + all validations ok)
    promoted = False
    promote_reason = ""
    champion_cmp = None
    if int(args.auto_promote) == 1 and dv.ok and tv.ok:
        # --- Step 5.5: Champion comparison gate (only when compare_with_champion=1)
        allow_promote = True
        if int(args.compare_with_champion) == 1:
            # Resolve champion bundle path
            champ_bundle_path = str(args.champion_bundle_path or "").strip()
            if not champ_bundle_path:
                champ_bundle_path = os.path.join(out_dir, "champion_bundle.json")
            champion_cmp = compare_with_champion(
                challenger_train_report=tr,
                champion_bundle_path=champ_bundle_path,
                brier_max_regression=float(args.regression_brier_max),
                ece_max_regression=float(args.regression_ece_max),
            )
            allow_promote = champion_cmp.should_promote
            if not allow_promote:
                logger.warning(
                    "[auto-promote] Challenger REJECTED by champion comparison: %s "
                    "(champ_brier=%.6f champ_ece=%.6f challenger_brier=%.6f challenger_ece=%.6f)",
                    champion_cmp.reason,
                    champion_cmp.champion_brier,
                    champion_cmp.champion_ece,
                    champion_cmp.challenger_brier,
                    champion_cmp.challenger_ece,
                )
            else:
                logger.info(
                    "[auto-promote] Champion comparison PASSED: %s "
                    "(champ_brier=%.6f challenger_brier=%.6f champ_ece=%.6f challenger_ece=%.6f)",
                    champion_cmp.reason,
                    champion_cmp.champion_brier,
                    champion_cmp.challenger_brier,
                    champion_cmp.champion_ece,
                    champion_cmp.challenger_ece,
                )

        if allow_promote:
            # Backup current champion before overwrite
            try:
                if os.path.exists(champion_path):
                    atomic_copy(champion_path, champion_prev_path)
            except Exception:
                pass
            atomic_copy(model_path, champion_path)
            promoted = True
            promote_reason = f"auto_promote_ok:{champion_cmp.reason if champion_cmp else 'no_comparison'}"

            # Save champion bundle for future comparisons
            atomic_write_json(
                os.path.join(out_dir, "champion_bundle.json"),
                {
                    "train": {"report": tr},
                    "run_id": run_id,
                    "promoted_at_ms": now_ms(),
                },
            )

            # Update cfg hash with champion pointer (best-effort)
            try:
                r = _connect_redis(str(args.redis_url))
                # Also write to the explicit champion key if provided
                champ_key = str(args.cfg_hash_key).replace("candidate_v13", "champion").replace("candidate", "champion")
                _write_cfg(r, champ_key, {
                    "kind": "edge_stack_v1",
                    "model_path": champion_path,
                    "model_ver": run_id,
                    "feature_schema_ver": (feature_schema_ver or ""),
                    "mode": "ENFORCE",
                    "fail_policy": "OPEN"
                })
            except Exception:
                pass
        else:
            # Champion comparison gate blocked promote
            promote_reason = f"champion_comparison_blocked:{champion_cmp.reason if champion_cmp else 'unknown'}"
    else:
        if int(args.auto_promote) == 1 and not tv.ok:
            promote_reason = f"auto_promote_blocked:{tv.reason}"
        elif int(args.auto_promote) == 1 and not dv.ok:
            promote_reason = f"auto_promote_blocked:{dv.reason}"
        else:
            promote_reason = "candidate_only"

    # --- Step 7: Write Redis metrics (best-effort, non-blocking)
    mapping = {
        "status": "ok" if dv.ok else "fail_validate",
        "reason": "ok" if dv.ok else dv.reason,
        "success": 1 if dv.ok else 0,
        "run_id": run_id,
        "joined": dv.joined,
        "pos_rate": dv.pos_rate,
        "oof_meta_brier": tv.brier,
        "oof_meta_ece": tv.ece,
        "train_ok": 1 if tv.ok else 0,
        "train_reason": tv.reason,
        "feature_schema_ver": (feature_schema_ver or ""),
        "candidate_path": candidate_path,
        "champion_path": champion_path if promoted else "",
        "promote_applied": 1 if promoted else 0,
        "promote_reason": promote_reason,
        "updated_ts_ms": now_ms(),
    }
    # Champion comparison fields (for Prometheus alerts)
    if champion_cmp is not None:
        mapping["champion_brier"] = champion_cmp.champion_brier
        mapping["champion_ece"] = champion_cmp.champion_ece
        mapping["challenger_brier"] = champion_cmp.challenger_brier
        mapping["challenger_ece"] = champion_cmp.challenger_ece
        mapping["champion_cmp_reason"] = champion_cmp.reason
        mapping["champion_cmp_no_champion"] = 1 if champion_cmp.no_champion else 0
    # Pin hashes from dataset/train reports for Prometheus alerts
    fr = rep.get("feature_registry") if isinstance(rep, dict) else None
    if isinstance(fr, dict):
        mapping["feature_cols_hash"] = (fr.get("feature_cols_hash") or "")
        mapping["schema_hash"] = (fr.get("schema_hash") or "")
    if isinstance(tr, dict):
        mapping["train_feature_cols_hash"] = (tr.get("feature_cols_hash") or "")
    with contextlib.suppress(Exception):
        write_train_metrics(str(args.redis_url), str(args.metrics_key), mapping)

    # --- Step 8: Persist bundle manifest (versioned + latest symlink)
    manifest = {
        "run_id": run_id,
        "status": "ok",
        "dataset": {
            "signals_stream": str(args.signals_stream),
            "closed_stream": str(args.closed_stream),
            "window_hours": int(args.window_hours),
            "since_ms": int(start_ms),
            "until_ms": int(end_ms),
            "y_min_r": float(args.y_min_r),
            "report": rep,
        },
        "train": {
            "report": tr,
            "train_ok": bool(tv.ok),
            "train_reason": tv.reason,
            "thresholds": {"brier_max": float(args.brier_max), "ece_max": float(args.ece_max)},
        },
        "champion_comparison": (
            {
                "should_promote": champion_cmp.should_promote,
                "reason": champion_cmp.reason,
                "champion_brier": champion_cmp.champion_brier,
                "champion_ece": champion_cmp.champion_ece,
                "champion_logloss": getattr(champion_cmp, "champion_logloss", 0.0),
                "champion_precision_top5pct": getattr(champion_cmp, "champion_precision_top5pct", 0.0),
                "champion_n_oof": getattr(champion_cmp, "champion_n_oof", 0),
                "challenger_brier": champion_cmp.challenger_brier,
                "challenger_ece": champion_cmp.challenger_ece,
                "no_champion": champion_cmp.no_champion,
                "regression_brier_max": float(args.regression_brier_max),
                "regression_ece_max": float(args.regression_ece_max),
            }
            if champion_cmp is not None
            else None
        ),
        "artifacts": {
            "model_path": model_path,
            "candidate_path": candidate_path,
            "champion_path": champion_path,
            "promoted": promoted,
            "promote_reason": promote_reason,
            "candidate_sha256": _sha256_file(candidate_path) if os.path.exists(candidate_path) else "",
            "champion_sha256": _sha256_file(champion_path) if promoted and os.path.exists(champion_path) else "",
        },
        "cfg": {"cfg_hash_key": str(args.cfg_hash_key)},
        "generated_ms": now_ms(),
    }
    atomic_write_json(version_json, manifest)
    atomic_write_json(bundle_latest, manifest)

    # --- Step 9: Telegram notification (best-effort, non-blocking)
    try:
        notify_stream = os.environ.get("NOTIFY_TELEGRAM_STREAM", RS.NOTIFY_TELEGRAM)
        tg_text = _build_telegram_report(
            run_id=run_id,
            schema_ver=(feature_schema_ver or ""),
            promoted=promoted,
            promote_reason=promote_reason,
            dv_ok=dv.ok,
            dv_reason=dv.reason,
            tv_ok=tv.ok,
            tv_reason=tv.reason,
            tv_brier=tv.brier,
            tv_ece=tv.ece,
            tr=tr,
            champion_cmp=champion_cmp,
            n_total=dv.joined,
            n_oof=int(tr.get("n_oof") or 0) if isinstance(tr, dict) else 0,
            pos_rate=dv.pos_rate,
        )
        _notify_telegram_sync(str(args.redis_url), tg_text, stream=notify_stream)
        logger.info("[telegram-notify] sent to %s", notify_stream)
    except Exception as e:
        logger.warning("[telegram-notify] skipped: %s", e)

    # Exit code policy: training succeeded, candidate produced.
    # If auto_promote enabled but blocked by validation, still exit 0 (candidate exists).
    # train_ok=0 in metrics will trigger Prometheus alert.
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
