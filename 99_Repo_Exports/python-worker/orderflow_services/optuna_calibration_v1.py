"""
orderflow_services/optuna_calibration_v1.py — Plan 3 / Step 4 runtime.

Periodic Optuna study over signal-policy parameters with PBO/DSR/ECE-aware
scoring. NEVER auto-promotes — produces a JSON promotion manifest with
decision="REPORT_ONLY" so a human reviewer (or downstream automation with
strict gating) decides next steps.

Pipeline:
  1. Pull resolved signal_outcome rows for last OPTUNA_WINDOW_DAYS.
  2. Build an evaluator closure that scores a parameter set on a purged
     walk-forward CV of those rows (using calibration.purged_cv).
  3. study.optimize(...) for OPTUNA_N_TRIALS or OPTUNA_TIMEOUT_SEC.
  4. Build PromotionManifest from study.best_trial; serialize to:
       * Redis: HSET optuna:manifest:<study_name> { manifest_json, ts_ms }
       * Disk (optional): OPTUNA_MANIFEST_DIR/<candidate_id>.json
  5. Emit Prometheus gauges for best score / pbo / dsr.

Optuna is imported lazily — module loads without the dep, and the runner
fails fast with a clear log when optuna is missing.

ENV:
  OPTUNA_CALIBRATION_ENABLED   = 0          master switch; 0 → run but don't write
  OPTUNA_REDIS_URL             = (REDIS_URL)
  OPTUNA_DB_DSN                = (TRADES_DB_DSN)
  OPTUNA_INTERVAL_SEC          = 86400      (24h)
  OPTUNA_WINDOW_DAYS           = 30
  OPTUNA_N_TRIALS              = 50         smaller default than plan's 200 to fit night budget
  OPTUNA_TIMEOUT_SEC           = 1800
  OPTUNA_STUDY_NAME            = signal_policy_v1
  OPTUNA_STORAGE_URL           = (none → in-memory study)
  OPTUNA_MIN_OOS_TRADES        = 300
  OPTUNA_MANIFEST_DIR          = (none → disk write disabled)
  OPTUNA_PROMOTION_ENFORCE     = 0          decision=REPORT_ONLY unless set
  OPTUNA_PORT                  = 9921

Prometheus:
  optuna_runs_total{outcome}                completed / failed / skipped_no_data
  optuna_best_score                          Gauge
  optuna_best_dsr                            Gauge
  optuna_best_pbo                            Gauge
  optuna_trials_pruned_total
  optuna_manifest_decision_total{decision}
"""
from __future__ import annotations

import json
import logging
import math
import os
import time
import uuid
from typing import Any

log = logging.getLogger("optuna_calibration")


# ─── ENV helpers ─────────────────────────────────────────────────────────────


def _env(k: str, d: str = "") -> str:
    return os.environ.get(k, d)


def _env_int(k: str, d: int) -> int:
    try:
        return int(_env(k, str(d)))
    except Exception:
        return d


def _env_bool(k: str, d: bool) -> bool:
    raw = _env(k, "")
    if not raw:
        return d
    return raw.strip().lower() in ("1", "true", "yes", "on")


def _git_sha_or_unknown() -> str:
    """Best-effort short git SHA; falls back to 'unknown' when not in a repo."""
    try:
        import subprocess  # noqa: PLC0415
        out = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            capture_output=True, text=True, timeout=2,
        )
        if out.returncode == 0:
            return out.stdout.strip() or "unknown"
    except Exception:
        pass
    return "unknown"


# ─── Data loading ────────────────────────────────────────────────────────────

_FETCH_SQL = """
    SELECT decision_time_ms, resolved_time_ms, realized_r, label, calib_prob
    FROM signal_outcome
    WHERE resolved_time_ms IS NOT NULL
      AND label IS NOT NULL
      AND decision_time_ms >= %s
    ORDER BY decision_time_ms ASC
    LIMIT %s
"""


def fetch_resolved_outcomes(conn: Any, since_ms: int, max_rows: int = 100_000) -> list[dict]:
    with conn.cursor() as cur:
        cur.execute(_FETCH_SQL, (since_ms, max_rows))
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]


# ─── Evaluator: param dict → WalkForwardResult ────────────────────────────────


def _filter_rows_by_params(rows: list[dict], params: dict[str, float]) -> list[dict]:
    """Apply policy params as a pass-through filter on resolved outcomes.

    This is a SIMPLIFIED scoring model — in absence of a full backtest the
    objective approximates "what would happen if we required calib_prob >= ml_p_min
    on the historical sample". The scoring still reflects: pass rate, edge under
    that filter, calibration on the filtered subset.

    Production-grade pass: rewrite to run a true policy backtest. The contract is
    that this function returns a row subset that the policy WOULD have taken.
    """
    p_min = float(params.get("ml_p_min", 0.0))
    if p_min <= 0:
        return rows
    out: list[dict] = []
    for r in rows:
        cp = r.get("calib_prob")
        if cp is None:
            # Without calib_prob the gate cannot decide; treat as filtered out.
            continue
        try:
            if float(cp) >= p_min:
                out.append(r)
        except (TypeError, ValueError):
            continue
    return out


def _compute_brier(rows: list[dict]) -> float:
    """Mean Brier score (calib_prob vs label∈{0,1}); 0.0 when no calibrated rows."""
    n = 0
    s = 0.0
    for r in rows:
        cp = r.get("calib_prob")
        lbl = r.get("label")
        if cp is None or lbl is None:
            continue
        try:
            y = 1 if int(lbl) == 1 else 0
            s += (float(cp) - y) ** 2
            n += 1
        except (TypeError, ValueError):
            continue
    return s / n if n > 0 else 0.0


def _expected_calibration_error(rows: list[dict], n_bins: int = 10) -> float:
    """Equal-width ECE across calib_prob bins."""
    buckets: list[tuple[int, float, float]] = []  # (count, sum_p, sum_y)
    for _ in range(n_bins):
        buckets.append((0, 0.0, 0.0))
    total = 0
    for r in rows:
        cp = r.get("calib_prob")
        lbl = r.get("label")
        if cp is None or lbl is None:
            continue
        try:
            p = float(cp)
            y = 1.0 if int(lbl) == 1 else 0.0
        except (TypeError, ValueError):
            continue
        if not math.isfinite(p) or p < 0 or p > 1:
            continue
        idx = min(n_bins - 1, int(p * n_bins))
        cnt, sp, sy = buckets[idx]
        buckets[idx] = (cnt + 1, sp + p, sy + y)
        total += 1
    if total == 0:
        return 0.0
    ece = 0.0
    for cnt, sp, sy in buckets:
        if cnt == 0:
            continue
        mean_p = sp / cnt
        mean_y = sy / cnt
        ece += (cnt / total) * abs(mean_p - mean_y)
    return ece


def build_evaluator(
    rows: list[dict],
    *,
    n_blocks: int = 8,
    embargo_ms: int = 600_000,
):
    """Factory: returns run_purged_walk_forward(params) → WalkForwardResult.

    Uses calibration.purged_cv.purged_walkforward to split labels into purged
    folds; evaluates the policy-filtered subset on each test fold.
    """
    import numpy as np  # noqa: PLC0415

    from calibration.optuna_signal_policy_objective import WalkForwardResult  # noqa: PLC0415
    from calibration.purged_cv import deflated_sharpe, pbo_estimate, purged_walkforward  # noqa: PLC0415

    base_decision = np.array([int(r["decision_time_ms"]) for r in rows], dtype=np.int64)
    base_resolved = np.array([int(r["resolved_time_ms"]) for r in rows], dtype=np.int64)
    base_returns = np.array([float(r.get("realized_r") or 0.0) for r in rows], dtype=np.float64)

    n_total = len(rows)
    span_days = 0
    if n_total > 0:
        span_days = max(1, int((int(base_decision.max() - base_decision.min())) // (86_400_000)))

    def evaluator(params: dict[str, float]) -> WalkForwardResult:
        filtered = _filter_rows_by_params(rows, params)
        n_filt = len(filtered)
        if n_filt == 0:
            return WalkForwardResult(
                oos_trades=0, mean_oos_profit_factor=0.0, mean_oos_sharpe=0.0,
                deflated_sharpe=-1.0, pbo=1.0, ece=1.0, pass_rate=0.0,
                max_drawdown_penalty=0.0,
            )

        pass_rate = n_filt / float(n_total) if n_total > 0 else 0.0

        # Run purged WF over the ORIGINAL (unfiltered) sorted set, then mask
        # train/test to the filtered indices.
        filt_set = {int(r["decision_time_ms"]) for r in filtered}

        fold_sharpes: list[float] = []
        fold_pfs: list[float] = []
        fold_returns_for_pbo: list[list[float]] = []
        all_test_returns: list[float] = []

        for _train_idx, test_idx in purged_walkforward(
            base_decision, base_resolved, n_blocks=n_blocks, embargo_ms=embargo_ms,
        ):
            test_mask = [i for i in test_idx if int(base_decision[i]) in filt_set]
            if not test_mask:
                continue
            rets = base_returns[test_mask]
            if rets.size == 0:
                continue
            all_test_returns.extend(rets.tolist())
            mean = float(rets.mean())
            std = float(rets.std(ddof=1)) if rets.size > 1 else 0.0
            sharpe = mean / std if std > 1e-9 else 0.0
            wins = rets[rets > 0].sum()
            losses = -rets[rets < 0].sum()
            pf = float(wins / losses) if losses > 1e-9 else (1.0 + float(wins))
            fold_sharpes.append(sharpe)
            fold_pfs.append(pf)
            fold_returns_for_pbo.append(rets.tolist())

        if not fold_sharpes:
            return WalkForwardResult(
                oos_trades=n_filt, mean_oos_profit_factor=0.0, mean_oos_sharpe=0.0,
                deflated_sharpe=-1.0, pbo=1.0, ece=1.0, pass_rate=pass_rate,
                max_drawdown_penalty=0.0,
            )

        mean_sr = float(sum(fold_sharpes) / len(fold_sharpes))
        mean_pf = float(sum(fold_pfs) / len(fold_pfs))

        # DSR + PBO using existing utilities
        import numpy as _np  # noqa: PLC0415
        all_r = _np.array(all_test_returns, dtype=_np.float64)
        n_obs = int(all_r.size)
        skew = float(((all_r - all_r.mean()) ** 3).mean() / (all_r.std(ddof=1) ** 3 + 1e-12)) if n_obs > 2 else 0.0
        kurt = float(((all_r - all_r.mean()) ** 4).mean() / (all_r.std(ddof=1) ** 4 + 1e-12) - 3.0) if n_obs > 3 else 0.0
        try:
            dsr = float(deflated_sharpe(mean_sr, n_trials=1, skew=skew, kurt=kurt, n_obs=n_obs))
        except Exception:
            dsr = 0.5
        try:
            pbo = float(pbo_estimate(fold_returns_for_pbo))
        except Exception:
            pbo = 0.5

        ece = _expected_calibration_error(filtered)

        # Max drawdown (R-units) — sum cumulative, take max peak-to-trough
        if all_r.size > 0:
            cum = _np.cumsum(all_r)
            running_max = _np.maximum.accumulate(cum)
            dd = float((running_max - cum).max())
        else:
            dd = 0.0

        return WalkForwardResult(
            oos_trades=n_filt,
            mean_oos_profit_factor=mean_pf,
            mean_oos_sharpe=mean_sr,
            deflated_sharpe=dsr,
            pbo=pbo,
            ece=ece,
            pass_rate=pass_rate,
            max_drawdown_penalty=dd,
        )

    return evaluator, n_total, span_days


# ─── Manifest write ──────────────────────────────────────────────────────────


def publish_manifest(rc: Any, study_name: str, manifest_json: str) -> bool:
    """HSET optuna:manifest:<study_name> with the latest JSON manifest."""
    key = f"optuna:manifest:{study_name}"
    try:
        rc.hset(key, mapping={"manifest_json": manifest_json, "ts_ms": str(int(time.time() * 1000))})
        rc.expire(key, 30 * 24 * 3600)
        return True
    except Exception as e:
        log.warning("optuna manifest HSET %s failed: %s", key, e)
        return False


def maybe_write_to_disk(manifest_dir: str, candidate_id: str, manifest_json: str) -> bool:
    if not manifest_dir:
        return False
    try:
        os.makedirs(manifest_dir, exist_ok=True)
        path = os.path.join(manifest_dir, f"{candidate_id}.json")
        with open(path, "w", encoding="utf-8") as f:
            f.write(manifest_json)
        return True
    except Exception as e:
        log.warning("optuna manifest disk write %s failed: %s", manifest_dir, e)
        return False


# ─── Main ────────────────────────────────────────────────────────────────────


def main() -> None:
    import redis  # type: ignore
    from prometheus_client import Counter, Gauge, start_http_server

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")

    try:
        import optuna  # type: ignore  # noqa: PLC0415
    except ImportError:
        log.error("optuna not installed — install with `pip install optuna>=3.5.0`; service refusing to start")
        # Sleep so docker doesn't crashloop forever, but make absence visible.
        while True:
            time.sleep(3600)

    enabled = _env_bool("OPTUNA_CALIBRATION_ENABLED", False)
    enforce_decision = _env_bool("OPTUNA_PROMOTION_ENFORCE", False)
    redis_url = _env("OPTUNA_REDIS_URL", _env("REDIS_URL", "redis://redis-worker-1:6379/0"))
    db_dsn = _env("OPTUNA_DB_DSN", _env("TRADES_DB_DSN", ""))
    interval_sec = _env_int("OPTUNA_INTERVAL_SEC", 86400)
    window_days = _env_int("OPTUNA_WINDOW_DAYS", 30)
    n_trials = _env_int("OPTUNA_N_TRIALS", 50)
    timeout_sec = _env_int("OPTUNA_TIMEOUT_SEC", 1800) or None
    study_name = _env("OPTUNA_STUDY_NAME", "signal_policy_v1")
    storage_url = _env("OPTUNA_STORAGE_URL", "") or None
    min_oos_trades = _env_int("OPTUNA_MIN_OOS_TRADES", 300)
    manifest_dir = _env("OPTUNA_MANIFEST_DIR", "")
    port = _env_int("OPTUNA_PORT", 9921)

    log.info(
        "optuna_calibration starting | enabled=%s enforce=%s study=%s n_trials=%d window=%dd port=%d",
        enabled, enforce_decision, study_name, n_trials, window_days, port,
    )

    rc = redis.from_url(redis_url, decode_responses=True)

    start_http_server(port)
    c_runs = Counter("optuna_runs_total", "Completed Optuna runs", ["outcome"])
    g_best = Gauge("optuna_best_score", "Best trial score for latest run", [])
    g_dsr = Gauge("optuna_best_dsr", "Best trial deflated_sharpe", [])
    g_pbo = Gauge("optuna_best_pbo", "Best trial PBO", [])
    c_pruned = Counter("optuna_trials_pruned_total", "Trials pruned by sample-size guard", [])
    c_decision = Counter("optuna_manifest_decision_total", "Manifest decisions", ["decision"])

    conn = None

    def _get_conn():
        nonlocal conn
        if conn is None or conn.closed:
            import psycopg2  # type: ignore  # noqa: PLC0415
            conn = psycopg2.connect(db_dsn)
        return conn

    while True:
        try:
            time.sleep(interval_sec)
            if not db_dsn:
                log.debug("OPTUNA_DB_DSN not set; skipping")
                c_runs.labels(outcome="skipped_no_data").inc()
                continue

            now_ms = int(time.time() * 1000)
            since_ms = now_ms - window_days * 86_400_000

            try:
                rows = fetch_resolved_outcomes(_get_conn(), since_ms)
            except Exception as e:
                log.warning("optuna fetch error: %s", e)
                conn = None
                c_runs.labels(outcome="failed").inc()
                continue

            if len(rows) < min_oos_trades:
                log.info("optuna: %d resolved rows < min %d → skip", len(rows), min_oos_trades)
                c_runs.labels(outcome="skipped_no_data").inc()
                continue

            from calibration.optuna_signal_policy_objective import build_objective  # noqa: PLC0415
            from calibration.promotion_gate import PromotionMetrics, PromotionThresholds  # noqa: PLC0415
            from calibration.promotion_manifest import build_manifest, to_json  # noqa: PLC0415

            evaluator, n_total, span_days = build_evaluator(rows)
            objective = build_objective(
                run_purged_walk_forward=evaluator,
                min_oos_trades=min_oos_trades,
                prune_exc=optuna.TrialPruned,
            )

            study = optuna.create_study(
                study_name=study_name,
                storage=storage_url,
                direction="maximize",
                load_if_exists=True,
            )

            n_before = len(study.trials)
            study.optimize(objective, n_trials=n_trials, timeout=timeout_sec)
            n_after = len(study.trials)
            pruned = sum(1 for t in study.trials[n_before:n_after] if t.state.name == "PRUNED")
            if pruned > 0:
                c_pruned.inc(pruned)

            best = study.best_trial
            best_wf = evaluator(best.params)

            g_best.set(float(best.value or 0.0))
            g_dsr.set(float(best_wf.deflated_sharpe))
            g_pbo.set(float(best_wf.pbo))

            metrics = PromotionMetrics(
                n_oos_trades=best_wf.oos_trades,
                n_oos_days=span_days,
                mean_oos_profit_factor=best_wf.mean_oos_profit_factor,
                mean_oos_sharpe=best_wf.mean_oos_sharpe,
                deflated_sharpe=best_wf.deflated_sharpe,
                pbo=best_wf.pbo,
                ece=best_wf.ece,
                brier=_compute_brier(rows),
                pass_rate=best_wf.pass_rate,
                slippage_residual_p95_bps=None,
            )

            candidate_id = f"optuna_{study_name}_{int(time.time())}_{uuid.uuid4().hex[:6]}"
            manifest = build_manifest(
                candidate_id=candidate_id,
                code_sha=_git_sha_or_unknown(),
                schema_hash="optuna_runtime",
                feature_cols_hash="optuna_runtime",
                data_start_ms=since_ms,
                data_end_ms=now_ms,
                n_trials=n_trials,
                metrics=metrics,
                thresholds=PromotionThresholds(),
                enforce_decision=enforce_decision,
                extras={"best_params": best.params, "n_total_resolved": n_total},
            )
            mjson = to_json(manifest)
            c_decision.labels(decision=manifest.decision).inc()

            if enabled:
                publish_manifest(rc, study_name, mjson)
                maybe_write_to_disk(manifest_dir, candidate_id, mjson)
                log.info(
                    "optuna run done | trials=%d best_score=%.3f dsr=%.3f pbo=%.3f decision=%s",
                    n_trials, best.value or 0.0, best_wf.deflated_sharpe, best_wf.pbo, manifest.decision,
                )
            else:
                log.info(
                    "optuna SHADOW (OPTUNA_CALIBRATION_ENABLED=0) | best_score=%.3f decision=%s reasons=%s",
                    best.value or 0.0, manifest.decision, manifest.reasons,
                )
            c_runs.labels(outcome="completed").inc()

        except Exception as e:
            c_runs.labels(outcome="failed").inc()
            log.warning("optuna main loop error: %s", e)


if __name__ == "__main__":
    main()
