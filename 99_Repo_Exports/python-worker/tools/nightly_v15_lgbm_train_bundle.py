"""nightly_v15_lgbm_train_bundle.py — orchestrator for daily v15_lgbm refit.

Daily online adaptation cycle:

  1. Acquire Redis mutex `lock:v15_lgbm_train` (avoids concurrent refits).
  2. Pre-flight `count_positives_per_regime()`:
       - If any regime that already has a live sub-model dropped below
         N_POSITIVE_DEGRADATION_FLOOR → log warning (regime may be stale).
       - If the GLOBAL positives are below V15_MIN_POSITIVES → skip with reason.
  3. Run `tools.train_v15_lgbm` (subprocess) with `--per-regime --source=postgres`.
  4. Parse verdict.json.
  5. If verdict.status == ACCEPT AND --auto-promote=1:
       - Invoke `promote_v15_lgbm_to_live.py`.
       - On success: emit Telegram notify + update champion metric in Redis.
       - On failure: keep prior champion; emit alert.
  6. Always write the latest train metrics to `metrics:v15_lgbm_train:last`.
  7. Release lock.

Designed to run inside `scanner-v15-lgbm-train-timer` container
(env-driven loop in docker-compose).

Env (all optional, sensible defaults):
  REDIS_URL                          redis://redis-worker-1:6379/0
  ANALYTICS_DB_DSN | PG_DSN          PG connection for signal_snapshots
  V15_TRAIN_LOOKBACK_DAYS            30
  V15_LABEL_THRESHOLD_R              0.3
  V15_TRAIN_PER_REGIME               1
  V15_PER_REGIME_MIN                 60
  V15_MIN_POSITIVES                  100   (global guard — skip cycle if below)
  V15_LIVE_MODEL_PATH                /var/lib/trade/ml_models/scorer_v15_lgbm/scorer_v15_lgbm.joblib
  V15_CANDIDATE_DIR                  /var/lib/trade/of_reports/models
  V15_LOCK_KEY                       lock:v15_lgbm_train
  V15_LOCK_TTL_SEC                   3600
  V15_METRICS_KEY                    metrics:v15_lgbm_train:last
  V15_NOTIFY_STREAM                  notify:telegram
  V15_NOTIFY_ON_PROMOTE              1
  V15_NOTIFY_ON_REJECT               1
"""
from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import subprocess
import sys
import time
from typing import Any

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
log = logging.getLogger("nightly_v15_lgbm")

# ── Config ────────────────────────────────────────────────────────────────────

REDIS_URL = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
PG_DSN = os.getenv("ANALYTICS_DB_DSN") or os.getenv("PG_DSN") or ""

LOOKBACK_DAYS = int(os.getenv("V15_TRAIN_LOOKBACK_DAYS", "30"))
LABEL_THR_R = float(os.getenv("V15_LABEL_THRESHOLD_R", "0.3"))
PER_REGIME = os.getenv("V15_TRAIN_PER_REGIME", "1") == "1"
PER_REGIME_MIN = int(os.getenv("V15_PER_REGIME_MIN", "60"))
MIN_POSITIVES = int(os.getenv("V15_MIN_POSITIVES", "100"))
N_POSITIVE_DEGRADATION_FLOOR = int(os.getenv("V15_REGIME_DEGRADATION_FLOOR", "30"))

LIVE_PATH = os.getenv(
    "V15_LIVE_MODEL_PATH",
    "/var/lib/trade/ml_models/scorer_v15_lgbm/scorer_v15_lgbm.joblib",
)
CANDIDATE_DIR = os.getenv("V15_CANDIDATE_DIR", "/var/lib/trade/of_reports/models")
VERDICT_PATH = os.getenv("V15_VERDICT_PATH", "/tmp/v15_lgbm_verdict.json")

LOCK_KEY = os.getenv("V15_LOCK_KEY", "lock:v15_lgbm_train")
LOCK_TTL_SEC = int(os.getenv("V15_LOCK_TTL_SEC", "3600"))
METRICS_KEY = os.getenv("V15_METRICS_KEY", "metrics:v15_lgbm_train:last")

NOTIFY_STREAM = os.getenv("V15_NOTIFY_STREAM", "notify:telegram")
NOTIFY_ON_PROMOTE = os.getenv("V15_NOTIFY_ON_PROMOTE", "1") == "1"
NOTIFY_ON_REJECT = os.getenv("V15_NOTIFY_ON_REJECT", "1") == "1"

# P2.8: cost-aware label flag (propagated to train_v15_lgbm subprocess)
COST_AWARE_LABEL = os.getenv("V15_COST_AWARE_LABEL", "0") == "1"
COST_AWARE_FEE_MUL = float(os.getenv("V15_COST_AWARE_FEE_MUL", "2.0"))
COST_AWARE_SLIP_BPS_FALLBACK = float(os.getenv("V15_COST_AWARE_SLIP_BPS_FALLBACK", "4.0"))

# ── Helpers ───────────────────────────────────────────────────────────────────


def _now_ms() -> int:
    return int(time.time() * 1000)


def acquire_lock(r: Any, key: str, ttl_sec: int) -> str | None:
    """SET NX EX-based mutex. Returns owner token on success, None on contention."""
    token = hashlib.sha256(f"{os.getpid()}:{time.time()}".encode()).hexdigest()[:16]
    ok = r.set(key, token, nx=True, ex=ttl_sec)
    if ok:
        return token
    return None


def release_lock(r: Any, key: str, token: str) -> bool:
    """Lua-based delete-if-owner. Avoids the classic 'delete someone else's lock' bug."""
    script = (
        "if redis.call('get', KEYS[1]) == ARGV[1] then "
        "return redis.call('del', KEYS[1]) else return 0 end"
    )
    try:
        return bool(r.eval(script, 1, key, token))
    except Exception:
        return False


def count_positives_per_regime(pg_dsn: str, lookback_days: int, label_thr_r: float) -> dict[str, int]:
    """Return {regime: positive_count} for last `lookback_days` of joined trades.

    A 'positive' is a closed trade with `r_multiple >= label_thr_r`. We DON'T
    join with signal_snapshots here — instead we read regime directly from the
    enriched `trades_closed` table (the migration added entry_regime + backfill).

    Returns empty dict on connection failure (caller treats as 'unknown — skip cycle').
    """
    try:
        import psycopg2
    except ImportError:
        log.error("psycopg2 not installed")
        return {}
    conn = None
    try:
        conn = psycopg2.connect(pg_dsn)
        with conn.cursor() as cur:
            since_ms = _now_ms() - lookback_days * 24 * 3600 * 1000
            cur.execute(
                """
                SELECT
                    COALESCE(NULLIF(entry_regime, ''), 'na') AS regime,
                    COUNT(*) FILTER (WHERE r_multiple >= %s) AS n_positive
                FROM trades_closed
                WHERE exit_ts_ms >= %s
                  AND r_multiple IS NOT NULL
                GROUP BY 1
                ORDER BY n_positive DESC;
                """,
                (label_thr_r, since_ms),
            )
            return {r[0]: int(r[1]) for r in cur.fetchall()}
    except Exception as e:
        log.warning("count_positives_per_regime failed: %s", e)
        return {}
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass


def preflight(pg_dsn: str) -> tuple[bool, dict[str, Any]]:
    """Decide whether to proceed with the refit.

    Returns (proceed, info_dict). When proceed=False, info has 'reason'.
    """
    regime_pos = count_positives_per_regime(pg_dsn, LOOKBACK_DAYS, LABEL_THR_R)
    total_pos = sum(regime_pos.values())
    info: dict[str, Any] = {
        "lookback_days": LOOKBACK_DAYS,
        "label_thr_r": LABEL_THR_R,
        "regime_positives": regime_pos,
        "total_positives": total_pos,
        "min_positives_required": MIN_POSITIVES,
    }
    if total_pos < MIN_POSITIVES:
        info["reason"] = (
            f"insufficient_positives: total={total_pos} < required={MIN_POSITIVES}"
        )
        return False, info
    # Soft warning: regimes that previously had sub-models but dropped below floor
    degraded = {
        rg: n for rg, n in regime_pos.items()
        if rg not in ("na", "unknown", "") and n > 0 and n < N_POSITIVE_DEGRADATION_FLOOR
    }
    if degraded:
        info["degraded_regimes"] = degraded
        log.warning("regimes below degradation floor: %s", degraded)
    return True, info


# ── Trainer subprocess ────────────────────────────────────────────────────────


def run_trainer(verdict_path: str, candidate_out_path: str, *,
                source: str = "postgres", dry_run: bool = False) -> tuple[int, dict[str, Any]]:
    """Spawn `tools.train_v15_lgbm` and return (exit_code, verdict_dict)."""
    cmd = [
        sys.executable, "-m", "tools.train_v15_lgbm",
        "--redis-url", REDIS_URL,
        "--source", source,
        "--lookback-days", str(LOOKBACK_DAYS),
        "--label-threshold-r", str(LABEL_THR_R),
        "--n-folds", "5",
        "--verdict-out", verdict_path,
        "--out", candidate_out_path,
    ]
    if PG_DSN:
        cmd.extend(["--pg-dsn", PG_DSN])
    if PER_REGIME:
        cmd.append("--per-regime")
        cmd.extend(["--per-regime-min", str(PER_REGIME_MIN)])
    if COST_AWARE_LABEL:
        cmd.append("--cost-aware-label")
        cmd.extend(["--cost-aware-fee-mul", str(COST_AWARE_FEE_MUL)])
        cmd.extend(["--cost-aware-slip-bps-fallback", str(COST_AWARE_SLIP_BPS_FALLBACK)])
    if dry_run:
        cmd.append("--dry-run")

    log.info("trainer cmd: %s", " ".join(c if "@" not in c else "<dsn>" for c in cmd))
    proc = subprocess.run(cmd, capture_output=True, text=True, check=False, timeout=1800)
    if proc.stdout:
        for line in proc.stdout.splitlines()[-50:]:
            log.info("[trainer] %s", line)
    if proc.returncode != 0 and proc.stderr:
        for line in proc.stderr.splitlines()[-20:]:
            log.error("[trainer] %s", line)

    verdict = {}
    try:
        with open(verdict_path) as f:
            verdict = json.load(f)
    except Exception as e:
        log.warning("could not read verdict %s: %s", verdict_path, e)
    return proc.returncode, verdict


# ── Promotion ─────────────────────────────────────────────────────────────────


def run_promoter(candidate_path: str) -> tuple[int, str]:
    """Spawn `tools.promote_v15_lgbm_to_live`. Returns (exit_code, last_stdout)."""
    cmd = [
        sys.executable, "-m", "tools.promote_v15_lgbm_to_live",
        "--candidate", candidate_path,
        "--live", LIVE_PATH,
    ]
    log.info("promoter cmd: %s", " ".join(cmd))
    proc = subprocess.run(cmd, capture_output=True, text=True, check=False, timeout=120)
    if proc.stdout:
        for line in proc.stdout.splitlines()[-20:]:
            log.info("[promoter] %s", line)
    if proc.returncode != 0 and proc.stderr:
        for line in proc.stderr.splitlines()[-10:]:
            log.error("[promoter] %s", line)
    return proc.returncode, (proc.stdout.strip().splitlines() or [""])[-1]


# ── Notify ────────────────────────────────────────────────────────────────────


def notify_telegram(r: Any, text: str) -> None:
    if not NOTIFY_STREAM:
        return
    try:
        r.xadd(
            NOTIFY_STREAM,
            {"type": "report", "text": text, "parse_mode": "HTML",
             "source": "v15_lgbm_train"},
            maxlen=50_000, approximate=True,
        )
    except Exception as e:
        log.warning("notify send failed: %s", e)


# ── Metrics ───────────────────────────────────────────────────────────────────


def write_metrics(r: Any, payload: dict[str, Any]) -> None:
    try:
        r.set(METRICS_KEY, json.dumps(payload, default=str))
    except Exception as e:
        log.warning("metrics write failed: %s", e)


# ── Main ──────────────────────────────────────────────────────────────────────


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--auto-promote", type=int, default=int(os.getenv("V15_PROMOTE_AUTO", "0")),
                    help="0 = train only, 1 = also promote on ACCEPT")
    ap.add_argument("--dry-run", action="store_true",
                    help="Run trainer with --dry-run (no joblib write)")
    ap.add_argument("--force", action="store_true",
                    help="Skip preflight + lock check (debug only)")
    args = ap.parse_args()

    if not PG_DSN:
        log.error("ANALYTICS_DB_DSN / PG_DSN required for v15_lgbm bundle (signal_snapshots lives in PG)")
        return 2

    try:
        import redis
    except ImportError:
        log.error("redis-py not installed")
        return 2

    r = redis.from_url(REDIS_URL, decode_responses=True)

    token = None
    if not args.force:
        token = acquire_lock(r, LOCK_KEY, LOCK_TTL_SEC)
        if token is None:
            log.warning("lock %s held by another worker — skipping cycle", LOCK_KEY)
            return 0
        log.info("acquired lock %s token=%s ttl=%ds", LOCK_KEY, token, LOCK_TTL_SEC)

    started_at = _now_ms()
    final_status = "running"
    final_reason = ""
    promotion_run = False
    promotion_ok = False
    verdict: dict[str, Any] = {}
    pre_info: dict[str, Any] = {}

    try:
        # Preflight
        if not args.force:
            proceed, pre_info = preflight(PG_DSN)
            if not proceed:
                final_status = "skipped"
                final_reason = pre_info.get("reason", "preflight_failed")
                log.warning("preflight: %s", final_reason)
                log.info("regime_positives=%s total=%d", pre_info.get("regime_positives"),
                         pre_info.get("total_positives"))
                return 0
            log.info("preflight ok: total_positives=%d regimes=%d",
                     pre_info.get("total_positives", 0),
                     len(pre_info.get("regime_positives", {})))

        # Train
        candidate_dir = CANDIDATE_DIR
        os.makedirs(candidate_dir, exist_ok=True)
        candidate_path = os.path.join(
            candidate_dir, f"scorer_v15_lgbm_{int(started_at/1000)}.joblib",
        )
        rc, verdict = run_trainer(VERDICT_PATH, candidate_path, dry_run=args.dry_run)
        if rc != 0 and verdict.get("status") not in ("ACCEPT", "REJECT", "rejected"):
            final_status = "trainer_error"
            final_reason = f"trainer_exit_code={rc}"
            log.error("trainer failed: rc=%d", rc)
            return 1

        verdict_status = str(verdict.get("status", "")).upper()
        log.info("trainer verdict: %s", verdict_status)

        # Promote (only if accepted)
        if verdict_status == "ACCEPT" and args.auto_promote == 1 and not args.dry_run:
            if os.path.exists(candidate_path):
                promotion_run = True
                prc, ptail = run_promoter(candidate_path)
                promotion_ok = (prc == 0)
                if promotion_ok:
                    log.info("✓ promoted candidate → %s", LIVE_PATH)
                    final_status = "promoted"
                else:
                    log.error("promotion failed (rc=%d): %s", prc, ptail)
                    final_status = "promotion_failed"
                    final_reason = ptail[:200]
            else:
                final_status = "candidate_missing"
                final_reason = f"candidate file not at {candidate_path}"
                log.error(final_reason)
        elif verdict_status == "ACCEPT":
            final_status = "accepted_not_promoted"
            log.info("verdict ACCEPT but auto-promote=%d dry-run=%s — not promoting",
                     args.auto_promote, args.dry_run)
        else:
            final_status = "rejected"
            final_reason = "; ".join(
                f"{g['name']}: {g['value']} vs {g['threshold']}"
                for g in verdict.get("gates", []) if not g.get("ok", True)
            )[:300]
            log.warning("verdict %s — gates failed: %s", verdict_status, final_reason)

        # Notify
        if final_status == "promoted" and NOTIFY_ON_PROMOTE:
            notify_telegram(r, (
                "🚀 <b>v15_lgbm promoted</b>\n"
                f"AUC_OOF: {verdict.get('oof_metrics_raw', {}).get('auc', 'n/a')}\n"
                f"ECE_cal: {verdict.get('oof_metrics_calibrated', {}).get('ece', 'n/a')}\n"
                f"n_positive: {verdict.get('n_positive', 'n/a')}\n"
                f"file: <code>{LIVE_PATH}</code>"
            ))
        elif final_status == "rejected" and NOTIFY_ON_REJECT:
            notify_telegram(r, (
                "⚠️ <b>v15_lgbm REJECTED</b>\n"
                f"reason: {final_reason}\n"
                f"n_samples: {verdict.get('n_samples', 'n/a')}, "
                f"n_positive: {verdict.get('n_positive', 'n/a')}\n"
                f"<i>Auto-rollback: keep prior champion.</i>"
            ))

        return 0 if final_status in ("promoted", "accepted_not_promoted", "skipped", "rejected") else 1

    finally:
        # Always emit metrics + release lock
        metrics_payload = {
            "started_at_ms": started_at,
            "ended_at_ms": _now_ms(),
            "duration_ms": _now_ms() - started_at,
            "status": final_status,
            "reason": final_reason,
            "promotion_run": promotion_run,
            "promotion_ok": promotion_ok,
            "preflight": pre_info,
            "verdict_summary": {
                "status": verdict.get("status"),
                "n_samples": verdict.get("n_samples"),
                "n_positive": verdict.get("n_positive"),
                "oof_metrics_raw": verdict.get("oof_metrics_raw"),
                "oof_metrics_calibrated": verdict.get("oof_metrics_calibrated"),
            },
        }
        write_metrics(r, metrics_payload)
        if token:
            release_lock(r, LOCK_KEY, token)
            log.info("released lock %s", LOCK_KEY)


if __name__ == "__main__":
    sys.exit(main())
