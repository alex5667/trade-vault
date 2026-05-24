"""Auto-promote orchestrator for v14_of champion (LR) + challenger (GBDT).

What it does (every interval, default 1h):
  1. Read `metrics:v14_of_train:last` for fresh candidate model metrics
     (written by nightly_v14_of_train_bundle).
  2. (Optional) Query Prometheus for live champion/challenger performance
     (ml_outcome_*{kind=meta_lr}, ml_outcome_*{kind=edge_stack_v1}).
  3. Evaluate promotion gates per role:
        Champion (meta_lr):
          * candidate brier_mean ≤ V14_PROMOTE_BRIER_MAX
          * candidate pr_auc_mean ≥ existing champion live PR_AUC − ε (when live metric is available)
          * candidate finished_at fresh (≤ FRESHNESS_HOURS)
        Challenger (edge_stack_v1):
          * candidate brier_oof ≤ V14_PROMOTE_BRIER_MAX
          * candidate pr_auc_oof ≥ current production challenger PR_AUC (when known)
          * live ECE gap ≤ V14_PROMOTE_ECE_MAX  OR  live ECE not yet available
  4. On pass: backup existing champion/challenger cfg → `*_prev_auto_<ts>` key,
     SET new cfg with `mode=SHADOW, enforce_share=0`. NO enforce auto-bump (human gate).
  5. Emit a Telegram message via `notify:telegram` for both pass and skip outcomes
     (skip messages are throttled to once per AUTO_SKIP_NOTIFY_HOURS).

Safety:
  - `V14_AUTO_PROMOTE_ENABLED=0` (default) — dry-run only, only sends Telegram message
    with planned action but does NOT write Redis cfgs.
  - `V14_AUTO_PROMOTE_ENABLED=1` — apply promotion AND notify Telegram.
  - Promotion always sets mode=SHADOW first; bumping to ENFORCE remains a human action.

Env:
  REDIS_URL                     redis://redis-worker-1:6379/0
  V14_AUTO_PROMOTE_ENABLED      0 | 1
  V14_AUTO_INTERVAL_SEC         3600
  V14_AUTO_FRESHNESS_HOURS      12      reject candidate older than this
  V14_TRAIN_METRICS_KEY         metrics:v14_of_train:last
  V14_CHAMPION_KEY              cfg:ml_confirm:champion
  V14_CHALLENGER_KEY            cfg:ml_confirm:challenger
  V14_PROMOTE_BRIER_MAX         0.20
  V14_PROMOTE_ECE_MAX           0.10
  V14_PROMOTE_PR_AUC_EPSILON    0.02    allow new model to be ε worse than live PR AUC
  PROMETHEUS_URL                http://scanner-prometheus:9090
  NOTIFY_STREAM                 notify:telegram
  V14_AUTO_SKIP_NOTIFY_HOURS    6        suppress repeated skip notifications
  V14_AUTO_STATE_KEY            state:v14_of_auto_promote
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlencode
from urllib.request import urlopen

import redis

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("v14_of_auto_promote")


def _env(k: str, d: str = "") -> str:
    return os.environ.get(k, d)


def _env_int(k: str, d: int) -> int:
    try:
        return int(_env(k, str(d)))
    except Exception:
        return d


def _env_float(k: str, d: float) -> float:
    try:
        return float(_env(k, str(d)))
    except Exception:
        return d


def _env_bool(k: str, d: bool = False) -> bool:
    v = _env(k, "1" if d else "0").strip().lower()
    return v in ("1", "true", "yes", "on")


# ---------------------------------------------------------------------------
# Prometheus query helper
# ---------------------------------------------------------------------------

def _prom_query(prom_url: str, q: str, timeout: float = 5.0) -> float | None:
    """Run instant query, return scalar value or None."""
    try:
        params = urlencode({"query": q})
        url = f"{prom_url.rstrip('/')}/api/v1/query?{params}"
        with urlopen(url, timeout=timeout) as resp:
            data = json.loads(resp.read().decode("utf-8"))
        if data.get("status") != "success":
            return None
        res = data.get("data", {}).get("result") or []
        if not res:
            return None
        v = res[0].get("value", [None, None])[1]
        return float(v) if v is not None else None
    except Exception as e:
        log.debug("prom query failed q=%s err=%s", q[:120], e)
        return None


# ---------------------------------------------------------------------------
# Decision logic
# ---------------------------------------------------------------------------

@dataclass
class PromoteDecision:
    role: str           # "champion" | "challenger"
    kind: str           # "meta_lr" | "edge_stack_v1"
    apply: bool
    reasons: list[str]  # green/red gate decisions
    candidate_cfg: dict[str, Any] | None = None
    prev_cfg_snapshot: str | None = None


def _is_fresh(payload: dict[str, Any], max_age_hours: float) -> bool:
    fin = int(payload.get("finished_at_ms", 0) or 0)
    if fin <= 0:
        return False
    age_h = (time.time() * 1000 - fin) / 1000.0 / 3600.0
    return age_h <= max_age_hours


def _read_live_champion_kind(r: redis.Redis, champion_key: str) -> str:
    """Read current champion `kind` from Redis cfg for Prometheus label matching.

    Live deployment may run `meta_lr_blend` (or future variants); hardcoding
    `meta_lr` in the PromQL filter silently breaks the live-vs-candidate
    comparison gate. Fall back to `meta_lr` only if cfg is unreadable.
    """
    try:
        raw = r.get(champion_key)
        if not raw:
            return "meta_lr"
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8", "ignore")
        cfg = json.loads(raw)
        k = str(cfg.get("kind") or "").strip()
        return k or "meta_lr"
    except Exception as e:
        log.warning("read champion kind failed: %s — falling back to meta_lr", e)
        return "meta_lr"


def _eval_champion(
    *,
    payload: dict[str, Any],
    prom_url: str,
    brier_max: float,
    pr_auc_epsilon: float,
    live_kind: str = "meta_lr",
) -> PromoteDecision:
    reasons: list[str] = []
    apply = True

    lr_info = payload.get("lr") or {}
    if not lr_info:
        return PromoteDecision("champion", "meta_lr", False, ["no_lr_in_metrics"], None)

    m = lr_info.get("metrics") or {}
    brier = float(m.get("brier_mean", 1.0))
    pr_auc = float(m.get("pr_auc_mean", 0.0))

    if brier > brier_max:
        apply = False
        reasons.append(f"brier_too_high({brier:.4f}>{brier_max:.4f})")
    else:
        reasons.append(f"brier_ok({brier:.4f})")

    live_pr = _prom_query(prom_url,
                          f'ml_outcome_precision_top5pct{{kind="{live_kind}"}}')
    if live_pr is not None:
        if pr_auc + pr_auc_epsilon < live_pr:
            apply = False
            reasons.append(
                f"candidate_pr_auc_below_live(cand={pr_auc:.4f}, live={live_pr:.4f}, ε={pr_auc_epsilon:.4f})"
            )
        else:
            reasons.append(f"pr_auc_vs_live_ok(cand={pr_auc:.4f}, live={live_pr:.4f})")
    else:
        reasons.append("live_pr_auc_unavailable")

    candidate_cfg = {
        # cfg envelope shape version (Redis cfg:ml_confirm:* DTO). Validated
        # strictly by services/ml_confirm/champion_cfg.py:_as_int(schema_version)
        # — must remain 1. NOT the model-file `schema_version`
        # (= feature_schema_version, 14/15) read by MetaModelLR.load.
        "schema_version": 1,
        "kind": "meta_lr",
        "run_id": lr_info.get("run_id"),
        "created_ms": int(time.time() * 1000),
        "model_path": lr_info.get("path"),
        "mode": "SHADOW",
        "enforce_share": 0.0,
        "p_min": 0.5,
        "feature_schema_ver": "v14_of",
        "fail_policy": "OPEN",
        "model_signature": lr_info.get("signature", ""),
        "metrics": m,
    }
    return PromoteDecision("champion", "meta_lr", apply, reasons, candidate_cfg)


def _eval_challenger(
    *,
    payload: dict[str, Any],
    prom_url: str,
    brier_max: float,
    ece_max: float,
    pr_auc_epsilon: float,
) -> PromoteDecision:
    reasons: list[str] = []
    apply = True

    gbdt_info = payload.get("gbdt") or {}
    if not gbdt_info:
        return PromoteDecision("challenger", "edge_stack_v1", False, ["no_gbdt_in_metrics"], None)

    m = gbdt_info.get("metrics") or {}
    brier = float(m.get("brier_oof", 1.0))
    pr_auc = float(m.get("pr_auc_oof", 0.0))

    if brier > brier_max:
        apply = False
        reasons.append(f"brier_too_high({brier:.4f}>{brier_max:.4f})")
    else:
        reasons.append(f"brier_ok({brier:.4f})")

    live_pr = _prom_query(prom_url,
                          'ml_outcome_precision_top5pct{kind="edge_stack_v1"}')
    if live_pr is not None:
        if pr_auc + pr_auc_epsilon < live_pr:
            apply = False
            reasons.append(
                f"candidate_pr_auc_below_live(cand={pr_auc:.4f}, live={live_pr:.4f})"
            )
        else:
            reasons.append(f"pr_auc_vs_live_ok(cand={pr_auc:.4f}, live={live_pr:.4f})")
    else:
        reasons.append("live_pr_auc_unavailable")

    live_ece = _prom_query(prom_url, 'ml_outcome_ece{kind="edge_stack_v1"}')
    if live_ece is not None:
        if live_ece > ece_max:
            apply = False
            reasons.append(f"live_ece_too_high({live_ece:.4f}>{ece_max:.4f})")
        else:
            reasons.append(f"live_ece_ok({live_ece:.4f})")
    else:
        reasons.append("live_ece_unavailable")

    candidate_cfg = {
        # cfg envelope shape version (see champion_cfg.py:_as_int validator).
        "schema_version": 1,
        "kind": "edge_stack_v1",
        "run_id": gbdt_info.get("run_id"),
        "created_ms": int(time.time() * 1000),
        "model_path": gbdt_info.get("path"),
        "mode": "SHADOW",
        "enforce_share": 0.0,
        "p_min": 0.5,
        "feature_schema_ver": "v14_of",
        "fail_policy": "OPEN",
        "metrics": m,
    }
    return PromoteDecision("challenger", "edge_stack_v1", apply, reasons, candidate_cfg)


# ---------------------------------------------------------------------------
# Apply + notify
# ---------------------------------------------------------------------------

def _apply_promotion(
    r: redis.Redis,
    target_key: str,
    cfg: dict[str, Any],
) -> str | None:
    """Backup existing cfg → SET new cfg. Returns backup key or None."""
    ts = int(time.time())
    backup_key = f"{target_key}_prev_auto_{ts}"
    try:
        prev = r.get(target_key)
        if prev:
            r.set(backup_key, prev)
    except Exception as e:
        log.warning("backup write failed: %s", e)
        return None
    try:
        r.set(target_key, json.dumps(cfg, separators=(",", ":")))
    except Exception as e:
        log.error("set new cfg failed: %s", e)
        return None
    return backup_key


def _format_telegram(
    *,
    decisions: list[PromoteDecision],
    payload: dict[str, Any],
    dry_run: bool,
    applied: dict[str, str | None],
) -> str:
    """Multi-line text for Telegram."""
    mode = "DRY-RUN" if dry_run else "APPLY"
    lines = [
        f"🤖 *v14_of auto-promote* ({mode})",
        f"train_status: `{payload.get('status', '?')}` elapsed: `{payload.get('elapsed_sec', '?')}s`",
        f"dataset rows: `{(payload.get('dataset') or {}).get('joined_rows', '?')}`"
        f"  pos_rate: `{(payload.get('dataset') or {}).get('pos_rate', '?'):.4f}`",
        "",
    ]
    for d in decisions:
        emoji = "✅" if d.apply else "⏸"
        if d.apply:
            applied_msg = "applied" if not dry_run else "dry-run plan"
            backup = applied.get(d.role)
            if backup:
                applied_msg += f", backup→`{backup}`"
        else:
            applied_msg = "skipped"
        lines.append(f"{emoji} *{d.role}* (`{d.kind}`) — {applied_msg}")
        for r in d.reasons:
            lines.append(f"  • {r}")
        if d.candidate_cfg:
            lines.append(f"  • run_id: `{d.candidate_cfg.get('run_id')}`")
        lines.append("")

    return "\n".join(lines).rstrip()


def _notify_telegram(r: redis.Redis, *, notify_stream: str, text: str, subtype: str = "info") -> bool:
    try:
        r.xadd(
            notify_stream,
            {
                "type": "report",
                "subtype": f"v14_of_auto_promote_{subtype}",
                "ts_ms": str(int(time.time() * 1000)),
                "text": text,
            },
            maxlen=5000,
            approximate=True,
        )
        return True
    except Exception as e:
        log.error("notify xadd failed: %s", e)
        return False


# ---------------------------------------------------------------------------
# State (throttle skip notifications)
# ---------------------------------------------------------------------------

def _load_state(r: redis.Redis, key: str) -> dict[str, Any]:
    try:
        raw = r.get(key)
        if raw:
            if isinstance(raw, bytes):
                raw = raw.decode("utf-8", "ignore")
            return json.loads(raw)
    except Exception as e:
        log.debug("state load failed: %s", e)
    return {}


def _save_state(r: redis.Redis, key: str, state: dict[str, Any]) -> None:
    try:
        r.set(key, json.dumps(state, separators=(",", ":")))
    except Exception as e:
        log.debug("state save failed: %s", e)


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

def run_once(*, dry_run: bool | None = None) -> dict[str, Any]:
    redis_url = _env("REDIS_URL", "redis://redis-worker-1:6379/0")
    metrics_key = _env("V14_TRAIN_METRICS_KEY", "metrics:v14_of_train:last")
    champion_key = _env("V14_CHAMPION_KEY", "cfg:ml_confirm:champion")
    challenger_key = _env("V14_CHALLENGER_KEY", "cfg:ml_confirm:challenger")
    notify_stream = _env("NOTIFY_STREAM", "notify:telegram")
    state_key = _env("V14_AUTO_STATE_KEY", "state:v14_of_auto_promote")
    prom_url = _env("PROMETHEUS_URL", "http://scanner-prometheus:9090")

    auto_enabled = _env_bool("V14_AUTO_PROMOTE_ENABLED", False)
    if dry_run is None:
        dry_run = not auto_enabled

    # OE-readiness gate: block any apply until signals:of:inputs has ≥7d span
    # and ≥70% coverage of Group OE required fields. Without this, the
    # challenger model is trained on 0.0-padded Group OE keys (silent skew).
    # Override only for incident response: V14_SKIP_OE_READINESS_GATE=1.
    skip_oe_gate = _env_bool("V14_SKIP_OE_READINESS_GATE", False)
    oe_status: dict[str, Any] = {"skipped": True} if skip_oe_gate else {}
    if not skip_oe_gate:
        try:
            from tools.check_v14_oe_readiness import evaluate_readiness
            oe_status = evaluate_readiness(redis_client=redis.Redis.from_url(
                redis_url, decode_responses=True
            ))
        except Exception as e:
            log.error("OE readiness check crashed: %s — blocking apply", e)
            oe_status = {"ready": False, "reasons": [f"check_crashed: {e}"]}
        if not oe_status.get("ready"):
            log.warning(
                "OE readiness gate not met: %s — forcing dry_run",
                "; ".join(oe_status.get("reasons", ["unknown"])),
            )
            dry_run = True

    # Runtime-schema guard (audit 2026-05-19): candidate feature_schema_ver
    # must match runtime ML_FEATURE_SCHEMA_VER. Otherwise the freshly-promoted
    # champion cfg references a model whose features the scoring services
    # cannot vectorize → forced SHADOW via of_confirm_engine schema_guard,
    # while the auto-promote run looks "successful" (silent ML enforce dropout).
    runtime_schema_ver = _env("ML_FEATURE_SCHEMA_VER", "").strip()
    require_runtime_match = _env_bool("V14_REQUIRE_RUNTIME_MATCH", True)
    runtime_mismatch_reasons: list[str] = []

    brier_max = _env_float("V14_PROMOTE_BRIER_MAX", 0.20)
    ece_max = _env_float("V14_PROMOTE_ECE_MAX", 0.10)
    pr_auc_eps = _env_float("V14_PROMOTE_PR_AUC_EPSILON", 0.02)
    freshness_h = _env_float("V14_AUTO_FRESHNESS_HOURS", 12.0)
    skip_throttle_h = _env_float("V14_AUTO_SKIP_NOTIFY_HOURS", 6.0)

    r = redis.Redis.from_url(redis_url, decode_responses=False)

    raw = r.get(metrics_key)
    if not raw:
        log.warning("no metrics blob at %s — nothing to promote", metrics_key)
        return {"status": "no_metrics"}
    if isinstance(raw, bytes):
        raw = raw.decode("utf-8", "ignore")
    try:
        payload = json.loads(raw)
    except Exception as e:
        log.error("metrics parse failed: %s", e)
        return {"status": "parse_error"}

    if not _is_fresh(payload, freshness_h):
        log.info("candidate too old (>%sh), skipping", freshness_h)
        return {"status": "stale_candidate"}

    live_champion_kind = _read_live_champion_kind(r, champion_key)
    decision_champ = _eval_champion(
        payload=payload, prom_url=prom_url,
        brier_max=brier_max, pr_auc_epsilon=pr_auc_eps,
        live_kind=live_champion_kind,
    )
    decision_chal = _eval_challenger(
        payload=payload, prom_url=prom_url,
        brier_max=brier_max, ece_max=ece_max, pr_auc_epsilon=pr_auc_eps,
    )
    decisions = [decision_champ, decision_chal]

    # Apply runtime-schema guard to decisions.
    if runtime_schema_ver:
        for dec in decisions:
            cand_ver = ""
            if dec.candidate_cfg:
                cand_ver = str(dec.candidate_cfg.get("feature_schema_ver", "")).strip()
            if cand_ver and cand_ver != runtime_schema_ver:
                reason = (
                    f"runtime_schema_mismatch(runtime={runtime_schema_ver},"
                    f" candidate={cand_ver})"
                )
                dec.reasons.append(reason)
                if require_runtime_match:
                    dec.apply = False
                runtime_mismatch_reasons.append(f"{dec.role}:{reason}")
        if runtime_mismatch_reasons and require_runtime_match:
            log.warning(
                "Runtime schema mismatch — forcing dry_run: %s",
                "; ".join(runtime_mismatch_reasons),
            )
            dry_run = True
    else:
        log.warning(
            "ML_FEATURE_SCHEMA_VER env not set; runtime-schema guard disabled."
        )

    applied: dict[str, str | None] = {}
    if not dry_run:
        for dec in decisions:
            if not dec.apply or not dec.candidate_cfg:
                continue
            target = champion_key if dec.role == "champion" else challenger_key
            backup = _apply_promotion(r, target, dec.candidate_cfg)
            applied[dec.role] = backup
            log.info("promoted %s (%s) → %s, backup=%s", dec.role, dec.kind, target, backup)

    # Throttle skip-only notifications: if no apply and no fresh metrics shift, skip notify.
    any_apply = any(d.apply for d in decisions)
    state = _load_state(r, state_key)
    last_notify_ms = int(state.get("last_notify_ms", 0) or 0)
    now_ms = int(time.time() * 1000)
    should_notify = any_apply or (
        (now_ms - last_notify_ms) / 1000.0 / 3600.0 >= skip_throttle_h
    )

    if should_notify:
        text = _format_telegram(
            decisions=decisions, payload=payload, dry_run=dry_run, applied=applied,
        )
        log.info("notify:\n%s", text)
        _notify_telegram(
            r, notify_stream=notify_stream, text=text,
            subtype=("apply" if (any_apply and not dry_run) else "report"),
        )
        state["last_notify_ms"] = now_ms
        _save_state(r, state_key, state)
    else:
        log.info("skip notify (throttled, last %.1fh ago)", (now_ms - last_notify_ms) / 1000.0 / 3600.0)

    return {
        "status": "ok",
        "dry_run": dry_run,
        "applied": applied,
        "oe_readiness": {
            "ready": bool(oe_status.get("ready")),
            "skipped": bool(oe_status.get("skipped")),
            "coverage": oe_status.get("coverage"),
            "span_days": oe_status.get("span_days"),
            "reasons": oe_status.get("reasons", []),
        },
        "runtime_schema": {
            "runtime_ver": runtime_schema_ver,
            "require_match": require_runtime_match,
            "mismatches": runtime_mismatch_reasons,
        },
        "decisions": [
            {"role": d.role, "kind": d.kind, "apply": d.apply, "reasons": d.reasons}
            for d in decisions
        ],
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--once", action="store_true", help="run a single evaluation and exit")
    ap.add_argument("--dry-run", action="store_true", help="never write Redis cfg (overrides env)")
    args = ap.parse_args()

    if args.once:
        result = run_once(dry_run=True if args.dry_run else None)
        print(json.dumps(result, indent=2, default=str))
        sys.exit(0 if result.get("status") in ("ok", "no_metrics", "stale_candidate") else 1)

    interval = _env_int("V14_AUTO_INTERVAL_SEC", 3600)
    log.info("auto-promote loop interval=%ds", interval)
    while True:
        try:
            run_once(dry_run=True if args.dry_run else None)
        except Exception as e:
            log.error("run_once crashed: %s", e)
        time.sleep(interval)


if __name__ == "__main__":
    main()
