"""schema_arbiter_v1 — auto-select winning feature_schema (v14_of vs v5_of vs meta_lr_blend).

Reads training metrics for each schema, ranks by composite score, requires
K stable wins before recommending a change, then EITHER notifies (dry-run) OR
applies new champion (with backup) — depending on V_ARBITER_AUTO_APPLY.

Inputs (Redis STRING JSON):
  metrics:v14_of_train:last       → {auc_val/lr/gbdt + brier + ece + ...}
  metrics:v5_of_train:last        → {lr_cv_roc_auc, gbdt_oof_roc_auc, ...}
  metrics:v_meta_train:last       → {auc_v14, auc_v5, auc_meta, uplift_*, ...}

State (Redis):
  state:schema_arbiter:last       last cycle decision + history
  state:schema_arbiter:wins:<sch> sliding window of recent winners

Outputs:
  cfg:ml_confirm:champion         (only if V_ARBITER_AUTO_APPLY=1)
  cfg:ml_confirm:champion_prev_arbiter_<ts>  (backup)
  notify:telegram                 (always — recommendation or apply event)

Safety:
  * Default DRY-RUN (V_ARBITER_AUTO_APPLY=0).
  * Promotion always sets mode=SHADOW + enforce_share=0 — human bump to ENFORCE.
  * Requires K=3 consecutive wins (configurable).
  * Hysteresis margin (default 0.005 AUC) — avoid flapping.
"""
from __future__ import annotations

import json
import logging
import os
import time
from typing import Any

import redis

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("schema_arbiter")


def _env(k: str, d: str = "") -> str:
    return os.environ.get(k, d)


def _env_int(k: str, d: int) -> int:
    try: return int(_env(k, str(d)))
    except Exception: return d


def _env_float(k: str, d: float) -> float:
    try: return float(_env(k, str(d)))
    except Exception: return d


def _env_bool(k: str, d: bool = False) -> bool:
    return _env(k, "1" if d else "0").strip().lower() in ("1", "true", "yes", "on")


# Schema → which metrics keys to read.
_SCHEMAS = ("v14_of", "v5_of", "meta_lr_blend")

# Whitelist of cfg["kind"] values that the inference runtime
# (services.ml_confirm_gate / services.ml_confirm) knows how to load and score.
# A schema may not be promoted unless its candidate cfg "kind" is in this set —
# otherwise the worker crashloops on startup with bad_model_type.
# Keep in sync with model_loader.py dispatch + decision_policy.py / facade.py routing.
_RUNTIME_SUPPORTED_KINDS: frozenset[str] = frozenset({
    "meta_lr",
    "meta_lr_blend",
    "edge_stack_v1",
    "edge_stack_mh_v1",
    "edge_stack_v5_of",
    "util_mh_v1",
    "util_mh_fastlinear",
})


def _cfg_is_runtime_supported(cfg: dict[str, Any]) -> tuple[bool, str]:
    """Validate that a candidate champion cfg can actually be served.

    Returns (ok, reason). reason is empty when ok=True.
    Checks:
      - cfg["kind"] is in the runtime allowlist
      - cfg["model_path"] exists on disk (best-effort; absent path = skip)
    """
    if not isinstance(cfg, dict):
        return False, "cfg_not_dict"
    kind = str(cfg.get("kind", "")).strip().lower()
    if not kind:
        return False, "missing_kind"
    if kind not in _RUNTIME_SUPPORTED_KINDS:
        return False, f"unsupported_kind:{kind}"
    model_path = str(cfg.get("model_path", "") or "")
    if not model_path:
        return False, "missing_model_path"
    if not os.path.exists(model_path):
        return False, f"model_path_missing:{model_path}"
    return True, ""


def _safe_get(r: redis.Redis, key: str) -> dict[str, Any]:
    try:
        raw = r.get(key)
        if not raw: return {}
        if isinstance(raw, (bytes, bytearray)):
            raw = raw.decode("utf-8", "ignore")
        return json.loads(raw)
    except Exception as e:
        log.warning("read %s failed: %s", key, e)
        return {}


def fetch_metrics(r: redis.Redis) -> dict[str, dict[str, Any]]:
    """Pull latest training metrics for each schema."""
    return {
        "v14_of":         _safe_get(r, "metrics:v14_of_train:last"),
        "v5_of":          _safe_get(r, "metrics:v5_of_train:last"),
        "meta_lr_blend":  _safe_get(r, "metrics:v_meta_train:last"),
    }


def composite_score(schema: str, m: dict[str, Any]) -> tuple[float, dict[str, float]]:
    """Build composite score per schema. Returns (score, components).

    Weights: AUC 0.5, PR_AUC 0.3, (1-Brier_norm) 0.2.
    Brier normalized to [0,1] by dividing by 0.25 (theoretical max for 50/50).
    """
    if not m: return -1.0, {}
    # Pick canonical fields per schema (different naming conventions).
    if schema == "v14_of":
        # v14_of bundle writes lr.metrics.* and gbdt.metrics.* under nested dicts.
        lr = m.get("lr", {}).get("metrics", {}) if isinstance(m.get("lr"), dict) else {}
        auc = float(lr.get("roc_auc_mean") or 0.0)
        pr_auc = float(lr.get("pr_auc_mean") or 0.0)
        brier = float(lr.get("brier_mean") or 0.25)
    elif schema == "v5_of":
        auc = float(m.get("lr_cv_roc_auc") or 0.0)
        pr_auc = float(m.get("lr_cv_pr_auc") or 0.0)
        # v5 bundle doesn't expose brier directly in short_summary → estimate from logloss
        # or default to 0.25 if missing (neutral).
        brier = 0.25  # fallback; v5 doesn't write brier in short summary
    elif schema == "meta_lr_blend":
        auc = float(m.get("auc_meta") or 0.0)
        pr_auc = float(m.get("pr_auc_meta") or 0.0)
        brier = float(m.get("brier_meta") or 0.25)
    else:
        return -1.0, {}

    if auc <= 0: return -1.0, {"reason": "no_auc"}  # type: ignore[dict-item]

    brier_norm = min(1.0, max(0.0, 1.0 - (brier / 0.25)))  # 1.0 best, 0 worst
    score = 0.5 * auc + 0.3 * pr_auc + 0.2 * brier_norm
    return score, {"auc": auc, "pr_auc": pr_auc, "brier": brier, "brier_norm": brier_norm}


def load_state(r: redis.Redis, key: str) -> dict[str, Any]:
    return _safe_get(r, key)


def save_state(r: redis.Redis, key: str, state: dict[str, Any]) -> None:
    try:
        r.set(key, json.dumps(state, separators=(",", ":"), ensure_ascii=False))
    except Exception as e:
        log.warning("save state failed: %s", e)


def notify_telegram(r: redis.Redis, text: str) -> None:
    stream = os.environ.get("NOTIFY_STREAM", "notify:telegram")
    try:
        r.xadd(stream, {"text": text}, maxlen=200000, approximate=True)
    except Exception as e:
        log.warning("telegram notify failed: %s", e)


# ─────────────────────────────────────────────────────────────────────────────
# Decision logic
# ─────────────────────────────────────────────────────────────────────────────

def rank_schemas(metrics: dict[str, dict[str, Any]]) -> list[tuple[str, float, dict]]:
    """Return list of (schema, score, components) sorted descending."""
    ranked: list[tuple[str, float, dict]] = []
    for schema, m in metrics.items():
        score, comps = composite_score(schema, m)
        ranked.append((schema, score, comps))
    ranked.sort(key=lambda t: -t[1])
    return ranked


def current_champion_schema(r: redis.Redis) -> str:
    d = _safe_get(r, "cfg:ml_confirm:champion")
    return str(d.get("feature_schema_ver") or "unknown")


def update_win_history(state: dict[str, Any], winner: str, max_len: int) -> list[str]:
    """Append winner to sliding window. Returns new list."""
    hist = list(state.get("winner_history") or [])
    hist.append(winner)
    hist = hist[-max_len:]
    state["winner_history"] = hist
    return hist


def stable_winner(history: list[str], k: int) -> str | None:
    """Return schema if it has won K consecutive cycles, else None."""
    if len(history) < k: return None
    last_k = history[-k:]
    return last_k[0] if all(x == last_k[0] for x in last_k) else None


def fmt_components(c: dict) -> str:
    if not c: return "n/a"
    return f"auc={c.get('auc',0):.4f} pr_auc={c.get('pr_auc',0):.4f} brier={c.get('brier',0):.4f}"


def candidate_cfg_for(schema: str, r: redis.Redis) -> dict[str, Any] | None:
    """Return the latest known candidate cfg for a schema (what to copy into champion)."""
    if schema == "v14_of":
        return _safe_get(r, "cfg:ml_confirm:v14_of:lr_candidate") or None
    if schema == "v5_of":
        # v5_of doesn't write a champion-format cfg yet; build a synthetic one
        # from the latest train metrics + artifact path.
        m = _safe_get(r, "metrics:v5_of_train:last")
        if not m: return None
        return {
            "schema_version": 1,
            "feature_schema_ver": "v5_of",
            "kind": "edge_stack_v5_of",
            "mode": "SHADOW",
            "enforce_share": 0.0,
            "p_min": 0.55,
            "fail_policy": "OPEN",
            "run_id": m.get("ts_str") or f"v5_of_arbiter_{int(time.time())}",
            "model_path": f"{m.get('out_dir','')}/edge_stack_v5_of.joblib",
            "created_ms": int(m.get("ts_ms") or time.time() * 1000),
            "source": "schema_arbiter_v1",
        }
    if schema == "meta_lr_blend":
        return _safe_get(r, "cfg:ml_confirm:meta_lr_blend:candidate") or None
    return None


def apply_promotion(r: redis.Redis, new_schema: str, new_cfg: dict[str, Any]) -> dict[str, Any]:
    """Backup current champion, write new cfg with mode=SHADOW. Returns event log entry."""
    ts = int(time.time() * 1000)
    cur = _safe_get(r, "cfg:ml_confirm:champion")
    backup_key = f"cfg:ml_confirm:champion_prev_arbiter_{ts}"
    try:
        if cur:
            r.set(backup_key, json.dumps(cur, separators=(",", ":")))
    except Exception as e:
        log.error("backup failed: %s — aborting promotion", e)
        return {"applied": False, "error": f"backup_failed: {e}"}

    # Force-safe defaults on the new champion.
    new_cfg = dict(new_cfg)
    new_cfg["mode"] = "SHADOW"
    new_cfg["enforce_share"] = 0.0
    new_cfg["promoted_by"] = "schema_arbiter_v1"
    new_cfg["promoted_ms"] = ts
    new_cfg["promoted_from_schema"] = cur.get("feature_schema_ver", "unknown") if cur else "none"

    try:
        r.set("cfg:ml_confirm:champion", json.dumps(new_cfg, separators=(",", ":"), ensure_ascii=False))
    except Exception as e:
        log.error("champion write failed: %s", e)
        return {"applied": False, "error": f"write_failed: {e}", "backup_key": backup_key}

    return {
        "applied": True,
        "backup_key": backup_key,
        "ts_ms": ts,
        "from_schema": cur.get("feature_schema_ver", "unknown") if cur else "none",
        "to_schema": new_schema,
    }


# ─────────────────────────────────────────────────────────────────────────────
# Main cycle
# ─────────────────────────────────────────────────────────────────────────────

def run_cycle(r: redis.Redis) -> dict[str, Any]:
    """One arbiter cycle: rank schemas, decide, optionally apply."""
    state_key = _env("V_ARBITER_STATE_KEY", "state:schema_arbiter:last")
    auto_apply = _env_bool("V_ARBITER_AUTO_APPLY", False)
    stability_k = _env_int("V_ARBITER_STABILITY_K", 3)
    history_max = _env_int("V_ARBITER_HISTORY_MAX", 12)
    hysteresis = _env_float("V_ARBITER_HYSTERESIS_AUC", 0.005)
    skip_notify_h = _env_float("V_ARBITER_SKIP_NOTIFY_HOURS", 6.0)

    metrics = fetch_metrics(r)
    ranked = rank_schemas(metrics)
    cur_schema = current_champion_schema(r)
    log.info("current champion schema: %s", cur_schema)
    for sch, score, comps in ranked:
        log.info("  %s: score=%.4f %s", sch, score, fmt_components(comps))

    state = load_state(r, state_key)
    raw_winner = ranked[0][0] if ranked and ranked[0][1] > 0 else None
    if raw_winner is None:
        notify_telegram(r, "🟡 schema_arbiter: no schema has metrics, skipping")
        save_state(r, state_key, state)
        return {"status": "no_metrics"}

    history = update_win_history(state, raw_winner, history_max)
    winner = stable_winner(history, stability_k)

    now_ms = int(time.time() * 1000)
    decision: dict[str, Any] = {
        "ts_ms": now_ms,
        "current_schema": cur_schema,
        "raw_winner": raw_winner,
        "stable_winner": winner,
        "stability_k": stability_k,
        "history": history,
        "ranking": [
            {"schema": s, "score": round(sc, 6), **{k: round(v, 6) for k, v in c.items() if isinstance(v, (int, float))}}
            for s, sc, c in ranked
        ],
        "auto_apply": auto_apply,
    }

    # Hysteresis check: winner must beat current by margin.
    winner_score = next((sc for s, sc, _ in ranked if s == winner), 0.0)
    cur_score = next((sc for s, sc, _ in ranked if s == cur_schema), 0.0)
    decision["winner_score"] = winner_score
    decision["current_score"] = cur_score
    decision["margin"] = winner_score - cur_score

    if winner is None:
        decision["action"] = "no_stable_winner"
    elif winner == cur_schema:
        decision["action"] = "no_change_already_champion"
    elif (winner_score - cur_score) < hysteresis:
        decision["action"] = "no_change_below_hysteresis"
        decision["hysteresis"] = hysteresis
    else:
        decision["action"] = "promote_recommended"
        cfg = candidate_cfg_for(winner, r)
        if cfg is None:
            decision["action"] = "promote_failed_no_candidate_cfg"
        else:
            ok_runtime, runtime_reason = _cfg_is_runtime_supported(cfg)
            if not ok_runtime:
                decision["action"] = "promote_blocked_runtime_unsupported"
                decision["runtime_block_reason"] = runtime_reason
                decision["winner_kind"] = str(cfg.get("kind", ""))
                text = (
                    f"⛔ schema_arbiter: BLOCKED promote {cur_schema} → {winner}\n"
                    f"  reason: {runtime_reason}\n"
                    f"  kind={cfg.get('kind')!r} model_path={cfg.get('model_path')!r}\n"
                    f"  add handler in services.ml_confirm_gate before promoting"
                )
                notify_telegram(r, text)
            else:
                text = (
                    f"🟢 schema_arbiter: RECOMMENDS promote {cur_schema} → {winner}\n"
                    f"  score: {cur_score:.4f} → {winner_score:.4f} (Δ {winner_score-cur_score:+.4f})\n"
                    f"  stable {stability_k} cycles. auto_apply={auto_apply}"
                )
                if auto_apply:
                    event = apply_promotion(r, winner, cfg)
                    decision["promotion_event"] = event
                    if event.get("applied"):
                        text = f"✅ schema_arbiter: PROMOTED {cur_schema} → {winner} (mode=SHADOW)\n  backup={event['backup_key']}\n  score Δ {winner_score-cur_score:+.4f}"
                    else:
                        text = f"❌ schema_arbiter: APPLY FAILED for {winner}: {event.get('error')}"
                notify_telegram(r, text)

    # Throttle "no change" notifications.
    last_notify_ms = float(state.get("last_notify_ms") or 0)
    if decision["action"].startswith("no_change") or decision["action"] == "no_stable_winner":
        if (now_ms - last_notify_ms) > skip_notify_h * 3_600_000:
            notify_telegram(
                r,
                f"ℹ️ schema_arbiter: {decision['action']} (champion stays {cur_schema}, "
                f"raw_winner={raw_winner}, history={history[-5:]})",
            )
            state["last_notify_ms"] = now_ms

    state["last_decision"] = decision
    save_state(r, state_key, state)
    log.info("decision: %s", decision["action"])
    return decision


def main() -> int:
    redis_url = _env("REDIS_URL", "redis://redis-worker-1:6379/0")
    r = redis.from_url(redis_url, socket_timeout=3.0, decode_responses=False)
    log.info("schema_arbiter connected: %s", redis_url)
    d = run_cycle(r)
    print(json.dumps({
        "action": d.get("action"),
        "current_schema": d.get("current_schema"),
        "stable_winner": d.get("stable_winner"),
        "margin": d.get("margin"),
        "ranking": d.get("ranking"),
    }, indent=2, default=str))
    return 0


def loop_main() -> int:
    import signal as _signal
    interval = _env_int("V_ARBITER_INTERVAL_SEC", 3600)
    enabled = _env_bool("V_ARBITER_ENABLED", True)
    stop = {"flag": False}
    def _sig(_a, _b): stop["flag"] = True
    _signal.signal(_signal.SIGTERM, _sig)
    _signal.signal(_signal.SIGINT, _sig)
    log.info("schema_arbiter loop starting (interval=%ds enabled=%s)", interval, enabled)
    while not stop["flag"]:
        if not enabled:
            log.info("V_ARBITER_ENABLED=0, sleeping")
        else:
            try: main()
            except Exception as e:
                log.exception("cycle failed: %s", e)
        for _ in range(interval):
            if stop["flag"]: break
            time.sleep(1)
    log.info("schema_arbiter stopped")
    return 0


if __name__ == "__main__":
    import sys
    if "--loop" in sys.argv:
        sys.argv = [a for a in sys.argv if a != "--loop"]
        raise SystemExit(loop_main())
    raise SystemExit(main())
