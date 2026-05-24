#!/usr/bin/env python3
"""ml_confirm posterior calibrator autopilot (2026-05-23 follow-up).

Generalises `meta_lr_blend_calibrator_refit_v1.py` to ALL kinds present
in the active `cfg:ml_confirm:{champion,challenger}` Redis configs.
For each discovered (kind, model_path) it:

  1. Decides whether to scope refit by `target_version=<kind>` or accept
     any version (`target_version=""`). Bootstrap-friendly: starts in
     broad mode until ≥ML_AUTOPILOT_AUTO_SWITCH_AFTER_N closed trades
     carry `ml_version` matching the kind name — then locks (sticky).
  2. Runs `fit_and_evaluate` against `trades:closed` over a configurable
     look-back window.
  3. If accepted (Brier and ECE improvements), atomically writes
     `calibrator.json` next to `model_path`. The sibling-discovery branch
     in `services/ml_confirm/config_loader.py:_load_calibrator_sync`
     picks it up on the next cfg cache refresh — for any kind.
  4. Emits per-kind Prometheus metrics + persists run state to
     `cfg:ml_calibrator_autopilot:state:<kind>` (sticky lock + last run).

Manual override: ENV `ML_AUTOPILOT_FORCE_TARGET_VER_<KIND>=<value>`
(case-insensitive kind label) bypasses the auto-switch logic.

ENV
---
ML_CALIBRATOR_AUTOPILOT_ENABLED   Master switch (default 1)
ML_AUTOPILOT_INTERVAL_S           Refit cadence (default 21600 = 6h)
ML_AUTOPILOT_LOOKBACK_H           Window in hours (default 168 = 7d)
ML_AUTOPILOT_MIN_N                Min samples per kind (default 300)
ML_AUTOPILOT_BRIER_DELTA          Required Brier improvement (default 0.005)
ML_AUTOPILOT_ECE_DELTA            Required ECE improvement (default 0.01)
ML_AUTOPILOT_AUTO_SWITCH_AFTER_N  Lock target_version=kind once kind-matched
                                  rows in window ≥ N (default 300)
ML_AUTOPILOT_SKIP_KINDS           CSV of kinds to skip (default empty)
ML_AUTOPILOT_PORT                 Prometheus :port (default 9868)
ML_AUTOPILOT_CHAMPION_KEY         Redis key for champion (default cfg:ml_confirm:champion)
ML_AUTOPILOT_CHALLENGER_KEY       Redis key for challenger (default cfg:ml_confirm:challenger)
ML_AUTOPILOT_STATE_PREFIX         Redis state key prefix (default cfg:ml_calibrator_autopilot:state:)
"""
from __future__ import annotations

import json
import logging
import os
import sys
import time
from typing import Any

log = logging.getLogger("ml_calibrator_autopilot")


_METRICS: dict[str, object] = {}


def _init_metrics() -> None:
    if _METRICS:
        return
    try:
        from prometheus_client import Counter, Gauge
        _METRICS["last_ts"] = Gauge(
            "ml_calibrator_autopilot_last_run_ts",
            "Unix ts of last refit attempt per kind", ["kind"],
        )
        _METRICS["accepted"] = Gauge(
            "ml_calibrator_autopilot_accepted",
            "1 if last refit was accepted, 0 otherwise", ["kind"],
        )
        _METRICS["n"] = Gauge(
            "ml_calibrator_autopilot_n",
            "Sample count of last refit", ["kind"],
        )
        _METRICS["brier_delta"] = Gauge(
            "ml_calibrator_autopilot_brier_delta",
            "Brier improvement (raw - cal)", ["kind"],
        )
        _METRICS["ece_delta"] = Gauge(
            "ml_calibrator_autopilot_ece_delta",
            "ECE improvement (raw - cal)", ["kind"],
        )
        _METRICS["mode"] = Gauge(
            "ml_calibrator_autopilot_target_version_mode",
            "target_version mode: 0=broad (accept any), 1=kind-locked",
            ["kind"],
        )
        _METRICS["kinds_discovered"] = Gauge(
            "ml_calibrator_autopilot_kinds_discovered",
            "Number of distinct (kind, model_path) pairs discovered in last pass",
        )
        _METRICS["runs"] = Counter(
            "ml_calibrator_autopilot_runs_total",
            "Refit attempts by (kind, outcome)",
            ["kind", "outcome"],
        )
        _METRICS["discovery_errors"] = Counter(
            "ml_calibrator_autopilot_discovery_errors_total",
            "Errors when reading champion/challenger cfg from Redis",
        )
    except Exception as e:
        log.warning("metric init failed: %s", e)


def _emit(*, kind: str, accepted: bool, n: int, brier_delta: float,
          ece_delta: float, locked: bool, reason: str) -> None:
    _init_metrics()
    try:
        if _METRICS:
            _METRICS["last_ts"].labels(kind=kind).set(int(time.time()))  # type: ignore[attr-defined]
            _METRICS["accepted"].labels(kind=kind).set(1 if accepted else 0)  # type: ignore[attr-defined]
            _METRICS["n"].labels(kind=kind).set(n)  # type: ignore[attr-defined]
            _METRICS["brier_delta"].labels(kind=kind).set(brier_delta)  # type: ignore[attr-defined]
            _METRICS["ece_delta"].labels(kind=kind).set(ece_delta)  # type: ignore[attr-defined]
            _METRICS["mode"].labels(kind=kind).set(1 if locked else 0)  # type: ignore[attr-defined]
            outcome = "accepted" if accepted else (reason or "rejected").split("(")[0]
            _METRICS["runs"].labels(kind=kind, outcome=outcome).inc()  # type: ignore[attr-defined]
    except Exception as e:
        log.warning("metric emit failed kind=%s: %s", kind, e)


def _discover_kinds(r: Any) -> list[tuple[str, str]]:
    """Read champion + challenger ml_confirm cfgs, return [(kind, model_path), ...].

    Deduplicates by kind (champion overrides challenger if same kind).
    Returns empty list on any read failure (fail-soft).
    """
    keys = [
        os.getenv("ML_AUTOPILOT_CHAMPION_KEY", "cfg:ml_confirm:champion"),
        os.getenv("ML_AUTOPILOT_CHALLENGER_KEY", "cfg:ml_confirm:challenger"),
    ]
    found: dict[str, str] = {}
    for key in keys:
        try:
            raw = r.get(key)
            if not raw:
                continue
            if isinstance(raw, bytes):
                raw = raw.decode("utf-8", "ignore")
            cfg = json.loads(raw)
            if not isinstance(cfg, dict):
                continue
            kind = str(cfg.get("kind") or "").strip()
            model_path = str(cfg.get("model_path") or "").strip()
            if kind and model_path and kind not in found:
                found[kind] = model_path
        except Exception as e:
            log.warning("discovery read failed key=%s: %s", key, e)
            try:
                if _METRICS:
                    _METRICS["discovery_errors"].inc()  # type: ignore[attr-defined]
            except Exception:
                pass
    return [(k, p) for k, p in found.items()]


def _load_state(r: Any, kind: str) -> dict[str, Any]:
    state_key = os.getenv("ML_AUTOPILOT_STATE_PREFIX", "cfg:ml_calibrator_autopilot:state:") + kind
    try:
        raw = r.get(state_key)
        if not raw:
            return {}
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8", "ignore")
        st = json.loads(raw)
        return st if isinstance(st, dict) else {}
    except Exception:
        return {}


def _save_state(r: Any, kind: str, state: dict[str, Any]) -> None:
    state_key = os.getenv("ML_AUTOPILOT_STATE_PREFIX", "cfg:ml_calibrator_autopilot:state:") + kind
    try:
        r.set(state_key, json.dumps(state, separators=(",", ":")))
    except Exception as e:
        log.warning("state save failed kind=%s: %s", kind, e)


def _decide_target_version(
    r: Any, kind: str, pairs_kind_matched: int, switch_after_n: int,
) -> tuple[str, bool, str]:
    """Return (target_version, locked, reason).

    Priority order:
      1. ML_AUTOPILOT_FORCE_TARGET_VER_<KIND>  — manual override
      2. Existing sticky lock in Redis state    — kind-locked, return kind
      3. ml_version match count >= switch_after_n → promote to kind-locked
         (persist in Redis state)
      4. Default: broad mode (target_version="")
    """
    override = os.getenv(f"ML_AUTOPILOT_FORCE_TARGET_VER_{kind.upper()}", "")
    if override:
        return override, True, "env_override"

    state = _load_state(r, kind)
    if bool(state.get("locked")):
        return kind, True, "sticky_lock"

    if pairs_kind_matched >= switch_after_n:
        new_state = {
            "locked": True,
            "lock_ts_ms": int(time.time() * 1000),
            "lock_n": pairs_kind_matched,
            "kind": kind,
        }
        _save_state(r, kind, new_state)
        log.info(
            "kind=%s: auto-switched to kind-locked target_version (n_matched=%d >= %d)",
            kind, pairs_kind_matched, switch_after_n,
        )
        return kind, True, "auto_switch"

    return "", False, "broad_bootstrap"


def _count_kind_matched(pairs_all: list[tuple[float, int, str]], kind: str) -> int:
    """Count rows where ml_version contains kind substring (case-insensitive)."""
    kind_lc = kind.lower()
    return sum(1 for _, _, v in pairs_all if kind_lc in (v or "").lower())


def _read_trades_closed_with_version(
    r: Any, *, stream: str, lookback_hours: int, limit: int = 100_000,
) -> list[tuple[float, int, str]]:
    """Like tools.refit_meta_lr_blend_calibrator._read_trades_closed but also
    returns ml_version per row (needed for auto-switch decision).
    """
    from tools.refit_meta_lr_blend_calibrator import _safe_float, _decode_field

    now_ms = int(time.time() * 1000)
    start_id = f"{now_ms - lookback_hours * 3600 * 1000}-0"
    out: list[tuple[float, int, str]] = []
    cursor = start_id
    while True:
        try:
            chunk = r.xrange(stream, min=cursor, max="+", count=1000)
        except Exception as e:
            log.warning("xrange failed: %s", e)
            break
        if not chunk:
            break
        last_id = None
        for msg_id, fields in chunk:
            last_id = _decode_field(msg_id)
            rec = {_decode_field(k): _decode_field(v) for k, v in fields.items()}
            ml_version = rec.get("ml_version") or rec.get("model_ver") or ""
            p_raw = _safe_float(rec.get("p_edge_raw") or rec.get("ml_prob"), float("nan"))
            if p_raw != p_raw or not (0.0 <= p_raw <= 1.0):
                continue
            result = (rec.get("result") or "").upper()
            if result not in ("WIN", "LOSS"):
                continue
            win = 1 if result == "WIN" else 0
            out.append((p_raw, win, ml_version))
            if len(out) >= limit:
                break
        if len(out) >= limit or last_id is None:
            break
        cursor = f"({last_id}"
    return out


def _run_once(r: Any) -> None:
    """One autopilot pass: discover kinds, refit each."""
    from tools.refit_meta_lr_blend_calibrator import (
        _atomic_write_json,
        fit_and_evaluate,
    )

    # Eager init so the kinds_discovered gauge is registered BEFORE the
    # first set() (otherwise the first set() runs against an empty
    # _METRICS dict and the gauge shows 0 until the next pass).
    _init_metrics()

    skip = {
        k.strip().lower()
        for k in (os.getenv("ML_AUTOPILOT_SKIP_KINDS") or "").split(",")
        if k.strip()
    }

    kinds = _discover_kinds(r)
    try:
        if _METRICS:
            _METRICS["kinds_discovered"].set(len(kinds))  # type: ignore[attr-defined]
    except Exception:
        pass
    if not kinds:
        log.warning("no champion/challenger cfgs discovered; nothing to refit")
        return

    log.info("autopilot pass: kinds=%s", [k for k, _ in kinds])

    lookback_hours = int(os.getenv("ML_AUTOPILOT_LOOKBACK_H", "168"))
    min_n = int(os.getenv("ML_AUTOPILOT_MIN_N", "300"))
    switch_after_n = int(os.getenv("ML_AUTOPILOT_AUTO_SWITCH_AFTER_N", "300"))
    brier_delta_required = float(os.getenv("ML_AUTOPILOT_BRIER_DELTA", "0.005"))
    ece_delta_required = float(os.getenv("ML_AUTOPILOT_ECE_DELTA", "0.01"))
    stream = os.getenv("REFIT_STREAM", "trades:closed")
    report_dir = os.getenv("REFIT_REPORT_DIR", "/var/lib/trade/of_reports")

    # Pull window ONCE; reuse for all kinds.
    all_pairs = _read_trades_closed_with_version(
        r, stream=stream, lookback_hours=lookback_hours,
    )
    log.info("read trades:closed window: %d rows in last %dh", len(all_pairs), lookback_hours)

    for kind, model_path in kinds:
        if kind.lower() in skip:
            log.info("kind=%s: skipped via ML_AUTOPILOT_SKIP_KINDS", kind)
            continue
        if not os.path.exists(model_path):
            log.warning("kind=%s: model_path missing, skipping: %s", kind, model_path)
            _emit(kind=kind, accepted=False, n=0, brier_delta=0.0,
                  ece_delta=0.0, locked=False, reason="model_path_missing")
            continue

        # Decide target_version (broad vs kind-locked)
        n_matched = _count_kind_matched(all_pairs, kind)
        target_ver, locked, lock_reason = _decide_target_version(
            r, kind, n_matched, switch_after_n,
        )

        # Filter pairs by target_version
        if target_ver:
            tv_lc = target_ver.lower()
            pairs = [(p, w) for p, w, v in all_pairs if tv_lc in (v or "").lower()]
        else:
            pairs = [(p, w) for p, w, _v in all_pairs]

        log.info(
            "kind=%s: target_ver=%r (%s) n_matched_to_kind=%d kept=%d",
            kind, target_ver, lock_reason, n_matched, len(pairs),
        )

        result = fit_and_evaluate(
            pairs,
            min_n=min_n,
            require_brier_improvement=brier_delta_required,
            require_ece_improvement=ece_delta_required,
        )
        accepted = bool(result.get("accepted"))
        n = int(result.get("n", 0))
        brier_delta = float(result.get("brier_delta", 0.0) or 0.0)
        ece_delta = float(result.get("ece_delta", 0.0) or 0.0)
        reason = str(result.get("reason", "unknown"))

        log.info(
            "kind=%s: n=%d accepted=%s reason=%s brier_delta=%.4f ece_delta=%.4f",
            kind, n, accepted, reason, brier_delta, ece_delta,
        )

        _emit(kind=kind, accepted=accepted, n=n, brier_delta=brier_delta,
              ece_delta=ece_delta, locked=locked, reason=reason)

        # Persist a report regardless (audit trail).
        try:
            os.makedirs(report_dir, exist_ok=True)
            ts_ms = int(time.time() * 1000)
            report_path = os.path.join(
                report_dir, f"ml_calibrator_refit_{kind}_{ts_ms}.json",
            )
            _atomic_write_json(report_path, {
                **result,
                "kind": kind,
                "model_path": model_path,
                "target_version": target_ver,
                "target_version_mode": "kind_locked" if locked else "broad",
                "target_version_lock_reason": lock_reason,
                "n_matched_to_kind": n_matched,
                "lookback_hours": lookback_hours,
            })
        except Exception as e:
            log.warning("kind=%s: report write failed: %s", kind, e)

        if not accepted:
            continue

        # Promote calibrator artifact next to model. We write TWO files:
        #   1) model-specific:  calibrator_<model_basename_no_ext>.json
        #      (used by sibling-discovery when multiple kinds share a dir
        #       — autopilot discovered champion+challenger in same registry)
        #   2) generic:         calibrator.json
        #      (back-compat with meta_lr_blend_calibrator_refit_v1 which
        #       only writes the generic name)
        # The reader (config_loader._load_calibrator_sync) prefers the
        # model-specific name first, then falls back to the generic one.
        model_basename = os.path.splitext(os.path.basename(model_path))[0]
        cal_path_specific = os.path.join(
            os.path.dirname(model_path), f"calibrator_{model_basename}.json",
        )
        cal_path_generic = os.path.join(os.path.dirname(model_path), "calibrator.json")
        artifact = dict(result["calibrator"])
        artifact["meta"] = {
            "kind": f"{kind}_posterior",
            "n": n,
            "brier_raw": result["brier_raw"],
            "brier_cal": result["brier_cal"],
            "ece_raw": result["ece_raw"],
            "ece_cal": result["ece_cal"],
            "pos_rate": result["pos_rate"],
            "fit_ts_ms": int(time.time() * 1000),
            "lookback_hours": lookback_hours,
            "target_version": target_ver,
            "target_version_mode": "kind_locked" if locked else "broad",
            "service": "ml_calibrator_autopilot_v1",
        }
        for cal_path in (cal_path_specific, cal_path_generic):
            try:
                _atomic_write_json(cal_path, artifact)
                log.info("kind=%s: promoted calibrator: %s", kind, cal_path)
            except Exception as e:
                log.warning("kind=%s: calibrator write failed at %s: %s",
                            kind, cal_path, e)


def main() -> int:
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
        stream=sys.stderr,
    )

    if os.getenv("ML_CALIBRATOR_AUTOPILOT_ENABLED", "1") != "1":
        log.warning("ML_CALIBRATOR_AUTOPILOT_ENABLED=0 — autopilot disabled, exiting.")
        return 0

    try:
        from prometheus_client import start_http_server
        port = int(os.getenv("ML_AUTOPILOT_PORT", "9868"))
        start_http_server(port)
        log.info("prometheus on :%d", port)
    except Exception as e:
        log.warning("prometheus startup failed: %s", e)

    from core.redis_client import get_redis
    r = get_redis()

    interval = max(60, int(os.getenv("ML_AUTOPILOT_INTERVAL_S", "21600")))
    log.info(
        "ml_calibrator_autopilot loop: interval=%ds lookback=%sh min_n=%s",
        interval,
        os.getenv("ML_AUTOPILOT_LOOKBACK_H", "168"),
        os.getenv("ML_AUTOPILOT_MIN_N", "300"),
    )

    while True:
        try:
            _run_once(r)
        except Exception as e:
            log.exception("autopilot pass failed: %s", e)
        time.sleep(interval)


if __name__ == "__main__":
    raise SystemExit(main())
