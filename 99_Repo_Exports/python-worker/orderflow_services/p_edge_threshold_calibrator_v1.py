#!/usr/bin/env python3
"""p_edge_threshold_calibrator_v1.py

Feed-side service for the p_edge cutoff autocalibrator. Joins decision-time
`ml_prob` to realized `r_multiple` from `trades:closed` and maintains a
per-(symbol × regime × kind) `PEdgeThresholdCalibrator`, publishing snapshots
to Redis (`autocal:p_edge:state`) and periodic counterfactual reports to disk.

Wiring:

  trades:closed (XREAD) → calibrator.observe() ────────────┐
                                                           │
                                snapshot() ──→ Redis SET ──┤
                                                           ↓
              EdgeCostGate._p_min_for_kind() ←── PEdgeThresholdReader (cache)

Counterfactual reports (per `report_interval_sec`) compare:
  * `committed_tau`  — what the calibrator currently enforces
  * `shadow_tau`     — latest proposal (always recomputed)
  * `default_tau`    — static EDGE_EV_P_MIN
…against realized samples in each bin, including EV and kept-count at each.

Defaults: enforce=False (shadow only) until operator promotes via
`P_EDGE_CAL_ENFORCE=1`.

ENV
  P_EDGE_CAL_REDIS_URL          (default REDIS_URL or redis://redis-worker-1:6379/0)
  P_EDGE_CAL_GROUP              (default "p-edge-cal")
  P_EDGE_CAL_CONSUMER           (default "p-edge-cal-1")
  P_EDGE_CAL_PORT               (default 9145)
  P_EDGE_CAL_BATCH              (default 200)
  P_EDGE_CAL_ENFORCE            (default 0 — shadow mode)
  P_EDGE_CAL_TARGET_EV_R        (default 0.10)
  P_EDGE_CAL_WINDOW_DAYS        (default 7)
  P_EDGE_CAL_MIN_KEPT           (default 200)
  P_EDGE_CAL_MIN_TOTAL          (default 100)
  P_EDGE_CAL_SNAPSHOT_SEC       (default 30 — Redis SET interval)
  P_EDGE_CAL_REPORT_SEC         (default 600 — counterfactual report interval)
  P_EDGE_CAL_REPORTS_DIR        (default /var/lib/trade/of_reports)
  P_EDGE_CAL_DEFAULT_P_MIN      (default 0.55 — matches EDGE_EV_P_MIN)

Reject-reason weighting (IPS-style outcome reliability):
  REJECT_REASON_WEIGHTS_ENABLED (default 0 — back-compat; every sample w=1.0)
  REJECT_REASON_WEIGHTS_JSON    optional override of `core.reject_reason_weights.DEFAULT_WEIGHTS`
  See `core/reject_reason_weights.py` for full taxonomy and rationale.
"""
from __future__ import annotations

import json
import logging
import math
import os
import signal
import time
from pathlib import Path
from typing import Any

from prometheus_client import REGISTRY, Counter, Gauge, start_http_server  # type: ignore

from core.p_edge_threshold_calibrator import (
    DEFAULT_P_MIN,
    PEdgeThresholdCalibrator,
)
from core.redis_client import get_redis
from core.redis_keys import RK, RS
from core.reject_reason_weights import (
    is_enabled as reject_weights_enabled,
    reason_family,
    weight_for_reason,
)

logger = logging.getLogger("p-edge-cal")


# --------------------------------------------------------------------------
# env helpers
# --------------------------------------------------------------------------


def _env(name: str, default: str = "") -> str:
    return os.getenv(name, default)


def _env_int(name: str, default: int) -> int:
    raw = _env(name, "")
    try:
        return int(raw) if raw else default
    except ValueError:
        return default


def _env_float(name: str, default: float) -> float:
    raw = _env(name, "")
    try:
        return float(raw) if raw else default
    except ValueError:
        return default


def _env_bool(name: str, default: bool) -> bool:
    raw = _env(name, "")
    if not raw:
        return default
    return raw.strip().lower() in ("1", "true", "yes", "on")


def _safe_float(v: Any, default: float = float("nan")) -> float:
    try:
        if v is None:
            return default
        if isinstance(v, (bytes, bytearray)):
            v = v.decode("utf-8", "ignore")
        f = float(v)
        return f if math.isfinite(f) else default
    except (TypeError, ValueError):
        return default


def _decode(v: Any) -> str:
    if isinstance(v, (bytes, bytearray)):
        return v.decode("utf-8", "ignore")
    return str(v) if v is not None else ""


# --------------------------------------------------------------------------
# Prometheus (idempotent registration helpers)
# --------------------------------------------------------------------------


def _gauge(name: str, doc: str, labels: list[str]) -> Gauge:
    try:
        return Gauge(name, doc, labels)
    except ValueError:
        for c in REGISTRY._collector_to_names:
            if name in REGISTRY._collector_to_names[c]:
                return c  # type: ignore[return-value]
        raise


def _counter(name: str, doc: str, labels: list[str]) -> Counter:
    try:
        return Counter(name, doc, labels)
    except ValueError:
        for c in REGISTRY._collector_to_names:
            if name in REGISTRY._collector_to_names[c]:
                return c  # type: ignore[return-value]
        raise


# --------------------------------------------------------------------------
# Counterfactual report
# --------------------------------------------------------------------------


def _realized_ev_at_threshold(
    samples: list[tuple[float, float, int]] | list[tuple[float, float, int, float]],
    tau: float,
) -> tuple[float, int]:
    """For (p, r, win[, weight]) tuples (win ∈ {0,1}; BE excluded by caller),
    return (mean_R_above_tau, n_kept).

    Accepts both legacy 3-tuples and 4-tuples with an explicit IPS weight —
    when weight is omitted (3-tuple) each sample is treated as weight=1.0, so
    the result is identical to the pre-weighting code path.
    """
    kept: list[tuple[float, float]] = []
    for tup in samples:
        if len(tup) >= 4:
            p, r, _win, w = tup[0], tup[1], tup[2], tup[3]  # type: ignore[misc]
        else:
            p, r, _win = tup[0], tup[1], tup[2]
            w = 1.0
        if p >= tau:
            kept.append((r, w))
    if not kept:
        return (float("nan"), 0)
    total_w = sum(w for _r, w in kept)
    if total_w <= 0.0:
        return (float("nan"), 0)
    return (sum(r * w for r, w in kept) / total_w, int(total_w))


def _build_counterfactual_report(
    cal: PEdgeThresholdCalibrator,
    *,
    default_tau: float,
    generated_ms: int,
) -> dict[str, Any]:
    """Compare committed vs shadow vs default cutoff EV per bin."""
    out_bins: list[dict[str, Any]] = []
    for (sym, reg, knd, drc), b in cal.bins.items():
        # Snapshot eligible samples (exclude BE — same rule as calibrator).
        # Tuple is (p, r, win, weight) so the report uses the same weighted EV
        # math as the live calibrator (`_maybe_recompute`).
        eligible = [
            (s.p, s.r, s.win, s.w) for s in b.buf if s.win != -1
        ]
        committed_ev, committed_n = _realized_ev_at_threshold(eligible, b.p_min if b.p_min > 0 else default_tau)
        shadow_ev, shadow_n = _realized_ev_at_threshold(eligible, b.shadow_p_min if b.shadow_p_min > 0 else default_tau)
        default_ev, default_n = _realized_ev_at_threshold(eligible, default_tau)

        out_bins.append({
            "symbol": sym,
            "regime": reg,
            "kind": knd,
            "direction": drc,
            "n_total": len(b.buf),
            "n_eligible": len(eligible),
            "committed_tau": b.p_min,
            "committed_ev_r": committed_ev,
            "committed_n_kept": committed_n,
            "shadow_tau": b.shadow_p_min,
            "shadow_ev_r": shadow_ev,
            "shadow_n_kept": shadow_n,
            "default_tau": default_tau,
            "default_ev_r": default_ev,
            "default_n_kept": default_n,
            "last_apply_ms": b.last_apply_ms,
            "last_recompute_ms": b.last_recompute_ms,
        })
    out_bins.sort(key=lambda r: (r["symbol"], r["regime"], r["kind"]))
    return {
        "generated_ms": generated_ms,
        "enforce": cal.enforce,
        "target_ev_r": cal.target_ev_r,
        "default_p_min": cal.default_p_min,
        "n_bins": len(out_bins),
        "bins": out_bins,
    }


# --------------------------------------------------------------------------
# Main loop
# --------------------------------------------------------------------------


def main() -> None:  # pragma: no cover — integration entrypoint
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    port = _env_int("P_EDGE_CAL_PORT", 9145)
    group = _env("P_EDGE_CAL_GROUP", "p-edge-cal")
    consumer = _env("P_EDGE_CAL_CONSUMER", "p-edge-cal-1")
    batch = _env_int("P_EDGE_CAL_BATCH", 200)
    enforce = _env_bool("P_EDGE_CAL_ENFORCE", False)
    target_ev = _env_float("P_EDGE_CAL_TARGET_EV_R", 0.10)
    window_days = _env_int("P_EDGE_CAL_WINDOW_DAYS", 7)
    min_kept = _env_int("P_EDGE_CAL_MIN_KEPT", 200)
    min_total = _env_int("P_EDGE_CAL_MIN_TOTAL", 100)
    snap_sec = _env_int("P_EDGE_CAL_SNAPSHOT_SEC", 30)
    report_sec = _env_int("P_EDGE_CAL_REPORT_SEC", 600)
    reports_dir = _env("P_EDGE_CAL_REPORTS_DIR", "/var/lib/trade/of_reports")
    default_p_min = _env_float("P_EDGE_CAL_DEFAULT_P_MIN", DEFAULT_P_MIN)

    logger.info(
        "p-edge-cal start: port=%d group=%s consumer=%s enforce=%s "
        "target_ev=%.2fR window=%dd min_kept=%d min_total=%d snap=%ds report=%ds",
        port, group, consumer, enforce, target_ev, window_days, min_kept, min_total,
        snap_sec, report_sec,
    )

    cal = PEdgeThresholdCalibrator(
        target_ev_r=target_ev,
        window_ms=window_days * 24 * 60 * 60 * 1000,
        min_kept_trades=min_kept,
        min_total_trades=min_total,
        default_p_min=default_p_min,
        enforce=enforce,
    )

    # ----- Prometheus metrics ----------------------------------------------
    g_up = _gauge("p_edge_cal_up", "Service up", [])
    g_state = _gauge(
        "p_edge_cal_threshold",
        "p_edge cutoff per bin and source (committed/shadow/default)",
        ["symbol", "regime", "kind", "direction", "source"],
    )
    g_n_eligible = _gauge(
        "p_edge_cal_n_eligible",
        "Eligible (non-BE) samples per bin",
        ["symbol", "regime", "kind", "direction"],
    )
    g_last_apply = _gauge(
        "p_edge_cal_last_apply_ms",
        "Last apply timestamp (ms) per bin",
        ["symbol", "regime", "kind", "direction"],
    )
    g_enforce = _gauge("p_edge_cal_enforce", "Enforce flag (1=enforce, 0=shadow)", [])
    g_snap_lag = _gauge("p_edge_cal_snapshot_age_ms", "Wall-clock since last snapshot publish", [])
    c_obs = _counter("p_edge_cal_observed_total", "Trades observed", ["result"])
    c_skip = _counter("p_edge_cal_skipped_total", "Trades skipped", ["reason"])
    c_snap = _counter("p_edge_cal_snapshots_total", "Snapshot publishes", ["outcome"])
    c_report = _counter("p_edge_cal_reports_total", "Counterfactual reports written", ["outcome"])
    # IPS-weighting telemetry — bounded cardinality by `reason_family()`.
    c_reason = _counter(
        "p_edge_cal_input_by_reason_family_total",
        "Trades observed per reject_reason family (after IPS weighting policy)",
        ["family"],
    )
    g_weights_on = _gauge(
        "p_edge_cal_reject_weights_enabled",
        "1 if REJECT_REASON_WEIGHTS_ENABLED=1, else 0 (every sample w=1.0)",
        [],
    )
    g_weights_on.set(1.0 if reject_weights_enabled() else 0.0)

    g_up.set(1)
    g_enforce.set(1.0 if enforce else 0.0)

    # ----- Redis init -------------------------------------------------------
    redis_client = get_redis()
    stream_key = RS.TRADES_CLOSED

    # Idempotent consumer group bootstrap.
    try:
        redis_client.xgroup_create(stream_key, group, id="$", mkstream=True)
        logger.info("Created consumer group %s on %s", group, stream_key)
    except Exception as e:  # noqa: BLE001
        if "BUSYGROUP" in str(e):
            logger.info("Consumer group %s already exists on %s", group, stream_key)
        else:
            logger.error("xgroup_create failed: %s", e)

    # Warm-start: try loading existing snapshot.
    try:
        raw = redis_client.get(RK.AUTOCAL_P_EDGE_STATE)
        if raw:
            if isinstance(raw, (bytes, bytearray)):
                raw = raw.decode("utf-8", "ignore")
            cal.load_state(json.loads(raw))
            logger.info("Warm-started from %s (%d bins)", RK.AUTOCAL_P_EDGE_STATE, len(cal.bins))
    except Exception as e:  # noqa: BLE001
        logger.warning("Warm-start failed: %s", e)

    # Reports dir (best-effort).
    try:
        Path(reports_dir).mkdir(parents=True, exist_ok=True)
    except Exception as e:  # noqa: BLE001
        logger.warning("Cannot create reports_dir=%s: %s", reports_dir, e)

    start_http_server(port)
    logger.info("HTTP /metrics on :%d", port)

    stop = {"flag": False}

    def _sig(_signo: int, _frame: Any) -> None:  # noqa: ANN401
        stop["flag"] = True
        logger.info("Shutdown signal")

    signal.signal(signal.SIGTERM, _sig)
    signal.signal(signal.SIGINT, _sig)

    last_snap_ms = 0
    last_report_ms = 0

    while not stop["flag"]:
        try:
            resp = redis_client.xreadgroup(
                groupname=group,
                consumername=consumer,
                streams={stream_key: ">"},
                count=batch,
                block=2000,
            )
        except Exception as e:  # noqa: BLE001
            logger.error("XREADGROUP failed: %s", e)
            time.sleep(1.0)
            continue

        now_ms = int(time.time() * 1000)
        ack_ids: list[Any] = []

        if resp:
            for _sk, messages in resp:  # type: ignore[union-attr]
                for msg_id, fields in messages:
                    ack_ids.append(msg_id)
                    try:
                        fields = {_decode(k): _decode(v) for k, v in fields.items()}
                        p_edge = _safe_float(fields.get("ml_prob"))
                        if not math.isfinite(p_edge):
                            c_skip.labels(reason="ml_prob_missing").inc()
                            continue
                        result = (fields.get("result", "") or "").upper()
                        if result not in ("WIN", "LOSS", "BE"):
                            c_skip.labels(reason="result_invalid").inc()
                            continue
                        r_mult = _safe_float(fields.get("r_multiple"))
                        if not math.isfinite(r_mult):
                            # For BE we tolerate missing r_mult (typically 0.0).
                            if result == "BE":
                                r_mult = 0.0
                            else:
                                c_skip.labels(reason="r_multiple_invalid").inc()
                                continue
                        symbol = fields.get("symbol", "") or ""
                        regime = fields.get("market_regime", "") or fields.get("regime", "") or "*"
                        kind = fields.get("kind", "") or fields.get("signal_kind", "") or "*"
                        # Phase B: direction-specific bins. trade_close_joiner
                        # writes `side` as LONG/SHORT in trades:closed; legacy
                        # producers may use "direction" or be missing entirely
                        # (→ "*", which preserves pre-Phase-B aggregation).
                        direction = (
                            fields.get("side", "")
                            or fields.get("direction", "")
                            or "*"
                        )
                        ts_close = int(_safe_float(fields.get("ts_close"), default=now_ms))

                        # IPS-style outcome-reliability weight derived from the
                        # gate reject_reason recorded at signal time.
                        # `v_gate_reason` is written by trade_monitor.on_audit
                        # (see infra/redis_repo.py:1216) — empty / "OK" /
                        # "passed" all map to weight=1.0. When
                        # REJECT_REASON_WEIGHTS_ENABLED=0 every weight=1.0,
                        # preserving pre-weighting behaviour exactly.
                        v_gate_reason = (
                            fields.get("v_gate_reason", "")
                            or fields.get("reject_reason", "")
                            or ""
                        )
                        w = weight_for_reason(v_gate_reason)
                        fam = reason_family(v_gate_reason)

                        cal.observe(
                            symbol=symbol,
                            regime=regime,
                            kind=kind,
                            p_edge=p_edge,
                            r_multiple=r_mult,
                            result=result,
                            ts_ms=ts_close,
                            direction=direction,
                            weight=w,
                        )
                        c_obs.labels(result=result).inc()
                        c_reason.labels(family=fam).inc()
                    except Exception as e:  # noqa: BLE001
                        c_skip.labels(reason="exception").inc()
                        logger.warning("observe failed for %s: %s", msg_id, e)

            if ack_ids:
                try:
                    redis_client.xack(stream_key, group, *ack_ids)
                except Exception as e:  # noqa: BLE001
                    logger.error("XACK failed: %s", e)

        # ----- periodic snapshot ------------------------------------------
        if (now_ms - last_snap_ms) >= snap_sec * 1000:
            try:
                snap = cal.snapshot()
                redis_client.set(RK.AUTOCAL_P_EDGE_STATE, json.dumps(snap))
                last_snap_ms = now_ms
                g_snap_lag.set(0.0)
                c_snap.labels(outcome="ok").inc()
                _publish_bin_metrics(cal, g_state, g_n_eligible, g_last_apply, default_p_min)
            except Exception as e:  # noqa: BLE001
                c_snap.labels(outcome="error").inc()
                logger.error("snapshot publish failed: %s", e)
        else:
            g_snap_lag.set(float(now_ms - last_snap_ms))

        # ----- periodic counterfactual report -----------------------------
        if (now_ms - last_report_ms) >= report_sec * 1000:
            try:
                report = _build_counterfactual_report(
                    cal, default_tau=default_p_min, generated_ms=now_ms,
                )
                path = Path(reports_dir) / "p_edge_calibrator_counterfactual.json"
                with path.open("w", encoding="utf-8") as f:
                    json.dump(report, f, indent=2, sort_keys=True, default=str)
                last_report_ms = now_ms
                c_report.labels(outcome="ok").inc()
            except Exception as e:  # noqa: BLE001
                c_report.labels(outcome="error").inc()
                logger.warning("report write failed: %s", e)

    g_up.set(0)
    logger.info("p-edge-cal stopped")


def _publish_bin_metrics(
    cal: PEdgeThresholdCalibrator,
    g_state: Gauge,
    g_n_eligible: Gauge,
    g_last_apply: Gauge,
    default_p_min: float,
) -> None:
    for (sym, reg, knd, drc), b in cal.bins.items():
        labels = {"symbol": sym, "regime": reg, "kind": knd, "direction": drc}
        n_elig = sum(1 for s in b.buf if s.win != -1)
        g_n_eligible.labels(**labels).set(float(n_elig))
        g_last_apply.labels(**labels).set(float(b.last_apply_ms))
        g_state.labels(**labels, source="committed").set(float(b.p_min))
        g_state.labels(**labels, source="shadow").set(float(b.shadow_p_min))
        g_state.labels(**labels, source="default").set(float(default_p_min))


if __name__ == "__main__":  # pragma: no cover
    main()
