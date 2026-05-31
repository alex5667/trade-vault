#!/usr/bin/env python3
"""
confirmation_barrier_cal_v1.py — OBI threshold autocalibrator with auto-promote.

Reads the snapshot from autocal:confirm_barrier:state (written by signal_pipeline
_pipeline_calib_snap every 60s), evaluates calibration warmup across all
(symbol × kind) bins, and when conditions are met, writes a promote record to
autocal:confirm_barrier:promote so ConfirmationBarrierReader activates enforce
mode without container restart.

Sends Telegram notification on promote (and on rollback detection).

ENV
  CB_CAL_REDIS_URL        default REDIS_URL or redis://redis:6379/0
  CB_CAL_NOTIFY_STREAM    default notify:telegram
  CB_CAL_POLL_SEC         default 60
  CB_CAL_PORT             default 9155 (Prometheus)
  CB_CAL_MIN_DAYS         default 7.0   days since first observation
  CB_CAL_MIN_BINS         default 2     min (sym,kind) bins that are calibrated
  CB_CAL_MIN_SAMPLES_BIN  default 30    min samples per active bin
  CB_CAL_DWELL_MIN        default 60    dwell minutes before promote
  CB_CAL_AUTO_PROMOTE     default 1     0 = compute/report only; 1 = promote
  CB_CAL_SNAPSHOT_TTL     default 1209600 (14d in seconds)
  CB_CAL_DEDUP_TTL_H      default 24    dedup window for repeated Telegram alerts
"""
from __future__ import annotations

import json
import logging
import math
import os
import signal
import sys
import time
from typing import Any

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger("cb-cal")

# ---------------------------------------------------------------------------
# ENV helpers
# ---------------------------------------------------------------------------

def _env(k: str, d: str = "") -> str:
    return os.environ.get(k, d)

def _env_int(k: str, d: int) -> int:
    try: return int(_env(k, str(d)))
    except Exception: return d

def _env_float(k: str, d: float) -> float:
    try: return float(_env(k, str(d)))
    except Exception: return d

def _env_bool(k: str, d: bool) -> bool:
    v = _env(k, "")
    return v.strip().lower() in ("1", "true", "yes") if v else d

# ---------------------------------------------------------------------------
# Prometheus
# ---------------------------------------------------------------------------

try:
    from prometheus_client import Counter, Gauge, start_http_server

    _g_bins_total        = Gauge("cb_cal_bins_total", "Total (sym,kind) bins in snapshot")
    _g_bins_calibrated   = Gauge("cb_cal_bins_calibrated", "Bins with committed_tau")
    _g_bins_warmed       = Gauge("cb_cal_bins_warmed", "Bins with ≥ min_samples")
    _g_promote_ready     = Gauge("cb_cal_promote_ready", "1 if all warmup conditions met")
    _g_promoted          = Gauge("cb_cal_promoted", "1 once auto-promoted")
    _g_dwell_sec         = Gauge("cb_cal_dwell_sec", "Seconds since all conditions first met")
    _g_first_obs_days    = Gauge("cb_cal_first_obs_days", "Days since first observation across all bins")
    _c_polls             = Counter("cb_cal_polls_total", "Snapshot polls")
    _c_promote           = Counter("cb_cal_promote_total", "Promote attempts", ["result"])
    _g_snapshot_age_sec  = Gauge("cb_cal_snapshot_age_sec", "Age of snapshot in Redis (seconds)")

    def _metrics_ok() -> bool: return True
except Exception:
    class _Stub:  # type: ignore[no-redef]
        def inc(self, *a, **k): pass
        def set(self, *a, **k): pass
        def labels(self, *a, **k): return self

    _g_bins_total = _g_bins_calibrated = _g_bins_warmed = _g_promote_ready = _Stub()  # type: ignore[assignment]
    _g_promoted = _g_dwell_sec = _g_first_obs_days = _g_snapshot_age_sec = _Stub()   # type: ignore[assignment]
    _c_polls = _c_promote = _Stub()  # type: ignore[assignment]

    def _metrics_ok() -> bool: return False

# ---------------------------------------------------------------------------
# Telegram notify (dedup via Redis SET NX)
# ---------------------------------------------------------------------------

def _send_telegram(r: Any, *, notify_stream: str, text: str, dedup_key: str | None,
                   dedup_ttl_h: int) -> None:
    if dedup_key and r is not None:
        try:
            dk = f"dedup:reporting:{dedup_key}"
            if not r.set(dk, "1", nx=True, ex=dedup_ttl_h * 3600):
                logger.debug("Telegram notify suppressed (dedup): %s", dedup_key)
                return
        except Exception as e:
            logger.warning("dedup set failed (proceeding anyway): %s", e)
    try:
        r.xadd(
            notify_stream,
            {
                "type": "report",
                "subtype": "cb_cal_autopromote",
                "ts": str(int(time.time() * 1000)),
                "text": text,
                "parse_mode": "HTML",
                "source": "confirmation_barrier_cal_v1",
            },
            maxlen=50_000,
        )
        logger.info("Telegram notify sent (stream=%s)", notify_stream)
    except Exception as e:
        logger.warning("Telegram notify failed: %s", e)

# ---------------------------------------------------------------------------
# Snapshot analysis
# ---------------------------------------------------------------------------

def _parse_snapshot(snap: dict[str, Any]) -> dict[str, Any]:
    """
    Returns:
        {
          "bins": {bin_key: {"committed_tau": float|None, "n": int, "last_apply_ms": int}},
          "first_obs_ms": int,   # earliest last_apply_ms across all bins (proxy for first obs)
          "total_n": int,
        }
    """
    bins_raw = snap.get("bins") or {}
    bins: dict[str, Any] = {}
    first_obs_ms = 0
    total_n = 0

    for bk, bdata in bins_raw.items():
        if not isinstance(bdata, dict):
            continue
        committed_tau = bdata.get("committed_tau")
        n = int(bdata.get("n", 0) or 0)
        last_apply_ms = int(bdata.get("last_apply_ms", 0) or 0)
        tau_f: float | None = None
        if committed_tau is not None:
            try:
                v = float(committed_tau)
                tau_f = v if math.isfinite(v) and v > 0 else None
            except Exception:
                pass
        bins[bk] = {"committed_tau": tau_f, "n": n, "last_apply_ms": last_apply_ms}
        total_n += n
        if last_apply_ms > 0:
            first_obs_ms = min(first_obs_ms, last_apply_ms) if first_obs_ms > 0 else last_apply_ms

    return {"bins": bins, "first_obs_ms": first_obs_ms, "total_n": total_n}

def _check_ready(
    parsed: dict[str, Any],
    *,
    min_days: float,
    min_bins: int,
    min_samples_bin: int,
    now_ms: int,
) -> tuple[bool, str]:
    """Return (ready, reason_str)."""
    bins = parsed["bins"]
    first_obs_ms = parsed["first_obs_ms"]

    # Exclude aggregate "*" bins
    real_bins = {k: v for k, v in bins.items() if not k.startswith("*:")}
    n_real = len(real_bins)
    if n_real == 0:
        return False, "no_real_bins"

    # Bins with committed_tau
    calibrated = [k for k, v in real_bins.items() if v["committed_tau"] is not None]
    warmed = [k for k, v in real_bins.items() if v["n"] >= min_samples_bin]

    _g_bins_total.set(n_real)
    _g_bins_calibrated.set(len(calibrated))
    _g_bins_warmed.set(len(warmed))

    if len(calibrated) < min_bins:
        return False, f"calibrated_bins={len(calibrated)}<{min_bins}"
    if len(warmed) < min_bins:
        return False, f"warmed_bins={len(warmed)}<{min_bins}"

    # Time elapsed since first observation
    if first_obs_ms <= 0:
        return False, "no_first_obs"
    elapsed_days = (now_ms - first_obs_ms) / (86_400_000.0)
    _g_first_obs_days.set(elapsed_days)
    if elapsed_days < min_days:
        return False, f"elapsed={elapsed_days:.1f}d<{min_days}d"

    return True, "ok"

# ---------------------------------------------------------------------------
# Promote action
# ---------------------------------------------------------------------------

def _do_promote(
    r: Any,
    *,
    parsed: dict[str, Any],
    promote_key: str,
    snapshot_ttl: int,
    notify_stream: str,
    dedup_ttl_h: int,
    now_ms: int,
) -> None:
    """Write promote record to Redis, notify Telegram."""
    bins_summary: dict[str, Any] = {}
    for bk, bdata in parsed["bins"].items():
        if bk.startswith("*:"):
            continue
        tau = bdata["committed_tau"]
        bins_summary[bk] = {
            "committed_tau": round(tau, 4) if tau is not None else None,
            "n": bdata["n"],
        }

    promote_state = {
        "promoted": True,
        "promoted_ms": now_ms,
        "promoted_iso": _ms_to_iso(now_ms),
        "bins_summary": bins_summary,
        "rollback_cmd": f"docker exec redis redis-cli DEL {promote_key}",
    }
    try:
        r.set(promote_key, json.dumps(promote_state, separators=(",", ":")), ex=snapshot_ttl)
        logger.info("AUTO-PROMOTE: wrote promote state to %s", promote_key)
    except Exception as e:
        logger.error("Failed to write promote state: %s", e)
        _c_promote.labels(result="redis_error").inc()
        return

    _g_promoted.set(1.0)

    # Build Telegram message
    lines = [
        "✅ <b>Confirmation Barrier Calibrator — ENFORCE АКТИВИРОВАН</b>",
        "",
        f"Автокалибратор OBI-порогов перешёл из shadow → enforce.",
        f"Bins откалиброваны: <b>{len(bins_summary)}</b>",
        "",
        "<b>Адаптивные пороги:</b>",
    ]
    for bk, info in sorted(bins_summary.items()):
        sym, _, kind = bk.partition(":")
        tau = info["committed_tau"]
        n = info["n"]
        lines.append(f"  • <b>{sym}</b> [{kind}]: τ={tau:.4f}  n={n}" if tau is not None else
                     f"  • <b>{sym}</b> [{kind}]: n={n} (не откалиброван)")
    lines += [
        "",
        "Workers подхватят enforce через ≤30с (TTL кэша reader).",
        "",
        f"Rollback: <code>docker exec redis redis-cli DEL {promote_key}</code>",
        "",
        f"Дата: {_ms_to_iso(now_ms)}",
    ]

    _send_telegram(
        r,
        notify_stream=notify_stream,
        text="\n".join(lines),
        dedup_key="cb_cal_promote",
        dedup_ttl_h=dedup_ttl_h,
    )
    _c_promote.labels(result="success").inc()

def _notify_blocked(
    r: Any,
    *,
    reason: str,
    notify_stream: str,
    dedup_ttl_h: int,
    dwell_sec: float,
    now_ms: int,
) -> None:
    text = (
        "⚠️ <b>Confirmation Barrier Calibrator — promote ЗАБЛОКИРОВАН</b>\n\n"
        f"Условия готовности были выполнены ({dwell_sec/60:.0f} мин dwell), "
        f"но promote отклонён: <code>{reason}</code>\n\n"
        f"Дата: {_ms_to_iso(now_ms)}"
    )
    _send_telegram(r, notify_stream=notify_stream, text=text,
                   dedup_key=f"cb_cal_blocked_{reason}", dedup_ttl_h=dedup_ttl_h)
    _c_promote.labels(result=f"blocked_{reason}").inc()

# ---------------------------------------------------------------------------
# Sanity check before promote
# ---------------------------------------------------------------------------

def _sanity_ok(
    parsed: dict[str, Any],
    *,
    obi_floor: float = 1.01,
    obi_ceil: float = 3.0,
) -> tuple[bool, str]:
    """Verify all committed_tau values are within sane range."""
    for bk, bdata in parsed["bins"].items():
        if bk.startswith("*:"):
            continue
        tau = bdata["committed_tau"]
        if tau is None:
            continue
        if not (obi_floor <= tau <= obi_ceil):
            return False, f"{bk}: tau={tau:.4f} out of [{obi_floor},{obi_ceil}]"
    return True, "ok"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _ms_to_iso(ms: int) -> str:
    try:
        import datetime
        return datetime.datetime.fromtimestamp(ms / 1000.0, tz=datetime.timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    except Exception:
        return str(ms)

def _load_snapshot(r: Any, snapshot_key: str) -> dict[str, Any] | None:
    try:
        raw = r.get(snapshot_key)
        if not raw:
            return None
        data = json.loads(raw)
        return data if isinstance(data, dict) else None
    except Exception as e:
        logger.warning("Failed to read snapshot: %s", e)
        return None

# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

def main() -> None:
    from core.redis_keys import RK

    poll_sec        = _env_int("CB_CAL_POLL_SEC",        60)
    port            = _env_int("CB_CAL_PORT",            9155)
    min_days        = _env_float("CB_CAL_MIN_DAYS",       7.0)
    min_bins        = _env_int("CB_CAL_MIN_BINS",         2)
    min_samples_bin = _env_int("CB_CAL_MIN_SAMPLES_BIN",  30)
    dwell_min       = _env_int("CB_CAL_DWELL_MIN",        60)
    auto_promote    = _env_bool("CB_CAL_AUTO_PROMOTE",    True)
    snapshot_ttl    = _env_int("CB_CAL_SNAPSHOT_TTL",     14 * 24 * 3600)
    notify_stream   = _env("CB_CAL_NOTIFY_STREAM",        "notify:telegram")
    dedup_ttl_h     = _env_int("CB_CAL_DEDUP_TTL_H",      24)
    redis_url       = _env("CB_CAL_REDIS_URL", _env("REDIS_URL", "redis://redis:6379/0"))

    snapshot_key    = RK.AUTOCAL_CONFIRM_BARRIER
    promote_key     = RK.AUTOCAL_CONFIRM_BARRIER_PROMOTE

    logger.info(
        "confirmation_barrier_cal_v1 starting | port=%d poll_sec=%d "
        "min_days=%.1f min_bins=%d min_samples_bin=%d dwell_min=%d "
        "auto_promote=%s redis=%s",
        port, poll_sec, min_days, min_bins, min_samples_bin, dwell_min,
        auto_promote, redis_url,
    )

    if _metrics_ok():
        try:
            start_http_server(port)
            logger.info("Prometheus started on :%d", port)
        except Exception as e:
            logger.warning("Prometheus start failed: %s", e)

    import redis as redis_lib
    r = redis_lib.Redis.from_url(redis_url, decode_responses=True, socket_timeout=5)

    # Shutdown signal
    _shutdown = [False]
    def _handle_sig(*_: Any) -> None:
        _shutdown[0] = True
        logger.info("Shutdown signal received")
    signal.signal(signal.SIGTERM, _handle_sig)
    signal.signal(signal.SIGINT,  _handle_sig)

    # Auto-promote state (in-process dwell tracker)
    _ready_since: float = 0.0   # monotonic time when all conditions first became true
    _promoted: bool = False      # set True once promote written

    # Check if already promoted (warm restart)
    try:
        raw_p = r.get(promote_key)
        if raw_p:
            pdata = json.loads(raw_p)
            if isinstance(pdata, dict) and pdata.get("promoted"):
                _promoted = True
                _g_promoted.set(1.0)
                logger.info("Already promoted (Redis state: %s)", pdata.get("promoted_iso", "?"))
    except Exception:
        pass

    logger.info("Main loop started (auto_promote=%s, already_promoted=%s)", auto_promote, _promoted)

    while not _shutdown[0]:
        time.sleep(poll_sec)
        if _shutdown[0]:
            break

        _c_polls.inc()
        now_ms = int(time.time() * 1000)

        snap = _load_snapshot(r, snapshot_key)
        if snap is None:
            logger.debug("No snapshot in Redis yet (%s) — waiting", snapshot_key)
            _g_bins_total.set(0)
            _g_bins_calibrated.set(0)
            _g_bins_warmed.set(0)
            _g_promote_ready.set(0)
            _ready_since = 0.0
            continue

        # Measure snapshot freshness
        try:
            snap_ts = int(snap.get("ts_ms", 0) or 0)
            if snap_ts > 0:
                age_sec = (now_ms - snap_ts) / 1000.0
                _g_snapshot_age_sec.set(max(0.0, age_sec))
        except Exception:
            pass

        parsed = _parse_snapshot(snap)
        ready, reason = _check_ready(
            parsed,
            min_days=min_days,
            min_bins=min_bins,
            min_samples_bin=min_samples_bin,
            now_ms=now_ms,
        )
        _g_promote_ready.set(1.0 if ready else 0.0)

        if not ready:
            if _ready_since > 0.0:
                logger.info("Conditions no longer met — resetting dwell timer (%s)", reason)
            _ready_since = 0.0
            _g_dwell_sec.set(0.0)
            logger.debug("Promote not ready: %s", reason)
            continue

        # Conditions met — track dwell
        if _ready_since == 0.0:
            _ready_since = time.monotonic()
            logger.info(
                "All warmup conditions met (bins=%d/%d) — dwell timer starts (%d min)",
                len([k for k in parsed["bins"] if not k.startswith("*:")]),
                len(parsed["bins"]),
                dwell_min,
            )

        dwell_elapsed = time.monotonic() - _ready_since
        _g_dwell_sec.set(dwell_elapsed)
        dwell_sec = dwell_min * 60.0

        if dwell_elapsed < dwell_sec:
            logger.debug("Dwell pending: %.1f / %.0f min", dwell_elapsed / 60, dwell_min)
            continue

        # Dwell satisfied
        if _promoted:
            logger.debug("Already promoted — skipping")
            continue

        if not auto_promote:
            logger.info("auto_promote=False — conditions met, not promoting")
            continue

        # Sanity check
        sane, sane_reason = _sanity_ok(parsed)
        if not sane:
            logger.warning("Sanity check blocked promote: %s", sane_reason)
            _notify_blocked(
                r,
                reason=sane_reason,
                notify_stream=notify_stream,
                dedup_ttl_h=dedup_ttl_h,
                dwell_sec=dwell_elapsed,
                now_ms=now_ms,
            )
            continue

        # Promote!
        _do_promote(
            r,
            parsed=parsed,
            promote_key=promote_key,
            snapshot_ttl=snapshot_ttl,
            notify_stream=notify_stream,
            dedup_ttl_h=dedup_ttl_h,
            now_ms=now_ms,
        )
        _promoted = True

    logger.info("Shutdown complete")


if __name__ == "__main__":
    main()
