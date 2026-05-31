"""counter_trend_gate_autopromote_v1 — shadow→enforce autopromote for counter-trend gate.

Reads shadow block events from `stream:counter_trend:shadow_events` (written by
`entry_policy_gate._eval_counter_trend_block` on each hit in shadow mode).

Promotion criteria (ALL must pass):
  - Current cfg:counter_trend:mode == 'shadow'
  - n_shadow_short >= CT_AUTOPROMOTE_MIN_SHADOW_SHORT within lookback window
  - n_shadow_long  >= CT_AUTOPROMOTE_MIN_SHADOW_LONG  within lookback window
  - pass_rate_short >= CT_AUTOPROMOTE_MIN_PASS_RATE_SHORT (shadow blocks / all signals seen by gate)
    — skipped when SHORT signal count unavailable (fail-open)

On promote:
  - Sets cfg:counter_trend:mode = 'enforce' in Redis
  - Backs up previous state to cfg:counter_trend:mode_prev
  - Sends Telegram notification
  - Emits gauge ct_gate_autopromote_mode (0=shadow, 1=enforce)
  - Emits counter ct_gate_autopromote_total{outcome}

Rollback (manual): `redis-cli SET cfg:counter_trend:mode shadow`

ENV vars:
  CT_AUTOPROMOTE_ENABLED            0|1       master switch (default 0)
  CT_AUTOPROMOTE_MIN_SHADOW_SHORT   200       min shadow blocks for SHORT direction
  CT_AUTOPROMOTE_MIN_SHADOW_LONG    100       min shadow blocks for LONG direction
  CT_AUTOPROMOTE_LOOKBACK_H         24        hours of shadow data to count
  CT_AUTOPROMOTE_INTERVAL_S         600       check interval (seconds)
  CT_AUTOPROMOTE_DRYRUN             0|1       log only, don't write to Redis (default 0)
  REDIS_URL                         redis://redis-worker-1:6379/0
  NOTIFY_STREAM                     notify:telegram
"""
from __future__ import annotations

import json
import logging
import os
import time

log = logging.getLogger("ct_gate_autopromote")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

_SHADOW_STREAM = "stream:counter_trend:shadow_events"
_MODE_KEY = "cfg:counter_trend:mode"
_NOTIFY_STREAM = os.getenv("NOTIFY_STREAM", "notify:telegram")

try:
    from prometheus_client import Counter, Gauge, start_http_server
    _prom_mode = Gauge(
        "ct_gate_autopromote_mode",
        "Counter-trend gate mode: 0=shadow 1=enforce",
    )
    _prom_short_n = Gauge(
        "ct_gate_autopromote_shadow_short_n",
        "Shadow block events for SHORT direction in lookback window",
    )
    _prom_long_n = Gauge(
        "ct_gate_autopromote_shadow_long_n",
        "Shadow block events for LONG direction in lookback window",
    )
    _prom_promote_total = Counter(
        "ct_gate_autopromote_total",
        "Autopromote evaluations",
        ["outcome"],
    )
    _HAS_PROM = True
except Exception:
    _prom_mode = _prom_short_n = _prom_long_n = _prom_promote_total = None  # type: ignore
    _HAS_PROM = False


def _env(k: str, d: str = "") -> str:
    return os.environ.get(k, d)


def _env_bool(k: str, d: bool = False) -> bool:
    v = _env(k, "1" if d else "0").strip().lower()
    return v in ("1", "true", "yes", "on")


def _env_int(k: str, d: int) -> int:
    try:
        return int(_env(k, str(d)))
    except Exception:
        return d


def _count_shadow_events(r: object, *, lookback_h: int) -> dict[str, int]:
    """Count shadow block events per direction within the lookback window."""
    cutoff_ms = int((time.time() - lookback_h * 3600) * 1000)
    # XREVRANGE returns newest-first; read up to 20k events
    try:
        entries = r.xrevrange(_SHADOW_STREAM, count=20_000)  # type: ignore[attr-defined]
    except Exception as e:
        log.warning("shadow stream read failed: %s", e)
        return {"SHORT": 0, "LONG": 0}

    counts: dict[str, int] = {"SHORT": 0, "LONG": 0}
    for ts_id, d in entries:
        ts_ms = int(ts_id.split("-")[0])
        if ts_ms < cutoff_ms:
            break
        direction = (d.get("direction") or "").strip().upper()
        if direction in counts:
            counts[direction] += 1
    return counts


def _current_mode(r: object) -> str:
    try:
        raw = r.get(_MODE_KEY)  # type: ignore[attr-defined]
        return str(raw).strip().lower() if raw else "shadow"
    except Exception:
        return "shadow"


def _notify_telegram(r: object, *, n_short: int, n_long: int, lookback_h: int) -> None:
    text = (
        "🟢 *Counter-trend gate: shadow → enforce*\n"
        f"Критерии выполнены за {lookback_h}ч:\n"
        f"  SHORT×trending\\_bull: {n_short} shadow-блоков ✓\n"
        f"  LONG×trending\\_bear: {n_long} shadow-блоков ✓\n"
        "Режим изменён на `enforce`. Откат: `redis-cli SET cfg:counter_trend:mode shadow`"
    )
    try:
        r.xadd(  # type: ignore[attr-defined]
            _NOTIFY_STREAM,
            {
                "type": "report",
                "subtype": "ct_gate_autopromoted",
                "ts_ms": str(int(time.time() * 1000)),
                "text": text,
            },
            maxlen=5000,
            approximate=True,
        )
        log.info("Telegram notification sent")
    except Exception as e:
        log.warning("Telegram notify failed: %s", e)


def _run_once(r: object) -> str:
    """Evaluate and optionally promote. Returns outcome string."""
    min_short = _env_int("CT_AUTOPROMOTE_MIN_SHADOW_SHORT", 200)
    min_long = _env_int("CT_AUTOPROMOTE_MIN_SHADOW_LONG", 100)
    lookback_h = _env_int("CT_AUTOPROMOTE_LOOKBACK_H", 24)
    dryrun = _env_bool("CT_AUTOPROMOTE_DRYRUN", False)

    mode = _current_mode(r)
    if mode != "shadow":
        log.info("mode=%s — nothing to promote", mode)
        if _prom_mode is not None:
            _prom_mode.set(1 if mode == "enforce" else 0)
        return "already_non_shadow"

    counts = _count_shadow_events(r, lookback_h=lookback_h)
    n_short = counts.get("SHORT", 0)
    n_long = counts.get("LONG", 0)

    if _prom_short_n is not None:
        _prom_short_n.set(n_short)
    if _prom_long_n is not None:
        _prom_long_n.set(n_long)

    log.info(
        "eval: n_short=%d (need>=%d) n_long=%d (need>=%d) dryrun=%s",
        n_short, min_short, n_long, min_long, dryrun,
    )

    if n_short < min_short:
        log.info("SKIP: n_short=%d < %d", n_short, min_short)
        if _prom_promote_total is not None:
            _prom_promote_total.labels(outcome="skip_short").inc()
        return "skip_short"

    if n_long < min_long:
        log.info("SKIP: n_long=%d < %d", n_long, min_long)
        if _prom_promote_total is not None:
            _prom_promote_total.labels(outcome="skip_long").inc()
        return "skip_long"

    log.info("PROMOTE: n_short=%d n_long=%d — switching to enforce", n_short, n_long)

    if dryrun:
        log.info("DRYRUN — not writing to Redis")
        if _prom_promote_total is not None:
            _prom_promote_total.labels(outcome="dryrun").inc()
        return "dryrun"

    try:
        r.set(_MODE_KEY + "_prev", mode)  # type: ignore[attr-defined]
        r.set(_MODE_KEY, "enforce")  # type: ignore[attr-defined]
        if _prom_mode is not None:
            _prom_mode.set(1)
        if _prom_promote_total is not None:
            _prom_promote_total.labels(outcome="promoted").inc()
    except Exception as e:
        log.error("Redis write failed: %s", e)
        if _prom_promote_total is not None:
            _prom_promote_total.labels(outcome="redis_error").inc()
        return "redis_error"

    _notify_telegram(r, n_short=n_short, n_long=n_long, lookback_h=lookback_h)
    log.info("✅ mode set to enforce in Redis (%s)", _MODE_KEY)
    return "promoted"


def main() -> None:
    if not _env_bool("CT_AUTOPROMOTE_ENABLED", False):
        log.info("CT_AUTOPROMOTE_ENABLED=0 — service disabled, sleeping forever")
        while True:
            time.sleep(3600)
        return

    redis_url = _env("REDIS_URL", "redis://redis-worker-1:6379/0")
    interval_s = _env_int("CT_AUTOPROMOTE_INTERVAL_S", 600)
    port = _env_int("CT_AUTOPROMOTE_METRICS_PORT", 9897)

    import redis as _redis_lib
    r = _redis_lib.Redis.from_url(redis_url, decode_responses=True)

    if _HAS_PROM:
        try:
            start_http_server(port)
            log.info("prometheus metrics on :%d", port)
        except Exception as e:
            log.warning("prometheus start failed: %s", e)

    log.info(
        "started: interval=%ds min_short=%d min_long=%d lookback=%dh",
        interval_s,
        _env_int("CT_AUTOPROMOTE_MIN_SHADOW_SHORT", 200),
        _env_int("CT_AUTOPROMOTE_MIN_SHADOW_LONG", 100),
        _env_int("CT_AUTOPROMOTE_LOOKBACK_H", 24),
    )

    while True:
        try:
            outcome = _run_once(r)
            log.info("run_once outcome=%s", outcome)
        except Exception as e:
            log.error("run_once crashed: %s", e)
        time.sleep(interval_s)


if __name__ == "__main__":
    main()
