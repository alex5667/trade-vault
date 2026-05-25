#!/usr/bin/env python3
"""
score_component_weight_calibrator_v1.py — feed service for walk-forward IR scoring weights.

Wiring:
  trades:closed (XREAD) → ScoreComponentWeightCalibrator.observe()
    → snapshot() → Redis SET autocal:score_weights:state
                 → Redis HSET autocal:score_weights:{symbol}:{regime}

ENV:
  SCORE_W_CAL_REDIS_URL      default REDIS_URL
  SCORE_W_CAL_GROUP          default score-w-cal
  SCORE_W_CAL_CONSUMER       default score-w-cal-1
  SCORE_W_CAL_PORT           default 9155
  SCORE_W_CAL_BATCH          default 100
  SCORE_W_CAL_WINDOW_DAYS    default 30
  SCORE_W_CAL_MIN_SAMPLES    default 50
  SCORE_W_CAL_SNAPSHOT_SEC   default 60
  SCORE_W_CAL_PROMOTE        default 0 (shadow; set 1 to auto-promote all keys)
  SCORE_W_CAL_ENFORCE        default 0 (reader in scoring uses shadow only if 0)
"""
from __future__ import annotations

import json
import logging
import os
import signal
import time
from typing import Any

import redis  # type: ignore
from prometheus_client import Counter, Gauge, Histogram, start_http_server  # type: ignore

from core.redis_keys import RS
from core.score_component_weight_calibrator import (
    DEFAULT_WEIGHTS,
    COMPONENTS,
    ScoreComponentWeightCalibrator,
    extract_component_scores,
)

logger = logging.getLogger("score-w-cal")


def _env(name: str, default: str = "") -> str:
    return os.getenv(name, default)


def _env_int(name: str, default: int) -> int:
    try:
        return int(_env(name)) or default
    except (ValueError, TypeError):
        return default


def _env_float(name: str, default: float) -> float:
    try:
        return float(_env(name)) or default
    except (ValueError, TypeError):
        return default


def _env_bool(name: str, default: bool) -> bool:
    return _env(name, "1" if default else "0").strip().lower() in ("1", "true", "yes")


# ---------------------------------------------------------------------------
# Redis keys
# ---------------------------------------------------------------------------

REDIS_STATE_KEY = "autocal:score_weights:state"


def _per_key(symbol: str, regime: str) -> str:
    return f"autocal:score_weights:{symbol}:{regime}"


# ---------------------------------------------------------------------------
# Prometheus
# ---------------------------------------------------------------------------

_trades_observed = Counter(
    "score_w_cal_trades_observed_total",
    "Trades observed by score component calibrator",
    ["symbol", "regime"],
)
_trades_skipped = Counter(
    "score_w_cal_trades_skipped_total",
    "Trades skipped (missing component scores)",
    ["reason"],
)
_snapshot_writes = Counter(
    "score_w_cal_snapshot_writes_total",
    "Redis snapshot writes",
)
_snapshot_errors = Counter(
    "score_w_cal_snapshot_errors_total",
    "Redis snapshot write errors",
)
_promoted_keys = Counter(
    "score_w_cal_promoted_keys_total",
    "Keys auto-promoted shadow→committed",
)
_buf_sizes = Gauge(
    "score_w_cal_buffer_sizes",
    "Rolling buffer sample counts",
    ["symbol", "regime"],
)
_ir_gauge = Gauge(
    "score_w_cal_ir",
    "Last computed IR per component",
    ["symbol", "regime", "component"],
)
_lag_hist = Histogram(
    "score_w_cal_event_lag_ms",
    "Lag from event_time_ms to observe",
    buckets=[10, 50, 200, 1000, 5000, 30000],
)


# ---------------------------------------------------------------------------
# Payload parsing
# ---------------------------------------------------------------------------

def _lookup_signal_score_components(
    redis_client: Any | None,
    *,
    symbol: str,
    sid: str | None,
    ts_close_ms: int,
) -> dict[str, Any]:
    """Best-effort fallback: scan signals:of:inputs to find score_components by sid.

    Producers do not propagate `score_components` into the trades:closed flat
    schema — they live only inside the original signal payload. We search the
    last ~30 minutes of `signals:of:inputs` (cheap XRANGE) for a matching sid
    and return `confidence_breakdown` / `score_components` from indicators.
    """
    if redis_client is None or not sid:
        return {}
    try:
        # Signals are XADD'd around signal-time ≤ trade-open-time. The trade
        # we are processing closed at ts_close_ms; the signal predates it by
        # at most a few hours. Search a 6h window backward from ts_close.
        since_ms = max(0, int(ts_close_ms) - 6 * 3600 * 1000)
        cursor = f"{since_ms}-0"
        # bound the scan so a hot loop never stalls
        MAX_SCAN = 5000
        scanned = 0
        norm_sid_target = str(sid).split(":")
        target_symbol = norm_sid_target[1] if len(norm_sid_target) >= 2 else symbol
        target_ts = norm_sid_target[2] if len(norm_sid_target) >= 3 else ""
        while scanned < MAX_SCAN:
            chunk = redis_client.xrange("signals:of:inputs", min=cursor, count=500)
            if not chunk:
                break
            for entry_id, fields in chunk:
                scanned += 1
                if scanned >= MAX_SCAN:
                    break
                payload = fields.get("payload") if isinstance(fields, dict) else None
                if not payload:
                    continue
                try:
                    p = json.loads(payload)
                except Exception:
                    continue
                inner = p.get("data", p) if isinstance(p, dict) else p
                if isinstance(inner, str):
                    try:
                        inner = json.loads(inner)
                    except Exception:
                        continue
                if not isinstance(inner, dict):
                    continue
                ssid = str(inner.get("sid") or inner.get("signal_id") or "")
                # Normalize comparison: ignore kind-prefix + direction-suffix
                ssid_parts = ssid.split(":")
                if len(ssid_parts) < 3:
                    continue
                if ssid_parts[1] != target_symbol:
                    continue
                if target_ts and ssid_parts[2] != target_ts:
                    continue
                ind = inner.get("indicators") or {}
                if isinstance(ind, dict):
                    out: dict[str, Any] = {}
                    out.update(dict(ind.get("score_components") or {}))
                    out.update(dict(ind.get("confidence_breakdown") or {}))
                    for k in ("s_z", "s_obi20", "s_obi", "s_l3", "s_l3_pressure",
                              "s_microprice", "s_micro_block", "s_mp", "s_mode",
                              "regime_class_raw", "regime"):
                        if k in ind:
                            out.setdefault(k, ind[k])
                    return out
            cursor = f"{chunk[-1][0].split('-')[0]}-{int(chunk[-1][0].split('-')[1]) + 1}"
            if len(chunk) < 500:
                break
    except Exception:
        return {}
    return {}


def _parse_trade(raw: dict[str, Any], *, redis_client: Any | None = None) -> dict[str, Any] | None:
    """Extract fields needed by the calibrator from a trades:closed entry."""
    try:
        payload_raw = raw.get("data") or raw.get("payload")
        if payload_raw:
            payload = json.loads(payload_raw) if isinstance(payload_raw, str) else payload_raw
        else:
            payload = raw

        symbol = payload.get("symbol") or raw.get("symbol", "")
        if not symbol:
            return None

        regime = str(payload.get("regime") or payload.get("market_regime") or "unknown").lower()
        r_multiple = payload.get("r_multiple")
        if r_multiple is None:
            r_multiple = payload.get("pnl_r") or payload.get("net_r")
        if r_multiple is None:
            _trades_skipped.labels(reason="no_r_multiple").inc()
            return None

        ts_close_ms = payload.get("ts_close_ms") or payload.get("exit_ts_ms") or int(time.time() * 1000)

        # Component scores from indicators / score_parts stored in signal
        indicators = payload.get("indicators") or {}
        if isinstance(indicators, str):
            try:
                indicators = json.loads(indicators)
            except Exception:
                indicators = {}

        config_snapshot = payload.get("config_snapshot") or {}
        if isinstance(config_snapshot, str):
            try:
                config_snapshot = json.loads(config_snapshot)
            except Exception:
                config_snapshot = {}

        # Look for component scores in priority order
        score_parts: dict[str, Any] = {}
        # 1) Direct score_components in indicators
        score_parts.update(dict(indicators.get("score_components") or {}))
        # 2) confidence_breakdown sub-scores
        score_parts.update(dict(indicators.get("confidence_breakdown") or {}))
        # 3) Top-level indicators (s_z, s_obi20, etc)
        for k in ["s_z", "s_obi20", "s_obi", "s_l3", "s_l3_pressure",
                  "s_microprice", "s_micro_block", "s_mp", "s_mode",
                  "regime_class_raw", "regime"]:
            if k in indicators:
                score_parts.setdefault(k, indicators[k])
        # 4) config_snapshot.indicators
        cs_ind = config_snapshot.get("indicators") or {}
        if isinstance(cs_ind, str):
            try:
                cs_ind = json.loads(cs_ind)
            except Exception:
                cs_ind = {}
        for k in ["s_z", "s_obi20", "s_obi", "s_l3", "s_l3_pressure",
                  "s_microprice", "s_micro_block", "s_mp", "s_mode",
                  "regime_class_raw", "regime"]:
            if k in cs_ind:
                score_parts.setdefault(k, cs_ind[k])

        if not score_parts:
            # Fallback: producer didn't propagate score_components into
            # trades:closed flat schema; look up the original signal payload
            # in signals:of:inputs by sid.
            sid = payload.get("sid") or payload.get("signal_id") or raw.get("sid")
            score_parts = _lookup_signal_score_components(
                redis_client,
                symbol=symbol,
                sid=sid,
                ts_close_ms=int(ts_close_ms),
            )
            if not score_parts:
                _trades_skipped.labels(reason="no_score_parts").inc()
                return None
            _trades_skipped.labels(reason="recovered_via_sid_lookup").inc()

        return {
            "symbol": symbol,
            "regime": regime,
            "r_multiple": float(r_multiple),
            "score_parts": score_parts,
            "ts_ms": int(ts_close_ms),
        }

    except Exception as e:
        logger.debug("parse_trade error: %s", e)
        _trades_skipped.labels(reason="parse_error").inc()
        return None


# ---------------------------------------------------------------------------
# Snapshot publish
# ---------------------------------------------------------------------------

def _publish_snapshot(
    r: Any,
    cal: ScoreComponentWeightCalibrator,
    *,
    snapshot_ttl: int = 7 * 86400,
) -> None:
    try:
        snap = cal.snapshot()
        r.set(REDIS_STATE_KEY, json.dumps(snap, default=str), ex=snapshot_ttl)

        for (sym, reg), w in cal._committed.items():
            pipe = r.pipeline()
            key = _per_key(sym, reg)
            for comp, wv in w.items():
                pipe.hset(key, comp, str(round(wv, 6)))
            pipe.expire(key, snapshot_ttl)
            pipe.execute()

        for (sym, reg) in cal._buffers:
            size = len(cal._buffers[(sym, reg)])
            _buf_sizes.labels(symbol=sym, regime=reg).set(size)
            ir = cal.ir_last(sym, reg)
            if ir:
                for comp, irv in ir.items():
                    _ir_gauge.labels(symbol=sym, regime=reg, component=comp).set(irv)

        _snapshot_writes.inc()
        logger.debug("Snapshot published: %d keys", len(cal._committed))
    except Exception as e:
        _snapshot_errors.inc()
        logger.error("Snapshot publish error: %s", e)


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

def run(
    redis_url: str,
    *,
    group: str,
    consumer: str,
    batch: int,
    window_days: int,
    min_samples: int,
    snapshot_sec: int,
    auto_promote: bool,
) -> None:
    r = redis.Redis.from_url(redis_url, decode_responses=True)

    stream = RS.TRADES_CLOSED if hasattr(RS, "TRADES_CLOSED") else "stream:trades:closed"

    try:
        r.xgroup_create(stream, group, id="$", mkstream=True)
    except Exception:
        pass

    cal = ScoreComponentWeightCalibrator(
        window_days=window_days,
        min_samples=min_samples,
    )

    # Restore state
    try:
        raw_state = r.get(REDIS_STATE_KEY)
        if raw_state:
            cal.load_state(json.loads(raw_state))
            logger.info("State restored from Redis")
    except Exception as e:
        logger.warning("Could not restore state: %s", e)

    last_snapshot = time.monotonic()
    logger.info(
        "score-w-cal started: stream=%s group=%s window=%dd min_samples=%d promote=%s",
        stream, group, window_days, min_samples, auto_promote,
    )

    _stop = False

    def _sig(_n: int, _f: Any) -> None:
        nonlocal _stop
        _stop = True

    signal.signal(signal.SIGTERM, _sig)
    signal.signal(signal.SIGINT, _sig)

    while not _stop:
        try:
            msgs = r.xreadgroup(
                group, consumer,
                {stream: ">"},
                count=batch,
                block=2000,
            )
        except Exception as e:
            logger.error("XREADGROUP error: %s", e)
            time.sleep(2)
            continue

        if not msgs:
            continue

        for _stream, entries in msgs:
            for msg_id, raw in entries:
                try:
                    trade = _parse_trade(raw, redis_client=r)
                    if trade is None:
                        r.xack(stream, group, msg_id)
                        continue

                    comp_scores = extract_component_scores(trade["score_parts"])
                    now_ms = int(time.time() * 1000)
                    _lag_hist.observe(now_ms - trade["ts_ms"])

                    cal.observe(
                        symbol=trade["symbol"],
                        regime=trade["regime"],
                        component_scores=comp_scores,
                        outcome_r=trade["r_multiple"],
                        ts_ms=trade["ts_ms"],
                    )
                    _trades_observed.labels(
                        symbol=trade["symbol"], regime=trade["regime"]
                    ).inc()
                    r.xack(stream, group, msg_id)

                except Exception as e:
                    logger.error("Processing error %s: %s", msg_id, e)
                    r.xack(stream, group, msg_id)

        if auto_promote:
            promoted = cal.promote_all()
            if promoted:
                _promoted_keys.inc(len(promoted))
                logger.info("Auto-promoted: %s", promoted)

        now = time.monotonic()
        if now - last_snapshot >= snapshot_sec:
            _publish_snapshot(r, cal)
            last_snapshot = now

    logger.info("Shutting down score-w-cal")
    _publish_snapshot(r, cal)


def main() -> None:
    logging.basicConfig(level=logging.INFO)

    port = _env_int("SCORE_W_CAL_PORT", 9155)
    try:
        start_http_server(port)
        logger.info("Metrics: :%d/metrics", port)
    except Exception as e:
        logger.warning("Could not start metrics server: %s", e)

    run(
        redis_url=_env("SCORE_W_CAL_REDIS_URL") or _env("REDIS_URL", "redis://redis-worker-1:6379/0"),
        group=_env("SCORE_W_CAL_GROUP", "score-w-cal"),
        consumer=_env("SCORE_W_CAL_CONSUMER", "score-w-cal-1"),
        batch=_env_int("SCORE_W_CAL_BATCH", 100),
        window_days=_env_int("SCORE_W_CAL_WINDOW_DAYS", 30),
        min_samples=_env_int("SCORE_W_CAL_MIN_SAMPLES", 50),
        snapshot_sec=_env_int("SCORE_W_CAL_SNAPSHOT_SEC", 60),
        auto_promote=_env_bool("SCORE_W_CAL_PROMOTE", False),
    )


if __name__ == "__main__":
    main()
