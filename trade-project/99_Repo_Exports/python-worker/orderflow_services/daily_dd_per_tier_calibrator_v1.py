"""
daily_dd_per_tier_calibrator_v1.py — streaming service for DailyDDPerTierCalibrator.

Reads trades:closed, aggregates per-UTC-day P&L per (tier, regime),
calibrates adaptive daily DD limits per tier.

Master switch: DAILY_DD_TIER_CAL_ENFORCE=0 (shadow default).
"""
from __future__ import annotations

import json
import logging
import os
import time
from datetime import datetime, timezone
from typing import Any

from prometheus_client import Counter, Gauge, start_http_server

log = logging.getLogger("daily_dd_tier_cal")


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


def _env_bool(k: str, d: bool) -> bool:
    raw = _env(k, "")
    if not raw:
        return d
    return raw.strip().lower() in ("1", "true", "yes", "on")


def _safe_float(v: Any, default: float = 0.0) -> float:
    try:
        import math
        f = float(v)
        return f if math.isfinite(f) else default
    except (TypeError, ValueError):
        return default


def _gauge(name: str, doc: str, labels: list[str]) -> Gauge:
    from prometheus_client import REGISTRY
    try:
        return Gauge(name, doc, labels)
    except ValueError:
        for c in REGISTRY._collector_to_names:
            if name in REGISTRY._collector_to_names.get(c, []):
                return c  # type: ignore[return-value]
        raise


def _counter(name: str, doc: str, labels: list[str]) -> Counter:
    from prometheus_client import REGISTRY
    try:
        return Counter(name, doc, labels)
    except ValueError:
        for c in REGISTRY._collector_to_names:
            if name in REGISTRY._collector_to_names.get(c, []):
                return c  # type: ignore[return-value]
        raise


# Tier classification based on symbol (mirror of risk_policy_engine)
_TIER_A = frozenset({"BTCUSDT", "ETHUSDT"})
_TIER_B = frozenset({"SOLUSDT", "XRPUSDT", "BNBUSDT", "ADAUSDT", "DOGEUSDT", "AVAXUSDT"})


def _classify_tier(symbol: str) -> str:
    s = (symbol or "").upper()
    if s in _TIER_A:
        return "A"
    if s in _TIER_B:
        return "B"
    return "C"


def main() -> None:
    import redis  # type: ignore

    from core.redis_keys import RedisKeyPrefixes as RK
    from core.daily_dd_per_tier_calibrator import DailyDDPerTierCalibrator

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")

    redis_url = _env("DAILY_DD_TIER_CAL_REDIS_URL", _env("REDIS_URL", "redis://redis-worker-1:6379/0"))
    in_stream = _env("DAILY_DD_TIER_CAL_IN_STREAM", "trades:closed")
    group = _env("DAILY_DD_TIER_CAL_GROUP", "daily-dd-tier-cal")
    consumer = _env("DAILY_DD_TIER_CAL_CONSUMER", "daily-dd-tier-cal-1")
    out_key = _env("DAILY_DD_TIER_CAL_OUT_KEY", RK.AUTOCAL_DAILY_DD_TIER)
    port = _env_int("DAILY_DD_TIER_CAL_PORT", 9892)
    batch = _env_int("DAILY_DD_TIER_CAL_BATCH", 500)
    snap_sec = _env_int("DAILY_DD_TIER_CAL_SNAPSHOT_SEC", 300)
    enforce = _env_bool("DAILY_DD_TIER_CAL_ENFORCE", False)
    auto_enforce = _env_bool("DAILY_DD_TIER_CAL_AUTO_ENFORCE", True)
    window_days = _env_int("DAILY_DD_TIER_CAL_WINDOW_DAYS", 30)
    min_days = _env_int("DAILY_DD_TIER_CAL_MIN_DAYS", 10)

    rc = redis.from_url(redis_url, decode_responses=True)

    try:
        rc.xgroup_create(in_stream, group, id="0", mkstream=True)
    except Exception as e:
        if "BUSYGROUP" not in str(e):
            log.warning("xgroup_create: %s", e)

    cal = DailyDDPerTierCalibrator(
        enforce=enforce,
        auto_enforce=auto_enforce,
        window_days=window_days,
        min_days=min_days,
    )

    try:
        raw = rc.get(out_key)
        if raw:
            cal.load_state(json.loads(raw))
            log.info("Warm-started (enforce=%s)", cal.enforce)
    except Exception as e:
        log.warning("Warm-start failed: %s", e)

    start_http_server(port)
    g_soft = _gauge("daily_dd_tier_cal_soft_limit", "Calibrated soft DD limit pct", ["tier", "regime"])
    g_hard = _gauge("daily_dd_tier_cal_hard_limit", "Calibrated hard DD limit pct", ["tier", "regime"])
    c_obs = _counter("daily_dd_tier_cal_trades_total", "Trade observations", ["tier"])
    c_skip = _counter("daily_dd_tier_cal_skipped_total", "Skipped trades", ["reason"])

    # Daily aggregator: {(tier, regime, date_str): pnl_pct}
    _daily: dict[tuple[str, str, str], float] = {}
    last_snap_ms = 0

    log.info("daily_dd_tier_cal started (enforce=%s, port=%d)", enforce, port)

    while True:
        try:
            resp = rc.xreadgroup(
                groupname=group, consumername=consumer,
                streams={in_stream: ">"}, count=batch, block=5000,
            )
        except Exception as e:
            log.warning("XREADGROUP error: %s", e)
            time.sleep(1)
            continue

        if resp:
            ack_ids = []
            for _stream, messages in resp:
                for msg_id, fields in messages:
                    try:
                        pnl_pct = _safe_float(fields.get("pnl_pct") or fields.get("r_multiple"), float("nan"))
                        if pnl_pct != pnl_pct:
                            c_skip.labels(reason="pnl_missing").inc()
                            ack_ids.append(msg_id)
                            continue
                        symbol = fields.get("symbol", "") or ""
                        tier = _classify_tier(symbol)
                        regime = fields.get("market_regime", "") or fields.get("regime", "") or "*"
                        ts_ms = int(fields.get("ts_ms", int(time.time() * 1000)))
                        dt = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
                        date_str = dt.strftime("%Y-%m-%d")
                        agg_key = (tier, regime, date_str)
                        _daily[agg_key] = _daily.get(agg_key, 0.0) + pnl_pct
                        c_obs.labels(tier=tier).inc()
                    except Exception as ex:
                        log.debug("parse error: %s", ex)
                        c_skip.labels(reason="parse_error").inc()
                    ack_ids.append(msg_id)

            if ack_ids:
                try:
                    rc.xack(in_stream, group, *ack_ids)
                except Exception as e:
                    log.warning("XACK error: %s", e)

        # Flush aggregated days into calibrator
        now_ms = int(time.time() * 1000)
        for (tier, regime, date_str), pnl_pct in list(_daily.items()):
            cal.observe_day(tier=tier, regime=regime, date_str=date_str,
                            pnl_pct=pnl_pct, ts_ms=now_ms)

        if (now_ms - last_snap_ms) >= snap_sec * 1000:
            last_snap_ms = now_ms
            try:
                snap = cal.snapshot()
                rc.set(out_key, json.dumps(snap))
                for row in snap.get("bins", []):
                    g_soft.labels(tier=row["tier"], regime=row["regime"]).set(row["committed_soft_pct"])
                    g_hard.labels(tier=row["tier"], regime=row["regime"]).set(row["committed_hard_pct"])
            except Exception as e:
                log.warning("snapshot error: %s", e)


if __name__ == "__main__":
    main()
