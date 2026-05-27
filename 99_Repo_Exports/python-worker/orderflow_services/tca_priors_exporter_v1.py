#!/usr/bin/env python3
"""tca_priors_exporter_v1.py — ADR-0005 skeleton.

Subscribes to fills stream, maintains per-{symbol, kind, session} EMA state
in Redis hash `tca:ema:{symbol}:{kind}:{session_bucket}`, exposes Prometheus
gauges so of_confirm_engine can hydrate features via single HMGET.

STATUS: SKELETON. Redis schema + Prometheus contract are fixed, but EMA
formulas for realized spread / permanent impact / IS are placeholder. Promote
to production after ADR-0005 design review (see /home/alex/Apps/Obsidian/
trade-vault/80_Research/ADR-0005 TCA EMA Priors Pipeline.md).

ENV
  TCA_PRIORS_PORT                 (default 9144)
  TCA_PRIORS_GROUP                (default "tca-priors-exporter")
  TCA_PRIORS_CONSUMER             (default tca-priors-exporter-1)
  TCA_PRIORS_BATCH                (default 100)
  TCA_PRIORS_EMA_HL_FILLS         (default 200)
  TCA_PRIORS_TTL_SEC              (default 7200)
  TCA_PRIORS_MIN_SAMPLES          (default 30)
"""
from __future__ import annotations

import logging
import math
import os
import signal
import time
from collections import deque
from typing import Any

from prometheus_client import REGISTRY, Counter, Gauge, start_http_server  # type: ignore

from core.redis_client import get_redis
from utils.time_utils import get_ny_time_millis

logger = logging.getLogger("tca_priors_exporter")

# Canonical fills stream — sourced from binance_execution/
FILLS_STREAM = "stream:fills:filled"
_SESSIONS = ("asia", "europe", "us")


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name) or default)
    except Exception:
        return default


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name) or default)
    except Exception:
        return default


def _env_symbols() -> list[str]:
    for env_name in ("TCA_PRIORS_SYMBOLS", "CANARY_SYMBOLS", "PIT_PRIORS_SYMBOLS", "CRYPTO_SYMBOLS"):
        raw = (os.getenv(env_name) or "").strip()
        if not raw:
            continue
        out = [s.strip().upper() for s in raw.replace(";", ",").split(",") if s.strip()]
        if out:
            return out
    return ["BTCUSDT", "ETHUSDT"]


def _session_bucket(ts_ms: int) -> str:
    """UTC hour → session label aligned with Phase 7.5 session_* features."""
    h = (ts_ms // 3_600_000) % 24
    if 13 <= h < 22:
        return "us"
    if 7 <= h < 16:
        return "europe"
    return "asia"


def _get_or_create_gauge(name: str, doc: str, labels: list[str]) -> Gauge:
    try:
        return Gauge(name, doc, labels)
    except ValueError:
        for c in REGISTRY._collector_to_names:
            if name in REGISTRY._collector_to_names[c]:
                return c  # type: ignore
        raise


def _get_or_create_counter(name: str, doc: str, labels: list[str]) -> Counter:
    try:
        return Counter(name, doc, labels)
    except ValueError:
        for c in REGISTRY._collector_to_names:
            if name in REGISTRY._collector_to_names[c]:
                return c  # type: ignore
        raise


_P95_WINDOW = 500  # bounded deque depth for p95 computation per bucket


class TCAEMAState:
    """Per-{symbol, kind, session} EMA aggregator for TCA features."""

    def __init__(self, ema_half_life: float, ttl_sec: int) -> None:
        self.alpha = 1.0 - 0.5 ** (1.0 / max(1.0, ema_half_life))
        self.ttl_sec = ttl_sec
        # In-memory cache mirrors Redis hash for hot reads.
        self._cache: dict[tuple[str, str, str], dict[str, float]] = {}
        # Bounded deques for p95 computation: eff_spread and is_bps samples.
        self._spread_window: dict[tuple[str, str, str], deque[float]] = {}
        self._slippage_window: dict[tuple[str, str, str], deque[float]] = {}

    def update(
        self,
        redis_client: Any,
        symbol: str,
        kind: str,
        session: str,
        *,
        eff_spread_bps: float,
        realized_1s_bps: float,
        realized_5s_bps: float,
        perm_1s_bps: float,
        perm_5s_bps: float,
        is_bps: float,
    ) -> dict[str, float]:
        key = (symbol, kind, session)
        prev = self._cache.get(key, {})
        new_state: dict[str, float] = {}

        for metric, value in (
            ("eff_spread", eff_spread_bps),
            ("realized_1s", realized_1s_bps),
            ("realized_5s", realized_5s_bps),
            ("perm_1s", perm_1s_bps),
            ("perm_5s", perm_5s_bps),
            ("is_bps", is_bps),
        ):
            if not math.isfinite(value):
                new_state[metric] = float(prev.get(metric, 0.0))
                continue
            prev_val = prev.get(metric, value)
            new_state[metric] = (1 - self.alpha) * prev_val + self.alpha * value

        new_state["samples"] = float(prev.get("samples", 0.0)) + 1.0
        # Use ingestion time (now) so staleness reflects when data was processed,
        # not when the trade happened (virtual fills bridge uses historical ts_ms).
        import time as _time
        new_state["last_update_ms"] = float(int(_time.time() * 1000))

        # p95 of eff_spread (spread_p95) and is_bps (slippage_p95) via bounded window.
        if math.isfinite(eff_spread_bps):
            sw = self._spread_window.setdefault(key, deque(maxlen=_P95_WINDOW))
            sw.append(eff_spread_bps)
            _ss = sorted(sw)
            new_state["spread_p95_bps"] = _ss[min(len(_ss) - 1, int(0.95 * len(_ss)))]
        else:
            new_state["spread_p95_bps"] = prev.get("spread_p95_bps", 0.0)

        if math.isfinite(is_bps):
            lw = self._slippage_window.setdefault(key, deque(maxlen=_P95_WINDOW))
            lw.append(is_bps)
            _ls = sorted(lw)
            new_state["slippage_p95_bps"] = _ls[min(len(_ls) - 1, int(0.95 * len(_ls)))]
        else:
            new_state["slippage_p95_bps"] = prev.get("slippage_p95_bps", 0.0)

        self._cache[key] = new_state

        # Mirror to Redis hash for of_confirm_engine to read.
        redis_key = f"tca:ema:{symbol}:{kind}:{session}"
        try:
            redis_client.hset(redis_key, mapping={k: f"{v:.6f}" for k, v in new_state.items()})
            redis_client.expire(redis_key, self.ttl_sec)
        except Exception as e:
            logger.warning("Redis HSET tca:ema failed for %s: %s", redis_key, e)

        return new_state


def _decode(val: Any) -> str:
    if isinstance(val, (bytes, bytearray)):
        return val.decode("utf-8", "ignore")
    return str(val) if val is not None else ""


def _safe_float(val: Any) -> float:
    try:
        if val is None:
            return float("nan")
        if isinstance(val, (bytes, bytearray)):
            val = val.decode("utf-8", "ignore")
        f = float(val)
        return f if math.isfinite(f) else float("nan")
    except Exception:
        return float("nan")


def _extract_tca_update(fields: dict[str, str]) -> tuple[str, str, str, int, dict[str, float]] | None:
    """Parse a fills-stream message into TCA EMA inputs.

    Returns:
      (symbol, kind, session, ts_ms, metrics_dict) or None when required
      source fields are missing/invalid.
    """
    symbol = (fields.get("symbol") or "").upper()
    kind = fields.get("kind") or fields.get("scenario") or "default"
    ts_ms_raw = _safe_float(fields.get("ts_ms"))
    ts_ms = int(ts_ms_raw) if math.isfinite(ts_ms_raw) and ts_ms_raw > 0 else get_ny_time_millis()
    session = _session_bucket(ts_ms)

    fill_px = _safe_float(fields.get("price") or fields.get("avg_px"))
    arrival_mid = _safe_float(fields.get("arrival_mid") or fields.get("mid_at_arrival"))
    side = (fields.get("side") or "").upper()
    sign = 1.0 if side in ("BUY", "LONG") else -1.0

    if not symbol or not (math.isfinite(fill_px) and math.isfinite(arrival_mid) and arrival_mid > 0):
        return None

    eff_spread_bps = _safe_float(fields.get("eff_spread_bps"))
    if not math.isfinite(eff_spread_bps):
        eff_spread_bps = 10000.0 * 2.0 * sign * (fill_px - arrival_mid) / arrival_mid

    realized_1s = _safe_float(fields.get("realized_spread_1s_bps"))
    realized_5s = _safe_float(fields.get("realized_spread_5s_bps"))
    perm_1s = _safe_float(fields.get("perm_impact_1s_bps"))
    perm_5s = _safe_float(fields.get("perm_impact_5s_bps"))
    is_bps = _safe_float(fields.get("is_bps"))
    if not math.isfinite(is_bps):
        is_bps = eff_spread_bps

    # Fallback path: fills_tca_enricher publishes mid_after_*_bps even when
    # the derived TCA fields are absent. Recover missing metrics here so the
    # exporter does not silently collapse to zeros.
    mid_after_1s_bps = _safe_float(fields.get("mid_after_1s_bps"))
    if math.isfinite(mid_after_1s_bps):
        if not math.isfinite(perm_1s):
            perm_1s = sign * mid_after_1s_bps
        if not math.isfinite(realized_1s):
            realized_1s = eff_spread_bps - 2.0 * (perm_1s if math.isfinite(perm_1s) else sign * mid_after_1s_bps)

    mid_after_5s_bps = _safe_float(fields.get("mid_after_5s_bps"))
    if math.isfinite(mid_after_5s_bps):
        if not math.isfinite(perm_5s):
            perm_5s = sign * mid_after_5s_bps
        if not math.isfinite(realized_5s):
            realized_5s = eff_spread_bps - 2.0 * (perm_5s if math.isfinite(perm_5s) else sign * mid_after_5s_bps)

    return (
        symbol,
        kind,
        session,
        ts_ms,
        {
            "eff_spread_bps": eff_spread_bps,
            "realized_1s_bps": realized_1s if math.isfinite(realized_1s) else 0.0,
            "realized_5s_bps": realized_5s if math.isfinite(realized_5s) else 0.0,
            "perm_1s_bps": perm_1s if math.isfinite(perm_1s) else 0.0,
            "perm_5s_bps": perm_5s if math.isfinite(perm_5s) else 0.0,
            "is_bps": is_bps,
        },
    )


def seed_tca_placeholders(redis_client: Any, *, ttl_sec: int, symbols: list[str]) -> int:
    written = 0
    payload = {
        "eff_spread": "0.000000",
        "realized_1s": "0.000000",
        "realized_5s": "0.000000",
        "perm_1s": "0.000000",
        "perm_5s": "0.000000",
        "is_bps": "0.000000",
        "samples": "0.000000",
        "last_update_ms": "0.000000",
        "spread_p95_bps": "0.000000",
        "slippage_p95_bps": "0.000000",
    }
    for symbol in symbols:
        sym = (symbol or "").strip().upper()
        if not sym:
            continue
        for session in _SESSIONS:
            redis_key = f"tca:ema:{sym}:default:{session}"
            try:
                if redis_client.exists(redis_key):
                    continue
                redis_client.hset(redis_key, mapping=payload)
                redis_client.expire(redis_key, ttl_sec)
                written += 1
            except Exception as e:
                logger.debug("seed TCA placeholder failed for %s: %s", redis_key, e)
    return written


def main() -> None:
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    port = _env_int("TCA_PRIORS_PORT", 9144)
    group = os.getenv("TCA_PRIORS_GROUP", "tca-priors-exporter")
    consumer = os.getenv("TCA_PRIORS_CONSUMER", "tca-priors-exporter-1")
    batch = _env_int("TCA_PRIORS_BATCH", 100)
    ema_hl = _env_float("TCA_PRIORS_EMA_HL_FILLS", 200.0)
    ttl_sec = _env_int("TCA_PRIORS_TTL_SEC", 7200)
    seed_symbols = _env_symbols()

    logger.info(
        "Starting TCA priors exporter: port=%d group=%s ema_hl=%.0f ttl=%ds (SKELETON)",
        port, group, ema_hl, ttl_sec,
    )

    # Prometheus metrics — labels match ADR-0005 contract
    g_eff_spread = _get_or_create_gauge(
        "tca_eff_spread_bps_ema",
        "Effective spread EMA (bps) per symbol/kind/session",
        ["symbol", "kind", "session"],
    )
    g_realized_1s = _get_or_create_gauge(
        "tca_realized_spread_1s_bps_ema",
        "Realized spread EMA at 1s horizon (bps)",
        ["symbol", "kind", "session"],
    )
    g_realized_5s = _get_or_create_gauge(
        "tca_realized_spread_5s_bps_ema",
        "Realized spread EMA at 5s horizon (bps)",
        ["symbol", "kind", "session"],
    )
    g_perm_1s = _get_or_create_gauge(
        "tca_perm_impact_1s_bps_ema",
        "Permanent impact EMA at 1s horizon (bps)",
        ["symbol", "kind", "session"],
    )
    g_perm_5s = _get_or_create_gauge(
        "tca_perm_impact_5s_bps_ema",
        "Permanent impact EMA at 5s horizon (bps)",
        ["symbol", "kind", "session"],
    )
    g_is_bps = _get_or_create_gauge(
        "tca_is_bps_ema",
        "Implementation shortfall EMA (bps)",
        ["symbol", "kind", "session"],
    )
    g_samples = _get_or_create_gauge(
        "tca_samples",
        "Sample count for TCA EMA per symbol/kind/session",
        ["symbol", "kind", "session"],
    )
    g_stale_ms = _get_or_create_gauge(
        "tca_stale_ms",
        "Time since last TCA update per symbol/kind/session",
        ["symbol", "kind", "session"],
    )
    c_processed = _get_or_create_counter(
        "tca_priors_processed_total",
        "Total fills processed by TCA priors exporter",
        ["symbol", "kind"],
    )
    c_skipped = _get_or_create_counter(
        "tca_priors_skipped_total",
        "Fills skipped due to missing/invalid fields",
        ["reason"],
    )

    state = TCAEMAState(ema_hl, ttl_sec)
    redis_client = get_redis()
    seeded = seed_tca_placeholders(redis_client, ttl_sec=ttl_sec, symbols=seed_symbols)
    if seeded:
        logger.info("Seeded %d placeholder TCA hashes for symbols=%s", seeded, seed_symbols)

    try:
        redis_client.xgroup_create(FILLS_STREAM, group, id="$", mkstream=True)
    except Exception as e:
        if "BUSYGROUP" not in str(e):
            logger.error("xgroup_create failed: %s", e)

    start_http_server(port)
    logger.info("HTTP /metrics on :%d", port)

    stop = {"flag": False}

    def _sig(_a: int, _b: Any) -> None:
        stop["flag"] = True

    signal.signal(signal.SIGTERM, _sig)
    signal.signal(signal.SIGINT, _sig)

    last_stale_refresh = 0
    while not stop["flag"]:
        try:
            resp = redis_client.xreadgroup(
                groupname=group,
                consumername=consumer,
                streams={FILLS_STREAM: ">"},
                count=batch,
                block=2000,
            )
        except Exception as e:
            logger.error("XREADGROUP failed: %s", e)
            time.sleep(1.0)
            continue

        if resp:
            ack_ids: list[Any] = []
            for _sk, messages in resp:  # type: ignore[union-attr]
                for msg_id, fields in messages:
                    ack_ids.append(msg_id)
                    try:
                        fields = {_decode(k): _decode(v) for k, v in fields.items()}
                        parsed = _extract_tca_update(fields)
                        if parsed is None:
                            c_skipped.labels(reason="missing_prices").inc()
                            continue
                        symbol, kind, session, ts_ms, metrics = parsed

                        new_state = state.update(
                            redis_client, symbol, kind, session,
                            eff_spread_bps=metrics["eff_spread_bps"],
                            realized_1s_bps=metrics["realized_1s_bps"],
                            realized_5s_bps=metrics["realized_5s_bps"],
                            perm_1s_bps=metrics["perm_1s_bps"],
                            perm_5s_bps=metrics["perm_5s_bps"],
                            is_bps=metrics["is_bps"],
                        )

                        labels = {"symbol": symbol, "kind": kind, "session": session}
                        g_eff_spread.labels(**labels).set(new_state["eff_spread"])
                        g_realized_1s.labels(**labels).set(new_state["realized_1s"])
                        g_realized_5s.labels(**labels).set(new_state["realized_5s"])
                        g_perm_1s.labels(**labels).set(new_state["perm_1s"])
                        g_perm_5s.labels(**labels).set(new_state["perm_5s"])
                        g_is_bps.labels(**labels).set(new_state["is_bps"])
                        g_samples.labels(**labels).set(new_state["samples"])
                        c_processed.labels(symbol=symbol, kind=kind).inc()
                    except Exception as e:
                        logger.warning("Failed to process fill %s: %s", msg_id, e)
                        c_skipped.labels(reason="exception").inc()

            if ack_ids:
                try:
                    redis_client.xack(FILLS_STREAM, group, *ack_ids)
                except Exception:
                    pass

        # Periodic staleness refresh
        now_ms = get_ny_time_millis()
        if now_ms - last_stale_refresh >= 10_000:
            for (sym, kd, sess), st in state._cache.items():
                age = now_ms - int(st.get("last_update_ms", 0))
                g_stale_ms.labels(symbol=sym, kind=kd, session=sess).set(age)
            last_stale_refresh = now_ms

    logger.info("TCA priors exporter stopped")


if __name__ == "__main__":
    main()
