"""
signal_outcome_snapshot_writer.py — Phase 0 sidecar consumer.

Reads signals:of:inputs stream (consumer group), writes a draft signal_outcome
record (label=NULL) for each signal at decision time with features frozen.

Design:
  * SHADOW mode by default (SIGNAL_OUTCOME_ENABLED=0).
  * Never blocks the signal hot path — runs as a separate service.
  * entry_px = mid_px + side_sign * (½spread_bps + slip_prior_bps) / 10_000 * mid_px
  * sl_bps derived from atr_bps (1R = 1×ATR), fallback to SO_MIN_SL_BPS.
  * tp_r from signal's tp1_target_r field, fallback to SO_DEFAULT_TP_R.
  * ttl_ms per regime, fallback to SO_TTL_MS_DEFAULT.
  * Idempotent: UPSERT ON CONFLICT (sid, decision_time_ms) DO NOTHING.
  * Prometheus: :SO_SNAPSHOT_PORT/metrics

ENV (all optional with sensible defaults):
  SIGNAL_OUTCOME_ENABLED       = 0              master switch (0 = shadow/no-write)
  SO_SNAPSHOT_REDIS_URL        = redis://redis-worker-1:6379/0
  SO_SNAPSHOT_IN_STREAM        = signals:of:inputs
  SO_SNAPSHOT_GROUP            = so-snapshot-writer
  SO_SNAPSHOT_CONSUMER         = so-snapshot-writer-1
  SO_SNAPSHOT_BATCH            = 200
  SO_SNAPSHOT_PORT             = 9910
  SO_SNAPSHOT_DB_DSN           = (from TRADES_DB_DSN)
  SO_DEFAULT_TP_R              = 1.0
  SO_DEFAULT_SL_R              = 1.0
  SO_MIN_SL_BPS                = 5.0            floor for sl_bps
  SO_SLIP_BPS_PRIOR            = 1.5            prior slippage estimate
  SO_TTL_MS_DEFAULT            = 600000         10 min fallback
  SO_TTL_MS_BY_REGIME          = {"momentum":900000,"ranging":300000,"high_vol":180000}
  SO_WRITE_BATCH               = 200
  ADAPTIVE_TTL_READ_ENABLED    = 0              when 1: read tp_r/sl_r/ttl_ms from
                                                Redis adaptive_ttl:state instead of ENV
  ADAPTIVE_TTL_TTL_SEC         = 300            how long to cache the Redis payload
"""
from __future__ import annotations

import json
import logging
import math
import os
import time
from typing import Any

log = logging.getLogger("so_snapshot_writer")


# ─── ENV helpers ─────────────────────────────────────────────────────────────

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
        f = float(v)
        return f if math.isfinite(f) else default
    except (TypeError, ValueError):
        return default


# ─── Signal parsing ───────────────────────────────────────────────────────────

def _parse_signal(fields: dict[str, str]) -> dict | None:
    """Extract full signal dict from stream fields.

    signals:of:inputs is published with field="payload" (JSON blob).
    Fallback: treat fields themselves as the signal (flat format guard).
    """
    raw = fields.get("payload") or fields.get("data") or fields.get("signal")
    if raw:
        try:
            sig = json.loads(raw)
            if isinstance(sig, dict):
                return sig
        except Exception:
            pass
    # flat fallback — when stream uses individual fields (older publish path)
    if fields.get("symbol") or fields.get("signal_id"):
        return dict(fields)
    return None


def _extract_indicators(signal: dict) -> dict:
    inds = signal.get("indicators") or {}
    if isinstance(inds, str):
        try:
            inds = json.loads(inds)
        except Exception:
            inds = {}
    return inds if isinstance(inds, dict) else {}


# ─── Entry price calculation ──────────────────────────────────────────────────

def compute_entry_px(
    mid_px: float,
    direction: str,
    spread_bps: float,
    slip_prior_bps: float,
) -> float:
    """Realistic fill price: mid ± (½spread + slip_prior).

    Long  → buy at ask-equivalent: mid * (1 + (½spread + slip) / 10_000)
    Short → sell at bid-equivalent: mid * (1 - (½spread + slip) / 10_000)
    """
    if mid_px <= 0:
        return mid_px
    side_sign = 1.0 if str(direction).upper() == "LONG" else -1.0
    offset_bps = (spread_bps / 2.0) + slip_prior_bps
    return mid_px * (1.0 + side_sign * offset_bps / 10_000.0)


# ─── Barrier config extraction ────────────────────────────────────────────────

_DEFAULT_TP_R   = _env_float("SO_DEFAULT_TP_R", 1.0)
_DEFAULT_SL_R   = _env_float("SO_DEFAULT_SL_R", 1.0)
_MIN_SL_BPS     = _env_float("SO_MIN_SL_BPS", 5.0)
_SLIP_BPS_PRIOR = _env_float("SO_SLIP_BPS_PRIOR", 1.5)
_TTL_DEFAULT_MS = _env_int("SO_TTL_MS_DEFAULT", 600_000)

try:
    _TTL_BY_REGIME: dict[str, int] = {
        k: int(v)
        for k, v in json.loads(
            _env("SO_TTL_MS_BY_REGIME", '{"momentum":900000,"ranging":300000,"high_vol":180000}')
        ).items()
    }
except Exception:
    _TTL_BY_REGIME = {"momentum": 900_000, "ranging": 300_000, "high_vol": 180_000}


# ─── Adaptive TTL reader ──────────────────────────────────────────────────────
# Reads calibration:adaptive_ttl:state from Redis and looks up the best matching
# BarrierRec for (symbol, regime, direction). Falls back to ENV defaults on any miss.
# Cache TTL = ADAPTIVE_TTL_TTL_SEC (default 300s) to avoid per-signal Redis round-trips.

_ADAPTIVE_TTL_ENABLED = _env_bool("ADAPTIVE_TTL_READ_ENABLED", False)
_ADAPTIVE_TTL_CACHE_SEC = _env_int("ADAPTIVE_TTL_TTL_SEC", 300)

_adaptive_ttl_cache: dict | None = None
_adaptive_ttl_cache_at: float = 0.0


def _load_adaptive_ttl(rc: Any) -> dict | None:
    """Return parsed adaptive_ttl:state payload (cached). None on miss/error."""
    global _adaptive_ttl_cache, _adaptive_ttl_cache_at
    now = time.monotonic()
    if (now - _adaptive_ttl_cache_at) < _ADAPTIVE_TTL_CACHE_SEC and _adaptive_ttl_cache is not None:
        return _adaptive_ttl_cache
    try:
        from core.redis_keys import RedisKeyPrefixes as RK
        raw = rc.get(RK.ADAPTIVE_TTL_STATE)
        if raw:
            _adaptive_ttl_cache = json.loads(str(raw))
        else:
            _adaptive_ttl_cache = None
        _adaptive_ttl_cache_at = now
    except Exception:
        _adaptive_ttl_cache = None
    return _adaptive_ttl_cache


def _lookup_adaptive_barrier(
    rc: Any,
    symbol: str,
    regime: str,
    direction: int,
) -> dict | None:
    """Return {tp_r, sl_r} from adaptive_ttl snapshot for best-matching group.

    Lookup priority:
      1. exact (symbol, regime, direction)
      2. (symbol, *, direction)   — any regime for this symbol/side
      3. None → caller uses ENV defaults

    Args:
        direction: +1 long / -1 short (from signal side)
    """
    if not _ADAPTIVE_TTL_ENABLED:
        return None
    payload = _load_adaptive_ttl(rc)
    if not payload:
        return None
    recs: list[dict] = payload.get("recs", [])
    if not recs:
        return None

    regime_lower = (regime or "").lower()

    best: dict | None = None
    for rec in recs:
        if rec.get("symbol") != symbol:
            continue
        if int(rec.get("direction", 0)) != direction:
            continue
        if rec.get("regime", "").lower() == regime_lower:
            best = rec
            break
        if best is None:
            best = rec  # fallback: same symbol+direction, any regime

    if best is None:
        return None
    tp_r = float(best.get("tp_r", 0.0) or 0.0)
    sl_r = float(best.get("sl_r", 0.0) or 0.0)
    if tp_r <= 0.0 or sl_r <= 0.0:
        return None
    return {"tp_r": tp_r, "sl_r": sl_r}


def _barrier_config(indicators: dict, direction: str, mid_px: float, rc: Any = None) -> dict | None:
    """Returns barrier config dict or None if mid_px is unusable."""
    if mid_px <= 0:
        return None

    atr_bps   = _safe_float(indicators.get("atr_bps") or indicators.get("atr_pct_bps"), 0.0)
    spread_bps = _safe_float(
        indicators.get("spread_bps") or indicators.get("bid_ask_spread_bps"), 0.0
    )

    sl_bps = max(atr_bps * _DEFAULT_SL_R, _MIN_SL_BPS)

    regime  = str(indicators.get("regime") or indicators.get("market_regime") or "").lower()
    symbol  = str(indicators.get("symbol") or "")
    side    = int(indicators.get("side") or (1 if direction.upper() in ("LONG", "BUY") else -1))

    # tp_r / sl_r: adaptive_ttl Redis lookup → signal-level → ENV default (priority order)
    adaptive = _lookup_adaptive_barrier(rc, symbol, regime, side) if rc is not None else None
    if adaptive:
        tp_r = adaptive["tp_r"]
        sl_r = adaptive["sl_r"]
        qf_adaptive = 0
    else:
        tp_r = _safe_float(
            indicators.get("tp1_target_r") or indicators.get("profile_tp_rr"),
            _DEFAULT_TP_R,
        )
        sl_r = _DEFAULT_SL_R
        qf_adaptive = 0

    tp_r = max(tp_r, 0.1)  # sanity floor
    sl_r = max(sl_r, 0.1)

    entry_px  = compute_entry_px(mid_px, direction, spread_bps, _SLIP_BPS_PRIOR)
    r_unit_px = entry_px * sl_bps / 10_000.0

    ttl_ms = _TTL_BY_REGIME.get(regime, _TTL_DEFAULT_MS)

    # quality_flags: bit 1 = spread estimated, bit 2 = adaptive barriers used
    qf = (2 if spread_bps == 0.0 else 0) | (4 if adaptive else 0) | qf_adaptive

    return dict(
        tp_r=tp_r,
        sl_r=sl_r,
        r_unit_px=r_unit_px,
        entry_px=entry_px,
        ttl_ms=ttl_ms,
        atr_bps=atr_bps,
        regime=regime or None,
        quality_flags=qf,
    )


# ─── DB upsert ────────────────────────────────────────────────────────────────

_INSERT_SQL = """
    INSERT INTO signal_outcome (
        sid, decision_time_ms, ingest_time_ms, schema_version,
        source, symbol, side, trace_id, kind,
        features, raw_score, regime, atr_bps,
        ttl_ms, tp_r, sl_r, r_unit_px, entry_px,
        quality_flags, calib_prob, expected_px, fees_bps
    ) VALUES (
        %s, %s, %s, 1,
        %s, %s, %s, %s, %s,
        %s, %s, %s, %s,
        %s, %s, %s, %s, %s,
        %s, %s, %s, %s
    )
    ON CONFLICT (sid, decision_time_ms) DO NOTHING
"""


def _upsert_batch(conn: Any, rows: list[tuple]) -> int:
    """Batch-insert rows; returns count inserted (approximate — DO NOTHING skips not counted)."""
    if not rows:
        return 0
    with conn.cursor() as cur:
        from psycopg2.extras import execute_batch
        execute_batch(cur, _INSERT_SQL, rows, page_size=200)
    conn.commit()
    return len(rows)


# ─── Main service ─────────────────────────────────────────────────────────────

def main() -> None:
    import redis  # type: ignore

    from prometheus_client import Counter, Gauge, start_http_server

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )

    enabled   = _env_bool("SIGNAL_OUTCOME_ENABLED", False)
    redis_url = _env("SO_SNAPSHOT_REDIS_URL", _env("REDIS_WORKER_1_URL", _env("REDIS_URL", "redis://redis-worker-1:6379/0")))
    in_stream = _env("SO_SNAPSHOT_IN_STREAM", "signals:of:inputs")
    group     = _env("SO_SNAPSHOT_GROUP", "so-snapshot-writer")
    consumer  = _env("SO_SNAPSHOT_CONSUMER", "so-snapshot-writer-1")
    batch     = _env_int("SO_SNAPSHOT_BATCH", 200)
    port      = _env_int("SO_SNAPSHOT_PORT", 9910)
    db_dsn    = _env("SO_SNAPSHOT_DB_DSN", _env("TRADES_DB_DSN", ""))

    log.info(
        "so_snapshot_writer starting | enabled=%s port=%d stream=%s",
        enabled, port, in_stream,
    )

    rc = redis.from_url(redis_url, decode_responses=True)

    # Consumer group bootstrap
    try:
        rc.xgroup_create(in_stream, group, id="$", mkstream=True)
    except Exception as e:
        if "BUSYGROUP" not in str(e):
            log.warning("xgroup_create: %s", e)

    # Prometheus
    start_http_server(port)
    c_read   = Counter("so_snapshot_read_total",   "Messages read from stream", ["symbol"])
    c_skip   = Counter("so_snapshot_skipped_total", "Messages skipped",          ["reason"])
    c_write  = Counter("so_snapshot_written_total", "Draft records written",     ["symbol"])
    c_err    = Counter("so_snapshot_error_total",   "Write errors",              [])
    g_lag    = Gauge("so_snapshot_lag_ms",          "Processing lag ms",         [])

    # Lazy DB conn
    conn = None

    def _get_conn():
        nonlocal conn
        if conn is None or conn.closed:
            import psycopg2
            conn = psycopg2.connect(db_dsn)
        return conn

    pending_rows: list[tuple] = []

    while True:
        try:
            resp = rc.xreadgroup(
                groupname=group, consumername=consumer,
                streams={in_stream: ">"}, count=batch, block=2000,
            )
        except Exception as e:
            if "NOGROUP" in str(e):
                try:
                    rc.xgroup_create(in_stream, group, id="$", mkstream=True)
                except Exception as ex:
                    if "BUSYGROUP" not in str(ex):
                        log.warning("xgroup_create retry: %s", ex)
            else:
                log.warning("XREADGROUP error: %s", e)
            time.sleep(1)
            continue

        ack_ids = []
        now_ms  = int(time.time() * 1000)

        if resp:
            for _stream_name, messages in resp:
                for msg_id, fields in messages:
                    try:
                        sig = _parse_signal(fields)
                        if not sig:
                            c_skip.labels(reason="parse_failed").inc()
                            ack_ids.append(msg_id)
                            continue

                        sid = str(sig.get("signal_id") or sig.get("sid") or "").strip()
                        if not sid:
                            c_skip.labels(reason="no_sid").inc()
                            ack_ids.append(msg_id)
                            continue

                        symbol = str(sig.get("symbol") or "").strip().upper()
                        if not symbol:
                            c_skip.labels(reason="no_symbol").inc()
                            ack_ids.append(msg_id)
                            continue

                        decision_time_ms = int(sig.get("ts_ms") or sig.get("timestamp_ms") or now_ms)
                        direction = str(sig.get("direction") or sig.get("side") or "LONG").upper()
                        side_int  = 1 if direction != "SHORT" else -1

                        inds = _extract_indicators(sig)

                        # mid_px: prefer signal-level price, fallback from indicators
                        mid_px = _safe_float(
                            sig.get("price") or sig.get("entry") or sig.get("entry_price")
                            or inds.get("entry_price") or inds.get("price"),
                            0.0,
                        )
                        if mid_px <= 0:
                            c_skip.labels(reason="no_price").inc()
                            ack_ids.append(msg_id)
                            continue

                        barrier = _barrier_config(inds, direction, mid_px, rc=rc)
                        if barrier is None:
                            c_skip.labels(reason="barrier_config_failed").inc()
                            ack_ids.append(msg_id)
                            continue

                        source    = str(sig.get("source") or sig.get("strategy") or "crypto-of")
                        trace_id  = str(sig.get("trace_id") or sid)
                        kind      = str(sig.get("kind") or sig.get("signal_kind") or "")
                        raw_score = _safe_float(sig.get("raw_score") or sig.get("score") or inds.get("of_score"), 0.0) or None

                        # Freeze full indicators as JSONB at decision time
                        try:
                            features_json = json.dumps(inds)
                        except Exception:
                            features_json = "{}"

                        g_lag.set(now_ms - decision_time_ms)
                        c_read.labels(symbol=symbol).inc()

                        calib_prob = _safe_float(sig.get("calib_prob") or sig.get("ml_p_edge") or inds.get("p_edge"), 0.0) or None
                        expected_px = mid_px
                        fees_bps = _safe_float(sig.get("fees_bps") or inds.get("fees_bps") or 1.2, 0.0)

                        if enabled:
                            row = (
                                sid,
                                decision_time_ms,
                                now_ms,
                                source,
                                symbol,
                                side_int,
                                trace_id,
                                kind or None,
                                features_json,
                                raw_score,
                                barrier["regime"],
                                barrier["atr_bps"] or None,
                                barrier["ttl_ms"],
                                barrier["tp_r"],
                                barrier["sl_r"],
                                barrier["r_unit_px"],
                                barrier["entry_px"],
                                barrier["quality_flags"],
                                calib_prob,
                                expected_px,
                                fees_bps,
                            )
                            pending_rows.append(row)
                            c_write.labels(symbol=symbol).inc()
                        # shadow mode: count but don't write

                    except Exception as ex:
                        log.debug("snapshot field parse error: %s", ex)
                        c_skip.labels(reason="field_error").inc()
                    ack_ids.append(msg_id)

        # Flush pending rows to DB
        if pending_rows and enabled:
            try:
                db_conn = _get_conn()
                n = _upsert_batch(db_conn, pending_rows)
                log.debug("so_snapshot_writer: upserted %d rows", n)
                pending_rows = []
            except Exception as e:
                c_err.inc()
                log.warning("so_snapshot_writer DB flush error (fail-open): %s", e)
                try:
                    if conn and not conn.closed:
                        conn.rollback()
                except Exception:
                    pass
                conn = None  # force reconnect next iteration

        if ack_ids:
            try:
                rc.xack(in_stream, group, *ack_ids)
            except Exception as e:
                log.warning("XACK error: %s", e)


if __name__ == "__main__":
    main()
