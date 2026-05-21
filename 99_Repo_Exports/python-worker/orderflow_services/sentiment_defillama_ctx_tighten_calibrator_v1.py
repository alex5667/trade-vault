"""P2 — Sentiment / DefiLlama context tighten cap autocalibrator (v1).

Reads `trades:closed` stream and adaptively calibrates:
  - SENTIMENT_CTX_TIGHTEN_CAP_BPS    (default 2.0 bps)
  - DEFILLAMA_CTX_TIGHTEN_ADD_CAP_BPS (default 4.0 bps)

Attribution is provided by two new fields that trade_close_joiner promotes from
signal_payload.indicators into trades:closed top-level calib_fields:
  - ctx_sentiment_tighten_bps  (> 0 when SentimentContextGate fired TIGHTEN)
  - ctx_defillama_tighten_bps  (> 0 when DefiLlamaContextGate fired TIGHTEN)

Published state: autocal:ctx_tighten:state  (JSON, see core/sentiment_defillama_ctx_calibrator.py)
Prometheus:      /metrics  (HTTP, CTX_TIGHTEN_CAL_PORT default 9162)

ENV
---
  CTX_TIGHTEN_CAL_PORT            9162
  CTX_TIGHTEN_CAL_STREAM          trades:closed
  CTX_TIGHTEN_CAL_GROUP           ctx-tighten-cal
  CTX_TIGHTEN_CAL_CONSUMER        ctx-tighten-cal-1
  CTX_TIGHTEN_CAL_BATCH           200
  CTX_TIGHTEN_CAL_ENFORCE         0   (shadow by default)
  CTX_TIGHTEN_CAL_TARGET_EV_R     0.08
  CTX_TIGHTEN_CAL_WINDOW_DAYS     14
  CTX_TIGHTEN_CAL_MIN_TIGHTENED   50
  CTX_TIGHTEN_CAL_SNAPSHOT_SEC    60
  CTX_TIGHTEN_CAL_DEFAULT_SENTI   2.0
  CTX_TIGHTEN_CAL_DEFAULT_DEFI    4.0
  REJECT_REASON_WEIGHTS_ENABLED   1   (IPS weights, shared with p_edge cal)
  REDIS_URL                       redis://redis-worker-1:6379/0
"""
from __future__ import annotations

import json
import logging
import math
import os
import signal
import sys
import time

import redis

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("ctx_tighten_cal")

# ---------------------------------------------------------------------------
# Prometheus (optional import — graceful degradation if unavailable)
# ---------------------------------------------------------------------------
try:
    from prometheus_client import REGISTRY, Counter, Gauge, start_http_server

    def _counter(name: str, doc: str, labels: list[str]) -> Counter:
        return Counter(name, doc, labels)

    def _gauge(name: str, doc: str, labels: list[str]) -> Gauge:
        return Gauge(name, doc, labels)

    _PROM_OK = True
except ImportError:
    _PROM_OK = False

    class _Noop:  # type: ignore[misc]
        def labels(self, **_kw: object) -> "_Noop":
            return self
        def inc(self, _v: float = 1) -> None: ...
        def set(self, _v: float) -> None: ...

    def _counter(name: str, doc: str, labels: list[str]) -> "_Noop":  # type: ignore[misc]
        return _Noop()

    def _gauge(name: str, doc: str, labels: list[str]) -> "_Noop":  # type: ignore[misc]
        return _Noop()

    def start_http_server(port: int) -> None:  # type: ignore[misc]
        logger.warning("prometheus_client not available — metrics server not started")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _env(k: str, default: str) -> str:
    return os.getenv(k, default) or default

def _env_int(k: str, default: int) -> int:
    try:
        return int(os.getenv(k, str(default)) or default)
    except (ValueError, TypeError):
        return default

def _env_float(k: str, default: float) -> float:
    try:
        return float(os.getenv(k, str(default)) or default)
    except (ValueError, TypeError):
        return default

def _env_bool(k: str, default: bool) -> bool:
    raw = os.getenv(k, "")
    return raw.strip().lower() in {"1", "true", "yes", "on"} if raw else default

def _decode(b: object) -> str:
    if isinstance(b, (bytes, bytearray)):
        return b.decode("utf-8", errors="replace")
    return str(b) if b is not None else ""

def _safe_float(v: object, *, default: float = float("nan")) -> float:
    try:
        x = float(_decode(v))
        return x if math.isfinite(x) else default
    except Exception:
        return default

def _now_ms() -> int:
    return int(time.time() * 1000)


# ---------------------------------------------------------------------------
# Main service
# ---------------------------------------------------------------------------

def main() -> None:
    port = _env_int("CTX_TIGHTEN_CAL_PORT", 9162)
    stream_key = _env("CTX_TIGHTEN_CAL_STREAM", "trades:closed")
    group = _env("CTX_TIGHTEN_CAL_GROUP", "ctx-tighten-cal")
    consumer = _env("CTX_TIGHTEN_CAL_CONSUMER", "ctx-tighten-cal-1")
    batch = _env_int("CTX_TIGHTEN_CAL_BATCH", 200)
    enforce = _env_bool("CTX_TIGHTEN_CAL_ENFORCE", False)
    target_ev_r = _env_float("CTX_TIGHTEN_CAL_TARGET_EV_R", 0.08)
    window_days = _env_int("CTX_TIGHTEN_CAL_WINDOW_DAYS", 14)
    min_tightened = _env_int("CTX_TIGHTEN_CAL_MIN_TIGHTENED", 50)
    snap_sec = _env_int("CTX_TIGHTEN_CAL_SNAPSHOT_SEC", 60)
    default_senti = _env_float("CTX_TIGHTEN_CAL_DEFAULT_SENTI", 2.0)
    default_defi = _env_float("CTX_TIGHTEN_CAL_DEFAULT_DEFI", 4.0)
    redis_url = _env("REDIS_URL", "redis://redis-worker-1:6379/0")

    from core.redis_keys import RedisStreams as RS, RK  # type: ignore[import]
    stream_key = stream_key or RS.TRADES_CLOSED

    from core.reject_reason_weights import (  # type: ignore[import]
        is_enabled as reject_weights_enabled,
        reason_family,
        weight_for_reason,
    )
    from core.sentiment_defillama_ctx_calibrator import (  # type: ignore[import]
        SentimentDefiLlamaCtxCalibrator,
        CtxGateTightenCalibrator,
    )

    # ---- Prometheus metrics ----
    c_obs = _counter(
        "ctx_tighten_cal_observations_total",
        "Trades observed by ctx_tighten calibrator",
        ["gate", "tighten_class"],
    )
    c_skip = _counter(
        "ctx_tighten_cal_skip_total",
        "Trades skipped (missing/invalid fields)",
        ["reason"],
    )
    c_snap = _counter(
        "ctx_tighten_cal_snapshot_total",
        "Snapshot writes to Redis",
        ["outcome"],
    )
    c_reason = _counter(
        "ctx_tighten_cal_input_by_reason_family_total",
        "Trades observed per reject_reason family",
        ["family"],
    )
    g_senti_cap = _gauge(
        "ctx_tighten_cal_sentiment_cap_bps",
        "Committed sentiment ctx tighten cap (bps)",
        [],
    )
    g_senti_shadow = _gauge(
        "ctx_tighten_cal_sentiment_shadow_cap_bps",
        "Shadow/proposed sentiment ctx tighten cap (bps)",
        [],
    )
    g_senti_ev_tightened = _gauge(
        "ctx_tighten_cal_sentiment_ev_tightened",
        "Weighted EV of tightened-pass sentiment trades (R)",
        [],
    )
    g_senti_ev_baseline = _gauge(
        "ctx_tighten_cal_sentiment_ev_baseline",
        "Weighted EV of baseline sentiment trades (R)",
        [],
    )
    g_senti_n_tightened = _gauge(
        "ctx_tighten_cal_sentiment_n_tightened",
        "Effective tightened-trade count (sentiment, 14d)",
        [],
    )
    g_defi_cap = _gauge(
        "ctx_tighten_cal_defillama_cap_bps",
        "Committed defillama ctx tighten cap (bps)",
        [],
    )
    g_defi_shadow = _gauge(
        "ctx_tighten_cal_defillama_shadow_cap_bps",
        "Shadow/proposed defillama ctx tighten cap (bps)",
        [],
    )
    g_defi_ev_tightened = _gauge(
        "ctx_tighten_cal_defillama_ev_tightened",
        "Weighted EV of tightened-pass defillama trades (R)",
        [],
    )
    g_defi_ev_baseline = _gauge(
        "ctx_tighten_cal_defillama_ev_baseline",
        "Weighted EV of baseline defillama trades (R)",
        [],
    )
    g_defi_n_tightened = _gauge(
        "ctx_tighten_cal_defillama_n_tightened",
        "Effective tightened-trade count (defillama, 14d)",
        [],
    )
    g_enforce = _gauge(
        "ctx_tighten_cal_enforce_mode",
        "1 if enforce=True (caps committed to Redis), else 0 (shadow only)",
        [],
    )
    g_weights_on = _gauge(
        "ctx_tighten_cal_reject_weights_enabled",
        "1 if REJECT_REASON_WEIGHTS_ENABLED=1",
        [],
    )
    g_snap_lag = _gauge(
        "ctx_tighten_cal_snap_lag_sec",
        "Seconds since last successful snapshot",
        [],
    )

    g_enforce.set(1.0 if enforce else 0.0)
    g_weights_on.set(1.0 if reject_weights_enabled() else 0.0)

    # ---- Redis ----
    redis_client = redis.from_url(redis_url, decode_responses=False)

    # Ensure consumer group exists
    try:
        redis_client.xgroup_create(stream_key, group, id="0", mkstream=True)
    except redis.exceptions.ResponseError as e:
        if "BUSYGROUP" not in str(e):
            raise

    # ---- Calibrator ----
    cal = SentimentDefiLlamaCtxCalibrator(enforce=enforce)
    cal.sentiment.target_ev_r = target_ev_r
    cal.defillama.target_ev_r = target_ev_r
    cal.sentiment.window_ms = window_days * 24 * 60 * 60 * 1000
    cal.defillama.window_ms = window_days * 24 * 60 * 60 * 1000
    cal.sentiment.min_tightened = min_tightened
    cal.defillama.min_tightened = min_tightened
    cal.sentiment.default_cap_bps = default_senti
    cal.defillama.default_cap_bps = default_defi

    # Restore prior state if available
    try:
        raw = redis_client.get(RK.AUTOCAL_CTX_TIGHTEN_STATE)
        if raw:
            prior = json.loads(raw.decode("utf-8") if isinstance(raw, bytes) else raw)
            cal = SentimentDefiLlamaCtxCalibrator.loads(prior, enforce=enforce)
            # Re-apply runtime config on top of loaded caps
            cal.sentiment.target_ev_r = target_ev_r
            cal.defillama.target_ev_r = target_ev_r
            cal.sentiment.window_ms = window_days * 24 * 60 * 60 * 1000
            cal.defillama.window_ms = window_days * 24 * 60 * 60 * 1000
            cal.sentiment.min_tightened = min_tightened
            cal.defillama.min_tightened = min_tightened
            logger.info(
                "Restored state: senti_cap=%.3f, defi_cap=%.3f",
                cal.sentiment.cap_bps, cal.defillama.cap_bps,
            )
    except Exception as e:
        logger.warning("Could not restore prior state: %s", e)

    # ---- Graceful shutdown ----
    stop: dict[str, bool] = {"flag": False}

    def _sighandler(*_: object) -> None:
        stop["flag"] = True

    signal.signal(signal.SIGTERM, _sighandler)
    signal.signal(signal.SIGINT, _sighandler)

    # ---- Prometheus HTTP ----
    start_http_server(port)
    logger.info(
        "ctx_tighten_calibrator started — enforce=%s, port=%d, stream=%s, group=%s",
        enforce, port, stream_key, group,
    )

    last_snap_ms = _now_ms()

    while not stop["flag"]:
        now_ms = _now_ms()

        # ---- XREADGROUP ----
        try:
            result = redis_client.xreadgroup(
                group, consumer, {stream_key: ">"}, count=batch, block=2000
            )
        except Exception as e:
            logger.warning("xreadgroup error: %s", e)
            time.sleep(1)
            continue

        if not result:
            # no new messages — still run periodic tasks
            pass
        else:
            ack_ids: list[str] = []
            for _stream, messages in result:
                for msg_id, raw_fields in messages:
                    ack_ids.append(msg_id)
                    try:
                        fields = {_decode(k): _decode(v) for k, v in raw_fields.items()}

                        r_mult = _safe_float(fields.get("r_multiple") or fields.get("r_mult"))
                        if not math.isfinite(r_mult):
                            c_skip.labels(reason="r_multiple_invalid").inc()
                            continue

                        result_str = (fields.get("result", "") or "").upper()
                        if result_str not in ("WIN", "LOSS", "BE"):
                            c_skip.labels(reason="result_invalid").inc()
                            continue

                        ts_close = int(_safe_float(
                            fields.get("ts_close") or fields.get("ts_ms"),
                            default=float(now_ms),
                        ))

                        senti_tighten = _safe_float(
                            fields.get("ctx_sentiment_tighten_bps"), default=0.0
                        )
                        if not math.isfinite(senti_tighten):
                            senti_tighten = 0.0

                        defi_tighten = _safe_float(
                            fields.get("ctx_defillama_tighten_bps"), default=0.0
                        )
                        if not math.isfinite(defi_tighten):
                            defi_tighten = 0.0

                        v_gate_reason = (
                            fields.get("v_gate_reason", "")
                            or fields.get("reject_reason", "")
                            or ""
                        )
                        w = weight_for_reason(v_gate_reason)
                        fam = reason_family(v_gate_reason)

                        cal.observe(
                            r=r_mult,
                            sentiment_tighten_bps=senti_tighten,
                            defillama_tighten_bps=defi_tighten,
                            w=w,
                            ts_ms=ts_close,
                        )

                        c_obs.labels(
                            gate="sentiment",
                            tighten_class="tightened" if senti_tighten > 0 else "baseline",
                        ).inc()
                        c_obs.labels(
                            gate="defillama",
                            tighten_class="tightened" if defi_tighten > 0 else "baseline",
                        ).inc()
                        c_reason.labels(family=fam).inc()

                    except Exception as e:
                        c_skip.labels(reason="exception").inc()
                        logger.warning("observe failed for %s: %s", msg_id, e)

            if ack_ids:
                try:
                    redis_client.xack(stream_key, group, *ack_ids)
                except Exception as e:
                    logger.error("XACK failed: %s", e)

        # ---- Periodic recompute + snapshot ----
        if (now_ms - last_snap_ms) >= snap_sec * 1000:
            try:
                cal.recompute(now_ms)

                snap = cal.snapshot()
                redis_client.set(RK.AUTOCAL_CTX_TIGHTEN_STATE, json.dumps(snap))
                last_snap_ms = now_ms
                g_snap_lag.set(0.0)
                c_snap.labels(outcome="ok").inc()

                # Publish Prometheus metrics from snapshot
                s_snap = snap.get("sentiment", {})
                d_snap = snap.get("defillama", {})

                g_senti_cap.set(float(s_snap.get("cap_bps", default_senti) or default_senti))
                g_senti_shadow.set(float(s_snap.get("shadow_cap_bps", default_senti) or default_senti))
                g_senti_n_tightened.set(float(s_snap.get("n_tightened", 0) or 0))
                _ev_t = s_snap.get("ev_tightened")
                if _ev_t is not None:
                    g_senti_ev_tightened.set(float(_ev_t))
                _ev_b = s_snap.get("ev_baseline")
                if _ev_b is not None:
                    g_senti_ev_baseline.set(float(_ev_b))

                g_defi_cap.set(float(d_snap.get("cap_bps", default_defi) or default_defi))
                g_defi_shadow.set(float(d_snap.get("shadow_cap_bps", default_defi) or default_defi))
                g_defi_n_tightened.set(float(d_snap.get("n_tightened", 0) or 0))
                _ev_t2 = d_snap.get("ev_tightened")
                if _ev_t2 is not None:
                    g_defi_ev_tightened.set(float(_ev_t2))
                _ev_b2 = d_snap.get("ev_baseline")
                if _ev_b2 is not None:
                    g_defi_ev_baseline.set(float(_ev_b2))

                logger.info(
                    "snapshot: senti_cap=%.3f (shadow=%.3f, n_tightened=%d, ev=%.4f) | "
                    "defi_cap=%.3f (shadow=%.3f, n_tightened=%d, ev=%.4f)",
                    s_snap.get("cap_bps", default_senti),
                    s_snap.get("shadow_cap_bps", default_senti),
                    s_snap.get("n_tightened", 0),
                    s_snap.get("ev_tightened") or 0.0,
                    d_snap.get("cap_bps", default_defi),
                    d_snap.get("shadow_cap_bps", default_defi),
                    d_snap.get("n_tightened", 0),
                    d_snap.get("ev_tightened") or 0.0,
                )

            except Exception as e:
                c_snap.labels(outcome="error").inc()
                logger.error("Snapshot failed: %s", e)
                g_snap_lag.set((now_ms - last_snap_ms) / 1000.0)

    logger.info("ctx_tighten_calibrator shutting down")


if __name__ == "__main__":
    sys.exit(main() or 0)
