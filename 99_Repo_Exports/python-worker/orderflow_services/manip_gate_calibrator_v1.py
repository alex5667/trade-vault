#!/usr/bin/env python3
"""manip_gate_calibrator_v1.py

Auto-calibrator for the Manipulation Gate (MANIP).

Reads `signals:of:inputs` (which contains layering_score and quote_stuffing_score
in the `indicators` JSON field). Computes dynamic thresholds and tighten_bps
penalties.

Publishes to: autocal:manip_gate:state
Telegram: notify:telegram
"""
from __future__ import annotations

import json
import logging
import os
import signal
import time
from typing import Any

from prometheus_client import Gauge, start_http_server  # type: ignore

from core.redis_keys import RS, RK
from core.redis_client import get_redis
from core.manip_calibrator import ManipCalibrator

logger = logging.getLogger("manip-cal")

def _e(name: str, default: str = "") -> str:
    return os.getenv(name, default) or default

def _ei(name: str, default: int) -> int:
    try:
        return int(_e(name)) if _e(name) else default
    except ValueError:
        return default

def _eb(name: str, default: bool) -> bool:
    raw = _e(name)
    return (raw.strip().lower() in ("1", "true", "yes")) if raw else default

# Prometheus
_g_p95_layering = Gauge("manip_cal_p95_layering", "P95 layering score", ["symbol"])
_g_p95_qs = Gauge("manip_cal_p95_qs", "P95 quote stuffing score", ["symbol"])
_g_tighten_bps = Gauge("manip_cal_tighten_bps", "Dynamic tighten BPS", ["symbol"])
_g_layering_max = Gauge("manip_cal_layering_max", "Dynamic layering max threshold", ["symbol"])
_g_enforce = Gauge("manip_cal_enforce", "1 if in enforce mode, 0 if shadow")

def _send_telegram(r: Any, *, notify_stream: str, text: str) -> None:
    try:
        r.xadd(
            notify_stream,
            {
                "type": "report",
                "subtype": "manip_calibrator",
                "ts": str(int(time.time() * 1000)),
                "text": text,
                "parse_mode": "HTML",
            },
            maxlen=5_000,
        )
        logger.info("Telegram notification sent")
    except Exception as exc:
        logger.warning("Telegram notify failed: %s", exc)

def _read_inputs_stream(r: Any, cursor: str, calibrator: ManipCalibrator) -> tuple[str, int]:
    stream_key = RS.OF_INPUTS
    n = 0
    try:
        results = r.xread({stream_key: cursor}, count=1000)
    except Exception as exc:
        logger.warning("xread %s failed: %s", stream_key, exc)
        return cursor, 0

    for _stream, messages in (results or []):
        for msg_id, fields in messages:
            cursor = msg_id
            try:
                symbol = fields.get("symbol", "")
                if not symbol:
                    continue
                
                indicators_raw = fields.get("indicators", "{}")
                indicators = json.loads(indicators_raw)
                
                layering_score = float(indicators.get("layering_score", 0.0))
                qs_score = float(indicators.get("quote_stuffing_score", 0.0))
                
                # Only observe if there is some activity (avoid polluting with zeros if market is dead)
                if layering_score > 0.0 or qs_score > 0.0:
                    calibrator.observe(symbol, layering_score, qs_score)
                    n += 1
            except Exception as exc:
                logger.debug("ingest msg %s failed: %s", msg_id, exc)

    calibrator.evict_all()
    return cursor, n

def main() -> None:
    logging.basicConfig(
        level=_e("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    port = _ei("MANIP_CAL_PORT", 9923)
    poll_sec = _ei("MANIP_CAL_POLL_SEC", 10)
    snapshot_ttl = _ei("MANIP_CAL_SNAPSHOT_TTL", 7200)
    window_ms = _ei("MANIP_CAL_WINDOW_MS", 43_200_000)
    min_samples = _ei("MANIP_CAL_MIN_SAMPLES", 100)
    enforce = _eb("MANIP_CAL_ENFORCE", False)
    auto_enforce = _eb("MANIP_CAL_AUTO_ENFORCE", False)
    notify_stream = _e("MANIP_CAL_NOTIFY_STREAM", RS.NOTIFY_TELEGRAM)
    notify_interval_sec = _ei("MANIP_CAL_NOTIFY_INTERVAL", 3600) # 1 hour

    logger.info("manip_gate_calibrator_v1 starting | port=%d enforce=%s auto_enforce=%s", port, enforce, auto_enforce)
    start_http_server(port)

    r = get_redis()
    calibrator = ManipCalibrator(window_ms=window_ms)
    
    cursor = "0-0"
    try:
        info = r.xinfo_stream(RS.OF_INPUTS)
        cursor = str(info.get("last-generated-id", "0-0")) if isinstance(info, dict) else "0-0"
    except Exception as exc:
        logger.info("XINFO STREAM unavailable: %s", exc)

    _stop = False
    def _handle_signal(sig: int, _: Any) -> None:
        nonlocal _stop
        logger.info("Received signal %d", sig)
        _stop = True

    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT,  _handle_signal)

    next_poll = time.time()
    last_notify = time.time()

    while not _stop:
        now = time.time()
        if now < next_poll:
            time.sleep(min(0.5, next_poll - now))
            continue
        
        next_poll = now + poll_sec
        cursor, n_obs = _read_inputs_stream(r, cursor, calibrator)
        
        state = calibrator.dump_state(min_samples=min_samples)
        
        if state:
            # Update Prometheus
            for sym, st in state.items():
                _g_p95_layering.labels(symbol=sym).set(st["p95_layering"])
                _g_p95_qs.labels(symbol=sym).set(st["p95_qs"])
                _g_tighten_bps.labels(symbol=sym).set(st["tighten_bps"])
                _g_layering_max.labels(symbol=sym).set(st["layering_score_max"])
            
            is_promoted = enforce or (auto_enforce and len(state) > 0)
            _g_enforce.set(1.0 if is_promoted else 0.0)

            # Publish Snapshot
            snap = {
                "shadow": not is_promoted,
                "promoted": is_promoted,
                "published_ms": int(time.time() * 1000),
                "bins": state
            }
            r.set(RK.AUTOCAL_MANIP_GATE, json.dumps(snap, separators=(",", ":")), ex=snapshot_ttl)
            
            # Notify Telegram periodically
            if now - last_notify > notify_interval_sec:
                last_notify = now
                lines = []
                for sym, st in sorted(state.items()):
                    lines.append(
                        f"  • <b>{sym}</b>: n={st['n_samples']} p95_lay={st['p95_layering']:.2f} "
                        f"→ max={st['layering_score_max']:.2f} | <b>tighten={st['tighten_bps']:.1f} bps</b>"
                    )
                
                # Limit telegram lines if too many
                if len(lines) > 20:
                    lines = lines[:20] + ["  • ... (truncated)"]
                    
                mode_str = "LIVE ENFORCE" if enforce else "SHADOW"
                text = (
                    f"<b>📊 MANIP Gate Autocalibrator Report ({mode_str})</b>\n\n"
                    f"Current dynamic thresholds based on recent activity:\n"
                    + "\n".join(lines) + "\n\n"
                    f"<i>These overrides are published to {RK.AUTOCAL_MANIP_GATE}.</i>"
                )
                _send_telegram(r, notify_stream=notify_stream, text=text)

    logger.info("manip_gate_calibrator_v1 stopped")

if __name__ == "__main__":
    main()
