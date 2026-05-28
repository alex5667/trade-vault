"""Phase C.3 (P1.8, 2026-05-27): snapshot writer для regime_exec engine state.

Background
----------
`RegimeConditionalExecutionEngine` (core/regime_conditional_execution.py) живёт
в памяти каждого OF-worker'а и применяет per-bucket policy overrides без
публикации текущего состояния в Redis. Audit 2026-05-26 показал:
  - `autocal:regime_exec:state` — ключ отсутствует
  - Path_TP autocal promotion gating per-regime блокирован (нет видимости куда
    engine пришёл)
  - Observability: нельзя ответить «какой bucket сейчас активен для ETHUSDT»
    из Grafana без логов отдельного worker'а.

Что делает этот writer
----------------------
Раз в `INTERVAL_SEC` (default 60) собирает текущее состояние engine
(buckets, флаги, размер overrides_reader.snapshot если есть) и пишет
JSON в Redis key `autocal:regime_exec:state` с HMAC (если секрет задан).

Это **read-side observability snapshot**, НЕ promotion (тот живёт в
services/regime_exec_promotion_v1.py и пишет тот же ключ при ENFORCE=1).
Чтобы не затирать promotion snapshot, writer проверяет: если ключ
существует и содержит `source=promotion` — пропускает запись.

ENV:
  REGIME_EXEC_SNAPSHOT_ENABLED      default 1
  REGIME_EXEC_SNAPSHOT_INTERVAL_SEC default 60
  REGIME_EXEC_SNAPSHOT_REDIS_URL    fallback to REDIS_URL
  REGIME_EXEC_SNAPSHOT_KEY          default autocal:regime_exec:state
  REGIME_EXEC_AUTOCAL_HMAC_SECRET   optional
  REGIME_EXEC_SNAPSHOT_PROM_PORT    default 9870
"""

from __future__ import annotations

import dataclasses
import hashlib
import hmac
import json
import logging
import os
import signal
import sys
import time
from typing import Any

logger = logging.getLogger("regime_exec_snapshot")

_DEFAULT_KEY = "autocal:regime_exec:state"

try:
    from prometheus_client import Counter, Gauge, start_http_server
    _writes_total = Counter(
        "regime_exec_snapshot_writes_total",
        "Successful snapshot writes",
        ["result"],
    )
    _last_write_ms = Gauge(
        "regime_exec_snapshot_last_write_ms",
        "Last write timestamp (epoch ms)",
    )
    _buckets_count = Gauge(
        "regime_exec_snapshot_buckets_count",
        "Number of buckets in current snapshot",
    )
except Exception:
    Counter = Gauge = start_http_server = None  # type: ignore[assignment,misc]
    _writes_total = _last_write_ms = _buckets_count = None  # type: ignore[assignment]


def _env_int(k: str, d: int) -> int:
    try:
        return int(os.environ.get(k, str(d)))
    except (TypeError, ValueError):
        return d


def _env_bool(k: str, d: bool) -> bool:
    raw = os.environ.get(k, "")
    if not raw:
        return d
    return raw.strip().lower() in ("1", "true", "yes", "on")


def _now_ms() -> int:
    return int(time.time() * 1000)


def _redis_client():
    import redis  # type: ignore
    url = (
        os.environ.get("REGIME_EXEC_SNAPSHOT_REDIS_URL")
        or os.environ.get("REDIS_URL")
        or "redis://redis-worker-1:6379/0"
    )
    return redis.from_url(url, decode_responses=True, socket_timeout=2.0)


def _serialize_bucket(name: str, cfg: dict[str, Any]) -> dict[str, Any]:
    safe: dict[str, Any] = {"bucket": name}
    for k, v in cfg.items():
        if isinstance(v, (int, float, bool, str)) or v is None:
            safe[k] = v
        elif isinstance(v, (list, tuple)):
            safe[k] = list(v)
        elif isinstance(v, dict):
            safe[k] = {str(kk): vv for kk, vv in v.items()
                       if isinstance(vv, (int, float, bool, str, type(None)))}
        elif dataclasses.is_dataclass(v):
            try:
                safe[k] = dataclasses.asdict(v)
            except Exception:
                safe[k] = str(v)
        else:
            safe[k] = str(v)
    return safe


def build_snapshot(engine: Any, *, hmac_secret: str = "") -> dict[str, Any]:
    """Pure-function snapshot builder. engine: RegimeConditionalExecutionEngine."""
    ts_ms = _now_ms()
    buckets_dict: dict[str, dict[str, Any]] = {}
    try:
        for name, cfg in (getattr(engine, "buckets", None) or {}).items():
            buckets_dict[str(name)] = _serialize_bucket(str(name), cfg or {})
    except Exception as e:
        logger.debug("regime_exec snapshot: bucket serialize fail: %s", e)

    runtime_overrides = 0
    try:
        rdr = getattr(engine, "overrides_reader", None)
        if rdr is not None:
            snap = getattr(rdr, "_snapshot", None) or {}
            buck = (snap.get("buckets") if isinstance(snap, dict) else None) or {}
            runtime_overrides = len(buck)
    except Exception:
        runtime_overrides = 0

    payload: dict[str, Any] = {
        "source": "snapshot_writer",
        "ts_ms": ts_ms,
        "engine": {
            "enabled": bool(getattr(engine, "enabled", False)),
            "enforce_global": bool(getattr(engine, "enforce_global", False)),
            "skip_choppy": bool(getattr(engine, "skip_choppy", False)),
        },
        "buckets_count": len(buckets_dict),
        "buckets": buckets_dict,
        "runtime_overrides_count": runtime_overrides,
    }

    if hmac_secret:
        canon = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
        payload["sig"] = hmac.new(hmac_secret.encode(), canon, hashlib.sha256).hexdigest()

    return payload


def maybe_write(redis_client: Any, *, key: str, hmac_secret: str) -> dict[str, Any] | None:
    """Build engine snapshot and write to Redis unless a promotion snapshot exists."""
    try:
        from core.regime_conditional_execution import get_engine as _get_engine
        engine = _get_engine(redis_client=redis_client)
    except Exception as e:
        logger.debug("regime_exec snapshot: get_engine fail: %s", e)
        engine = None

    if engine is None:
        logger.debug("regime_exec snapshot: engine disabled or None — skip")
        if _writes_total is not None:
            try:
                _writes_total.labels(result="engine_none").inc()
            except Exception:
                pass
        return None

    # Не затираем promotion-written snapshot (source=promotion / sig).
    try:
        existing = redis_client.get(key)
        if existing:
            try:
                ex = json.loads(existing)
                if isinstance(ex, dict) and (ex.get("source") == "promotion"):
                    age_ms = _now_ms() - int(ex.get("ts_ms") or 0)
                    if age_ms < 24 * 3600 * 1000:
                        logger.info(
                            "regime_exec snapshot: promotion snapshot exists (age=%ds), skip write",
                            int(age_ms / 1000),
                        )
                        if _writes_total is not None:
                            _writes_total.labels(result="promotion_owns").inc()
                        return None
            except Exception:
                pass
    except Exception:
        pass

    payload = build_snapshot(engine, hmac_secret=hmac_secret)
    try:
        redis_client.set(key, json.dumps(payload), ex=3600)
        if _writes_total is not None:
            _writes_total.labels(result="ok").inc()
        if _last_write_ms is not None:
            _last_write_ms.set(float(payload["ts_ms"]))
        if _buckets_count is not None:
            _buckets_count.set(float(payload.get("buckets_count") or 0))
        logger.info(
            "regime_exec snapshot: written buckets=%d enforce_global=%s overrides=%d",
            payload.get("buckets_count") or 0,
            payload["engine"]["enforce_global"],
            payload.get("runtime_overrides_count") or 0,
        )
        return payload
    except Exception as e:
        logger.warning("regime_exec snapshot: write fail: %s", e)
        if _writes_total is not None:
            try:
                _writes_total.labels(result="write_fail").inc()
            except Exception:
                pass
        return None


def _main_loop() -> int:
    if not _env_bool("REGIME_EXEC_SNAPSHOT_ENABLED", True):
        logger.info("regime_exec snapshot writer: disabled via REGIME_EXEC_SNAPSHOT_ENABLED=0")
        return 0

    interval = max(15, _env_int("REGIME_EXEC_SNAPSHOT_INTERVAL_SEC", 60))
    key = os.environ.get("REGIME_EXEC_SNAPSHOT_KEY", _DEFAULT_KEY)
    hmac_secret = (
        os.environ.get("REGIME_EXEC_AUTOCAL_HMAC_SECRET", "")
        or os.environ.get("RECS_HMAC_SECRET", "")
        or os.environ.get("LAYERS_CAL_HMAC_SECRET", "")
    )

    if start_http_server is not None:
        port = _env_int("REGIME_EXEC_SNAPSHOT_PROM_PORT", 9870)
        try:
            start_http_server(port)
            logger.info("regime_exec snapshot: Prometheus on :%d", port)
        except Exception as e:
            logger.warning("regime_exec snapshot: prom server fail: %s", e)

    rc = _redis_client()

    stop = {"flag": False}

    def _sig(_signum, _frame):
        stop["flag"] = True
        logger.info("regime_exec snapshot: signal received, stopping")

    signal.signal(signal.SIGTERM, _sig)
    signal.signal(signal.SIGINT, _sig)

    logger.info(
        "regime_exec snapshot writer started: key=%s interval=%ds hmac=%s",
        key, interval, "yes" if hmac_secret else "no",
    )
    while not stop["flag"]:
        try:
            maybe_write(rc, key=key, hmac_secret=hmac_secret)
        except Exception as e:
            logger.warning("regime_exec snapshot: loop error: %s", e)
        # Sleep responsively to signals.
        for _ in range(interval):
            if stop["flag"]:
                break
            time.sleep(1.0)
    return 0


if __name__ == "__main__":
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO"),
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )
    sys.exit(_main_loop())
