"""Cross-venue direction-agree calibrator (P1.11, 2026-05-26).

Polls `ctx:crossvenue:{SYMBOL}` keys (written by Go CrossVenueAggregator),
maintains a per-symbol rolling buffer of `cross_venue_direction_agree`
values, and publishes streaming median + MAD statistics to
`autocal:crossvenue:agree:state` (HMAC-signed).

Reader: `services/crossvenue_agree_runtime_overrides.py` consumes the
published key and exposes `(median_agree, mad_agree, n_total)` per symbol;
the cross-venue gate uses these to switch off the hardcoded 0/0 thresholds.

Why a separate service:
    median/MAD over a 168h window is too heavy to compute inline in
    `crossvenue_context_gate.py`; the gate path is on the hot signal
    pipeline. This service runs out-of-band at 30 s tick.

ENV:
    CROSSVENUE_AGREE_CAL_PORT (default 9871)
    CROSSVENUE_AGREE_CAL_INTERVAL_SEC (default 30)
    CROSSVENUE_AGREE_CAL_WINDOW_H (default 168.0)
    CROSSVENUE_AGREE_CAL_MIN_N (default 200) — minimum samples for enforce=1
    CROSSVENUE_AGREE_CAL_ENFORCE (default 0) — promote enforce=1 once min_n met
    CROSSVENUE_AGREE_CAL_HMAC_SECRET / RECS_HMAC_SECRET
    REDIS_URL (default redis-worker-1)
"""

from __future__ import annotations

import hashlib
import hmac
import json
import logging
import os
import signal
import time
from collections import deque
from dataclasses import dataclass, field

logger = logging.getLogger("crossvenue_agree_calibrator")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)

try:
    from prometheus_client import Counter, Gauge, start_http_server
    _PROM_OK = True
    _publishes_total = Counter(
        "crossvenue_agree_cal_publishes_total",
        "Number of snapshot publishes (status ∈ ok|err)",
        ["status"],
    )
    _samples_total = Counter(
        "crossvenue_agree_cal_samples_total",
        "Total samples ingested per symbol",
        ["symbol"],
    )
    _buckets_gauge = Gauge(
        "crossvenue_agree_cal_buckets",
        "Active symbol buckets",
    )
    _last_run_ts = Gauge(
        "crossvenue_agree_cal_last_run_ts_ms",
        "Epoch ms of last completed cycle",
    )
except Exception:  # pragma: no cover
    _PROM_OK = False
    _publishes_total = _samples_total = _buckets_gauge = _last_run_ts = None  # type: ignore[assignment]


_REDIS_KEY = "autocal:crossvenue:agree:state"
_CTX_PREFIX = "ctx:crossvenue:"


@dataclass
class _Bucket:
    buf: deque[tuple[int, float]] = field(default_factory=lambda: deque(maxlen=20_000))

    def add(self, ts_ms: int, value: float) -> None:
        self.buf.append((int(ts_ms), float(value)))

    def prune(self, cutoff_ms: int) -> None:
        while self.buf and self.buf[0][0] < cutoff_ms:
            self.buf.popleft()

    def median_mad(self) -> tuple[float, float, int]:
        n = len(self.buf)
        if n == 0:
            return 0.0, 0.0, 0
        vals = sorted(v for _, v in self.buf)
        if n % 2:
            med = vals[n // 2]
        else:
            med = 0.5 * (vals[n // 2 - 1] + vals[n // 2])
        devs = sorted(abs(v - med) for v in vals)
        if n % 2:
            mad = devs[n // 2]
        else:
            mad = 0.5 * (devs[n // 2 - 1] + devs[n // 2])
        return med, mad, n


def _env_int(k: str, d: int) -> int:
    try:
        return int(os.environ.get(k, str(d)))
    except (TypeError, ValueError):
        return d


def _env_float(k: str, d: float) -> float:
    try:
        return float(os.environ.get(k, str(d)))
    except (TypeError, ValueError):
        return d


def _env_bool(k: str, d: bool) -> bool:
    return os.environ.get(k, "1" if d else "0").strip().lower() in ("1", "true", "yes", "on")


def _sign(payload: dict, secret: str) -> dict:
    if not secret:
        return payload
    canon = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    payload = dict(payload)
    payload["sig"] = hmac.new(secret.encode(), canon, hashlib.sha256).hexdigest()
    return payload


def _scan_ctx_keys(redis_client) -> list[str]:
    keys: list[str] = []
    cursor = 0
    while True:
        cursor, batch = redis_client.scan(cursor=cursor, match=f"{_CTX_PREFIX}*", count=200)
        keys.extend(batch)
        if cursor == 0:
            break
    return keys


def _read_value(redis_client, key: str) -> tuple[int, float] | None:
    try:
        raw = redis_client.get(key)
        if not raw:
            return None
        data = json.loads(raw if isinstance(raw, str) else raw.decode())
        v = data.get("cross_venue_direction_agree")
        ts_ms = int(data.get("ts_ms") or 0)
        if v is None or ts_ms <= 0:
            return None
        f = float(v)
        if not (0.0 <= f <= 1.0):
            return None
        return ts_ms, f
    except Exception:
        return None


def main() -> int:
    port = _env_int("CROSSVENUE_AGREE_CAL_PORT", 9871)
    interval_s = _env_int("CROSSVENUE_AGREE_CAL_INTERVAL_SEC", 30)
    window_h = _env_float("CROSSVENUE_AGREE_CAL_WINDOW_H", 168.0)
    min_n = _env_int("CROSSVENUE_AGREE_CAL_MIN_N", 200)
    enforce = _env_bool("CROSSVENUE_AGREE_CAL_ENFORCE", False)
    secret = (
        os.environ.get("CROSSVENUE_AGREE_CAL_HMAC_SECRET", "")
        or os.environ.get("RECS_HMAC_SECRET", "")
        or os.environ.get("LAYERS_CAL_HMAC_SECRET", "")
    )

    if _PROM_OK:
        try:
            start_http_server(port)
            logger.info("Prometheus on :%d", port)
        except Exception as e:  # pragma: no cover
            logger.warning("Prometheus start failed: %s", e)

    import redis  # type: ignore
    redis_url = os.environ.get("REDIS_URL", "redis://redis-worker-1:6379/0")
    rc = redis.from_url(redis_url, decode_responses=True)

    buckets: dict[str, _Bucket] = {}
    seen_ts: dict[str, int] = {}
    stop = {"flag": False}

    def _sig(_signo, _frame):  # pragma: no cover
        stop["flag"] = True

    signal.signal(signal.SIGTERM, _sig)
    signal.signal(signal.SIGINT, _sig)

    logger.info(
        "crossvenue_agree_calibrator started | interval=%ds window=%.1fh min_n=%d enforce=%s",
        interval_s, window_h, min_n, enforce,
    )

    while not stop["flag"]:
        t0 = time.time()
        try:
            keys = _scan_ctx_keys(rc)
            window_ms = int(window_h * 3600.0 * 1000.0)
            now_ms = int(time.time() * 1000)
            cutoff = now_ms - window_ms

            for key in keys:
                sym = key[len(_CTX_PREFIX):].upper()
                if not sym:
                    continue
                sample = _read_value(rc, key)
                if sample is None:
                    continue
                ts_ms, val = sample
                # Avoid replaying same ts (writer may not have ticked yet).
                if seen_ts.get(sym) == ts_ms:
                    continue
                seen_ts[sym] = ts_ms

                b = buckets.get(sym)
                if b is None:
                    b = _Bucket()
                    buckets[sym] = b
                b.add(ts_ms, val)
                b.prune(cutoff)
                if _samples_total is not None:
                    _samples_total.labels(symbol=sym).inc()

            # Build snapshot.
            out_buckets: dict[str, dict] = {}
            for sym, b in buckets.items():
                b.prune(cutoff)
                med, mad, n = b.median_mad()
                if n == 0:
                    continue
                out_buckets[sym] = {
                    "median_agree": round(med, 6),
                    "mad_agree": round(mad, 6),
                    "n_total": n,
                    "enforce": 1 if (enforce and n >= min_n) else 0,
                }

            if _buckets_gauge is not None:
                _buckets_gauge.set(len(out_buckets))

            snap = {
                "ts_ms": now_ms,
                "schema_version": 1,
                "buckets": out_buckets,
            }
            signed = _sign(snap, secret)
            try:
                rc.set(_REDIS_KEY, json.dumps(signed), ex=int(window_h * 3600 * 1.5))
                if _publishes_total is not None:
                    _publishes_total.labels(status="ok").inc()
                logger.info("publish | symbols=%d total_samples=%d",
                            len(out_buckets), sum(int(v["n_total"]) for v in out_buckets.values()))
            except Exception as e:  # noqa: BLE001
                if _publishes_total is not None:
                    _publishes_total.labels(status="err").inc()
                logger.error("publish failed: %s", e)

            if _last_run_ts is not None:
                _last_run_ts.set(now_ms)

        except Exception as e:  # noqa: BLE001
            logger.error("cycle failed: %s", e)

        elapsed = time.time() - t0
        sleep_for = max(1.0, interval_s - elapsed)
        for _ in range(int(sleep_for * 10)):
            if stop["flag"]:
                break
            time.sleep(0.1)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
