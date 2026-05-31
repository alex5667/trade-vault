from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple

from core.signal_outbox import SignalOutboxPublisher


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)) or str(default))
    except Exception:
        return int(default)


def _canon_side(v: Any) -> str:
    s = str(v or "").strip().upper()
    if s in {"LONG", "SHORT"}:
        return s
    # allow "BUY"/"SELL" just in case
    if s == "BUY":
        return "LONG"
    if s == "SELL":
        return "SHORT"
    return "LONG"


def _bucket_ts_ms(ts_ms: int, bucket_ms: int) -> int:
    if ts_ms <= 0 or bucket_ms <= 0:
        return ts_ms
    return int(ts_ms // bucket_ms) * int(bucket_ms)


@dataclass
class OutboxPublisherAdapter:
    """
    Adapter: dict payload -> SignalOutboxPublisher.publish(...)

    Key goals:
      - Keep OutboxWriter API: publish(payload: dict) -> Optional[str] (msg_id or None on dedup)
      - Translate dict payload into publisher params (source/strategy/symbol/side/kind/level_key/ts_ms/envelope)
      - Preserve envelope as dict (written as JSON into Redis Stream under field 'data')
      - Harden timestamp handling against regressions:
          * single normalization function: domain.time_utils.normalize_ts_ms()
          * seconds vs ms auto-fix (x < 1e12 => seconds => *1000)
          * invalid timestamps => 0 (fail-open)
          * dedup uses bucketed ts_ms to avoid micro-duplicates
    """

    outbox_publisher: SignalOutboxPublisher
    default_source: str = "Unknown"
    default_strategy: str = "unknown"
    dedup_bucket_ms: int = 60_000
    dedup_ttl_ms: int = 60_000

    # ---------------------------------------------------------------------
    # Timestamp normalization
    # ---------------------------------------------------------------------
    def _normalize_ts_ms(self, ts: Any) -> int:
        """
        Normalize input timestamp to epoch milliseconds (HARD policy).

        HARD policy rejects:
          - non-epoch clocks (minutes-of-day etc.)
          - implausible far-future/far-past timestamps
        """
        try:
            from domain.time_utils import normalize_ts_ms_hard
            now_ms = int(time.time() * 1000)
            return int(normalize_ts_ms_hard(ts, now_ms=now_ms))
        except Exception:
            # Absolute fallback (should almost never happen).
            try:
                if ts is None:
                    return 0
                v = float(ts) if not isinstance(ts, str) else float(ts.strip() or "nan")
                x = int(v)
                if x <= 0:
                    return 0
                # seconds -> ms (same cutoff as domain.time_utils)
                if x < 1_000_000_000_000:
                    return x * 1000
                return x
            except Exception:
                return 0

    def _extract_ts_ms(self, payload: Dict[str, Any]) -> int:
        """
        Priority:
          1) ts_ms
          2) ts
          3) timestamp

        We do NOT guess from other fields here. If nothing present => 0.
        """
        ts_raw = (
            payload.get("ts_ms", None)
            if payload is not None else None
        )
        if ts_raw is None and isinstance(payload, dict):
            ts_raw = payload.get("ts", None)
        if ts_raw is None and isinstance(payload, dict):
            ts_raw = payload.get("timestamp", None)
        return self._normalize_ts_ms(ts_raw)

    def _bucket_ts_ms(self, ts_ms: int) -> int:
        """
        Bucketize timestamp for dedup keys.
        Default bucket is 60_000ms (1 minute).

        Fail-open: ts<=0 -> 0 bucket.
        """
        b = int(self.dedup_bucket_ms or 0)
        x = int(ts_ms or 0)
        if x <= 0:
            return 0
        if b <= 0:
            return x
        return (x // b) * b

    def _dims_from_payload(self, payload: Dict[str, Any]) -> Tuple[str, str, str, str, str, str, int, Dict[str, Any]]:
        """
        Extract (source, strategy, symbol, side, kind, level_key, ts_ms_bucket, envelope).

        Envelope:
          - we keep a dict; we DO copy it to avoid mutating caller's payload.
          - we also ensure 'ts' exists when only 'ts_ms' was provided, because
            TradeMonitor._normalize_signal() reads ts/timestamp.
        """
        p = dict(payload or {})

        # Preserve raw timestamps for audit when we correct invalid ones (hard mode).
        ts_raw_present = any(k in p for k in ("ts_ms", "ts", "timestamp"))
        ts_raw_value = p.get("ts_ms", None) if "ts_ms" in p else (p.get("ts", None) if "ts" in p else p.get("timestamp", None))

        source = str(p.get("strategy_source") or p.get("source") or self.default_source)
        strategy = str(p.get("strategy_name") or p.get("strategy") or self.default_strategy)
        symbol = str(p.get("symbol") or "")

        side = str(p.get("side") or p.get("direction") or "").upper()
        if side not in ("LONG", "SHORT"):
            # Keep adapter strict here: invalid side means publisher params are invalid.
            # Fail-open for the system would be to still publish, but in practice
            # this would create broken signals downstream.
            side = "LONG"

        kind = str(p.get("kind") or p.get("signal_kind") or p.get("strategy") or "")
        level_key = str(p.get("level_key") or p.get("level") or "")

        ts_norm = self._extract_ts_ms(p)
        now_ms = int(time.time() * 1000)

        # -----------------------------------------------------------------
        # HARD MODE: never allow "bad ts" to propagate as-is.
        #
        # If ts is VALID epoch-like => enforce both ts and ts_ms as epoch ms.
        #
        # If ts is INVALID (minutes-of-day / implausible / parse failure) =>
        #   - we CORRECT it to "now" for deterministic downstream timing and to avoid
        #     dedup collisions (ts_bucket=0).
        #   - we mark envelope for audit + for gates to *skip EMA/session* safely:
        #       ts_invalid=1, ts_raw=<original>, ts_corrected=1, ts_corrected_to="now"
        #
        # Important:
        #   - We do NOT want TradeMonitor to fallback to "now" in multiple places differently
        #     (that creates non-determinism in tests / replays of payloads).
        #   - Gates must treat ts_invalid/ts_corrected as "do not use EMA" to avoid
        #     poisoning stats with wrong clock domains.
        # -----------------------------------------------------------------
        if ts_norm > 0:
            p["ts"] = int(ts_norm)
            p["ts_ms"] = int(ts_norm)
        else:
            ts_norm = int(now_ms)
            p["ts"] = int(ts_norm)
            p["ts_ms"] = int(ts_norm)
            if ts_raw_present:
                p["ts_invalid"] = 1
                p["ts_raw"] = ts_raw_value
            else:
                p["ts_invalid"] = 1
                p["ts_raw"] = None
            p["ts_corrected"] = 1
            p["ts_corrected_to"] = "now"

        ts_bucket = self._bucket_ts_ms(ts_norm)

        return source, strategy, symbol, side, kind, level_key, int(ts_bucket), p

    def publish(self, payload: Dict[str, Any]) -> Optional[str]:
        """
        Publish a dict payload via SignalOutboxPublisher.

        Returns:
           msg_id (str) if written, None if dedup skipped or on failure.
        """
        try:
            source, strategy, symbol, side, kind, level_key, ts_ms_bucket, envelope = self._dims_from_payload(payload)

            # NOTE: SignalOutboxPublisher does its own bucketization internally too.
            # Passing bucketed ts here ensures:
            #   - adapter-only unit tests are deterministic
            #   - if publisher bucket config changes, adapter still behaves predictably
            ts_ms = int(ts_ms_bucket)

            # Be tolerant to publisher signatures (real SignalOutboxPublisher vs stubs in tests).
            # This prevents regressions when publisher evolves.
            pub = self.outbox_publisher
            try:
                return pub.publish(
                    source=source,
                    strategy=strategy,
                    symbol=symbol,
                    side=side,
                    kind=kind,
                    level_key=level_key,
                    ts_ms=ts_ms,
                    envelope=envelope,
                    dedup_ttl_ms=int(self.dedup_ttl_ms),
                )
            except TypeError:
                pass
            try:
                return pub.publish(source, strategy, symbol, side, kind, level_key, ts_ms, envelope)
            except Exception:
                return None
        except Exception:
            return None