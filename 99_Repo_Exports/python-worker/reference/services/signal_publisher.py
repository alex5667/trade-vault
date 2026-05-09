from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

import redis

from services.signal_preprocess import preprocess_signal_for_publish
from utils.time_utils import get_ny_time_millis


def _json_dumps_safe(obj: Any) -> str:
    """
    Hot-path JSON: MUST NOT raise.
    default=str is deliberate: producers sometimes carry enums/decimals.
    """
    try:
        return json.dumps(obj, ensure_ascii=False, separators=(",", ":"), default=str)
    except Exception:
        # absolute last resort
        return '{"error":"json_dumps_failed"}'


def _r_incr_fail_open(r: Any, key: str) -> None:
    """Best-effort INCR used for publish metrics; never raises."""
    try:
        if r is not None and key:
            r.incr(key)
    except Exception:
        return


@dataclass(frozen=True)
class PublishSinks:
    """
    Where to publish:
      - store: key-value `store_prefix + id` (full payload), TTL applied
      - streams: raw/notify (XADD)
      - orders: LPUSH queue after building an order payload
    """
    store_prefix: str = "signals:"
    store_ttl_sec: int = 3600

    raw_stream: str = ""
    raw_maxlen: int = 2000

    notify_stream: str = ""
    notify_maxlen: int = 1000

    orders_queue: str = ""


@dataclass(frozen=True)
class PublishResult:
    """
    Structured publish report (useful for tests and dashboards).
    """
    ok: bool
    stored: bool
    raw_written: bool
    notify_written: bool
    order_pushed: bool
    busy_loading: bool


class SignalPublisher:
    """
    Shared publisher for multiple signal producers.

    Design goals:
      - single place for fail-open patterns and BusyLoading handling
      - single place for payload contract normalization
      - independent sinks (store/streams/orders) so partial outages don't blackhole everything
      - structured result for deterministic unit tests
    """

    def __init__(
        self,
        *,
        redis_client: Any,
        sinks: PublishSinks,
        source: str,
        metrics_prefix: str = "signals_publish",
        logger: Any = None,
        order_builder: Any = None,
    ) -> None:
        self.r = redis_client
        self.sinks = sinks
        self.source = source
        self.metrics_prefix = (metrics_prefix or "signals_publish")
        self.logger = logger
        self.order_builder = order_builder

    def publish(self, payload: dict[str, Any], *, symbol: str) -> PublishResult:
        """
        Publish pipeline (FAIL-OPEN). Never raises.
        Returns structured status for diagnostics/tests.
        """
        stored = raw_written = notify_written = order_pushed = False
        busy = False

        try:
            preprocess_signal_for_publish(payload, symbol=symbol, source=self.source, logger=self.logger)
        except Exception:
            # preprocessing must never block publishing
            pass

        sid = str(payload.get("signal_id") or payload.get("sid") or "").strip()
        if not sid:
            # preprocess should have generated it; fallback if someone disabled preprocessing
            sid = f"gen:{get_ny_time_millis()}"
            payload["signal_id"] = sid
            payload["sid"] = sid

        ser = _json_dumps_safe(payload)

        # --------------------------
        # 1) STORE: signals:{id}
        # --------------------------
        if self.sinks.store_prefix:
            try:
                self.r.set(f"{self.sinks.store_prefix}{sid}", ser, ex=int(self.sinks.store_ttl_sec))
                stored = True
            except redis.exceptions.BusyLoadingError:
                busy = True
            except Exception as e:
                _r_incr_fail_open(self.r, f"{self.metrics_prefix}:store_errors_total")
                if self.logger is not None:
                    try:
                        self.logger.error("publish.store failed sid=%s err=%r", sid, e)
                    except Exception:
                        pass

        # BusyLoading => do not spam further Redis ops
        if busy:
            _r_incr_fail_open(self.r, f"{self.metrics_prefix}:busyloading_total")
            return PublishResult(ok=False, stored=False, raw_written=False, notify_written=False, order_pushed=False, busy_loading=True)

        # --------------------------
        # 2) RAW STREAM
        # --------------------------
        if self.sinks.raw_stream:
            try:
                self.r.xadd(
                    self.sinks.raw_stream,
                    {"payload": ser},
                    maxlen=int(self.sinks.raw_maxlen),
                    approximate=True,
                )
                raw_written = True
            except redis.exceptions.BusyLoadingError:
                busy = True
            except Exception as e:
                _r_incr_fail_open(self.r, f"{self.metrics_prefix}:raw_xadd_errors_total")
                if self.logger is not None:
                    try:
                        self.logger.error("publish.raw_xadd failed sid=%s err=%r", sid, e)
                    except Exception:
                        pass

        if busy:
            _r_incr_fail_open(self.r, f"{self.metrics_prefix}:busyloading_total")
            return PublishResult(ok=False, stored=stored, raw_written=False, notify_written=False, order_pushed=False, busy_loading=True)

        # --------------------------
        # 3) NOTIFY STREAM
        # --------------------------
        if self.sinks.notify_stream:
            try:
                self.r.xadd(
                    self.sinks.notify_stream,
                    {"payload": ser},
                    maxlen=int(self.sinks.notify_maxlen),
                    approximate=True,
                )
                notify_written = True
            except redis.exceptions.BusyLoadingError:
                busy = True
            except Exception as e:
                _r_incr_fail_open(self.r, f"{self.metrics_prefix}:notify_xadd_errors_total")
                if self.logger is not None:
                    try:
                        self.logger.error("publish.notify_xadd failed sid=%s err=%r", sid, e)
                    except Exception:
                        pass

        if busy:
            _r_incr_fail_open(self.r, f"{self.metrics_prefix}:busyloading_total")
            return PublishResult(ok=False, stored=stored, raw_written=raw_written, notify_written=False, order_pushed=False, busy_loading=True)

        # --------------------------
        # 4) ORDER BUILD + QUEUE PUSH
        # --------------------------
        if self.sinks.orders_queue and self.order_builder is not None:
            order_payload: dict[str, Any] | None = None
            try:
                order_payload = self.order_builder.build_order_from_signal(payload)
            except Exception as e:
                _r_incr_fail_open(self.r, f"{self.metrics_prefix}:order_build_errors_total")
                if self.logger is not None:
                    try:
                        self.logger.error("publish.order_build failed sid=%s err=%r", sid, e)
                    except Exception:
                        pass

            if order_payload is not None:
                try:
                    self.r.lpush(self.sinks.orders_queue, _json_dumps_safe(order_payload))
                    order_pushed = True
                except redis.exceptions.BusyLoadingError:
                    busy = True
                except Exception as e:
                    _r_incr_fail_open(self.r, f"{self.metrics_prefix}:order_push_errors_total")
                    if self.logger is not None:
                        try:
                            self.logger.error("publish.order_push failed sid=%s err=%r", sid, e)
                        except Exception:
                            pass

        if busy:
            _r_incr_fail_open(self.r, f"{self.metrics_prefix}:busyloading_total")
            return PublishResult(ok=False, stored=stored, raw_written=raw_written, notify_written=notify_written, order_pushed=False, busy_loading=True)

        ok_any = bool(stored or raw_written or notify_written or order_pushed)
        _r_incr_fail_open(self.r, f"{self.metrics_prefix}:ok_total" if ok_any else f"{self.metrics_prefix}:all_failed_total")
        return PublishResult(ok=ok_any, stored=stored, raw_written=raw_written, notify_written=notify_written, order_pushed=order_pushed, busy_loading=False)
