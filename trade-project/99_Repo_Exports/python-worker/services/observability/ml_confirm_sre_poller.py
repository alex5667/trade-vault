from __future__ import annotations

"""
SRE поллер для метрик ML Confirm и labels:tb.

Проверяет:
  - labels:tb XLEN (длина стрима)
  - labels:tb XADD rate (скорость добавления)
  - Consumer group health для labels:tb

Экспортирует Prometheus метрики:
  - tb_labels_xlen (gauge)
  - tb_labels_xadd_rate (gauge)
  - tb_labeler_group_exists (gauge 0/1)
  - tb_train_empty_run_total (counter)
  - tb_promo_success_total / tb_promo_fail_total (counter)
"""


import logging
import os
import time

import redis
from core.redis_keys import RedisStreams as RS

# Prometheus metrics (optional, fail-open if not available)
try:
    from prometheus_client import Counter, Gauge, start_http_server
    PROMETHEUS_AVAILABLE = True
except Exception:
    PROMETHEUS_AVAILABLE = False
    class _MockMetric:  # type: ignore
        def labels(self, **kwargs):  # type: ignore
            return self
        def inc(self, *args, **kwargs):
            pass
        def set(self, *args, **kwargs):
            pass
    Counter = Gauge = lambda *args, **kwargs: _MockMetric()
    start_http_server = lambda *args, **kwargs: None


logger = logging.getLogger("ml_confirm_sre_poller")


# Prometheus metrics
if PROMETHEUS_AVAILABLE:
    tb_labels_xlen = Gauge(
        "tb_labels_xlen",
        "XLEN of labels:tb stream",
    )

    tb_labels_xadd_rate = Gauge(
        "tb_labels_xadd_rate",
        "XADD rate for labels:tb (approximate, per minute)",
    )

    tb_labeler_group_exists = Gauge(
        "tb_labeler_group_exists",
        "Consumer group exists for labels:tb (0/1)",
    )

    tb_train_empty_run_total = Counter(
        "tb_train_empty_run_total",
        "Count of training runs with 0 exported labels",
    )

    tb_promo_success_total = Counter(
        "tb_promo_success_total",
        "Count of successful champion promotions",
    )

    tb_promo_fail_total = Counter(
        "tb_promo_fail_total",
        "Count of failed champion promotions",
        ["reason"],
    )

    # ML Confirm config health metrics (from Redis)
    # Import from registry if available, otherwise define locally
    try:
        from services.observability.metrics_registry import (
            ml_confirm_cfg_present,
            ml_confirm_cfg_valid,
            ml_confirm_enforce_share,
        )
        ml_confirm_cfg_present_gauge = ml_confirm_cfg_present
        ml_confirm_cfg_valid_gauge = ml_confirm_cfg_valid
        ml_confirm_enforce_share_gauge = ml_confirm_enforce_share
    except Exception:
        # Fallback: define locally if registry not available
        ml_confirm_cfg_present_gauge = Gauge(
            "ml_confirm_cfg_present",
            "Whether cfg:ml_confirm:champion exists in Redis (1/0)",
            ["kind"],
        )
        ml_confirm_cfg_valid_gauge = Gauge(
            "ml_confirm_cfg_valid",
            "Whether cfg:ml_confirm:champion passed validation (1/0)",
            ["kind"],
        )
        ml_confirm_enforce_share_gauge = Gauge(
            "ml_confirm_enforce_share",
            "Current enforce_share from validated champion cfg",
            ["kind"],
        )
else:
    # Mock metrics
    class _MockMetric:
        def labels(self, **kwargs):
            return self
        def set(self, *args, **kwargs):
            pass
        def inc(self, *args, **kwargs):
            pass
    tb_labels_xlen = tb_labels_xadd_rate = tb_labeler_group_exists = _MockMetric()
    tb_train_empty_run_total = tb_promo_success_total = tb_promo_fail_total = _MockMetric()
    ml_confirm_cfg_present_gauge = ml_confirm_cfg_valid_gauge = ml_confirm_enforce_share_gauge = _MockMetric()


class MLConfirmSREPoller:
    """SRE поллер для мониторинга ML Confirm и labels:tb."""

    def __init__(
        self,
        *,
        r: redis.Redis,
        labels_stream: str = RS.TB_LABELS,
        poll_interval_sec: int = 60,
        champion_key: str = "cfg:ml_confirm:champion",
    ) -> None:
        self.r = r
        self.labels_stream = labels_stream
        self.poll_interval_sec = poll_interval_sec
        self.champion_key = champion_key
        self._last_xlen = 0
        self._last_xlen_ts = time.time()

    def poll_once(self) -> None:
        """Однократный опрос метрик."""
        try:
            # XLEN labels:tb
            xlen = self.r.xlen(self.labels_stream)
            if PROMETHEUS_AVAILABLE:
                tb_labels_xlen.set(xlen)

            # XADD rate (приблизительно, по изменению XLEN)
            now = time.time()
            if self._last_xlen_ts > 0:
                elapsed_min = (now - self._last_xlen_ts) / 60.0
                if elapsed_min > 0:
                    xadd_rate = max(0, (xlen - self._last_xlen) / elapsed_min)
                    if PROMETHEUS_AVAILABLE:
                        tb_labels_xadd_rate.set(xadd_rate)
            self._last_xlen = xlen
            self._last_xlen_ts = now

            # Consumer group health
            try:
                groups = self.r.xinfo_groups(self.labels_stream)
                group_exists = 1 if groups else 0
                if PROMETHEUS_AVAILABLE:
                    tb_labeler_group_exists.set(group_exists)
            except Exception as e:
                # Stream might not exist or no groups
                if PROMETHEUS_AVAILABLE:
                    tb_labeler_group_exists.set(0)
                logger.debug(f"Failed to get consumer groups for {self.labels_stream}: {e}")

            # ML Confirm config health (from Redis)
            self._poll_ml_confirm_cfg()

        except redis.exceptions.BusyLoadingError:
            logger.warning("Redis is loading the dataset in memory, skipping poll cycle")
            if PROMETHEUS_AVAILABLE:
                tb_labels_xlen.set(-1)
        except Exception as e:
            logger.error(f"Error polling labels:tb metrics: {e}", exc_info=True)
            if PROMETHEUS_AVAILABLE:
                tb_labels_xlen.set(-1)  # Error indicator

    def _poll_ml_confirm_cfg(self) -> None:
        """Опрос состояния ML Confirm конфигурации из Redis."""
        try:
            from core.champion_cfg_validator import CfgError, validate_champion_cfg

            # Check if champion config exists
            raw_cfg = self.r.get(self.champion_key)
            kind_for_metrics = "unknown"

            if raw_cfg:
                # Config present
                if PROMETHEUS_AVAILABLE:
                    ml_confirm_cfg_present_gauge.labels(kind=kind_for_metrics).set(1)  # type: ignore
  # type: ignore
                # Try to validate
                try:
                    if isinstance(raw_cfg, bytes):
                        raw_cfg = raw_cfg.decode("utf-8", "ignore")
                    raw_cfg_str = str(raw_cfg).strip()

                    if raw_cfg_str:
                        # Validate with strict mode (no defaulting)
                        champion_cfg, _ = validate_champion_cfg(
                            raw_cfg_str,
                            default_enforce_share=None  # Strict: missing enforce_share → error
                        )
                        kind_for_metrics = champion_cfg.kind or "unknown"

                        # Config is valid
                        if PROMETHEUS_AVAILABLE:
                            ml_confirm_cfg_present_gauge.labels(kind=kind_for_metrics).set(1)  # type: ignore
                            ml_confirm_cfg_valid_gauge.labels(kind=kind_for_metrics).set(1)  # type: ignore
                            ml_confirm_enforce_share_gauge.labels(kind=kind_for_metrics).set(  # type: ignore
                                champion_cfg.enforce_share  # type: ignore
                            )
                    else:
                        # Empty string
                        if PROMETHEUS_AVAILABLE:
                            ml_confirm_cfg_valid_gauge.labels(kind=kind_for_metrics).set(0)  # type: ignore
                except CfgError as e:  # type: ignore
                    # Validation failed
                    logger.debug(f"ML Confirm cfg validation failed: {e}")
                    if PROMETHEUS_AVAILABLE:
                        ml_confirm_cfg_valid_gauge.labels(kind=kind_for_metrics).set(0)  # type: ignore
                        ml_confirm_enforce_share_gauge.labels(kind=kind_for_metrics).set(0)  # type: ignore
                except Exception as e:  # type: ignore
                    # Parse error or other exception
                    logger.debug(f"ML Confirm cfg parse error: {e}")
                    if PROMETHEUS_AVAILABLE:
                        ml_confirm_cfg_valid_gauge.labels(kind=kind_for_metrics).set(0)  # type: ignore
                        ml_confirm_enforce_share_gauge.labels(kind=kind_for_metrics).set(0)  # type: ignore
            else:  # type: ignore
                # Config missing
                if PROMETHEUS_AVAILABLE:
                    ml_confirm_cfg_present_gauge.labels(kind=kind_for_metrics).set(0)  # type: ignore
                    ml_confirm_cfg_valid_gauge.labels(kind=kind_for_metrics).set(0)  # type: ignore
                    ml_confirm_enforce_share_gauge.labels(kind=kind_for_metrics).set(0)  # type: ignore
        except Exception as e:  # type: ignore
            logger.error(f"Error polling ML Confirm cfg: {e}", exc_info=True)
            if PROMETHEUS_AVAILABLE:
                ml_confirm_cfg_present_gauge.labels(kind="unknown").set(0)  # type: ignore
                ml_confirm_cfg_valid_gauge.labels(kind="unknown").set(0)  # type: ignore
                ml_confirm_enforce_share_gauge.labels(kind="unknown").set(0)  # type: ignore
  # type: ignore
    def run_forever(self) -> None:
        """Бесконечный цикл опроса."""
        logger.info(f"Starting ML Confirm SRE poller (stream={self.labels_stream}, interval={self.poll_interval_sec}s)")
        while True:
            try:
                self.poll_once()
            except Exception as e:
                logger.error(f"Error in poll loop: {e}", exc_info=True)
            time.sleep(self.poll_interval_sec)


def main() -> None:
    """Main entry point."""
    import argparse

    ap = argparse.ArgumentParser(description="ML Confirm SRE Poller")
    ap.add_argument("--redis-url", default=os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0"))
    ap.add_argument("--labels-stream", default=os.getenv("TB_LABELS_STREAM", RS.TB_LABELS))
    ap.add_argument("--poll-interval", type=int, default=int(os.getenv("SRE_POLL_INTERVAL_SEC", "60")))
    ap.add_argument("--prometheus-port", type=int, default=int(os.getenv("PROMETHEUS_PORT", "8005")))
    ap.add_argument("--champion-key", default=os.getenv("ML_CFG_CHAMPION_KEY", "cfg:ml_confirm:champion"))
    args = ap.parse_args()

    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    )

    # Connect to Redis
    r = redis.Redis.from_url(args.redis_url, decode_responses=True)

    # Start Prometheus server
    if PROMETHEUS_AVAILABLE:
        try:
            start_http_server(args.prometheus_port)
            logger.info(f"Prometheus metrics server started on port {args.prometheus_port}")
        except Exception as e:
            logger.warning(f"Failed to start Prometheus server: {e}")

    # Run poller
    poller = MLConfirmSREPoller(
        r=r,
        labels_stream=args.labels_stream,
        poll_interval_sec=args.poll_interval,
        champion_key=args.champion_key,
    )
    poller.run_forever()


if __name__ == "__main__":
    main()

