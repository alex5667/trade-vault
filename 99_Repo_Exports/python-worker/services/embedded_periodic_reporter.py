"""
Lightweight wrapper around PeriodicReporter so other services can reuse
the reporting logic without duplicating imports or scheduling.
"""

from __future__ import annotations

from typing import Optional

from common.log import setup_logger

try:  # pragma: no cover - optional dependency
    from services.periodic_reporter import PeriodicReporter
except Exception:  # pragma: no cover
    PeriodicReporter = None  # type: ignore[misc]


class EmbeddedPeriodicReporter:
    """Facade that exposes only the on-demand report sending behaviour."""

    def __init__(self) -> None:
        self.logger = setup_logger("EmbeddedPeriodicReporter")
        self._reporter: Optional[PeriodicReporter] = None

        if PeriodicReporter is None:
            self.logger.warning("PeriodicReporter module is not available, reports disabled")
            return

        try:
            self._reporter = PeriodicReporter()
            self.logger.info("PeriodicReporter instantiated successfully")
        except Exception as exc:  # pragma: no cover - init errors logged
            self.logger.error("Failed to initialize PeriodicReporter: %s", exc)
            self._reporter = None

    def available(self) -> bool:
        return self._reporter is not None

    def send_periodic_report(self, window_seconds: Optional[int] = None) -> None:
        if not self._reporter:
            raise RuntimeError("PeriodicReporter is not available")
        self._reporter.send_periodic_report(window_seconds=window_seconds)

    def send_daily_report(self) -> None:
        if not self._reporter:
            raise RuntimeError("PeriodicReporter is not available")
        self._reporter.send_daily_report()

