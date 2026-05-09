import logging
import os
from typing import Any

from utils.time_utils import get_ny_time_millis

try:
    import redis
except ImportError:
    redis = None

from tools.base_exporter import BaseExporter, PlainTextResponse

logger = logging.getLogger("monitoring_smoke_exporter")

def _env(name: str, default: str = "") -> str:
    return (os.getenv(name) or default).strip()

def _read_redis() -> dict[str, Any]:
    if not redis:
        return {}
    url = _env("REDIS_URL", "redis://redis-worker-1:6379/0")
    key = _env("MONITORING_SMOKE_METRICS_KEY", "metrics:monitoring_smoke:last")
    try:
        r = redis.Redis.from_url(url, decode_responses=True)
        res = r.hgetall(key)
        return res if res else {}
    except Exception as e:
        logger.error(f"Redis error: {e}")
        return {}

class MonitoringSmokeExporter(BaseExporter):
    def get_metrics_response(self) -> PlainTextResponse:
        d = _read_redis()
        success = 0.0
        updated_ts = 0.0
        runbooks_ok = 0.0
        dashboards_ok = 0.0
        failed_total = 0.0
        if d:
            try:
                success = 1.0 if float(d.get("success", "0")) > 0 else 0.0
            except Exception:
                success = 0.0
            try:
                updated_ts = float(d.get("updated_ts_ms", "0") or 0)
            except Exception:
                updated_ts = 0.0

            try:
                runbooks_ok = 1.0 if float(d.get("runbooks_ok", "0")) > 0 else 0.0
            except Exception:
                runbooks_ok = 0.0
            try:
                dashboards_ok = 1.0 if float(d.get("dashboards_ok", "0")) > 0 else 0.0
            except Exception:
                dashboards_ok = 0.0
            try:
                failed_total = float(d.get("failed_total", "0") or 0)
            except Exception:
                failed_total = 0.0

        age_s = 0.0
        if updated_ts > 0:
            age_s = max(0.0, (get_ny_time_millis() - updated_ts) / 1000.0)

        lines = []
        lines.append("# HELP monitoring_smoke_last_success 1 if last nightly smoke run succeeded")
        lines.append("# TYPE monitoring_smoke_last_success gauge")
        lines.append(f"monitoring_smoke_last_success {success}")

        lines.append("# HELP monitoring_smoke_last_updated_ts_ms Unix timestamp of last smoke run")
        lines.append("# TYPE monitoring_smoke_last_updated_ts_ms gauge")
        lines.append(f"monitoring_smoke_last_updated_ts_ms {updated_ts}")

        lines.append("# HELP monitoring_smoke_age_seconds Time since last smoke run")
        lines.append("# TYPE monitoring_smoke_age_seconds gauge")
        lines.append(f"monitoring_smoke_age_seconds {age_s}")

        lines.append("# HELP monitoring_smoke_runbooks_ok 1 if runbooks contract checks OK")
        lines.append("# TYPE monitoring_smoke_runbooks_ok gauge")
        lines.append(f"monitoring_smoke_runbooks_ok {runbooks_ok}")

        lines.append("# HELP monitoring_smoke_dashboards_ok 1 if grafana dashboards routes OK")
        lines.append("# TYPE monitoring_smoke_dashboards_ok gauge")
        lines.append(f"monitoring_smoke_dashboards_ok {dashboards_ok}")

        lines.append("# HELP monitoring_smoke_failed_checks_total Number of failed checks in last smoke run")
        lines.append("# TYPE monitoring_smoke_failed_checks_total gauge")
        lines.append(f"monitoring_smoke_failed_checks_total {failed_total}")

        return PlainTextResponse("\n".join(lines) + "\n", media_type="text/plain; version=0.0.4")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    port = int(_env("MONITORING_SMOKE_EXPORTER_PORT", "9814"))
    exporter = MonitoringSmokeExporter(port=port)
    exporter.run()
