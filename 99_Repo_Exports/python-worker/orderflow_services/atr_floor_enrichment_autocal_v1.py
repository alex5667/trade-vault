"""atr_floor_enrichment_autocal_v1.py — shadow→production promoter for ATR_FLOOR_ENRICHMENT_EARLY.

Monitors two signals every ATR_FLOOR_AUTOCAL_INTERVAL seconds:
  1. ind_atr_th_bps fill rate in trades_closed (PostgreSQL, last WINDOW_H hours).
  2. trading_feature_to_decision_ms p99 from Prometheus (latency regression guard).

State machine:
  monitoring → dwell (criteria met) → promote_ready → promoted

Auto-promote (ATR_FLOOR_AUTOCAL_EXECUTE_PROMOTE=1):
  • Appends ATR_FLOOR_ENRICHMENT_EARLY=1 to ATR_FLOOR_AUTOCAL_ENV_FILE.
  • Prints the docker compose command to deploy all 10 crypto-of services.
  • Does NOT exec docker compose directly — operator runs the printed command.

Redis state key: autocal:atr_floor_enrichment:state (HASH, TTL 7 days).

ENV:
  ATR_FLOOR_AUTOCAL_ENABLE            0        — master switch; 0 = monitor-only (no promote)
  ATR_FLOOR_AUTOCAL_INTERVAL          300      — poll interval (seconds)
  ATR_FLOOR_AUTOCAL_WINDOW_H          2.0      — trades_closed fill-rate window (hours)
  ATR_FLOOR_AUTOCAL_FILL_RATE_MIN     50.0     — required fill-rate % to pass (after shadow)
  ATR_FLOOR_AUTOCAL_P99_MAX_DELTA_MS  3.0      — max allowed p99 increase vs baseline (ms)
  ATR_FLOOR_AUTOCAL_DWELL_H           24.0     — hours both criteria must hold before promote
  ATR_FLOOR_AUTOCAL_EXECUTE_PROMOTE   0        — 1 = write env file when promote_ready
  ATR_FLOOR_AUTOCAL_ENV_FILE          /infra_config/crypto-of-common.env
  ATR_FLOOR_AUTOCAL_PROM_PORT         9176
  ATR_FLOOR_AUTOCAL_REDIS_URL         redis://redis-worker-1:6379/0
  ATR_FLOOR_AUTOCAL_DB_DSN            "" (falls back to ANALYTICS_DB_DSN / TRADES_DB_DSN)
  ATR_FLOOR_AUTOCAL_PROM_URL          http://prometheus:9090

Rollback:
  Remove ATR_FLOOR_ENRICHMENT_EARLY=1 from crypto-of-common.env, then:
    docker compose up -d --no-deps scanner-crypto-orderflow scanner-crypto-orderflow-eth \\
      scanner-crypto-orderflow-2 scanner-crypto-orderflow-3 scanner-crypto-orderflow-3b \\
      scanner-crypto-orderflow-meme scanner-crypto-orderflow-meme-2 scanner-crypto-orderflow-meme-3 \\
      scanner-crypto-orderflow-alt scanner-crypto-orderflow-alt-2
"""
from __future__ import annotations

import json
import logging
import os
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Any

import redis  # type: ignore
from prometheus_client import Counter, Gauge, start_http_server  # type: ignore

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [atr-floor-autocal] %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)

STATE_KEY = "autocal:atr_floor_enrichment:state"
STATE_TTL = 7 * 86400  # 7 days

CRYPTO_OF_SERVICES = [
    "scanner-crypto-orderflow",
    "scanner-crypto-orderflow-eth",
    "scanner-crypto-orderflow-2",
    "scanner-crypto-orderflow-3",
    "scanner-crypto-orderflow-3b",
    "scanner-crypto-orderflow-meme",
    "scanner-crypto-orderflow-meme-2",
    "scanner-crypto-orderflow-meme-3",
    "scanner-crypto-orderflow-alt",
    "scanner-crypto-orderflow-alt-2",
]

# ---------------------------------------------------------------------------
# ENV helpers
# ---------------------------------------------------------------------------


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


def _env_bool(k: str, d: bool = False) -> bool:
    raw = _env(k, "")
    if not raw:
        return d
    return raw.strip().lower() in ("1", "true", "yes", "on")


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------


@dataclass
class Cfg:
    enable: bool
    interval: int
    window_h: float
    fill_rate_min: float
    p99_max_delta_ms: float
    dwell_h: float
    execute_promote: bool
    env_file: str
    prom_port: int
    redis_url: str
    db_dsn: str
    prom_url: str


def _load_cfg() -> Cfg:
    db_dsn = (
        _env("ATR_FLOOR_AUTOCAL_DB_DSN")
        or _env("ANALYTICS_DB_DSN")
        or _env("TRADES_DB_DSN")
        or ""
    )
    return Cfg(
        enable=_env_bool("ATR_FLOOR_AUTOCAL_ENABLE", False),
        interval=_env_int("ATR_FLOOR_AUTOCAL_INTERVAL", 300),
        window_h=_env_float("ATR_FLOOR_AUTOCAL_WINDOW_H", 2.0),
        fill_rate_min=_env_float("ATR_FLOOR_AUTOCAL_FILL_RATE_MIN", 50.0),
        p99_max_delta_ms=_env_float("ATR_FLOOR_AUTOCAL_P99_MAX_DELTA_MS", 3.0),
        dwell_h=_env_float("ATR_FLOOR_AUTOCAL_DWELL_H", 24.0),
        execute_promote=_env_bool("ATR_FLOOR_AUTOCAL_EXECUTE_PROMOTE", False),
        env_file=_env("ATR_FLOOR_AUTOCAL_ENV_FILE", "/infra_config/crypto-of-common.env"),
        prom_port=_env_int("ATR_FLOOR_AUTOCAL_PROM_PORT", 9176),
        redis_url=_env("ATR_FLOOR_AUTOCAL_REDIS_URL") or _env("REDIS_URL", "redis://redis-worker-1:6379/0"),
        db_dsn=db_dsn,
        prom_url=_env("ATR_FLOOR_AUTOCAL_PROM_URL", "http://prometheus:9090"),
    )


# ---------------------------------------------------------------------------
# Prometheus metrics
# ---------------------------------------------------------------------------

_fill_rate_gauge = Gauge(
    "atr_floor_autocal_fill_rate_pct",
    "ind_atr_th_bps fill rate in trades_closed (percent, last window)",
)
_p99_gauge = Gauge(
    "atr_floor_autocal_p99_ms",
    "trading_feature_to_decision_ms p99 from Prometheus",
)
_p99_baseline_gauge = Gauge(
    "atr_floor_autocal_p99_baseline_ms",
    "Baseline trading_feature_to_decision_ms p99 (captured at startup)",
)
_phase_gauge = Gauge(
    "atr_floor_autocal_phase",
    "Current autocal phase: 0=idle 1=monitoring 2=dwell 3=promote_ready 4=promoted",
)
_criteria_ok_gauge = Gauge(
    "atr_floor_autocal_criteria_ok",
    "1 if both fill-rate and p99 criteria currently pass",
)
_promoted_total = Counter(
    "atr_floor_autocal_promoted_total",
    "Times auto-promote successfully wrote env file",
)
_rollback_needed_total = Counter(
    "atr_floor_autocal_rollback_needed_total",
    "Times fill rate dropped below threshold after promotion (rollback signal)",
)
_up_gauge = Gauge("atr_floor_autocal_up", "1 if service running without errors")

PHASE_MAP = {"idle": 0, "monitoring": 1, "dwell": 2, "promote_ready": 3, "promoted": 4}


# ---------------------------------------------------------------------------
# PostgreSQL fill-rate query
# ---------------------------------------------------------------------------


def _query_fill_rate(dsn: str, window_h: float) -> float | None:
    """Return ind_atr_th_bps fill % over last window_h hours. None = error/no data."""
    try:
        import psycopg2  # type: ignore
    except ImportError:
        log.warning("psycopg2 not available — skipping DB check")
        return None

    if not dsn:
        log.warning("ATR_FLOOR_AUTOCAL_DB_DSN not set — skipping DB check")
        return None

    window_ms = int(window_h * 3600 * 1000)
    sql = """
        SELECT
          COUNT(*) FILTER (WHERE ind_atr_th_bps IS NOT NULL) * 100.0
          / NULLIF(COUNT(*), 0) AS fill_pct
        FROM trades_closed
        WHERE exit_ts_ms > (EXTRACT(EPOCH FROM NOW()) * 1000)::bigint - %s
    """
    try:
        conn = psycopg2.connect(dsn, connect_timeout=5)
        try:
            with conn.cursor() as cur:
                cur.execute(sql, (window_ms,))
                row = cur.fetchone()
                if row and row[0] is not None:
                    return float(row[0])
                return 0.0
        finally:
            conn.close()
    except Exception as e:
        log.warning("DB fill-rate query failed: %s", e)
        return None


# ---------------------------------------------------------------------------
# Prometheus latency query
# ---------------------------------------------------------------------------


def _query_p99_ms(prom_url: str) -> float | None:
    """Return trading_feature_to_decision_ms p99 from Prometheus. None = error."""
    query = 'histogram_quantile(0.99, sum(rate(trading_feature_to_decision_ms_bucket[5m])) by (le))'
    url = f"{prom_url.rstrip('/')}/api/v1/query?{urllib.parse.urlencode({'query': query})}"
    try:
        req = urllib.request.urlopen(url, timeout=5)
        data = json.loads(req.read())
        if data.get("status") != "success":
            return None
        results = data.get("data", {}).get("result", [])
        if not results:
            return None
        val = results[0].get("value", [None, None])[1]
        return float(val) if val is not None else None
    except Exception as e:
        log.debug("Prometheus p99 query failed: %s", e)
        return None


# ---------------------------------------------------------------------------
# Redis state helpers
# ---------------------------------------------------------------------------


def _load_state(rc: redis.Redis) -> dict[str, Any]:
    raw = rc.hgetall(STATE_KEY)
    return {k.decode(): v.decode() for k, v in raw.items()} if raw else {}  # type: ignore[union-attr]


def _save_state(rc: redis.Redis, state: dict[str, Any]) -> None:
    if not state:
        return
    rc.hset(STATE_KEY, mapping={k: str(v) for k, v in state.items()})
    rc.expire(STATE_KEY, STATE_TTL)


# ---------------------------------------------------------------------------
# Env file modifier
# ---------------------------------------------------------------------------


def _set_env_var_in_file(path: str, key: str, value: str) -> bool:
    """Add or update KEY=VALUE in a .env file. Returns True on success."""
    try:
        try:
            with open(path) as f:
                lines = f.readlines()
        except FileNotFoundError:
            lines = []

        new_line = f"{key}={value}\n"
        replaced = False
        result = []
        for line in lines:
            stripped = line.strip()
            if stripped.startswith(f"{key}=") or stripped == key:
                result.append(new_line)
                replaced = True
            else:
                result.append(line)

        if not replaced:
            # Append at end with a blank line separator if needed
            if result and result[-1].strip():
                result.append("\n")
            result.append(f"# ATR floor enrichment early — set by atr_floor_enrichment_autocal_v1\n")
            result.append(new_line)

        with open(path, "w") as f:
            f.writelines(result)

        log.info("Wrote %s=%s to %s", key, value, path)
        return True
    except Exception as e:
        log.error("Failed to write %s to %s: %s", key, path, e)
        return False


# ---------------------------------------------------------------------------
# Main autocal loop
# ---------------------------------------------------------------------------


class AtrFloorEnrichmentAutocal:
    def __init__(self, cfg: Cfg) -> None:
        self.cfg = cfg
        self.rc = redis.from_url(cfg.redis_url, socket_connect_timeout=5)
        self._state: dict[str, Any] = {}
        self._p99_baseline: float | None = None

    def _phase(self) -> str:
        return self._state.get("phase", "monitoring" if self.cfg.enable else "idle")

    def _set_phase(self, phase: str) -> None:
        if self._state.get("phase") != phase:
            log.info("Phase transition: %s → %s", self._state.get("phase", "?"), phase)
            self._state["phase"] = phase
            self._state["phase_entered_ms"] = int(time.time() * 1000)
        _phase_gauge.set(PHASE_MAP.get(phase, 0))

    def _phase_age_h(self) -> float:
        entered = float(self._state.get("phase_entered_ms", 0))
        return (time.time() * 1000 - entered) / 3_600_000

    def _load(self) -> None:
        self._state = _load_state(self.rc)
        phase = self._state.get("phase", "")
        _phase_gauge.set(PHASE_MAP.get(phase, 0))
        stored_baseline = self._state.get("p99_baseline_ms")
        if stored_baseline:
            self._p99_baseline = float(stored_baseline)
            _p99_baseline_gauge.set(self._p99_baseline)

    def _capture_baseline(self) -> None:
        if self._p99_baseline is not None:
            return
        p99 = _query_p99_ms(self.cfg.prom_url)
        if p99 is not None:
            self._p99_baseline = p99
            self._state["p99_baseline_ms"] = str(p99)
            _p99_baseline_gauge.set(p99)
            log.info("Captured p99 baseline: %.2f ms", p99)

    def _check_criteria(self, fill_rate: float | None, p99: float | None) -> bool:
        if fill_rate is None:
            log.debug("fill_rate unavailable — criteria not met")
            return False

        fill_ok = fill_rate >= self.cfg.fill_rate_min
        if not fill_ok:
            log.debug("fill_rate %.1f%% < %.1f%% threshold", fill_rate, self.cfg.fill_rate_min)

        # p99 check: skip if baseline not captured or Prometheus unavailable
        p99_ok = True
        if p99 is not None and self._p99_baseline is not None:
            delta = p99 - self._p99_baseline
            p99_ok = delta <= self.cfg.p99_max_delta_ms
            if not p99_ok:
                log.warning("p99 delta %.2f ms > %.2f ms threshold", delta, self.cfg.p99_max_delta_ms)

        return fill_ok and p99_ok

    def _promote(self) -> bool:
        """Write ATR_FLOOR_ENRICHMENT_EARLY=1 to env file. Returns success."""
        path = self.cfg.env_file
        if not os.path.exists(path):
            log.error("Env file not found at %s (volume mounted?)", path)
            return False

        ok = _set_env_var_in_file(path, "ATR_FLOOR_ENRICHMENT_EARLY", "1")
        if ok:
            _promoted_total.inc()
            services = " ".join(CRYPTO_OF_SERVICES)
            log.info(
                "PROMOTED — restart services with:\n"
                "  docker compose up -d --no-deps %s",
                services,
            )
        return ok

    def step_inner(self, fill_rate: float | None, p99: float | None) -> None:
        """Core state-machine logic; receives already-fetched metrics. Testable without I/O."""
        if fill_rate is not None:
            _fill_rate_gauge.set(fill_rate)
            self._state["fill_rate_pct"] = str(round(fill_rate, 2))
        if p99 is not None:
            _p99_gauge.set(p99)
            self._state["p99_ms"] = str(round(p99, 3))

        self._state["last_check_ms"] = str(int(time.time() * 1000))

        log.info(
            "phase=%s fill_rate=%s%% p99=%sms baseline=%sms",
            self._phase(),
            f"{fill_rate:.1f}" if fill_rate is not None else "N/A",
            f"{p99:.2f}" if p99 is not None else "N/A",
            f"{self._p99_baseline:.2f}" if self._p99_baseline is not None else "N/A",
        )

        phase = self._phase()

        if phase == "idle":
            _up_gauge.set(1)
            _save_state(self.rc, self._state)
            return

        criteria_ok = self._check_criteria(fill_rate, p99)
        _criteria_ok_gauge.set(1 if criteria_ok else 0)

        if phase == "monitoring":
            if criteria_ok:
                self._set_phase("dwell")
                self._state["dwell_start_ms"] = str(int(time.time() * 1000))

        elif phase == "dwell":
            if not criteria_ok:
                log.info("Criteria no longer met — resetting to monitoring")
                self._set_phase("monitoring")
            elif self._phase_age_h() >= self.cfg.dwell_h:
                log.info("Dwell complete (%.1fh) — criteria held, promoting", self.cfg.dwell_h)
                self._set_phase("promote_ready")
            else:
                log.info(
                    "In dwell: %.1fh / %.1fh elapsed",
                    self._phase_age_h(),
                    self.cfg.dwell_h,
                )

        elif phase == "promote_ready":
            if self.cfg.execute_promote:
                ok = self._promote()
                if ok:
                    self._set_phase("promoted")
                    self._state["promoted_at_ms"] = str(int(time.time() * 1000))
                else:
                    log.error("Promote failed — will retry next cycle")
            else:
                log.info(
                    "PROMOTE READY — set ATR_FLOOR_AUTOCAL_EXECUTE_PROMOTE=1 to apply, "
                    "or manually add ATR_FLOOR_ENRICHMENT_EARLY=1 to crypto-of-common.env"
                )

        elif phase == "promoted":
            # Regression watchdog: fill rate should not drop back below 70% of threshold
            if fill_rate is not None and fill_rate < self.cfg.fill_rate_min * 0.7:
                log.warning(
                    "ROLLBACK SIGNAL: fill_rate %.1f%% dropped below 70%% of threshold after promotion",
                    fill_rate,
                )
                _rollback_needed_total.inc()
                self._state["last_rollback_signal_ms"] = str(int(time.time() * 1000))

        _up_gauge.set(1)
        _save_state(self.rc, self._state)

    def step(self) -> None:
        try:
            fill_rate = _query_fill_rate(self.cfg.db_dsn, self.cfg.window_h)
            p99 = _query_p99_ms(self.cfg.prom_url)

            # Try to capture p99 baseline on first run (before state machine runs)
            self._capture_baseline()

            self.step_inner(fill_rate, p99)

        except Exception as e:
            log.exception("step() error: %s", e)
            _up_gauge.set(0)

    def run(self) -> None:
        cfg = self.cfg
        start_http_server(cfg.prom_port)
        log.info(
            "Started: enable=%s interval=%ds fill_rate_min=%.0f%% dwell_h=%.0fh execute_promote=%s port=%d",
            cfg.enable,
            cfg.interval,
            cfg.fill_rate_min,
            cfg.dwell_h,
            cfg.execute_promote,
            cfg.prom_port,
        )

        self._load()

        if not cfg.enable:
            log.info("ATR_FLOOR_AUTOCAL_ENABLE=0 — running in monitor-only mode (no promote)")
            self._set_phase("idle")

        while True:
            self.step()
            time.sleep(cfg.interval)


def main() -> None:
    cfg = _load_cfg()
    AtrFloorEnrichmentAutocal(cfg).run()


if __name__ == "__main__":
    main()
