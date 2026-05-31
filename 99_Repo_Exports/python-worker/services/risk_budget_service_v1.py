"""risk_budget_service_v1.py — Unified risk budget aggregator (port 9925).

P0-3: Read-only aggregator. Reads existing state from:
  - risk:daily_dd:state         (daily_dd_kill_switch_v1)
  - risk:daily_dd:sym:{S}       (per-symbol daily cap)
  - risk:cooldown:symbol:{S}    (protection-fail cooldown, written by P0-1)
  - risk:overlay:*              (consec_loss/correlation/portfolio heat, risk_overlay_v1)

Publishes unified Prometheus metrics every 5s. Does NOT make trading decisions —
those live in the gates (EntryPolicyGate, risk_overlay_v1).

ENV:
  RISK_BUDGET_SERVICE_ENABLED=1
  RISK_BUDGET_POLL_INTERVAL_S=5
  PROMETHEUS_PORT=9925
  REDIS_URL=redis://redis-worker-1:6379/0
  RISK_BUDGET_TRACK_SYMBOLS=BTCUSDT,ETHUSDT,SOLUSDT  (comma-sep; empty=all cooldown keys)
"""
from __future__ import annotations

import contextlib
import os
import time
from typing import Any

try:
    import redis as _redis_mod
except ImportError:
    _redis_mod = None  # type: ignore[assignment]

try:
    from prometheus_client import Gauge, Counter, start_http_server
    _prom_ok = True
except Exception:
    Gauge = Counter = start_http_server = None  # type: ignore[assignment]
    _prom_ok = False


def _metric(factory, name, doc, labels=None):
    if factory is None:
        return None
    try:
        return factory(name, doc, labels or [])
    except ValueError:
        try:
            from prometheus_client import REGISTRY
            return REGISTRY._names_to_collectors.get(name)
        except Exception:
            return None


# ---------------------------------------------------------------------------
# Prometheus metrics
# ---------------------------------------------------------------------------

RISK_BUDGET_DAILY_DD_ARMED = _metric(
    Gauge, "risk_budget_daily_dd_armed",
    "1 if daily drawdown kill-switch is armed (kill_armed=1 AND mode=enforce).",
)
RISK_BUDGET_DAILY_DD_R_SUM = _metric(
    Gauge, "risk_budget_daily_dd_r_sum",
    "Cumulative R sum for today (from risk:daily_dd:state).",
)
RISK_BUDGET_COOLDOWN_ACTIVE = _metric(
    Gauge, "risk_budget_cooldown_active",
    "1 if symbol is in protection-fail cooldown, 0 otherwise.",
    ["symbol"],
)
RISK_BUDGET_COOLDOWN_REMAINING_S = _metric(
    Gauge, "risk_budget_cooldown_remaining_s",
    "Seconds remaining in protection-fail cooldown for symbol.",
    ["symbol"],
)
RISK_BUDGET_AGGREGATOR_LAST_RUN_TS = _metric(
    Gauge, "risk_budget_aggregator_last_run_ts",
    "Unix timestamp of last completed aggregator cycle.",
)
RISK_BUDGET_AGGREGATOR_ERRORS_TOTAL = _metric(
    Counter, "risk_budget_aggregator_errors_total",
    "Errors in risk budget aggregator poll cycles.",
    ["source"],
)


def _f(val: Any, default: float = 0.0) -> float:
    try:
        return float(val)
    except (TypeError, ValueError):
        return default


# ---------------------------------------------------------------------------
# Core aggregator (testable)
# ---------------------------------------------------------------------------

class RiskBudgetAggregator:
    def __init__(self, r: Any, track_symbols: list[str] | None = None) -> None:
        self.r = r
        self.track_symbols = [s.upper() for s in (track_symbols or [])]
        self._cooldown_prefix = "risk:cooldown:symbol:"

    # ------------------------------------------------------------------
    def _read_daily_dd(self) -> dict:
        try:
            h = self.r.hgetall("risk:daily_dd:state") or {}
            return {
                k.decode() if isinstance(k, bytes) else k:
                v.decode() if isinstance(v, bytes) else v
                for k, v in h.items()
            }
        except Exception:
            return {}

    def _read_cooldowns(self) -> dict[str, int]:
        """Return {SYMBOL: until_ms} for all tracked symbols that have active cooldowns."""
        result: dict[str, int] = {}
        now_ms = int(time.time() * 1000)
        symbols = self.track_symbols

        if not symbols:
            # Scan for all existing cooldown keys
            try:
                cursor = 0
                while True:
                    cursor, keys = self.r.scan(cursor, match=f"{self._cooldown_prefix}*", count=100)
                    for k in keys:
                        k_str = k.decode() if isinstance(k, bytes) else k
                        sym = k_str.replace(self._cooldown_prefix, "").upper()
                        val = self.r.get(k)
                        if val is not None:
                            try:
                                until_ms = int(val.decode() if isinstance(val, bytes) else val)
                                if until_ms > now_ms:
                                    result[sym] = until_ms
                            except (TypeError, ValueError):
                                pass
                    if cursor == 0:
                        break
            except Exception:
                pass
        else:
            for sym in symbols:
                key = f"{self._cooldown_prefix}{sym}"
                try:
                    val = self.r.get(key)
                    if val is not None:
                        until_ms = int(val.decode() if isinstance(val, bytes) else val)
                        if until_ms > now_ms:
                            result[sym] = until_ms
                except Exception:
                    pass
        return result

    # ------------------------------------------------------------------
    def run_once(self) -> dict:
        now_ms = int(time.time() * 1000)
        result: dict = {}

        # Daily DD state
        dd = self._read_daily_dd()
        kill_armed = dd.get("kill_armed", "0") == "1"
        dd_mode = dd.get("mode", "shadow")
        r_sum = _f(dd.get("r_sum"), 0.0)
        result["daily_dd_armed"] = kill_armed and dd_mode == "enforce"
        result["daily_dd_r_sum"] = r_sum

        with contextlib.suppress(Exception):
            if RISK_BUDGET_DAILY_DD_ARMED is not None:
                RISK_BUDGET_DAILY_DD_ARMED.set(1.0 if result["daily_dd_armed"] else 0.0)
        with contextlib.suppress(Exception):
            if RISK_BUDGET_DAILY_DD_R_SUM is not None:
                RISK_BUDGET_DAILY_DD_R_SUM.set(r_sum)

        # Cooldowns
        cooldowns = self._read_cooldowns()
        result["cooldowns"] = cooldowns
        for sym, until_ms in cooldowns.items():
            remaining_s = max(0.0, (until_ms - now_ms) / 1000.0)
            with contextlib.suppress(Exception):
                if RISK_BUDGET_COOLDOWN_ACTIVE is not None:
                    RISK_BUDGET_COOLDOWN_ACTIVE.labels(symbol=sym).set(1.0)
            with contextlib.suppress(Exception):
                if RISK_BUDGET_COOLDOWN_REMAINING_S is not None:
                    RISK_BUDGET_COOLDOWN_REMAINING_S.labels(symbol=sym).set(remaining_s)

        # Clear expired cooldowns for tracked symbols
        for sym in self.track_symbols:
            if sym not in cooldowns:
                with contextlib.suppress(Exception):
                    if RISK_BUDGET_COOLDOWN_ACTIVE is not None:
                        RISK_BUDGET_COOLDOWN_ACTIVE.labels(symbol=sym).set(0.0)
                with contextlib.suppress(Exception):
                    if RISK_BUDGET_COOLDOWN_REMAINING_S is not None:
                        RISK_BUDGET_COOLDOWN_REMAINING_S.labels(symbol=sym).set(0.0)

        with contextlib.suppress(Exception):
            if RISK_BUDGET_AGGREGATOR_LAST_RUN_TS is not None:
                RISK_BUDGET_AGGREGATOR_LAST_RUN_TS.set(time.time())

        return result

    def run_forever(self, poll_interval_s: float = 5.0) -> None:
        while True:
            try:
                self.run_once()
            except KeyboardInterrupt:
                break
            except Exception:
                with contextlib.suppress(Exception):
                    if RISK_BUDGET_AGGREGATOR_ERRORS_TOTAL is not None:
                        RISK_BUDGET_AGGREGATOR_ERRORS_TOTAL.labels(source="poll_cycle").inc()
            time.sleep(poll_interval_s)


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------

def main() -> None:
    enabled = os.getenv("RISK_BUDGET_SERVICE_ENABLED", "1").strip() not in {"0", "false"}
    if not enabled:
        import sys
        print("RISK_BUDGET_SERVICE_ENABLED=0, exiting", flush=True)
        sys.exit(0)

    prom_port = int(os.getenv("PROMETHEUS_PORT", "9925"))
    poll_s = float(os.getenv("RISK_BUDGET_POLL_INTERVAL_S", "5"))
    symbols_raw = os.getenv("RISK_BUDGET_TRACK_SYMBOLS", "BTCUSDT,ETHUSDT,SOLUSDT").strip()
    track_symbols = [s.strip().upper() for s in symbols_raw.split(",") if s.strip()]

    if _prom_ok and start_http_server:
        with contextlib.suppress(Exception):
            start_http_server(prom_port)

    redis_url = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
    if _redis_mod is None:
        raise RuntimeError("redis-py required")
    r = _redis_mod.from_url(redis_url, decode_responses=False)

    agg = RiskBudgetAggregator(r=r, track_symbols=track_symbols)
    print(
        f"[risk_budget_service] poll={poll_s}s symbols={track_symbols} prom=:{prom_port}",
        flush=True,
    )
    agg.run_forever(poll_interval_s=poll_s)


if __name__ == "__main__":
    main()
