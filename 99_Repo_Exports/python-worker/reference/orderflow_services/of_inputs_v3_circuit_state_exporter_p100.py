from __future__ import annotations

"""OFInputs V3 circuit state exporter (P100).

Purpose
- Export Redis-backed circuit breaker state for deterministic V3->V2 fallback.
- Use Redis cfg/state keys as the source-of-truth (survives worker restarts).

Keys read
- cfg:of_inputs:v3_disabled:{sym} (JSON + TTL)
- state:of_inputs:v3_downgrades:{reason}:{sym} (ZSET windowed)
- cfg:of_inputs_v3:auto_apply_block_global:{reason} (optional)
- cfg:of_inputs_v3:auto_apply_block:{sym}:{reason} (optional)

Outputs (Prometheus)
- of_inputs_v3_circuit_state_exporter_up
- of_inputs_v3_circuit_state_exporter_poll_ts_ms
- of_inputs_v3_circuit_state_exporter_errors_total

- of_inputs_v3_circuit_cfg_disabled{symbol,reason} = 1
- of_inputs_v3_circuit_cfg_disabled_until_ms{symbol,reason}
- of_inputs_v3_circuit_cfg_disabled_hard{symbol,reason} = 1
- of_inputs_v3_circuit_cfg_disabled_cooldown{symbol,reason} = 1
- of_inputs_v3_circuit_cfg_disabled_hard_until_ms{symbol,reason}
- of_inputs_v3_circuit_cfg_disabled_ttl_ms{symbol,reason}
- of_inputs_v3_circuit_disabled_symbols
- of_inputs_v3_circuit_disabled_symbols_by_reason{reason}

- of_inputs_v3_circuit_downgrades_window{symbol,reason}  (ZCARD)
- of_inputs_v3_circuit_downgrades_window_sum{reason}

- of_inputs_v3_circuit_auto_apply_block_global_active{reason}
- of_inputs_v3_circuit_auto_apply_block_global_ttl_ms{reason}
- of_inputs_v3_circuit_auto_apply_block_symbol_active{symbol,reason}
- of_inputs_v3_circuit_auto_apply_block_symbol_ttl_ms{symbol,reason}

ENV
- REDIS_URL or CRYPTO_NOTIFY_REDIS_URL (required)
- OF_INPUTS_V3_CIRCUIT_EXPORTER_PORT (default: 9164)
- OF_INPUTS_V3_CIRCUIT_EXPORTER_REFRESH_SEC (default: 10)

Cardinality control
- By default the exporter emits per-symbol series only for symbols that currently
  have circuit keys present (disabled keys, downgrade zsets, auto-apply block keys).
- Optional allowlist envs:
    OF_INPUTS_V3_CIRCUIT_EXPORTER_SYMBOLS="BTCUSDT,ETHUSDT"
    OF_INPUTS_V3_CIRCUIT_EXPORTER_REASONS="seq_gap,missing_lob_fields,latency"

Run
  python -m orderflow_services.of_inputs_v3_circuit_state_exporter_p100
"""

import json
import os
import time
from collections.abc import Iterable
from typing import Any

from prometheus_client import Gauge, start_http_server  # type: ignore

from utils.time_utils import get_ny_time_millis

# ---- helpers

def _now_ms() -> int:
    return get_ny_time_millis()


def _as_str(x: Any, default: str = "") -> str:
    try:
        if x is None:
            return default
        if isinstance(x, (bytes, bytearray)):
            return x.decode("utf-8", "ignore")
        return str(x)
    except Exception:
        return default


def _as_int(x: Any, default: int = 0) -> int:
    try:
        if x is None or isinstance(x, bool):
            return default
        if isinstance(x, (int, float)):
            return int(x)
        s = _as_str(x).strip()
        return int(float(s)) if s else default
    except Exception:
        return default


def _json_loads(s: str | None) -> dict[str, Any]:
    if not s:
        return {}
    try:
        obj = json.loads(s)
        return obj if isinstance(obj, dict) else {}
    except Exception:
        return {}


def _parse_csv(raw: str, upper: bool = True) -> list[str]:
    raw = (raw or "").strip()
    if not raw:
        return []
    xs: list[str] = []
    for p in raw.replace(";", ",").split(","):
        s = p.strip()
        s = s.upper() if upper else s
        if s and s not in xs:
            xs.append(s)
    return xs


def _connect_redis():
    rurl = os.getenv("REDIS_URL") or os.getenv("CRYPTO_NOTIFY_REDIS_URL") or ""
    if not str(rurl).strip():
        return None
    try:
        import redis  # type: ignore

        return redis.Redis.from_url(rurl, decode_responses=True)
    except Exception:
        return None


def _remove_stale(g: Gauge, prev: set[tuple[str, ...]], cur: set[tuple[str, ...]]) -> set[tuple[str, ...]]:
    stale = prev - cur
    for lv in stale:
        try:
            g.remove(*lv)
        except Exception:
            # best-effort
            pass
    return cur


# ---- metrics

G_UP = Gauge("of_inputs_v3_circuit_state_exporter_up", "1 if exporter loop is running")
G_POLL_TS_MS = Gauge("of_inputs_v3_circuit_state_exporter_poll_ts_ms", "Last poll timestamp (ms)")
G_ERRORS_TOTAL = Gauge("of_inputs_v3_circuit_state_exporter_errors_total", "Cumulative exporter loop errors")

G_DISABLED = Gauge(
    "of_inputs_v3_circuit_cfg_disabled",
    "1 if cfg disables V3 inputs for symbol",
    ["symbol", "reason"],
)
G_DISABLED_UNTIL_MS = Gauge(
    "of_inputs_v3_circuit_cfg_disabled_until_ms",
    "Disable until timestamp (ms) derived from cfg disable key",
    ["symbol", "reason"],
)

G_DISABLED_HARD = Gauge(
    "of_inputs_v3_circuit_cfg_disabled_hard",
    "1 if cfg disables V3 inputs for symbol (hard-disable phase)",
    ["symbol", "reason"],
)
G_DISABLED_COOLDOWN = Gauge(
    "of_inputs_v3_circuit_cfg_disabled_cooldown",
    "1 if cfg disables V3 inputs for symbol (cooldown/anti-flap phase)",
    ["symbol", "reason"],
)
G_DISABLED_HARD_UNTIL_MS = Gauge(
    "of_inputs_v3_circuit_cfg_disabled_hard_until_ms",
    "Hard-disable until timestamp (ms) derived from cfg disable key",
    ["symbol", "reason"],
)
G_DISABLED_TTL_MS = Gauge(
    "of_inputs_v3_circuit_cfg_disabled_ttl_ms",
    "Redis PTTL for cfg disable key (ms)",
    ["symbol", "reason"],
)
G_DISABLED_SYMBOLS = Gauge(
    "of_inputs_v3_circuit_disabled_symbols",
    "Count of currently disabled symbols",
)
G_DISABLED_BY_REASON = Gauge(
    "of_inputs_v3_circuit_disabled_symbols_by_reason",
    "Count of currently disabled symbols by reason",
    ["reason"],
)

G_DG = Gauge(
    "of_inputs_v3_circuit_downgrades_window",
    "Downgrades in sliding window (ZSET cardinality)",
    ["symbol", "reason"],
)
G_DG_SUM = Gauge(
    "of_inputs_v3_circuit_downgrades_window_sum",
    "Sum of downgrades across symbols in current window",
    ["reason"],
)

G_AP_GLOB = Gauge(
    "of_inputs_v3_circuit_auto_apply_block_global_active",
    "1 if global auto-apply block key is present",
    ["reason"],
)
G_AP_GLOB_TTL_MS = Gauge(
    "of_inputs_v3_circuit_auto_apply_block_global_ttl_ms",
    "TTL ms for global auto-apply block",
    ["reason"],
)
G_AP_SYM = Gauge(
    "of_inputs_v3_circuit_auto_apply_block_symbol_active",
    "1 if per-symbol auto-apply block key is present",
    ["symbol", "reason"],
)
G_AP_SYM_TTL_MS = Gauge(
    "of_inputs_v3_circuit_auto_apply_block_symbol_ttl_ms",
    "TTL ms for per-symbol auto-apply block",
    ["symbol", "reason"],
)


# ---- key parsing

CFG_DISABLED_PREFIX = "cfg:of_inputs:v3_disabled:"
STATE_DG_PREFIX = "state:of_inputs:v3_downgrades:"
AP_GLOB_PREFIX = "cfg:of_inputs_v3:auto_apply_block_global:"
AP_SYM_PREFIX = "cfg:of_inputs_v3:auto_apply_block:"


def _sym_from_cfg_disabled_key(key: str) -> str:
    if not key.startswith(CFG_DISABLED_PREFIX):
        return ""
    return key[len(CFG_DISABLED_PREFIX) :].strip().upper()


def _reason_sym_from_dg_key(key: str) -> tuple[str, str]:
    if not key.startswith(STATE_DG_PREFIX):
        return ("", "")
    rest = key[len(STATE_DG_PREFIX) :]
    if ":" not in rest:
        return ("", "")
    reason, sym = rest.split(":", 1)
    return (reason.strip().lower() or "unknown", sym.strip().upper())


def _reason_from_ap_glob_key(key: str) -> str:
    if not key.startswith(AP_GLOB_PREFIX):
        return ""
    return key[len(AP_GLOB_PREFIX) :].strip().lower() or "unknown"


def _sym_reason_from_ap_sym_key(key: str) -> tuple[str, str]:
    if not key.startswith(AP_SYM_PREFIX):
        return ("", "")
    rest = key[len(AP_SYM_PREFIX) :]
    if ":" not in rest:
        return ("", "")
    sym, reason = rest.split(":", 1)
    return (sym.strip().upper(), reason.strip().lower() or "unknown")


def _iter_scan(r, pattern: str) -> Iterable[str]:
    try:
        # scan_iter is available on redis-py
        yield from r.scan_iter(match=pattern, count=10000)
    except Exception:
        return


def _derive_until_ms(meta: dict[str, Any], now_ms: int, pttl_ms: int) -> tuple[int, str]:
    # Mirror services.orderflow.of_inputs_v3_circuit.refresh_disabled_state semantics.
    until_ms = _as_int(meta.get("until_ms"), 0)
    reason = _as_str(meta.get("reason") or meta.get("dq_code") or "cfg", "cfg")

    if until_ms <= 0:
        if pttl_ms < 0:
            # No TTL (manual) => represent as far future.
            until_ms = now_ms + 10 * 365 * 24 * 3600 * 1000
            reason = reason or "manual_no_ttl"
        else:
            until_ms = now_ms + max(0, int(pttl_ms))
            reason = reason or "cfg_ttl"

    return int(until_ms), (reason or "cfg")

def _derive_hard_until_ms(meta: dict[str, Any], until_ms: int) -> int:
    hard = _as_int(meta.get("hard_until_ms"), 0)
    return int(hard) if int(hard) > 0 else int(until_ms)



def main() -> None:
    port = int(os.environ.get("OF_INPUTS_V3_CIRCUIT_EXPORTER_PORT", "9164"))
    refresh = float(os.environ.get("OF_INPUTS_V3_CIRCUIT_EXPORTER_REFRESH_SEC", "10"))

    symbols_allow = _parse_csv(os.environ.get("OF_INPUTS_V3_CIRCUIT_EXPORTER_SYMBOLS", ""), upper=True)
    reasons_allow = _parse_csv(os.environ.get("OF_INPUTS_V3_CIRCUIT_EXPORTER_REASONS", ""), upper=False)

    r = _connect_redis()
    if r is None:
        raise SystemExit("REDIS_URL (or CRYPTO_NOTIFY_REDIS_URL) is required")

    start_http_server(port)

    prev_disabled: set[tuple[str, ...]] = set()
    prev_until: set[tuple[str, ...]] = set()
    prev_hard_until: set[tuple[str, ...]] = set()
    prev_hard: set[tuple[str, ...]] = set()
    prev_cooldown: set[tuple[str, ...]] = set()
    prev_ttl: set[tuple[str, ...]] = set()
    prev_dg: set[tuple[str, ...]] = set()
    prev_ap_glob: set[tuple[str, ...]] = set()
    prev_ap_glob_ttl: set[tuple[str, ...]] = set()
    prev_ap_sym: set[tuple[str, ...]] = set()
    prev_ap_sym_ttl: set[tuple[str, ...]] = set()

    while True:
        now_ms = _now_ms()
        G_UP.set(1)
        G_POLL_TS_MS.set(now_ms)

        cur_disabled: set[tuple[str, ...]] = set()
        cur_until: set[tuple[str, ...]] = set()
        cur_hard_until: set[tuple[str, ...]] = set()
        cur_hard: set[tuple[str, ...]] = set()
        cur_cooldown: set[tuple[str, ...]] = set()
        cur_ttl: set[tuple[str, ...]] = set()
        cur_dg: set[tuple[str, ...]] = set()
        cur_ap_glob: set[tuple[str, ...]] = set()
        cur_ap_glob_ttl: set[tuple[str, ...]] = set()
        cur_ap_sym: set[tuple[str, ...]] = set()
        cur_ap_sym_ttl: set[tuple[str, ...]] = set()

        disabled_by_reason: dict[str, int] = {}
        dg_sum: dict[str, int] = {}

        try:
            # ---- cfg disabled
            disabled_keys = list(_iter_scan(r, CFG_DISABLED_PREFIX + "*"))
            if symbols_allow:
                # ensure we also check allowlisted symbols even when key missing (for removal correctness)
                disabled_keys = disabled_keys + [CFG_DISABLED_PREFIX + s for s in symbols_allow]

            # Pipeline GET+PTTL for all unique keys.
            uniq_keys = []
            seen_k = set()
            for k in disabled_keys:
                if k in seen_k:
                    continue
                seen_k.add(k)
                uniq_keys.append(k)

            if uniq_keys:
                pipe = r.pipeline(transaction=False)
                for k in uniq_keys:
                    pipe.get(k)
                    pipe.pttl(k)
                raw = pipe.execute()

                # raw is [get1, pttl1, get2, pttl2, ...]
                for i in range(0, len(raw), 2):
                    k = uniq_keys[i // 2]
                    v = raw[i]
                    pttl = _as_int(raw[i + 1], -2)
                    sym = _sym_from_cfg_disabled_key(k)
                    if not sym:
                        continue

                    if v is None:
                        # no key => nothing to emit for this sym
                        continue

                    meta = _json_loads(v)
                    until_ms, reason = _derive_until_ms(meta, now_ms=now_ms, pttl_ms=pttl)
                    hard_until_ms = _derive_hard_until_ms(meta, until_ms=until_ms)
                    active = 1 if until_ms > now_ms else 0
                    hard_active = 1 if (active and hard_until_ms > 0 and now_ms < hard_until_ms) else 0
                    cooldown_active = 1 if (active and hard_active == 0 and hard_until_ms > 0 and hard_until_ms < until_ms and now_ms < until_ms) else 0

                    lv = (sym, reason)
                    G_DISABLED.labels(symbol=sym, reason=reason).set(active)
                    G_DISABLED_UNTIL_MS.labels(symbol=sym, reason=reason).set(until_ms)
                    G_DISABLED_TTL_MS.labels(symbol=sym, reason=reason).set(max(-2, pttl))
                    G_DISABLED_HARD.labels(symbol=sym, reason=reason).set(hard_active)
                    G_DISABLED_COOLDOWN.labels(symbol=sym, reason=reason).set(cooldown_active)
                    G_DISABLED_HARD_UNTIL_MS.labels(symbol=sym, reason=reason).set(hard_until_ms)

                    cur_disabled.add(lv)
                    cur_until.add(lv)
                    cur_ttl.add(lv)
                    cur_hard.add(lv)
                    cur_cooldown.add(lv)
                    cur_hard_until.add(lv)

                    if active:
                        disabled_by_reason[reason] = disabled_by_reason.get(reason, 0) + 1

            # disabled aggregates
            disabled_n = sum(disabled_by_reason.values())
            G_DISABLED_SYMBOLS.set(disabled_n)

            # Keep stable 0 series for allowlisted reasons if provided.
            reasons_for_zero = set(reasons_allow) if reasons_allow else set(disabled_by_reason.keys())
            for rsn in reasons_for_zero:
                rsn_s = (rsn or "unknown").lower()
                G_DISABLED_BY_REASON.labels(reason=rsn_s).set(int(disabled_by_reason.get(rsn_s, 0)))

            # ---- downgrades zsets
            dg_keys = list(_iter_scan(r, STATE_DG_PREFIX + "*"))
            # Optionally constrain to allowlisted symbols/reasons.
            for k in dg_keys:
                reason, sym = _reason_sym_from_dg_key(k)
                if not reason or not sym:
                    continue
                if symbols_allow and sym not in symbols_allow:
                    continue
                if reasons_allow and reason not in [x.lower() for x in reasons_allow]:
                    continue

                try:
                    c = _as_int(r.zcard(k), 0)
                except Exception:
                    c = 0

                lv = (sym, reason)
                G_DG.labels(symbol=sym, reason=reason).set(c)
                cur_dg.add(lv)

                dg_sum[reason] = dg_sum.get(reason, 0) + int(c)

            # dg sum series
            reasons_for_zero = set(reasons_allow) if reasons_allow else set(dg_sum.keys())
            for rsn in reasons_for_zero:
                rsn_s = (rsn or "unknown").lower()
                G_DG_SUM.labels(reason=rsn_s).set(int(dg_sum.get(rsn_s, 0)))

            # ---- auto-apply blocks (global)
            for k in _iter_scan(r, AP_GLOB_PREFIX + "*"):
                rsn = _reason_from_ap_glob_key(k)
                if not rsn:
                    continue
                if reasons_allow and rsn not in [x.lower() for x in reasons_allow]:
                    continue

                ttl = _as_int(r.pttl(k), -2)
                G_AP_GLOB.labels(reason=rsn).set(1)
                G_AP_GLOB_TTL_MS.labels(reason=rsn).set(max(-2, ttl))
                cur_ap_glob.add((rsn,))
                cur_ap_glob_ttl.add((rsn,))

            # ---- auto-apply blocks (per-symbol)
            for k in _iter_scan(r, AP_SYM_PREFIX + "*"):
                sym, rsn = _sym_reason_from_ap_sym_key(k)
                if not sym or not rsn:
                    continue
                if symbols_allow and sym not in symbols_allow:
                    continue
                if reasons_allow and rsn not in [x.lower() for x in reasons_allow]:
                    continue

                ttl = _as_int(r.pttl(k), -2)
                G_AP_SYM.labels(symbol=sym, reason=rsn).set(1)
                G_AP_SYM_TTL_MS.labels(symbol=sym, reason=rsn).set(max(-2, ttl))
                cur_ap_sym.add((sym, rsn))
                cur_ap_sym_ttl.add((sym, rsn))

        except Exception:
            G_ERRORS_TOTAL.inc()

        # prune stale labels
        prev_disabled = _remove_stale(G_DISABLED, prev_disabled, cur_disabled)
        prev_until = _remove_stale(G_DISABLED_UNTIL_MS, prev_until, cur_until)
        prev_ttl = _remove_stale(G_DISABLED_TTL_MS, prev_ttl, cur_ttl)
        prev_hard = _remove_stale(G_DISABLED_HARD, prev_hard, cur_hard)
        prev_cooldown = _remove_stale(G_DISABLED_COOLDOWN, prev_cooldown, cur_cooldown)
        prev_hard_until = _remove_stale(G_DISABLED_HARD_UNTIL_MS, prev_hard_until, cur_hard_until)
        prev_dg = _remove_stale(G_DG, prev_dg, cur_dg)
        prev_ap_glob = _remove_stale(G_AP_GLOB, prev_ap_glob, cur_ap_glob)
        prev_ap_glob_ttl = _remove_stale(G_AP_GLOB_TTL_MS, prev_ap_glob_ttl, cur_ap_glob_ttl)
        prev_ap_sym = _remove_stale(G_AP_SYM, prev_ap_sym, cur_ap_sym)
        prev_ap_sym_ttl = _remove_stale(G_AP_SYM_TTL_MS, prev_ap_sym_ttl, cur_ap_sym_ttl)

        time.sleep(max(1.0, float(refresh)))


if __name__ == "__main__":
    main()
