"""Inline LONG-gate for iceberg detector path (2026-05-27, P0.A).

Source of the problem
---------------------
`services/binance_iceberg_detector.py` publishes signals through
`SyncSignalPublisher` directly and **never** invokes `EntryPolicyGate`.
That means HTF_LONG_BIAS / BTC_DROP_BLOCK_LONG / KIND_KILL_LIST /
DAILY_DD_KILLSWITCH / VETO_REGIME_UNRESOLVED_LONG are silently bypassed
for every iceberg signal. Audit 2026-05-27 attributed -99R / 12h to
iceberg LONG (WR 0%).

What this module does
---------------------
Provides a thin, dependency-light decision function that mirrors the
subset of EntryPolicyGate checks relevant for the iceberg path. It
does **not** need a full `ctx` object — only the symbol, direction,
and access to redis (for the BTC drop reader and daily-dd reader).

Activation: `ICEBERG_INLINE_LONG_GATE_ENABLED=1`. SHADOW first (only
counter, no block) via `ICEBERG_INLINE_LONG_GATE_MODE=shadow|enforce`.
"""

from __future__ import annotations

import logging
import math
import os
from dataclasses import dataclass
from typing import Any

log = logging.getLogger("iceberg-inline-gate")

try:
    from prometheus_client import Counter
    _gate_total = Counter(
        "iceberg_inline_long_gate_total",
        "Inline LONG-gate decisions for iceberg detector path (P0.A)",
        ["symbol", "side", "decision", "reason"],
    )
except Exception:
    _gate_total = None  # type: ignore[assignment]


# Lazy regime-redis client. `regime:{SYMBOL}` lives on redis-worker-1, while
# iceberg detector uses main redis. To keep the helper drop-in we resolve the
# URL once via env: REGIME_REDIS_URL → REDIS_WORKER_1_URL → REDIS_URL.
_regime_rc: Any = None
_regime_rc_resolved: bool = False


def _get_regime_client() -> Any:
    global _regime_rc, _regime_rc_resolved
    if _regime_rc_resolved:
        return _regime_rc
    _regime_rc_resolved = True
    url = (
        os.getenv("REGIME_REDIS_URL")
        or os.getenv("REDIS_WORKER_1_URL")
        or os.getenv("REDIS_URL")
        or ""
    ).strip()
    if not url:
        return None
    try:
        import redis as _redis  # local import to avoid hard dep at module load
        _regime_rc = _redis.from_url(url, decode_responses=True, socket_timeout=0.5)
    except Exception as e:
        log.debug("iceberg inline gate: regime client init failed (fail-open): %s", e)
        _regime_rc = None
    return _regime_rc


def get_regime_for_symbol(symbol: str) -> str | None:
    """Read `regime:{SYMBOL}` from worker-1; return None on any failure."""
    rc = _get_regime_client()
    if rc is None:
        return None
    try:
        raw = rc.get(f"regime:{(symbol or '').upper()}")
    except Exception:
        return None
    if raw is None:
        return None
    if isinstance(raw, bytes):
        try:
            raw = raw.decode("utf-8", errors="ignore")
        except Exception:
            return None
    s = str(raw).strip().lower()
    if not s or s in {"na", "none", "null", "unknown"}:
        return None
    return s


@dataclass(frozen=True)
class InlineGateDecision:
    veto: bool
    reason: str
    notes: str = ""


def _env_bool(name: str, default: bool = False) -> bool:
    try:
        v = os.getenv(name, "")
        if v == "":
            return bool(default)
        return v.strip().lower() in {"1", "true", "yes", "on"}
    except Exception:
        return bool(default)


def _kind_kill_list_hit(*, kind: str, side: str, symbol: str) -> tuple[bool, str]:
    csv = (os.getenv("KIND_KILL_LIST") or "").strip()
    if not csv:
        return False, ""
    _k = (kind or "").strip().lower()
    _s = (side or "").strip().upper()
    _y = (symbol or "").strip().upper()
    for tok in csv.split(","):
        tok = tok.strip()
        if not tok:
            continue
        parts = [p.strip() for p in tok.split(":")]
        k = (parts[0] if len(parts) > 0 else "").lower()
        s = (parts[1] if len(parts) > 1 else "").upper()
        y = (parts[2] if len(parts) > 2 else "").upper()
        if k and k != _k:
            continue
        if s and s != _s:
            continue
        if y and y != _y:
            continue
        return True, tok
    return False, ""


def _btc_drop_hit(*, symbol: str) -> tuple[bool, float | None, str]:
    """Returns (hit, btc_ret_5m, notes). Uses core.btc_drop_reader. Fail-open."""
    if not _env_bool("BTC_DROP_BLOCK_LONG_ENABLED", False):
        return False, None, ""
    exempt_csv = (os.getenv("BTC_DROP_BLOCK_LONG_EXEMPT", "BTCUSDT") or "").strip()
    exempt = {s.strip().upper() for s in exempt_csv.split(",") if s.strip()}
    if (symbol or "").upper() in exempt:
        return False, None, ""
    try:
        thr = float(os.getenv("BTC_DROP_BLOCK_LONG_PCT_5M", "-0.01") or "-0.01")
    except Exception:
        thr = -0.01
    try:
        from core.btc_drop_reader import get_btc_ret_5m
        btc_ret = get_btc_ret_5m()
    except Exception:
        return False, None, ""
    if btc_ret is None or not math.isfinite(float(btc_ret)):
        return False, None, ""
    btc_ret = float(btc_ret)
    if btc_ret <= thr:
        return True, btc_ret, f"btc_ret_5m={btc_ret:.5f}<=thr={thr:.5f}"
    return False, btc_ret, ""


def _daily_dd_armed(redis_client: Any) -> tuple[bool, str]:
    """Fail-open: any failure → not armed. Reads `risk:daily_dd:state`."""
    if not _env_bool("DAILY_DD_KILLSWITCH_ENABLED", True):
        return False, ""
    try:
        state = redis_client.hgetall("risk:daily_dd:state")
    except Exception:
        return False, ""
    if not state:
        return False, ""
    norm: dict[str, str] = {}
    for k, v in state.items():
        ks = k.decode() if isinstance(k, bytes) else k
        vs = v.decode() if isinstance(v, bytes) else v
        norm[str(ks)] = str(vs)
    mode = (norm.get("mode") or "shadow").strip().lower()
    if mode != "enforce":
        return False, ""
    armed = (norm.get("kill_armed") or "0").strip()
    if armed in ("1", "true", "True"):
        return True, f"kill_armed mode=enforce reason={norm.get('reason','')}"
    return False, ""


def evaluate_iceberg_long(
    *,
    redis_client: Any,
    symbol: str,
    direction: str,
    kind: str = "iceberg",
) -> InlineGateDecision:
    """Inline LONG-gate decision for iceberg path.

    Side==LONG only triggers LONG-specific checks; SHORT bypasses LONG-only
    veto branches but still subjects to KIND_KILL_LIST and DAILY_DD.

    Returns InlineGateDecision(veto=True, reason=<code>) on block.
    """
    enabled = _env_bool("ICEBERG_INLINE_LONG_GATE_ENABLED", False)
    if not enabled:
        return InlineGateDecision(False, "DISABLED")
    mode = (os.getenv("ICEBERG_INLINE_LONG_GATE_MODE", "shadow") or "shadow").strip().lower()

    sym = (symbol or "").upper()
    side = "LONG" if (direction or "").strip().upper() in ("LONG", "BUY") else "SHORT"
    knd = (kind or "iceberg").strip().lower()

    def _emit(decision: str, reason: str) -> None:
        try:
            if _gate_total is not None:
                _gate_total.labels(symbol=sym, side=side, decision=decision, reason=reason).inc()
        except Exception:
            pass

    # 1) DAILY_DD_KILLSWITCH (account-wide; both sides)
    dd_armed, dd_notes = _daily_dd_armed(redis_client)
    if dd_armed:
        _emit("ENFORCE" if mode == "enforce" else "SHADOW", "VETO_DAILY_DD_KILLSWITCH")
        if mode == "enforce":
            return InlineGateDecision(True, "VETO_DAILY_DD_KILLSWITCH", dd_notes)

    # 2) KIND_KILL_LIST (both sides; matches per token)
    hit, tok = _kind_kill_list_hit(kind=knd, side=side, symbol=sym)
    if hit:
        _emit("ENFORCE" if mode == "enforce" else "SHADOW", "VETO_KIND_KILL_LIST")
        if mode == "enforce":
            return InlineGateDecision(True, "VETO_KIND_KILL_LIST", f"matched={tok}")

    # LONG-only checks below
    if side != "LONG":
        _emit("ALLOW", "OK_SHORT")
        return InlineGateDecision(False, "OK")

    # 3) BTC_DROP_BLOCK_LONG
    btc_hit, btc_ret, btc_notes = _btc_drop_hit(symbol=sym)
    if btc_hit:
        _emit("ENFORCE" if mode == "enforce" else "SHADOW", "VETO_BTC_DROP_BLOCK_LONG")
        if mode == "enforce":
            return InlineGateDecision(True, "VETO_BTC_DROP_BLOCK_LONG", btc_notes)

    # 4) Regime=NULL fail-CLOSED для LONG iceberg (P0.D mirror)
    if _env_bool("LONG_REQUIRE_REGIME_RESOLVED", False):
        regime = get_regime_for_symbol(sym)
        # Опционально ограничиваем kinds через CSV (default охватывает iceberg).
        scoped_csv = (os.getenv("LONG_REQUIRE_REGIME_KINDS", "iceberg,delta_spike,absorption") or "").strip().lower()
        scoped = {k.strip() for k in scoped_csv.split(",") if k.strip()}
        if knd in scoped and regime is None:
            _emit("ENFORCE" if mode == "enforce" else "SHADOW", "VETO_REGIME_UNRESOLVED_LONG")
            if mode == "enforce":
                return InlineGateDecision(True, "VETO_REGIME_UNRESOLVED_LONG", "regime missing")

    # 5) Regime bias: если known regime — block LONG iceberg в trending_bear (зеркало HTF_LONG_BIAS)
    if _env_bool("ICEBERG_LONG_REGIME_BIAS_ENABLED", False):
        regime = get_regime_for_symbol(sym)
        bad_csv = (os.getenv("ICEBERG_LONG_REGIME_BIAS_BLOCK", "trending_bear,squeeze") or "").strip().lower()
        bad = {x.strip() for x in bad_csv.split(",") if x.strip()}
        if regime in bad:
            _emit("ENFORCE" if mode == "enforce" else "SHADOW", "VETO_ICEBERG_LONG_REGIME_BIAS")
            if mode == "enforce":
                return InlineGateDecision(True, "VETO_ICEBERG_LONG_REGIME_BIAS", f"regime={regime}")

    _emit("ALLOW", "OK")
    return InlineGateDecision(False, "OK")
