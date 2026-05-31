#!/usr/bin/env python3
"""
Nightly monitor for ConfidenceThresholdCalibrator shadow→enforce proof-streak.

Checks 3 conditions over top-N symbols using reliability_calibrator Redis curves:
  1. shadow_min_conf stability: |today - yesterday| < STABILITY_THRESH for all symbols
  2. Realized WR above shadow threshold >= CONF_CAL_TARGET_WR for each symbol
  3. ECE-proxy not growing: WR didn't decline by more than ECE_DECLINE_MAX vs prev night

After PROOF_NIGHTS_REQUIRED consecutive passes:
  - Writes conf_cal_enforce=1 to settings:dynamic_cfg
  - Sends Telegram notification

Proof state stored in Redis hash PROOF_STATE_KEY.

ENV:
  REDIS_URL                   Redis URL (default redis://redis-worker-1:6379/0)
  CONF_CAL_TARGET_WR          WR target (default 0.55)
  CONF_CAL_OUTCOME            outcome to check (default tp2)
  CONF_CAL_MIN_SAMPLES        min samples above threshold to trust inversion (default 50)
  CONF_CAL_TOP_SYMBOLS        comma-separated (default BTCUSDT,ETHUSDT,SOLUSDT,BNBUSDT,XRPUSDT)
  STABILITY_THRESH            max shadow shift across nights (default 3.0 pct points)
  ECE_DECLINE_MAX             max WR decline vs prev night (default 0.03)
  PROOF_NIGHTS_REQUIRED       consecutive nights needed (default 3)
  PROOF_STATE_KEY             Redis hash key for proof state (default sre:conf_threshold_cal:proof)
  DYN_CFG_KEY                 Redis hash for dynamic config (default settings:dynamic_cfg)
  NOTIFY_TELEGRAM_STREAM      Telegram stream key (default notify:telegram)
  PROMOTE_DRY_RUN             1 → log promote but don't write to dynamic_cfg (default 0)
"""

from __future__ import annotations

import json
import os
import sys
import time
from dataclasses import dataclass
from typing import Any

import redis


# ─── helpers ──────────────────────────────────────────────────────────────────

def _si(x: Any, d: int = 0) -> int:
    try:
        return int(x)
    except Exception:
        return d


def _sf(x: Any, d: float = 0.0) -> float:
    try:
        return float(x)
    except Exception:
        return d


def _now_ms() -> int:
    return int(time.time() * 1000)


def _notify_telegram(r: redis.Redis, stream: str, msg: str) -> None:
    try:
        r.xadd(stream, {"text": msg, "ts_ms": str(_now_ms())}, maxlen=500)
    except Exception:
        pass


# ─── curve inversion (mirrors core/confidence_threshold_calibrator) ───────────

def _parse_buckets(hash_data: dict[str, str]) -> dict[int, tuple[int, int]]:
    out: dict[int, tuple[int, int]] = {}
    for k, v in hash_data.items():
        if not k.startswith("b"):
            continue
        colon = k.find(":")
        if colon < 2:
            continue
        try:
            bkt = int(k[1:colon])
        except ValueError:
            continue
        suffix = k[colon + 1:]
        n, h = out.get(bkt, (0, 0))
        if suffix == "n":
            out[bkt] = (_si(v), h)
        elif suffix == "h":
            out[bkt] = (n, _si(v))
    return out


def _invert(hash_data: dict[str, str], target_wr: float, min_n: int) -> tuple[float, float, int] | None:
    """Returns (threshold, wr_above, n_above) or None."""
    buckets = _parse_buckets(hash_data)
    if not buckets:
        return None
    cum_n = cum_h = 0
    best: tuple[float, float, int] | None = None
    for bkt in sorted(buckets.keys(), reverse=True):
        n, h = buckets[bkt]
        cum_n += n
        cum_h += h
        if cum_n < min_n:
            continue
        wr = cum_h / cum_n
        if wr >= target_wr:
            best = (float(bkt), wr, cum_n)
    return best


def _key(prefix: str, outcome: str, symbol: str) -> str:
    """Most-specific reliability key: kind=na, venue=na, session=na, tf=na, regime=na."""
    return f"{prefix}:{outcome}:na:{symbol}:na:na:na:na"


# ─── proof state (stored in Redis hash) ───────────────────────────────────────

@dataclass
class ProofState:
    nights_passed: int = 0
    last_check_ts_ms: int = 0
    promoted: bool = False
    # Per-symbol last night's values (JSON blob)
    prev_symbol_data: dict[str, dict[str, float]] = None  # type: ignore

    @classmethod
    def load(cls, r: redis.Redis, key: str) -> ProofState:
        try:
            h = r.hgetall(key) or {}
        except Exception:
            return cls()
        ps = cls()
        ps.nights_passed = _si(h.get("nights_passed", 0))
        ps.last_check_ts_ms = _si(h.get("last_check_ts_ms", 0))
        ps.promoted = h.get("promoted", "0") == "1"
        try:
            ps.prev_symbol_data = json.loads(h.get("prev_symbol_data", "{}") or "{}")
        except Exception:
            ps.prev_symbol_data = {}
        return ps

    def save(self, r: redis.Redis, key: str) -> None:
        try:
            r.hset(key, mapping={
                "nights_passed": str(self.nights_passed),
                "last_check_ts_ms": str(self.last_check_ts_ms),
                "promoted": "1" if self.promoted else "0",
                "prev_symbol_data": json.dumps(self.prev_symbol_data or {}),
            })
        except Exception:
            pass


# ─── main check ───────────────────────────────────────────────────────────────

@dataclass
class SymbolResult:
    symbol: str
    shadow_thr: float | None    # inverted threshold today
    wr_above: float | None      # realized WR above that threshold
    n_above: int
    prev_thr: float | None      # threshold last night
    stable: bool
    wr_ok: bool
    ece_ok: bool

    @property
    def passed(self) -> bool:
        return self.stable and self.wr_ok and self.ece_ok

    def __str__(self) -> str:
        thr = f"{self.shadow_thr:.1f}" if self.shadow_thr is not None else "?"
        wr = f"{self.wr_above:.3f}" if self.wr_above is not None else "?"
        pt = f"{self.prev_thr:.1f}" if self.prev_thr is not None else "?"
        icons = ("✅" if self.stable else "❌") + ("✅" if self.wr_ok else "❌") + ("✅" if self.ece_ok else "❌")
        return f"{self.symbol}: thr={thr}(prev={pt}) wr={wr} n={self.n_above} [{icons}stab/wr/ece]"


def check_symbol(
    r: redis.Redis,
    symbol: str,
    *,
    prefix: str,
    outcome: str,
    target_wr: float,
    min_n: int,
    stability_thresh: float,
    ece_decline_max: float,
    prev_data: dict[str, float],
) -> SymbolResult:
    key = _key(prefix, outcome, symbol)
    try:
        h = r.hgetall(key) or {}
    except Exception:
        h = {}

    shadow_thr: float | None = None
    wr_above: float | None = None
    n_above = 0

    if h:
        result = _invert(h, target_wr=target_wr, min_n=min_n)
        if result is not None:
            shadow_thr, wr_above, n_above = result

    prev_thr = prev_data.get("shadow_thr")
    prev_wr = prev_data.get("wr_above")

    # Condition 1: stability
    if shadow_thr is None or prev_thr is None:
        stable = False  # not enough history yet
    else:
        stable = abs(shadow_thr - prev_thr) <= stability_thresh

    # Condition 2: WR ≥ target
    wr_ok = wr_above is not None and wr_above >= target_wr

    # Condition 3: ECE-proxy — WR didn't decline by more than ece_decline_max
    if wr_above is None or prev_wr is None:
        ece_ok = False  # no history → conservative
    else:
        ece_ok = (prev_wr - wr_above) <= ece_decline_max

    return SymbolResult(
        symbol=symbol,
        shadow_thr=shadow_thr,
        wr_above=wr_above,
        n_above=n_above,
        prev_thr=prev_thr,
        stable=stable,
        wr_ok=wr_ok,
        ece_ok=ece_ok,
    )


def promote(r: redis.Redis, dyn_cfg_key: str, dry_run: bool) -> None:
    if dry_run:
        print("[DRY-RUN] Would write conf_cal_enforce=1 to", dyn_cfg_key)
        return
    try:
        r.hset(dyn_cfg_key, "conf_cal_enforce", "1")
        print(f"[PROMOTE] Written conf_cal_enforce=1 to {dyn_cfg_key}")
    except Exception as e:
        print(f"[PROMOTE] ERROR: {e}")


def main() -> None:
    redis_url = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
    target_wr = _sf(os.getenv("CONF_CAL_TARGET_WR", "0.55"))
    outcome = os.getenv("CONF_CAL_OUTCOME", "tp2")
    min_n = _si(os.getenv("CONF_CAL_MIN_SAMPLES", "50"))
    prefix = os.getenv("CONF_CAL_PREFIX", "relcal")
    top_symbols = [s.strip() for s in os.getenv("CONF_CAL_TOP_SYMBOLS", "BTCUSDT,ETHUSDT,SOLUSDT,BNBUSDT,XRPUSDT").split(",") if s.strip()]
    stability_thresh = _sf(os.getenv("STABILITY_THRESH", "3.0"))
    ece_decline_max = _sf(os.getenv("ECE_DECLINE_MAX", "0.03"))
    proof_nights = _si(os.getenv("PROOF_NIGHTS_REQUIRED", "3"))
    proof_key = os.getenv("PROOF_STATE_KEY", "sre:conf_threshold_cal:proof")
    dyn_cfg_key = os.getenv("DYN_CFG_KEY", "settings:dynamic_cfg")
    tg_stream = os.getenv("NOTIFY_TELEGRAM_STREAM", "notify:telegram")
    dry_run = os.getenv("PROMOTE_DRY_RUN", "0").lower() in ("1", "true", "yes")

    r = redis.Redis.from_url(redis_url, decode_responses=True)
    ps = ProofState.load(r, proof_key)

    if ps.promoted:
        print("[MONITOR] Already promoted. Nothing to do.")
        return

    # Cooldown: don't run more than once per 20h
    now_ms = _now_ms()
    if ps.last_check_ts_ms > 0 and (now_ms - ps.last_check_ts_ms) < 20 * 3600 * 1000:
        hrs = (now_ms - ps.last_check_ts_ms) / 3_600_000
        print(f"[MONITOR] Too soon (last check {hrs:.1f}h ago, cooldown 20h). Skip.")
        return

    print(f"[MONITOR] Checking {len(top_symbols)} symbols | target_wr={target_wr} nights_required={proof_nights}")

    results: list[SymbolResult] = []
    new_symbol_data: dict[str, dict[str, float]] = {}

    for sym in top_symbols:
        prev_data = (ps.prev_symbol_data or {}).get(sym, {})
        res = check_symbol(
            r, sym,
            prefix=prefix, outcome=outcome, target_wr=target_wr, min_n=min_n,
            stability_thresh=stability_thresh, ece_decline_max=ece_decline_max,
            prev_data=prev_data,
        )
        results.append(res)
        print(f"  {res}")
        new_symbol_data[sym] = {
            "shadow_thr": res.shadow_thr if res.shadow_thr is not None else 0.0,
            "wr_above": res.wr_above if res.wr_above is not None else 0.0,
        }

    all_passed = all(r.passed for r in results)

    # Symbols with no data yet get a grace check (don't fail the streak for cold symbols)
    any_data = any(r.shadow_thr is not None for r in results)
    if not any_data:
        print("[MONITOR] No data in reliability_calibrator yet — skipping (data not warm)")
        return

    # Update proof state
    if all_passed:
        ps.nights_passed += 1
        print(f"[MONITOR] PASS night {ps.nights_passed}/{proof_nights}")
    else:
        failed = [r.symbol for r in results if not r.passed]
        print(f"[MONITOR] FAIL — resetting streak (failed: {', '.join(failed)})")
        ps.nights_passed = 0

    ps.last_check_ts_ms = now_ms
    ps.prev_symbol_data = new_symbol_data
    ps.save(r, proof_key)

    # Build report lines
    lines = [
        f"ConfThresholdCal Monitor Night {ps.nights_passed}/{proof_nights}",
        f"Target WR: {target_wr:.0%} | Stability: ±{stability_thresh:.0f}pt | ECE decline max: {ece_decline_max:.2f}",
        "",
    ]
    for res in results:
        lines.append(str(res))
    lines.append("")

    if ps.nights_passed >= proof_nights and not ps.promoted:
        lines.append(f"ALL CONDITIONS MET x{proof_nights} NIGHTS — PROMOTING conf_cal_enforce=1")
        promote(r, dyn_cfg_key, dry_run)
        ps.promoted = not dry_run
        ps.save(r, proof_key)
        _notify_telegram(r, tg_stream, "\n".join(lines))
    elif all_passed:
        lines.append(f"Night {ps.nights_passed}/{proof_nights} passed. {proof_nights - ps.nights_passed} more nights needed.")
        _notify_telegram(r, tg_stream, "\n".join(lines))
    else:
        lines.append("Streak reset. Monitoring continues.")
        _notify_telegram(r, tg_stream, "\n".join(lines))

    print("\n" + "\n".join(lines))
    sys.exit(0)


if __name__ == "__main__":
    main()
