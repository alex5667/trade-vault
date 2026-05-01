from __future__ import annotations
"""Hawkes-like intensity proxies (cheap, online, O(1)).

Purpose
-------
Provide lightweight "event intensity" features for microstructure:
  - taker buy / taker sell
  - cancel bid / cancel ask
  - limit-add (total)

These are *not* full Hawkes processes. We use a single decayed state per stream:
  S(t) = exp(-beta * dt) * S(t-1) + rate(t)
  lambda(t) = mu + alpha * S(t)

Design goals
------------
- Deterministic (dt based on event timestamps)
- Low latency: uses already-computed L3-lite EMAs (qty/sec)
- Fail-open: safe defaults on missing data

Environment variables (optional)
-------------------------------
HKS_BETA
HKS_MU_TAKER_BUY, HKS_ALPHA_TAKER_BUY
HKS_MU_TAKER_SELL, HKS_ALPHA_TAKER_SELL
HKS_MU_CANCEL_BID, HKS_ALPHA_CANCEL_BID
HKS_MU_CANCEL_ASK, HKS_ALPHA_CANCEL_ASK
HKS_MU_LIMIT_ADD,  HKS_ALPHA_LIMIT_ADD
HKS_MU_TAKER, HKS_ALPHA_TAKER
HKS_MU_CANCEL, HKS_ALPHA_CANCEL
HKS_MU_CHURN,  HKS_ALPHA_CHURN

Config overrides
----------------
You can also override via runtime.config keys (if passed to update_hawkes_like):
  hawkes_beta
  hawkes_mu_* / hawkes_alpha_* (same suffixes as env names, lower-case)

State format
------------
We keep state in a plain dict (json-safe):
  {
    "ts_ms": int,
    "S_taker": float,
    "S_taker_buy": float,
    "S_taker_sell": float,
    "S_cancel": float,
    "S_cancel_bid": float,
    "S_cancel_ask": float,
    "S_limit_add": float,
    "S_churn": float,
  }
"""


import math
import os
from typing import Any, Dict, Optional, Tuple


def _f(v: Any, d: float = 0.0) -> float:
    try:
        x = float(v)
        return x if math.isfinite(x) else d
    except Exception:
        return d


def _cfg_or_env(cfg: Optional[Dict[str, Any]], cfg_key: str, env_key: str, default: float) -> float:
    if cfg and cfg_key in cfg:
        return _f(cfg.get(cfg_key), default)
    return _f(os.getenv(env_key, str(default)), default)


def _decay(beta: float, dt_s: float) -> float:
    if dt_s <= 0.0:
        return 1.0
    # clamp to avoid underflow explosions
    x = -beta * dt_s
    if x < -60.0:
        return 0.0
    if x > 0.0:
        x = 0.0
    return math.exp(x)


def update_hawkes_like(
    state: Optional[Dict[str, Any]],
    *,
    now_ts_ms: int,
    dt_s: float,
    rates: Dict[str, float],
    cfg: Optional[Dict[str, Any]] = None,
    eps: float = 1e-9,
) -> Tuple[Dict[str, Any], Dict[str, float]]:
    """Update hawkes-like state and return (state, snapshot).

    Args:
        state: previous state dict (may be None).
        now_ts_ms: timestamp of bucket end (ms).
        dt_s: bucket duration seconds.
        rates: per-stream instantaneous rates (qty/sec), keys:
            taker_buy_rate, taker_sell_rate,
            cancel_bid_rate, cancel_ask_rate,
            limit_add_rate
        cfg: optional runtime.config dict for overrides.

    Returns:
        (new_state, snapshot)
    """
    st: Dict[str, Any] = dict(state or {})

    beta = _cfg_or_env(cfg, "hawkes_beta", "HKS_BETA", 1.2)
    d = _decay(beta, float(dt_s))

    # Pull rates
    tb = _f(rates.get("taker_buy_rate"), 0.0)
    ts = _f(rates.get("taker_sell_rate"), 0.0)
    cb = _f(rates.get("cancel_bid_rate"), 0.0)
    ca = _f(rates.get("cancel_ask_rate"), 0.0)
    la = _f(rates.get("limit_add_rate"), 0.0)

    taker = tb + ts
    cancel = cb + ca
    churn = taker + cancel + la

    # Update states
    def _upd(k: str, add: float) -> float:
        prev = _f(st.get(k), 0.0)
        cur = d * prev + float(add) * max(0.0, float(dt_s))
        st[k] = float(cur)
        return float(cur)

    S_taker = _upd("S_taker", taker)
    S_tb = _upd("S_taker_buy", tb)
    S_ts = _upd("S_taker_sell", ts)

    S_cancel = _upd("S_cancel", cancel)
    S_cb = _upd("S_cancel_bid", cb)
    S_ca = _upd("S_cancel_ask", ca)

    S_la = _upd("S_limit_add", la)
    S_ch = _upd("S_churn", churn)

    st["ts_ms"] = int(now_ts_ms)

    # Lambdas: allow separate mu/alpha per stream + legacy aggregates.
    def _lam(mu_k: str, a_k: str, env_mu: str, env_a: str, S: float) -> float:
        mu = _cfg_or_env(cfg, mu_k, env_mu, 0.0)
        a = _cfg_or_env(cfg, a_k, env_a, 0.6)
        lam = float(mu) + float(a) * float(S)
        return lam if math.isfinite(lam) else 0.0

    snap: Dict[str, float] = {
        "hawkes_dt_s": float(dt_s),

        # New split intensities
        "hawkes_taker_buy_lam": _lam("hawkes_mu_taker_buy", "hawkes_alpha_taker_buy", "HKS_MU_TAKER_BUY", "HKS_ALPHA_TAKER_BUY", S_tb),
        "hawkes_taker_sell_lam": _lam("hawkes_mu_taker_sell", "hawkes_alpha_taker_sell", "HKS_MU_TAKER_SELL", "HKS_ALPHA_TAKER_SELL", S_ts),
        "hawkes_cancel_bid_lam": _lam("hawkes_mu_cancel_bid", "hawkes_alpha_cancel_bid", "HKS_MU_CANCEL_BID", "HKS_ALPHA_CANCEL_BID", S_cb),
        "hawkes_cancel_ask_lam": _lam("hawkes_mu_cancel_ask", "hawkes_alpha_cancel_ask", "HKS_MU_CANCEL_ASK", "HKS_ALPHA_CANCEL_ASK", S_ca),
        "hawkes_limit_add_lam": _lam("hawkes_mu_limit_add", "hawkes_alpha_limit_add", "HKS_MU_LIMIT_ADD", "HKS_ALPHA_LIMIT_ADD", S_la),

        # Legacy aggregate intensities (kept for backward compatibility)
        "hawkes_taker_lam": _lam("hawkes_mu_taker", "hawkes_alpha_taker", "HKS_MU_TAKER", "HKS_ALPHA_TAKER", S_taker),
        "hawkes_cancel_lam": _lam("hawkes_mu_cancel", "hawkes_alpha_cancel", "HKS_MU_CANCEL", "HKS_ALPHA_CANCEL", S_cancel),
        "hawkes_churn_lam": _lam("hawkes_mu_churn", "hawkes_alpha_churn", "HKS_MU_CHURN", "HKS_ALPHA_CHURN", S_ch),

        # Raw state magnitudes (debuggable, but low-cardinality)
        "hawkes_S_taker_buy": float(S_tb),
        "hawkes_S_taker_sell": float(S_ts),
        "hawkes_S_cancel_bid": float(S_cb),
        "hawkes_S_cancel_ask": float(S_ca),
        "hawkes_S_limit_add": float(S_la),
    }

    # sanitize
    for k, v in list(snap.items()):
        if not math.isfinite(float(v)):
            snap[k] = 0.0

    # avoid silly infinities when dt=0
    if abs(float(dt_s)) < eps:
        snap["hawkes_dt_s"] = 0.0

    return st, snap
