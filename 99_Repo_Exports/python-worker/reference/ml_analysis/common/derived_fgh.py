from __future__ import annotations

import bisect
import math
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence, Tuple


def _f(x: Any, default: float = 0.0) -> float:
    try:
        v = float(x)
        if not math.isfinite(v):
            return float(default)
        return v
    except Exception:
        return float(default)


def _pick(ind: Dict[str, Any], keys: Sequence[str]) -> Tuple[Optional[str], float]:
    for k in keys:
        if k in ind:
            v = _f(ind.get(k), float("nan"))
            if math.isfinite(v):
                return str(k), float(v)
    return None, float("nan")


@dataclass
class DerivedFGHStats:
    n_rows: int = 0
    n_rel_ok: int = 0
    n_rel_missing_leader: int = 0
    n_rel_lagged: int = 0
    n_replen_ok: int = 0
    n_vel_ok: int = 0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "n_rows": int(self.n_rows)
            "n_rel_ok": int(self.n_rel_ok)
            "n_rel_missing_leader": int(self.n_rel_missing_leader)
            "n_rel_lagged": int(self.n_rel_lagged)
            "n_replen_ok": int(self.n_replen_ok)
            "n_vel_ok": int(self.n_vel_ok)
        }


def derive_fgh_rows(
    rows: List[Dict[str, Any]]
    *
    leader_symbol: str = "BTCUSDT"
    leader_max_lag_ms: int = 2000
    eps: float = 1e-9
    vel_z_alpha: float = 0.06
    store_debug_flags: bool = False
) -> Dict[str, Any]:
    """Derive ROI-dense offline-only features F/G/H.

    This is runtime-agnostic: it mutates dataset rows produced by offline builders
    by appending extra numeric keys into `row['indicators']`.

    F) Relative strength vs leader (default BTCUSDT):
      - rel_ofi_ml_norm_btc
      - rel_lob_micro_shift_bps_btc

    G) Replenishment imbalance (taker vs limit-add / added liquidity):
      - ask_replenish_imb
      - bid_replenish_imb
      - lob_replenishment_pressure
      - replenish_ratio_ask
      - replenish_ratio_bid
      - replenish_ratio_diff

    H) Velocity (first derivative, per-second):
      - ofi_ml_wsum_vel
      - micro_shift_bps_vel
      - ofi_ml_wsum_vel_z_ema
      - micro_shift_bps_vel_z_ema

    Notes:
      - Missing inputs => derived keys omitted (not forced to 0).
      - Leader matching: nearest timestamp within `leader_max_lag_ms`.
      - z_ema uses EMA(mean) + EMA(abs deviation), cheap O(1) per step.
    """
    stats = DerivedFGHStats(n_rows=len(rows))
    if not rows:
        return {"ok": True, "stats": stats.to_dict()}

    leader = str(leader_symbol or "BTCUSDT").upper()
    leader_ts: List[int] = []
    leader_ind: List[Dict[str, Any]] = []

    # Normalize indicators to dict and collect leader index.
    for r in rows:
        ind = r.get("indicators")
        if not isinstance(ind, dict):
            # be tolerant: sometimes builders store indicators as JSON string
            try:
                if isinstance(ind, str) and ind.strip().startswith("{"):
                    import json

                    ind2 = json.loads(ind)
                    ind = ind2 if isinstance(ind2, dict) else {}
                else:
                    ind = {}
            except Exception:
                ind = {}
            r["indicators"] = ind

        sym = str(r.get("symbol") or ind.get("symbol") or "").upper()
        try:
            ts_ms = int(r.get("ts_ms") or ind.get("ts_ms") or 0)
        except Exception:
            ts_ms = 0
        if sym == leader and ts_ms > 0:
            leader_ts.append(ts_ms)
            leader_ind.append(ind)

    # Leader index must be sorted for bisect.
    if leader_ts:
        paired = sorted(zip(leader_ts, leader_ind), key=lambda x: int(x[0]))
        leader_ts = [int(x[0]) for x in paired]
        leader_ind = [x[1] for x in paired]

    # -----------------------------
    # H) per-symbol velocities
    # -----------------------------
    by_sym: Dict[str, List[Tuple[int, Dict[str, Any]]]] = {}
    for r in rows:
        ind = r.get("indicators") if isinstance(r.get("indicators"), dict) else {}
        sym = str(r.get("symbol") or ind.get("symbol") or "").upper()
        try:
            ts_ms = int(r.get("ts_ms") or ind.get("ts_ms") or 0)
        except Exception:
            ts_ms = 0
        if sym and ts_ms > 0:
            by_sym.setdefault(sym, []).append((ts_ms, ind))

    for _, seq in by_sym.items():
        seq.sort(key=lambda x: int(x[0]))
        prev_ts: Optional[int] = None
        prev_ofi_wsum: Optional[float] = None
        prev_micro_shift: Optional[float] = None

        m_ofi = 0.0
        d_ofi = 0.0
        m_mp = 0.0
        d_mp = 0.0
        a = max(0.001, min(0.5, float(vel_z_alpha)))

        for ts_ms, ind in seq:
            _, cur_ofi_wsum = _pick(ind, ["ofi_ml_wsum", "ofi_wsum", "ofi"]) 
            _, cur_micro_shift = _pick(ind, ["lob_micro_shift_bps", "mp_shift_bps", "mp_shift", "mp_shift_bps"]) 

            if prev_ts is not None:
                dt_ms = int(ts_ms) - int(prev_ts)
                dt_s = float(dt_ms) / 1000.0
                if dt_s > 0.0 and dt_s <= 60.0:
                    if prev_ofi_wsum is not None and math.isfinite(cur_ofi_wsum) and math.isfinite(prev_ofi_wsum):
                        v = (float(cur_ofi_wsum) - float(prev_ofi_wsum)) / max(float(eps), dt_s)
                        ind["ofi_ml_wsum_vel"] = float(v)
                        m_ofi = m_ofi + a * (v - m_ofi)
                        d_ofi = d_ofi + a * (abs(v - m_ofi) - d_ofi)
                        ind["ofi_ml_wsum_vel_z_ema"] = float((v - m_ofi) / max(float(eps), d_ofi))
                        stats.n_vel_ok += 1

                    if prev_micro_shift is not None and math.isfinite(cur_micro_shift) and math.isfinite(prev_micro_shift):
                        v2 = (float(cur_micro_shift) - float(prev_micro_shift)) / max(float(eps), dt_s)
                        ind["micro_shift_bps_vel"] = float(v2)
                        m_mp = m_mp + a * (v2 - m_mp)
                        d_mp = d_mp + a * (abs(v2 - m_mp) - d_mp)
                        ind["micro_shift_bps_vel_z_ema"] = float((v2 - m_mp) / max(float(eps), d_mp))
                        stats.n_vel_ok += 1
                elif store_debug_flags:
                    ind["fgh_bad_time"] = 1.0

            prev_ts = int(ts_ms)
            prev_ofi_wsum = float(cur_ofi_wsum) if math.isfinite(cur_ofi_wsum) else prev_ofi_wsum
            prev_micro_shift = float(cur_micro_shift) if math.isfinite(cur_micro_shift) else prev_micro_shift

    # -----------------------------
    # F) leader-relative
    # -----------------------------
    if not leader_ts:
        stats.n_rel_missing_leader = len(rows)
        if store_debug_flags:
            for r in rows:
                ind = r.get("indicators") if isinstance(r.get("indicators"), dict) else {}
                ind["fgh_no_leader"] = 1.0
    else:
        max_lag = max(0, int(leader_max_lag_ms))
        for r in rows:
            ind = r.get("indicators") if isinstance(r.get("indicators"), dict) else {}
            sym = str(r.get("symbol") or ind.get("symbol") or "").upper()
            try:
                ts_ms = int(r.get("ts_ms") or ind.get("ts_ms") or 0)
            except Exception:
                ts_ms = 0
            if not sym or ts_ms <= 0:
                continue

            j = bisect.bisect_left(leader_ts, ts_ms)
            cand: List[int] = []
            if 0 <= j < len(leader_ts):
                cand.append(j)
            if j - 1 >= 0:
                cand.append(j - 1)
            best_i: Optional[int] = None
            best_lag: Optional[int] = None
            for i in cand:
                lag = abs(int(leader_ts[i]) - int(ts_ms))
                if best_lag is None or lag < best_lag:
                    best_lag = lag
                    best_i = i

            if best_i is None or best_lag is None:
                continue
            if best_lag > max_lag:
                stats.n_rel_lagged += 1
                if store_debug_flags:
                    ind["fgh_leader_lagged"] = 1.0
                continue

            lind = leader_ind[int(best_i)]

            # rel OFI
            k1, v1 = _pick(ind, ["ofi_ml_norm"])
            k2, v2 = _pick(lind, ["ofi_ml_norm"])
            if k1 and k2 and math.isfinite(v1) and math.isfinite(v2):
                ind["rel_ofi_ml_norm_btc"] = float(v1 - v2)

            # rel micro shift
            k1, v1 = _pick(ind, ["lob_micro_shift_bps", "mp_shift_bps", "mp_shift"])
            k2, v2 = _pick(lind, ["lob_micro_shift_bps", "mp_shift_bps", "mp_shift"])
            if k1 and k2 and math.isfinite(v1) and math.isfinite(v2):
                ind["rel_lob_micro_shift_bps_btc"] = float(v1 - v2)

            stats.n_rel_ok += 1

    # -----------------------------
    # G) replenishment imbalance
    # -----------------------------
    tb_keys = ["hawkes_taker_buy_lam", "lambda_trade_buy", "taker_buy_rate_ema", "taker_buy_rate"]
    ts_keys = ["hawkes_taker_sell_lam", "lambda_trade_sell", "taker_sell_rate_ema", "taker_sell_rate"]
    la_ask_keys = [
        "hawkes_limit_add_ask_lam"
        "lambda_limit_add_ask"
        "limit_add_ask_rate_ema"
        "added_ask_rate_ema"
        "l2_added_ask_rate_ema"
    ]
    la_bid_keys = [
        "hawkes_limit_add_bid_lam"
        "lambda_limit_add_bid"
        "limit_add_bid_rate_ema"
        "added_bid_rate_ema"
        "l2_added_bid_rate_ema"
    ]

    for r in rows:
        ind = r.get("indicators") if isinstance(r.get("indicators"), dict) else {}
        _, tb = _pick(ind, tb_keys)
        _, ts_ = _pick(ind, ts_keys)
        _, laa = _pick(ind, la_ask_keys)
        _, lab = _pick(ind, la_bid_keys)

        ok = False
        if math.isfinite(tb) and math.isfinite(laa) and (tb > 0.0 or laa > 0.0):
            ask_imb = (tb - laa) / max(float(eps), (tb + laa + float(eps)))
            ind["ask_replenish_imb"] = float(ask_imb)
            ind["replenish_ratio_ask"] = float(laa / max(float(eps), (tb + float(eps))))
            ok = True
        if math.isfinite(ts_) and math.isfinite(lab) and (ts_ > 0.0 or lab > 0.0):
            bid_imb = (ts_ - lab) / max(float(eps), (ts_ + lab + float(eps)))
            ind["bid_replenish_imb"] = float(bid_imb)
            ind["replenish_ratio_bid"] = float(lab / max(float(eps), (ts_ + float(eps))))
            ok = True

        if "ask_replenish_imb" in ind and "bid_replenish_imb" in ind:
            ind["lob_replenishment_pressure"] = float(ind.get("ask_replenish_imb", 0.0)) - float(ind.get("bid_replenish_imb", 0.0))
            ind["replenish_ratio_diff"] = float(ind.get("replenish_ratio_ask", 0.0)) - float(ind.get("replenish_ratio_bid", 0.0))
            ok = True

        if ok:
            stats.n_replen_ok += 1
        elif store_debug_flags:
            ind["fgh_replen_missing"] = 1.0

    return {"ok": True, "stats": stats.to_dict(), "leader": leader, "leader_max_lag_ms": int(leader_max_lag_ms)}
