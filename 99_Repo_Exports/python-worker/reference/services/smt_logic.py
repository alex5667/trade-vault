from __future__ import annotations

import math
from collections import deque
from dataclasses import dataclass
from typing import Any

from core.smt_symbol_snapshot import SMTDiv, SymbolSnapshot, detect_smt_divergence


def _clip01(x: float) -> float:
    if not math.isfinite(x):
        return 0.0
    return 0.0 if x <= 0 else (1.0 if x >= 1.0 else float(x))


def _safe_float(x: Any, d: float = 0.0) -> float:
    try:
        v = float(x)
        return v if math.isfinite(v) else d
    except Exception:
        return d


    sd = math.sqrt(var) if var > 1e-12 else 0.0
    if sd <= 1e-12:
        return 0.0
    return (x - mu) / sd


@dataclass
class LeaderStatus:
    leader: str
    coh: float
    confirmed: bool
    rejected: bool
    confirm_reason: str
    reject_reason: str
    trend_dir: str   # "UP"|"DOWN"|"NONE"


def leader_confirm_reject(leader: SymbolSnapshot, cfg: dict) -> tuple[bool, bool, float, float, str, str]:
    """
    LeaderConfirm = closeCross ∧ of_strong ∧ ¬reclaimOpp
    LeaderReject  = sweep ∧ reclaim ∧ (weak_progress OR div_regular_against_sweep)

    reclaimOpp: if reclaim direction is opposite to leader of_dir / trend_dir.
    """
    trend_dir = str(leader.trend_dir or "NONE").upper()
    of_dir = str(leader.of_dir or "NONE").upper()

    # closeCross is proxy BOS fact
    close_cross = bool(leader.close_cross == 1)
    of_strong = bool(leader.of_strong == 1)

    # reclaimOpp: reclaim exists AND reclaim_dir != expected direction (expected = of_dir if present else trend_dir)
    expected = of_dir if of_dir in ("LONG", "SHORT") else ("LONG" if trend_dir == "UP" else ("SHORT" if trend_dir == "DOWN" else "NONE"))
    reclaim_opp = bool(leader.reclaim == 1 and leader.reclaim_dir in ("LONG","SHORT") and expected in ("LONG","SHORT") and leader.reclaim_dir != expected)

    confirm = bool(close_cross and of_strong and (not reclaim_opp))

    # Reject: sweep+reclaim plus weakness evidence
    weak = bool(leader.weak_progress == 1)
    div = str(leader.div_kind or "none")
    # if sweep_dir is SHORT (EQH sweep) => bearish reversal evidence, else bullish
    sweep_dir = str(leader.sweep_dir or "NONE").upper()
    div_ok = False
    if sweep_dir == "SHORT":
        div_ok = div.startswith("bearish_regular") or div.startswith("bearish")
    if sweep_dir == "LONG":
        div_ok = div.startswith("bullish_regular") or div.startswith("bullish")

    reject = bool(leader.sweep == 1 and leader.reclaim == 1 and (weak or div_ok))

    # ---------------------------
    # confScore (strength)
    # confScore = 0.6*ΔZ_eff + 0.4*zoneDistanceScore
    # We use robust fallbacks:
    #  - ΔZ_eff: prefer leader.delta_eff_norm if present, else abs(delta_z)/3
    #  - zoneDistanceScore: prefer leader.zone_dist_bp if present, else 0.5 (unknown)
    # ---------------------------
    dz = _safe_float(getattr(leader, "delta_z", 0.0), 0.0)
    delta_eff_norm = _safe_float(getattr(leader, "delta_eff_norm", 0.0), 0.0)
    if delta_eff_norm <= 0:
        delta_eff_norm = _clip01(abs(dz) / 3.0)
    zone_dist_bp = _safe_float(getattr(leader, "zone_dist_bp", 0.0), 0.0)
    zone_max_bp = _safe_float(cfg.get("smt_zone_max_bp", 15.0), 15.0)
    if zone_dist_bp > 0 and zone_max_bp > 0:
        zone_score = 1.0 - min(1.0, zone_dist_bp / zone_max_bp)
    else:
        zone_score = 0.5
    conf_score = 0.6 * _clip01(delta_eff_norm) + 0.4 * _clip01(zone_score)

    # rejectScore (minimal): stronger if weak_progress and div aligns with sweep
    rej_score = 0.0
    if reject:
        rej_score = 0.6 * (1.0 if weak else 0.0) + 0.4 * (1.0 if div_ok else 0.0)

    return confirm, reject, float(conf_score), float(rej_score), ("closeCross+ofStrong" if confirm else ""), ("sweep+reclaim+weak/div" if reject else "")


@dataclass
class SMTDiv:
    kind: str            # "bullish_smt"|"bearish_smt"
    leader: str
    satellite: str
    ts_ms: int


def detect_smt_divergence(leader: SymbolSnapshot, sat: SymbolSnapshot) -> SMTDiv | None:
    """
    SMT based on swing lows/highs (last two swings):
      bullish SMT: leader makes LL (low0 < low1), satellite makes HL (low0 > low1)
      bearish SMT: leader makes HH (high0 > high1), satellite makes LH (high0 < high1)
    """
    # Need at least two swings of each relevant type
    if leader.swing_low_1 > 0 and sat.swing_low_1 > 0:
        leader_ll = leader.swing_low_0 > 0 and leader.swing_low_0 < leader.swing_low_1
        sat_hl = sat.swing_low_0 > 0 and sat.swing_low_0 > sat.swing_low_1
        if leader_ll and sat_hl:
            return SMTDiv(kind="bullish_smt", leader=leader.symbol, satellite=sat.symbol, ts_ms=max(leader.ts_ms, sat.ts_ms))

    if leader.swing_high_1 > 0 and sat.swing_high_1 > 0:
        leader_hh = leader.swing_high_0 > 0 and leader.swing_high_0 > leader.swing_high_1
        sat_lh = sat.swing_high_0 > 0 and sat.swing_high_0 < sat.swing_high_1
        if leader_hh and sat_lh:
            return SMTDiv(kind="bearish_smt", leader=leader.symbol, satellite=sat.symbol, ts_ms=max(leader.ts_ms, sat.ts_ms))

    return None


@dataclass
class Ranked:
    symbol: str
    rank: float
    rs_z: float
    cvd_z: float
    pb: float


def _zscore(vals: list[float], x: float) -> float:
    if not vals:
        return 0.0
    mu = sum(vals) / len(vals)
    var = sum((v - mu) * (v - mu) for v in vals) / max(1, len(vals) - 1)
    sd = math.sqrt(var) if var > 1e-12 else 0.0
    if sd <= 1e-12:
        return 0.0
    return (x - mu) / sd


_rsi_hist: dict[str, deque[float]] = {}
_cvd_hist: dict[str, deque[float]] = {}


def _z_ts(hist: deque[float], x: float) -> float:
    xs = list(hist)
    if len(xs) < 30:
        return 0.0
    mu = sum(xs) / float(len(xs))
    var = sum((v - mu) * (v - mu) for v in xs) / float(max(1, len(xs) - 1))
    sd = math.sqrt(var) if var > 1e-12 else 0.0
    if sd <= 0:
        return 0.0
    return float((x - mu) / sd)


def rank_satellites(snaps: list[SymbolSnapshot], leader_symbol: str, trend_dir: str, cfg: dict) -> list[Ranked]:
    """
    rank = 0.4*RS_z + 0.4*CVD_z + 0.2*(-PullbackDepth)
    PullbackDepth: retrace_atr (smaller is better for continuation).
    
    Ranking modes:
      - cross-sectional (default): zscore within satellites at this moment
      - time-series: zscore per symbol vs its own rolling window
    """
    sats = [s for s in snaps if s.symbol != leader_symbol]
    mode = (cfg.get("smt_rank_mode", "ts") or "ts").lower()
    win = int(cfg.get("smt_rank_ts_window", 240))

    for s in sats:
        sym = str(s.symbol)
        _rsi_hist.setdefault(sym, deque(maxlen=max(60, win))).append(float(getattr(s, "rsi14", 0.0) or 0.0))
        _cvd_hist.setdefault(sym, deque(maxlen=max(60, win))).append(float(getattr(s, "cvd_slope", 0.0) or 0.0))

    rsi_all = [float(getattr(s, "rsi14", 0.0) or 0.0) for s in sats]
    cvd_all = [float(getattr(s, "cvd_slope", 0.0) or 0.0) for s in sats]

    out: list[Ranked] = []
    for s in sats:
        sym = str(s.symbol)
        rsi_v = float(getattr(s, "rsi14", 0.0) or 0.0)
        cvd_v = float(getattr(s, "cvd_slope", 0.0) or 0.0)

        if mode == "cross":
            rs_z = _zscore(rsi_all, rsi_v)
            cvd_z = _zscore(cvd_all, cvd_v)
        else:
            rs_z = _z_ts(_rsi_hist.get(sym) or deque(), rsi_v)
            cvd_z = _z_ts(_cvd_hist.get(sym) or deque(), cvd_v)

        pb = float(s.retrace_atr)
        rank = 0.4 * rs_z + 0.4 * cvd_z + 0.2 * (-pb)
        out.append(Ranked(symbol=s.symbol, rank=rank, rs_z=rs_z, cvd_z=cvd_z, pb=pb))
    out.sort(key=lambda x: x.rank, reverse=True)
    return out


@dataclass
class SMTDecision:
    kind: str  # "continuation"|"reversal"|"none"
    leader: str
    coh: float
    trend_dir: str
    pick: str | None
    div: str | None
    reason: str
    conf_score: float = 0.0
    reject_score: float = 0.0
    news_blocked: int = 0
    news_until_ts_ms: int = 0
    risk_factor_bps: int = 10000  # risk multiplier 0..10000


def decide_smt(
    leader: SymbolSnapshot,
    snaps: list[SymbolSnapshot],
    coh: float,
    cfg: dict,
) -> SMTDecision:
    """
    If leader confirmed and coh >= thr => continuation:
      pick top satellite by rank
    Else if leader rejected and SMT divergence exists => reversal:
      bullish SMT -> buy strongest satellite
      bearish SMT -> short weakest satellite (bottom rank)
    """
    thr = float(cfg.get("smt_coh_threshold", 0.65))
    confirm, reject, conf_score, rej_score, creason, rreason = leader_confirm_reject(leader, cfg)
    trend_dir = str(leader.trend_dir or "NONE").upper()
    ranked = rank_satellites(snaps, leader.symbol, trend_dir=trend_dir, cfg=cfg)

    # News gate (bundle-level). Upstream passes cfg["news_blocked"] from aggregator.
    risk_factor_bps = int(cfg.get("risk_factor_bps", 10000))
    if int(cfg.get("news_blocked", 0) or 0) == 1:
        return SMTDecision(
            kind="none",
            leader=leader.symbol,
            coh=coh,
            trend_dir=trend_dir,
            pick=None,
            div=None,
            reason=(cfg.get("news_reason", "news_gate")),
            conf_score=float(conf_score),
            reject_score=float(rej_score),
            news_blocked=1,
            news_until_ts_ms=int(cfg.get("news_until_ts_ms", 0) or 0),
            risk_factor_bps=risk_factor_bps,
        )

    conf_min = float(cfg.get("smt_leader_conf_min_score", 0.65))
    if confirm and coh >= thr and ranked:
        if conf_score < conf_min:
            return SMTDecision(
                kind="none",
                leader=leader.symbol,
                coh=coh,
                trend_dir=trend_dir,
                pick=None,
                div=None,
                reason="confirm_but_weak",
                conf_score=float(conf_score),
                reject_score=float(rej_score),
                risk_factor_bps=risk_factor_bps,
            )
        return SMTDecision(
            kind="continuation",
            leader=leader.symbol,
            coh=coh,
            trend_dir=trend_dir,
            pick=ranked[0].symbol,
            div=None,
            reason="leader_confirm+coh",
            conf_score=float(conf_score),
            reject_score=float(rej_score),
            risk_factor_bps=risk_factor_bps,
        )

    # reversal path
    if reject and ranked:
        # basket SMT: require K satellites confirming same divergence kind
        k = int(cfg.get("smt_basket_k", 2))
        if k < 1:
            k = 1
        divs: list[SMTDiv] = []
        for s in snaps:
            if s.symbol == leader.symbol:
                continue
            dv = detect_smt_divergence(leader, s)
            if dv is not None:
                divs.append(dv)
        bull_n = sum(1 for d in divs if d.kind == "bullish_smt")
        bear_n = sum(1 for d in divs if d.kind == "bearish_smt")
        best_kind: str | None = None
        if bull_n >= k:
            best_kind = "bullish_smt"
        if bear_n >= k and (best_kind is None):
            best_kind = "bearish_smt"
        if best_kind is not None:
            pick = ranked[0].symbol if best_kind == "bullish_smt" else ranked[-1].symbol
            return SMTDecision(
                kind="reversal",
                leader=leader.symbol,
                coh=coh,
                trend_dir=trend_dir,
                pick=pick,
                div=best_kind,
                reason=f"leader_reject+basket_smt_k{int(k)}",
                conf_score=float(conf_score),
                reject_score=float(rej_score),
                risk_factor_bps=risk_factor_bps,
            )

    return SMTDecision(
        kind="none",
        leader=leader.symbol,
        coh=coh,
        trend_dir=trend_dir,
        pick=None,
        div=None,
        reason="no_setup",
        conf_score=float(conf_score),
        reject_score=float(rej_score),
        risk_factor_bps=risk_factor_bps,
    )
