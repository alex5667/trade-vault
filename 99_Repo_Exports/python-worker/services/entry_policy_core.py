from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Tuple


def _i(x: Any, d: int = 0) -> int:
    try:
        return int(x)
    except Exception:
        return d


def _f(x: Any, d: float = 0.0) -> float:
    try:
        return float(x)
    except Exception:
        return d


def _s(x: Any, d: str = "") -> str:
    try:
        return str(x) if x is not None else d
    except Exception:
        return d


def _b(x: Any) -> bool:
    try:
        return int(x) == 1
    except Exception:
        return False


@dataclass
class EntryPolicyCfg:
    coh_thr: float = 0.65
    leader_conf_min: float = 0.65
    min_of_score: float = 1.0
    max_zone_bp: float = 15.0
    max_zone_bp_thin: float = 10.0
    obi_min_sec: float = 1.5
    dedup_ms: int = 60_000
    allow_zone_id_change_if_near: bool = False


@dataclass
class EntryPolicyDecision:
    ok: bool
    reason_code: str
    notes: str = ""
    emit: bool = False


def desired_side_from_candidate(cand: Dict[str, Any]) -> str:
    return _s(cand.get("side", "NONE"), "NONE").upper()


def bundle_ok(bundle: Dict[str, Any], symbol: str, cfg: EntryPolicyCfg) -> Tuple[bool, str]:
    if not bundle:
        return False, "no_bundle_state"
    decision = _s(bundle.get("decision", "none")).lower()
    pick = _s(bundle.get("pick", "")).upper()
    if decision not in ("continuation", "reversal"):
        return False, f"decision={decision}"
    if pick and pick != symbol.upper():
        return False, f"pick_mismatch pick={pick}"
    # News gate (if aggregator provides)
    if _b(bundle.get("news_blocked", 0)):
        until = _i(bundle.get("news_until_ts_ms", 0), 0)
        return False, f"news_blocked until={until}"
    coh = _f(bundle.get("coh", 0.0), 0.0)
    if coh < float(cfg.coh_thr):
        return False, f"coh={coh:.3f}<thr"
    lcs = _f(bundle.get("leader_conf_score", 0.0), 0.0)
    if lcs < float(cfg.leader_conf_min):
        return False, f"leader_conf_score={lcs:.3f}<thr"
    return True, "bundle_ok"


def need_extra_confirm(snap: Dict[str, Any]) -> bool:
    regime = _s(snap.get("regime", "na")).lower()
    unstable = _i(snap.get("abs_lvl_th_unstable", 0), 0)
    return bool(regime in ("thin", "news", "illiquid") or unstable == 1)


def extra_confirm_ok(snap: Dict[str, Any], cfg: EntryPolicyCfg) -> bool:
    obi = _f(snap.get("obi_stable_sec", 0.0), 0.0)
    ice = _i(snap.get("iceberg_strict", 0), 0)
    return bool(obi >= float(cfg.obi_min_sec) or ice == 1)


def zone_bp_thr(snap: Dict[str, Any], cfg: EntryPolicyCfg) -> float:
    if need_extra_confirm(snap):
        return float(cfg.max_zone_bp_thin)
    return float(cfg.max_zone_bp)


def zone_side_ok(snap: Dict[str, Any], side: str) -> bool:
    zs = _s(snap.get("zone_side", "NA")).upper()
    if zs in ("MID", "NA", ""):
        return True
    if side == "LONG":
        return zs == "SUP"
    if side == "SHORT":
        return zs == "RES"
    return True


def of_ok(snap: Dict[str, Any], side: str, cfg: EntryPolicyCfg) -> Tuple[bool, str]:
    if not _b(snap.get("of_strong", 0)):
        return False, "of_strong=0"
    of_score = _f(snap.get("of_confirm_score", 0.0), 0.0)
    if of_score < float(cfg.min_of_score):
        return False, f"of_score={of_score:.3f}<min"
    of_dir = _s(snap.get("of_dir", "NONE")).upper()
    if side in ("LONG", "SHORT") and of_dir in ("LONG", "SHORT") and of_dir != side:
        return False, f"of_dir={of_dir}!=side={side}"
    return True, "of_ok"


def zone_ok(cand_zone_id: str, snap: Dict[str, Any], cfg: EntryPolicyCfg) -> Tuple[bool, str]:
    zid = _s(snap.get("zone_id", ""))
    if cand_zone_id and zid and cand_zone_id != zid and (not cfg.allow_zone_id_change_if_near):
        return False, "zone_id_changed"
    dist_bp = _f(snap.get("zone_dist_bp", 0.0), 0.0)
    thr = zone_bp_thr(snap, cfg)
    # inside band may yield dist 0; rely on zone_ok flag
    if dist_bp <= 0 and _b(snap.get("zone_ok", 0)):
        return True, "inside_zone"
    if dist_bp > 0 and dist_bp <= thr:
        return True, f"dist_bp<=thr({thr})"
    return False, f"dist_bp={dist_bp:.2f}>thr({thr})"


def dedup_key(symbol: str, zone_id: str, side: str) -> str:
    return f"{symbol}:{zone_id}:{side}"


def dedup_ok(now_ms: int, *, symbol: str, zone_id: str, side: str, cfg: EntryPolicyCfg, state: Dict[str, int]) -> bool:
    k = dedup_key(symbol, zone_id, side)
    last = int(state.get(k, 0) or 0)
    if last > 0 and (now_ms - last) < int(cfg.dedup_ms):
        return False
    state[k] = now_ms
    return True


def evaluate_entry_policy(
    *,
    now_ms: int,
    cand: Dict[str, Any],
    snap: Dict[str, Any],
    bundle: Dict[str, Any],
    cfg: EntryPolicyCfg,
    dedup_state: Dict[str, int],
) -> EntryPolicyDecision:
    """
    Pure decision engine for EntryPolicyService and replay/golden.
    Returns ok + reason_code + notes. "emit" implies trade-entry should be produced.
    """
    symbol = _s(cand.get("symbol", "")).upper()
    side = _s(cand.get("side", "NONE")).upper()
    cand_zone_id = _s(cand.get("zone_id", ""))

    # --- Adverse selection gate (expected slippage model) ---
    try:
        exp_slip = float(snap.get("expected_slippage_bps", snap.get("micro", {}).get("expected_slippage_bps", 0.0)) or 0.0)
        max_slip = float(getattr(cfg, "max_expected_slippage_bps", 0.0) or 0.0)
        if max_slip > 0 and exp_slip > 0 and exp_slip >= max_slip:
            return EntryPolicyDecision(ok=False, emit=False, reason_code="DENY_SLIPPAGE", notes=f"exp_slip_bps={exp_slip:.2f}")
    except Exception:
        pass

    # --- Execution health veto (P6) ---
    # This is fed by SMT EntryPolicyService via Redis TCA rollups.
    # If missing, it's fail-open.
    try:
        if int(snap.get("exec_health_veto", 0) or 0) == 1:
            return EntryPolicyDecision(ok=False, emit=False, reason_code="DENY_EXEC_HEALTH", notes=str(snap.get("exec_health_flags", "")))
    except Exception:
        pass

    # shadow-only hint: do not emit entry even if ok
    try:
        if int(snap.get("data_health_shadow_only", 0) or 0) == 1 or int(snap.get("slippage_shadow_only", 0) or 0) == 1:
            # still ok for audit, but never emit
            return EntryPolicyDecision(ok=True, emit=False, reason_code="ALLOW_SHADOW_QUALITY", notes="shadow_only")
    except Exception:
        pass

    ok_b, note_b = bundle_ok(bundle, symbol, cfg)
    if not ok_b:
        return EntryPolicyDecision(ok=False, reason_code="BUNDLE_FAIL", notes=note_b, emit=False)

    ok_z, note_z = zone_ok(cand_zone_id, snap, cfg)
    if not ok_z:
        return EntryPolicyDecision(ok=False, reason_code="ZONE_FAIL", notes=note_z, emit=False)

    if not zone_side_ok(snap, side):
        return EntryPolicyDecision(ok=False, reason_code="ZONE_SIDE_MISMATCH", notes="", emit=False)

    ok_of, note_of = of_ok(snap, side, cfg)
    if not ok_of:
        return EntryPolicyDecision(ok=False, reason_code="OF_FAIL", notes=note_of, emit=False)

    if need_extra_confirm(snap) and (not extra_confirm_ok(snap, cfg)):
        return EntryPolicyDecision(ok=False, reason_code="EXTRA_CONFIRM_FAIL", notes="need obi>=min or iceberg_strict", emit=False)

    # Dedup uses *resolved* zone_id from snap when available (more stable than candidate field).
    zid = _s(snap.get("zone_id", cand_zone_id))
    if not dedup_ok(now_ms, symbol=symbol, zone_id=zid, side=side, cfg=cfg, state=dedup_state):
        return EntryPolicyDecision(ok=False, reason_code="DEDUP", notes="", emit=False)

    return EntryPolicyDecision(ok=True, reason_code="ALLOW", notes="", emit=True)
