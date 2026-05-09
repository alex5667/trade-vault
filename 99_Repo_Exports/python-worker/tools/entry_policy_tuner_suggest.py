from __future__ import annotations

import json
import os
from dataclasses import dataclass
from typing import Any

from utils.time_utils import get_ny_time_millis


def _now_ms() -> int:
    return get_ny_time_millis()


def _f(x: Any, d: float = 0.0) -> float:
    try:
        return float(x)
    except Exception:
        return d


def _i(x: Any, d: int = 0) -> int:
    try:
        return int(x)
    except Exception:
        return d


def _s(x: Any, d: str = "") -> str:
    try:
        return str(x) if x is not None else d
    except Exception:
        return d


def _clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))


def _quantile(xs: list[float], q: float) -> float:
    if not xs:
        return 0.0
    ys = sorted(float(x) for x in xs)
    n = len(ys)
    if n == 1:
        return ys[0]
    q = _clamp(float(q), 0.0, 1.0)
    # simple linear interpolation
    pos = (n - 1) * q
    i0 = int(pos)
    i1 = min(n - 1, i0 + 1)
    frac = pos - i0
    return ys[i0] * (1.0 - frac) + ys[i1] * frac


@dataclass
class TunerCfg:
    enable: bool = True
    tighten_only: bool = True
    min_total: int = 50
    min_allow: int = 10
    # quantiles
    q_coh: float = 0.20
    q_leader_conf: float = 0.20
    q_zone_bp: float = 0.90
    q_zone_bp_thin: float = 0.80
    q_obi_thin: float = 0.60
    # step limits (per day)
    step_zone_bp: float = 3.0
    step_zone_bp_thin: float = 3.0
    step_coh: float = 0.03
    step_leader_conf: float = 0.03
    step_obi: float = 0.30
    # hard bounds
    coh_bounds: tuple[float, float] = (0.55, 0.90)
    leader_conf_bounds: tuple[float, float] = (0.55, 0.90)
    zone_bp_bounds: tuple[float, float] = (6.0, 30.0)
    zone_bp_thin_bounds: tuple[float, float] = (6.0, 25.0)
    obi_bounds: tuple[float, float] = (0.5, 5.0)

    @staticmethod
    def from_env() -> TunerCfg:
        return TunerCfg(
            enable=bool(int(os.getenv("EP_TUNER_ENABLE", "1"))),
            tighten_only=bool(int(os.getenv("EP_TUNER_TIGHTEN_ONLY", "1"))),
            min_total=int(os.getenv("EP_TUNER_MIN_TOTAL", "50")),
            min_allow=int(os.getenv("EP_TUNER_MIN_ALLOW", "10")),
            q_coh=float(os.getenv("EP_TUNER_Q_COH", "0.20")),
            q_leader_conf=float(os.getenv("EP_TUNER_Q_LEADER_CONF", "0.20")),
            q_zone_bp=float(os.getenv("EP_TUNER_Q_ZONE_BP", "0.90")),
            q_zone_bp_thin=float(os.getenv("EP_TUNER_Q_ZONE_BP_THIN", "0.80")),
            q_obi_thin=float(os.getenv("EP_TUNER_Q_OBI_THIN", "0.60")),
            step_zone_bp=float(os.getenv("EP_TUNER_STEP_ZONE_BP", "3.0")),
            step_zone_bp_thin=float(os.getenv("EP_TUNER_STEP_ZONE_BP_THIN", "3.0")),
            step_coh=float(os.getenv("EP_TUNER_STEP_COH", "0.03")),
            step_leader_conf=float(os.getenv("EP_TUNER_STEP_LEADER_CONF", "0.03")),
            step_obi=float(os.getenv("EP_TUNER_STEP_OBI", "0.30")),
        )


def _read_current_env() -> dict[str, float]:
    return {
        "SMT_COH_THRESHOLD": float(os.getenv("SMT_COH_THRESHOLD", "0.65")),
        "SMT_LEADER_CONF_MIN_SCORE": float(os.getenv("SMT_LEADER_CONF_MIN_SCORE", "0.65")),
        "SMT_ENTRY_MAX_ZONE_BP": float(os.getenv("SMT_ENTRY_MAX_ZONE_BP", "15")),
        "SMT_ENTRY_MAX_ZONE_BP_THIN": float(os.getenv("SMT_ENTRY_MAX_ZONE_BP_THIN", "10")),
        "SMT_ENTRY_OBI_MIN_SEC": float(os.getenv("SMT_ENTRY_OBI_MIN_SEC", "1.5")),
    }


def _apply_step(current: float, target: float, step: float, tighten_only: bool, direction: str) -> float:
    """
    direction:
      - "up": higher value means stricter (coh/leader_conf/obi)
      - "down": lower value means stricter (zone_bp)
    """
    cur = float(current)
    tgt = float(target)
    if direction == "up":
        if tighten_only and tgt < cur:
            return cur
        d = tgt - cur
        if d > step:
            tgt = cur + step
        if d < -step:
            tgt = cur - step
        return tgt
    # direction == "down"
    if tighten_only and tgt > cur:
        return cur
    d = cur - tgt  # positive means tightening
    if d > step:
        tgt = cur - step
    if d < -step:
        tgt = cur + step
    return tgt


def suggest_from_records(
    *,
    records: list[dict[str, Any]],
    tuner: TunerCfg | None = None,
    current_env: dict[str, float] | None = None,
) -> dict[str, Any]:
    """
    records: output of entry_policy_daily_job capture_and_replay (or replay ndjson list)
    Must include fields:
      ok, regime, zone_dist_bp, obi_stable_sec, iceberg_strict, coh, leader_conf_score
    """
    tcfg = tuner or TunerCfg.from_env()
    cur = current_env or _read_current_env()

    out: dict[str, Any] = {
        "ts_ms": _now_ms(),
        "enable": int(tcfg.enable),
        "safe_to_apply": 0,
        "current": cur,
        "proposed": {},
        "changes": [],
        "rationales": [],
        "stats": {},
    }
    if not tcfg.enable:
        out["rationales"].append("tuner disabled (EP_TUNER_ENABLE=0)")
        return out

    total = len(records)
    allow = [r for r in records if int(r.get("ok", 0) or 0) == 1]
    if total < int(tcfg.min_total):
        out["rationales"].append(f"insufficient total samples: total={total} &lt; min_total={tcfg.min_total}")
        out["stats"]["total"] = total
        out["stats"]["allow"] = len(allow)
        return out
    if len(allow) < int(tcfg.min_allow):
        out["rationales"].append(f"insufficient allow samples: allow={len(allow)} < min_allow={tcfg.min_allow}")
        out["stats"]["total"] = total
        out["stats"]["allow"] = len(allow)
        return out

    # Split regimes
    def _is_thin(rg: str) -> bool:
        rg0 = (rg or "na").lower()
        return rg0 in ("thin", "news", "illiquid")

    allow_thin = [r for r in allow if _is_thin((r.get("regime", "na")))]
    allow_norm = [r for r in allow if not _is_thin((r.get("regime", "na")))]

    # Distributions
    coh_vals = [_f(r.get("coh", 0.0), 0.0) for r in allow if _f(r.get("coh", 0.0), 0.0) > 0]
    lcs_vals = [_f(r.get("leader_conf_score", 0.0), 0.0) for r in allow if _f(r.get("leader_conf_score", 0.0), 0.0) > 0]
    zone_norm = [_f(r.get("zone_dist_bp", 0.0), 0.0) for r in allow_norm if _f(r.get("zone_dist_bp", 0.0), 0.0) > 0]
    zone_thin = [_f(r.get("zone_dist_bp", 0.0), 0.0) for r in allow_thin if _f(r.get("zone_dist_bp", 0.0), 0.0) > 0]

    obi_thin = [
        _f(r.get("obi_stable_sec", 0.0), 0.0)
        for r in allow_thin
        if int(r.get("iceberg_strict", 0) or 0) == 0
    ]

    # Targets (tighten by cutting tail)
    # coh/leader_conf: raise to q20 of allowed (cuts weakest confirmations)
    tgt_coh = _quantile(coh_vals, tcfg.q_coh) if coh_vals else cur["SMT_COH_THRESHOLD"]
    tgt_lcs = _quantile(lcs_vals, tcfg.q_leader_conf) if lcs_vals else cur["SMT_LEADER_CONF_MIN_SCORE"]

    # zone bp: tighten by setting to q90 (normal) and q80 (thin), + small buffer, capped
    tgt_zone = (_quantile(zone_norm, tcfg.q_zone_bp) + 1.0) if zone_norm else cur["SMT_ENTRY_MAX_ZONE_BP"]
    tgt_zone_thin = (_quantile(zone_thin, tcfg.q_zone_bp_thin) + 1.0) if zone_thin else cur["SMT_ENTRY_MAX_ZONE_BP_THIN"]

    # obi_min_sec: in thin, raise to q60 among non-iceberg allows, capped
    tgt_obi = _quantile(obi_thin, tcfg.q_obi_thin) if obi_thin else cur["SMT_ENTRY_OBI_MIN_SEC"]

    # Apply step-limits + tighten direction
    new_coh = _apply_step(cur["SMT_COH_THRESHOLD"], tgt_coh, tcfg.step_coh, tcfg.tighten_only, "up")
    new_lcs = _apply_step(cur["SMT_LEADER_CONF_MIN_SCORE"], tgt_lcs, tcfg.step_leader_conf, tcfg.tighten_only, "up")
    new_zone = _apply_step(cur["SMT_ENTRY_MAX_ZONE_BP"], tgt_zone, tcfg.step_zone_bp, tcfg.tighten_only, "down")
    new_zone_thin = _apply_step(cur["SMT_ENTRY_MAX_ZONE_BP_THIN"], tgt_zone_thin, tcfg.step_zone_bp_thin, tcfg.tighten_only, "down")
    new_obi = _apply_step(cur["SMT_ENTRY_OBI_MIN_SEC"], tgt_obi, tcfg.step_obi, tcfg.tighten_only, "up")

    # Clamp to bounds
    new_coh = _clamp(new_coh, *tcfg.coh_bounds)
    new_lcs = _clamp(new_lcs, *tcfg.leader_conf_bounds)
    new_zone = _clamp(new_zone, *tcfg.zone_bp_bounds)
    new_zone_thin = _clamp(new_zone_thin, *tcfg.zone_bp_thin_bounds)
    new_obi = _clamp(new_obi, *tcfg.obi_bounds)

    # Build changes list
    def _chg(k: str, curv: float, newv: float, why: str) -> None:
        if abs(float(newv) - float(curv)) < 1e-9:
            return
        out["proposed"][k] = float(newv)
        out["changes"].append({"key": k, "from": float(curv), "to": float(newv), "why": why})

    _chg("SMT_COH_THRESHOLD", cur["SMT_COH_THRESHOLD"], new_coh, f"raise to q{int(tcfg.q_coh*100)} of allowed coh (+step cap)")
    _chg("SMT_LEADER_CONF_MIN_SCORE", cur["SMT_LEADER_CONF_MIN_SCORE"], new_lcs, f"raise to q{int(tcfg.q_leader_conf*100)} of allowed leader_conf (+step cap)")
    _chg("SMT_ENTRY_MAX_ZONE_BP", cur["SMT_ENTRY_MAX_ZONE_BP"], new_zone, f"tighten to q{int(tcfg.q_zone_bp*100)} of allowed zone_dist_bp +1bp (+step cap)")
    _chg("SMT_ENTRY_MAX_ZONE_BP_THIN", cur["SMT_ENTRY_MAX_ZONE_BP_THIN"], new_zone_thin, f"tighten thin to q{int(tcfg.q_zone_bp_thin*100)} +1bp (+step cap)")
    _chg("SMT_ENTRY_OBI_MIN_SEC", cur["SMT_ENTRY_OBI_MIN_SEC"], new_obi, f"raise thin OBI min to q{int(tcfg.q_obi_thin*100)} among thin allows without iceberg (+step cap)")

    out["stats"] = {
        "total": total,
        "allow": len(allow),
        "allow_thin": len(allow_thin),
        "allow_norm": len(allow_norm),
        "q_coh_val": float(tgt_coh),
        "q_leader_conf_val": float(tgt_lcs),
        "q_zone_norm_val": float(tgt_zone),
        "q_zone_thin_val": float(tgt_zone_thin),
        "q_obi_thin_val": float(tgt_obi),
    }

    if out["changes"]:
        out["safe_to_apply"] = 1
        out["rationales"].append("changes proposed with step caps + tighten-only guardrails")
    else:
        out["rationales"].append("no changes suggested (already at/stricter than targets or insufficient distributions)")
    return out


def main() -> None:
    # Offline mode: read replay ndjson produced by daily job
    inp = os.getenv("IN", "")
    if not inp:
        raise SystemExit("IN=... ndjson required")
    records: list[dict[str, Any]] = []
    with open(inp, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            records.append(json.loads(line))
    sugg = suggest_from_records(records=records)
    print(json.dumps(sugg, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
