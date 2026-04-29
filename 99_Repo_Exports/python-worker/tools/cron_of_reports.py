# python-worker/tools/cron_of_reports.py
from __future__ import annotations
from utils.time_utils import get_ny_time_millis

import argparse
import json
import os
import secrets
import subprocess
import sys
import time
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, List, Optional, Tuple

import hashlib
import html


def _stable_hash_u32(s: str) -> int:
    h = hashlib.sha1(s.encode("utf-8")).digest()
    return int.from_bytes(h[:4], byteorder="big", signed=False)


def _pass_share(symbol: str, share: float) -> bool:
    if share <= 0:
        return False
    if share >= 1:
        return True
    v = _stable_hash_u32(symbol.upper()) / float(2**32 - 1)
    return v < share


def iter_ndjson(path: str) -> Iterator[Dict[str, Any]]:
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            s = line.strip()
            if not s:
                continue
            yield json.loads(s)


def write_ndjson(path: str, rows: Iterable[Dict[str, Any]]) -> int:
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    n = 0
    with open(path, "w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False, separators=(",", ":")))
            f.write("\n")
            n += 1
    return n


def filter_inputs(rows: Iterable[Dict[str, Any]], *, canary_symbols: Optional[List[str]], canary_share: float) -> Iterator[Dict[str, Any]]:
    allow = None
    if canary_symbols:
        allow = set([s.strip().upper() for s in canary_symbols if s and s.strip()])

    for r in rows:
        sym = str(r.get("symbol") or "").upper()
        if not sym:
            continue

        if allow is not None:
            if sym in allow:
                yield r
            continue

        if canary_share > 0:
            if _pass_share(sym, float(canary_share)):
                yield r
            continue

        yield r


def pick_baseline(baseline_dir: str, symbol: str) -> Optional[str]:
    d = Path(baseline_dir)
    if not d.exists():
        return None
    s = symbol.upper()
    p1 = d / f"baseline_{s}.ndjson"
    if p1.exists():
        return str(p1)
    p0 = d / "baseline.ndjson"
    if p0.exists():
        return str(p0)
    return None


def _get(r: Dict[str, Any], key: str) -> Any:
    if key in r:
        return r.get(key)
    ev = r.get("evidence")
    if isinstance(ev, dict) and key in ev:
        return ev.get(key)
    return None


def _envf(name: str, d: float) -> float:
    """Helper: read float from env with default."""
    try:
        return float(os.getenv(name, d) or d)
    except Exception:
        return float(d)


def _envi(name: str, d: int) -> int:
    """Helper: read int from env with default."""
    try:
        return int(os.getenv(name, d) or d)
    except Exception:
        return int(d)


def _envs(name: str, d: str = "") -> str:
    """Helper: read string from env with default."""
    try:
        v = os.getenv(name)
        return d if v is None else str(v)
    except Exception:
        return d


def _clamp(x: float, lo: float, hi: float) -> float:
    """Clamp value between lo and hi."""
    return max(lo, min(hi, x))


def _parse_csv(s: str) -> List[str]:
    """Parse comma-separated string into list of uppercase strings."""
    return [x.strip().upper() for x in (s or "").split(",") if x.strip()]


def row_key(r: Dict[str, Any]) -> str:
    sid = r.get("sid")
    if sid:
        return str(sid)
    ts = r.get("ts_ms") or r.get("tick_ts_ms") or 0
    return f"{r.get('symbol','')}|{ts}|{r.get('direction','')}"


FIELDS = ["ok", "ok_soft", "score", "have", "need", "scenario", "reason", "scenario_v4", "need_reason"]


def diff_report(baseline_path: str, candidate_path: str, *, symbol_filter: Optional[str] = None) -> Dict[str, Any]:
    base_map: Dict[str, Dict[str, Any]] = {}
    for r in iter_ndjson(baseline_path):
        if symbol_filter and str(r.get("symbol") or "").upper() != symbol_filter.upper():
            continue
        base_map[row_key(r)] = r

    mismatches = 0
    mismatch_by_field = Counter()
    mismatch_by_type = Counter()
    mismatch_by_scenario_v4 = Counter()
    mismatch_by_reason = Counter()
    n = 0

    for r in iter_ndjson(candidate_path):
        if symbol_filter and str(r.get("symbol") or "").upper() != symbol_filter.upper():
            continue
        k = row_key(r)
        b = base_map.get(k)
        if not b:
            continue  # only compare overlap to avoid noise on rolling datasets
        n += 1

        scn = str(_get(r, "scenario_v4") or "")
        mismatch_scn = scn or "na"

        for f in FIELDS:
            av = _get(b, f)
            bv = _get(r, f)
            if f == "score":
                # tolerate tiny float drift
                try:
                    if av is not None and bv is not None and abs(float(av) - float(bv)) < 1e-9:
                        continue
                except Exception:
                    pass
            if av != bv:
                mismatches += 1
                mismatch_by_field[f] += 1
                mismatch_by_type[f"{f}:{av}->{bv}"] += 1
                mismatch_by_scenario_v4[mismatch_scn] += 1
                mismatch_by_reason[f"{_get(b,'reason')}->{_get(r,'reason')}"] += 1

    return {
        "n": n,
        "mismatches": mismatches,
        "mismatch_by_field": dict(mismatch_by_field),
        "mismatch_by_type_top": mismatch_by_type.most_common(15),
        "mismatch_by_scenario_v4_top": mismatch_by_scenario_v4.most_common(10),
        "mismatch_by_reason_top": mismatch_by_reason.most_common(10),
    }


@dataclass
class ReplayStats:
    n: int
    ok_rate: Optional[float]
    ok_soft_rate: Optional[float]
    no_data: int
    by_scenario: Dict[str, Dict[str, Any]]
    exec_risk_norm_p50: float
    exec_risk_norm_p90: float
    vol_shock_cap_hit_rate: float
    saw_chop_hard_miss_rate: float
    top_missing_legs: List[Tuple[str, int]]


def _percentile(xs: List[float], p: float) -> float:
    if not xs:
        return 0.0
    xs = sorted(xs)
    if not xs: return 0.0
    i = int(round((len(xs) - 1) * p))
    i = max(0, min(len(xs) - 1, i))
    return float(xs[i])


def compute_replay_stats(path: str) -> ReplayStats:
    n = 0
    ok = 0
    ok_soft = 0
    by_scn = defaultdict(lambda: {"n": 0, "ok": 0, "ok_soft": 0})
    exec_norms: List[float] = []
    vol_cap_hit = 0
    vol_n = 0
    saw_hard_miss = 0
    saw_n = 0
    missing = Counter()

    for r in iter_ndjson(path):
        n += 1
        ok_i = int(_get(r, "ok") or 0)
        ok += ok_i
        ok_soft_i = int(_get(r, "ok_soft") or 0)
        ok_soft += ok_soft_i

        scn = str(_get(r, "scenario_v4") or _get(r, "scenario") or "na")
        by_scn[scn]["n"] += 1
        by_scn[scn]["ok"] += ok_i
        by_scn[scn]["ok_soft"] += ok_soft_i

        try:
            ev = r.get("evidence") or {}
            if isinstance(ev, dict):
                exec_risk_norm = float(ev.get("exec_risk_norm", 0.0) or 0.0)
                if exec_risk_norm is not None:
                    exec_norms.append(exec_risk_norm)

                if scn == "vol_shock_news_proxy":
                    vol_n += 1
                    if int(ev.get("policy_vol_shock_exec_risk_cap_hit", 0) or 0) == 1:
                        vol_cap_hit += 1
                if scn == "saw_chop_spoof_proxy":
                    saw_n += 1
                    # hard evidence miss = missing_hard_evidence reason OR missing legs contain core items
                    rsn = str(_get(r, "reason") or "")
                    if "missing_hard_evidence" in rsn:
                        saw_hard_miss += 1
        except Exception:
            pass

        ml = _get(r, "missing_legs")
        if isinstance(ml, list):
            for x in ml:
                missing[str(x)] += 1

    p50 = _percentile(exec_norms, 0.50)
    p90 = _percentile(exec_norms, 0.90)

    out_by_scn = {}
    for k, v in by_scn.items():
        nn = int(v["n"])
        oo = int(v["ok"])
        oos = int(v["ok_soft"])
        out_by_scn[k] = {"n": nn, "ok_rate": (oo / nn) if nn > 0 else None, "ok_soft_rate": (oos / nn) if nn > 0 else None, "no_data": 1 if nn == 0 else 0}

    ok_rate = (ok / n) if n > 0 else None
    ok_soft_rate = (ok_soft / n) if n > 0 else None
    no_data = 1 if n == 0 else 0
    return ReplayStats(
        n=n,
        ok_rate=ok_rate,
        ok_soft_rate=ok_soft_rate,
        no_data=no_data,
        by_scenario=out_by_scn,
        exec_risk_norm_p50=p50,
        exec_risk_norm_p90=p90,
        vol_shock_cap_hit_rate=(vol_cap_hit / vol_n) if vol_n > 0 else 0.0,
        saw_chop_hard_miss_rate=(saw_hard_miss / saw_n) if saw_n > 0 else 0.0,
        top_missing_legs=missing.most_common(12),
    )


def compute_ok_fail_breakdown(path: str) -> Dict[str, int]:
    """Scan replay NDJSON for ok=0 rows and count how many times each
    blocking condition fires.  Returns condition_name -> count mapping.
    One row can fail multiple conditions simultaneously."""
    fail_conds: Counter = Counter()
    fail_reasons: Counter = Counter()

    for r in iter_ndjson(path):
        ok_i = int(_get(r, "ok") or 0)
        if ok_i != 0:
            continue  # only care about rejected rows

        ev = r.get("evidence") or {}
        if not isinstance(ev, dict):
            ev = {}

        # 1. score veto — threshold matches engine default or signal payload
        _score_veto_min = float(os.getenv("OF_SCORE_MIN", "0.60"))
        legacy_min = _get(r, "legacy_of_score_min")
        if legacy_min is not None:
            try:
                _score_veto_min = float(legacy_min)
            except Exception:
                pass

        score = _get(r, "score")
        hv = _get(r, "have")
        nd = _get(r, "need")
        has_legs = False
        if hv is not None and nd is not None:
            try:
                has_legs = float(hv) >= float(nd)
            except Exception:
                pass
        rsn = str(_get(r, "reason") or "")
        if score is not None:
            try:
                if float(score) < _score_veto_min and (has_legs or "score_veto" in rsn):
                    fail_conds[f"score_veto (score<{_score_veto_min:.2f})"] += 1
            except Exception:
                pass

        # 2. exec_risk cap
        try:
            ern = float(ev.get("exec_risk_norm", 0.0) or 0.0)
            if ern >= 1.0:
                fail_conds["exec_risk_cap (norm>=1.0)"] += 1
        except Exception:
            pass

        # 3. have < need
        have = _get(r, "have")
        need = _get(r, "need")
        try:
            if have is not None and need is not None and float(have) < float(need):
                fail_conds["have<need (legs not met)"] += 1
        except Exception:
            pass

        # 4. missing_legs
        ml = _get(r, "missing_legs")
        if isinstance(ml, list) and ml:
            fail_conds[f"missing_legs (top: {ml[0]})"] += 1
        elif ml and not isinstance(ml, list):
            fail_conds["missing_legs (non-list)"] += 1

        # 5. dn_veto / meta_veto scenario
        scn = str(_get(r, "scenario_v4") or _get(r, "scenario") or "")
        if scn in ("dn_veto", "meta_veto"):
            fail_conds[f"scenario={scn}"] += 1

        # 6. raw reason string (first meaningful token)
        reason_raw = str(_get(r, "reason") or "")
        if reason_raw:
            reason_short = reason_raw.split(",")[0].split(";")[0].strip()[:80]
            if reason_short:
                fail_reasons[reason_short] += 1

    # Merge: structured conditions first, then top reason strings
    result: Dict[str, int] = dict(fail_conds)
    for rs, cnt in fail_reasons.most_common(8):
        result[f"reason: {rs}"] = cnt
    return result


def compute_ok_soft_fail_breakdown(path: str) -> Dict[str, int]:
    """Scan replay NDJSON for ok_soft=1 rows and count why they are not ok=1."""
    fail_conds: Counter = Counter()
    fail_reasons: Counter = Counter()

    for r in iter_ndjson(path):
        ok_i = int(_get(r, "ok") or 0)
        ok_soft_i = int(_get(r, "ok_soft") or 0)
        if ok_i != 0 or ok_soft_i != 1:
            continue

        rsn = str(_get(r, "reason") or "")
        
        # 1. score veto
        _score_veto_min = float(os.getenv("OF_SCORE_MIN", "0.60"))
        legacy_min = _get(r, "legacy_of_score_min")
        if legacy_min is not None:
            try:
                _score_veto_min = float(legacy_min)
            except Exception:
                pass

        score = _get(r, "score")
        if score is not None:
            try:
                if float(score) < _score_veto_min:
                    fail_conds[f"score_veto (score<{_score_veto_min:.2f})"] += 1
            except Exception:
                pass

        # 2. have < need
        have = _get(r, "have")
        need = _get(r, "need")
        if have is not None and need is not None:
            try:
                if float(have) < float(need):
                    fail_conds["have<need (legs not met)"] += 1
            except Exception:
                pass

        # 3. missing legs (which one is blocking)
        ml = _get(r, "missing_legs")
        if isinstance(ml, list) and ml:
            fail_conds[f"missing_legs (top: {ml[0]})"] += 1

        # 4. reasons
        if rsn:
            reason_short = rsn.split(",")[0].split(";")[0].strip()[:80]
            if reason_short:
                fail_reasons[reason_short] += 1

    result: Dict[str, int] = dict(fail_conds)
    for rs, cnt in fail_reasons.most_common(5):
        result[f"reason: {rs}"] = cnt
    return result


def propose_cfg_recs(stats: ReplayStats, *, mode: str, outcome: Optional[dict] = None) -> List[Dict[str, Any]]:
    """
    Returns list of recommendations as dicts:
      {key, value, scope, why, cmd}
    scope:
      - per_symbol (default) -> HSET config:orderflow:<SYMBOL> ...
      - global (optional)    -> not used by default
      - info                 -> diagnostics only
    
    Args:
        stats: Статистика replay
        mode: Режим работы (monitor/regress)
        outcome: Опциональные outcome метрики (R-multiple анализ) для более точных рекомендаций
    """
    if int(_envs("RECS_ENABLE", "1")) != 1:
        return []

    # thresholds from env (you already have similar in script)
    pass_rate_min = float(os.getenv("PASS_RATE_MIN", "0.25") or 0.25)
    exec_p90_warn = float(os.getenv("EXEC_RISK_NORM_P90_WARN", "0.85") or 0.85)
    vol_cap_warn  = float(os.getenv("VOL_SHOCK_CAP_HIT_WARN", "0.20") or 0.20)
    saw_miss_warn = float(os.getenv("SAW_CHOP_HARD_MISS_WARN", "0.30") or 0.30)

    step_w_exec   = _envf("RECS_STEP_W_EXEC", 0.02)
    step_ref_bps  = _envf("RECS_STEP_EXEC_REF_BPS", 1.0)
    step_scoremin = _envf("RECS_STEP_SCORE_MIN", 0.02)

    # Outcome thresholds
    outcome_min_n = int(os.getenv("OUTCOME_MIN_N", "50") or 50)
    outcome_tail_max = float(os.getenv("OUTCOME_TAIL_LOSS_MAX", "0.18") or 0.18)
    outcome_meanr_min = float(os.getenv("OUTCOME_MEANR_MIN", "0.10") or 0.10)
    outcome_bigwin_min = float(os.getenv("OUTCOME_BIGWIN_MIN", "0.10") or 0.10)

    # These are "suggested targets" (you can tune defaults)
    # We do NOT read current cfg live here to keep script infra-light.
    # Recommendations are deltas around known defaults.
    base_w_exec = _envf("RECS_BASE_W_EXEC", 0.18)
    base_exec_ref_bps = _envf("RECS_BASE_EXEC_REF_BPS", 10.0)
    base_score_min = _envf("OF_SCORE_MIN", 0.60)  # Baseline score for recommendations

    recs: List[Dict[str, Any]] = []

    # Targets for commands (symbols)
    sym_list = _parse_csv(_envs("CFG_TARGET_SYMBOLS", ""))
    if not sym_list:
        # fallback: canary symbols from env if set
        sym_list = _parse_csv(_envs("CANARY_SYMBOLS", ""))

    prefix = _envs("CFG_HASH_PREFIX", "config:orderflow:")

    def cmd_hset(sym: str, kv: Dict[str, str]) -> str:
        # Template command (not executed)
        pairs = " ".join([f"{k} {kv[k]}" for k in kv.keys()])
        return f"redis-cli HSET {prefix}{sym} {pairs}"

    # 1) Execution-risk / "пила" tightening
    # If exec_risk_norm p90 too high, we want stricter penalty and/or lower cap thresholds.
    if stats.exec_risk_norm_p90 >= exec_p90_warn:
        w_exec = _clamp(base_w_exec + step_w_exec, 0.10, 0.30)
        exec_ref = _clamp(base_exec_ref_bps - step_ref_bps, 6.0, 14.0)  # lower ref => higher norm => higher penalty
        for sym in sym_list:
            kv = {
                "w_exec_risk": f"{w_exec:.3f}",
                "exec_risk_ref_bps": f"{exec_ref:.2f}",
            }
            recs.append({
                "scope": "per_symbol",
                "symbol": sym,
                "key": "w_exec_risk, exec_risk_ref_bps",
                "value": kv,
                "why": f"exec_risk_norm p90={stats.exec_risk_norm_p90:.2f} ≥ {exec_p90_warn:.2f}: усилить penalty против 'пилы' и плохого исполнения",
                "cmd": cmd_hset(sym, kv),
            })

    # 2) Если ok_rate слишком низкий — вероятно перетянули гайки.
    # Без outcome нельзя "ослаблять" уверенно, поэтому даём аккуратный совет: снизить score_min слегка.
    if stats.ok_rate is not None and stats.ok_rate < pass_rate_min:
        score_min = _clamp(base_score_min - step_scoremin, 0.30, 0.80)
        ok_rate_str = f"{stats.ok_rate:.2f}" if stats.ok_rate is not None else "NA"
        
        warning = ""
        if stats.exec_risk_norm_p90 >= exec_p90_warn:
            warning = f"\n⚠️ <b>Внимание (Риск):</b> Мягкое снижение of_score_min (до {score_min:.3f}), которое также есть в репорте, я применять пока не советую. При текущем критическом уровне lat_p99 &gt; 25ms и плохом экзекьюшене снижать порог ML Score опасно — в сделку пойдет слишком много маргинального мусора. Сначала \"потушите\" пилу ужесточением exec_risk."

        for sym in sym_list:
            kv = {"of_score_min": f"{score_min:.3f}"}
            recs.append({
                "scope": "per_symbol",
                "symbol": sym,
                "key": "of_score_min",
                "value": kv,
                "why": f"ok_rate={ok_rate_str} &lt; {pass_rate_min:.2f}: возможно слишком строгий score veto; мягкое снижение на {step_scoremin:.2f}{warning}",
                "cmd": cmd_hset(sym, kv),
            })

    # 3) vol_shock policy: если cap-hit часто, безопаснее fail-closed (по плану B2)
    if stats.vol_shock_cap_hit_rate >= vol_cap_warn and int(_envs("RECS_VOL_SHOCK_FAIL_CLOSED_ON_CAP", "1")) == 1:
        for sym in sym_list:
            kv = {"vol_shock_fail_closed": "1"}
            recs.append({
                "scope": "per_symbol",
                "symbol": sym,
                "key": "vol_shock_fail_closed",
                "value": kv,
                "why": f"vol_shock cap-hit rate={stats.vol_shock_cap_hit_rate:.2f} ≥ {vol_cap_warn:.2f}: включить fail-closed (защита от новостного шума/плохого исполнения)",
                "cmd": cmd_hset(sym, kv),
            })

    # 4) saw_chop policy: если hard-evidence miss высоко — fail-closed (или чинить детекторы)
    if stats.saw_chop_hard_miss_rate >= saw_miss_warn and int(_envs("RECS_SAW_CHOP_FAIL_CLOSED_ON_MISS", "1")) == 1:
        for sym in sym_list:
            kv = {"saw_chop_fail_closed": "1"}
            recs.append({
                "scope": "per_symbol",
                "symbol": sym,
                "key": "saw_chop_fail_closed",
                "value": kv,
                "why": f"saw_chop hard-miss rate={stats.saw_chop_hard_miss_rate:.2f} ≥ {saw_miss_warn:.2f}: fail-closed до стабилизации fp_edge/ofi/iceberg",
                "cmd": cmd_hset(sym, kv),
            })

    # Outcome-based recommendations (только если outcome доступен и включен)
    if outcome and int(_envs("RECS_USE_OUTCOME", "1") or 1) == 1:
        n = int(outcome.get("n", 0) or 0)
        if n >= outcome_min_n:
            tail = float(outcome.get("tail_loss_rate", 0.0) or 0.0)
            meanR = float(outcome.get("meanR", 0.0) or 0.0)
            bigwin = float(outcome.get("bigwin_rate", 0.0) or 0.0)

            # 3.1 Если tail-loss высокий → ужесточаем exec-risk и включаем fail-closed для vol_shock/saw_chop
            if tail >= outcome_tail_max:
                # Tightening exec-risk (если еще не было рекомендации выше)
                if stats.exec_risk_norm_p90 < exec_p90_warn:
                    w_exec = _clamp(base_w_exec + step_w_exec, 0.10, 0.30)
                    exec_ref = _clamp(base_exec_ref_bps - step_ref_bps, 6.0, 14.0)
                    for sym in sym_list:
                        kv = {
                            "w_exec_risk": f"{w_exec:.3f}",
                            "exec_risk_ref_bps": f"{exec_ref:.2f}",
                        }
                        recs.append({
                            "scope": "per_symbol",
                            "symbol": sym,
                            "key": "w_exec_risk, exec_risk_ref_bps",
                            "value": kv,
                            "why": f"outcome tail-loss rate={tail:.3f} ≥ {outcome_tail_max:.3f}: ужесточить exec-risk для защиты от больших потерь",
                            "cmd": cmd_hset(sym, kv),
                        })
                
                # Plus fail-closed toggles (если еще не были включены выше)
                if stats.vol_shock_cap_hit_rate < vol_cap_warn and int(_envs("RECS_VOL_SHOCK_FAIL_CLOSED_ON_CAP", "1")) == 1:
                    for sym in sym_list:
                        kv = {"vol_shock_fail_closed": "1"}
                        recs.append({
                            "scope": "per_symbol",
                            "symbol": sym,
                            "key": "vol_shock_fail_closed",
                            "value": kv,
                            "why": f"outcome tail-loss rate={tail:.3f} ≥ {outcome_tail_max:.3f}: включить fail-closed для vol_shock (защита от tail-loss)",
                            "cmd": cmd_hset(sym, kv),
                        })
                
                if stats.saw_chop_hard_miss_rate < saw_miss_warn and int(_envs("RECS_SAW_CHOP_FAIL_CLOSED_ON_MISS", "1")) == 1:
                    for sym in sym_list:
                        kv = {"saw_chop_fail_closed": "1"}
                        recs.append({
                            "scope": "per_symbol",
                            "symbol": sym,
                            "key": "saw_chop_fail_closed",
                            "value": kv,
                            "why": f"outcome tail-loss rate={tail:.3f} ≥ {outcome_tail_max:.3f}: включить fail-closed для saw_chop (защита от tail-loss)",
                            "cmd": cmd_hset(sym, kv),
                        })

            # 3.2 Если meanR низкий и ok_rate высокий → фильтр пропускает мусор
            if meanR < outcome_meanr_min and stats.ok_rate is not None and stats.ok_rate > 0.5:
                # tighten exec-risk + raise score_min slightly
                score_min = _clamp(base_score_min + step_scoremin, 0.55, 0.80)
                for sym in sym_list:
                    kv = {"of_score_min": f"{score_min:.3f}"}
                    recs.append({
                        "scope": "per_symbol",
                        "symbol": sym,
                        "key": "of_score_min",
                        "value": kv,
                        "why": f"outcome meanR={meanR:.3f} < {outcome_meanr_min:.3f} при ok_rate={stats.ok_rate:.2f}: фильтр пропускает мусор, поднять score_min",
                        "cmd": cmd_hset(sym, kv),
                    })

            # 3.3 Если bigwins мало → возможно слишком строгий gate (ослабление только при нормальном tail-loss)
            if bigwin < outcome_bigwin_min and tail < outcome_tail_max:
                # slight relaxation: lower score_min немного
                score_min = _clamp(base_score_min - step_scoremin, 0.30, 0.75)
                for sym in sym_list:
                    kv = {"of_score_min": f"{score_min:.3f}"}
                    recs.append({
                        "scope": "per_symbol",
                        "symbol": sym,
                        "key": "of_score_min",
                        "value": kv,
                        "why": f"outcome bigwin rate={bigwin:.3f} < {outcome_bigwin_min:.3f} при tail-loss={tail:.3f} < {outcome_tail_max:.3f}: возможно слишком строгий gate, мягкое ослабление",
                        "cmd": cmd_hset(sym, kv),
                    })

    # 5) Топ missing legs -> диагностические рекомендации (не меняют риск напрямую)
    # Это всегда полезно в Telegram как action list.
    top = [name for name, _cnt in stats.top_missing_legs[:5]]
    if top:
        recs.append({
            "scope": "info",
            "symbol": "",
            "key": "diagnostics",
            "value": {"top_missing_legs": top},
            "why": "Top missing legs: это первичная точка тюнинга детекторов/TTL/порогов",
            "cmd": "",
        })

    return recs


def export_closed_trades(run_dir: Path) -> str:
    """
    Экспортирует POSITION_CLOSED события из Redis Stream events:trades за окно (TRADES_SINCE_HOURS).
    Использует tools/export_trade_closed_ndjson.py для экспорта.
    
    Returns:
        Путь к файлу с экспортированными данными (closed_trades.ndjson)
    """
    since_h = float(os.getenv("TRADES_SINCE_HOURS", "24") or 24)
    max_scan = int(os.getenv("TRADES_MAX_SCAN", "500000") or 500000)
    stream = os.getenv("TRADE_EVENTS_STREAM", "events:trades")
    redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")

    out_path = str(run_dir / "closed_trades.ndjson")
    cmd = [
        sys.executable, "tools/export_trade_closed_ndjson.py",
        "--since-hours", str(since_h),
        "--out", out_path,
        "--stream", stream,
        "--redis-url", redis_url,
        "--max-scan", str(max_scan),
    ]
    subprocess.check_call(cmd)
    return out_path


def analyze_outcome(closed_ndjson: str) -> dict:
    """
    Анализирует outcome (R-multiple) по экспортированным POSITION_CLOSED событиям.
    
    Читает POSITION_CLOSED/CLOSE события из NDJSON файла, ожидает r_mult в payload root
    (per TradeEventsLogger flattening).
    
    Считает метрики:
    - Общие: n, winrate, meanR, percentiles (p10, p50, p90), tail_loss_rate, bigwin_rate
    - По scenario_v4: группировка по сценариям
    - По of_confirm_ok: группировка по статусу подтверждения
    
    Args:
        closed_ndjson: Путь к NDJSON файлу с закрытыми трейдами
        
    Returns:
        Словарь с метриками outcome
    """
    from collections import defaultdict

    def _f(x, d=0.0):
        """Безопасное преобразование в float."""
        try:
            return float(x)
        except Exception:
            return float(d)

    n = 0
    wins = 0
    sum_r = 0.0
    rs = []
    tail_loss = 0
    bigwin = 0

    by_scn = defaultdict(lambda: {"n": 0, "wins": 0, "sum_r": 0.0, "tail": 0, "bigwin": 0})
    by_ok = defaultdict(lambda: {"n": 0, "wins": 0, "sum_r": 0.0, "tail": 0, "bigwin": 0})
    
    # ok-soft metrics (only for those where ok=0)
    ok_soft_stats = {"n": 0, "wins": 0, "sum_r": 0.0}

    for r in iter_ndjson(closed_ndjson):
        # safe filters (export already filters, но на всякий случай)
        et = str(r.get("event_type") or r.get("type") or "").upper()
        if et not in ("POSITION_CLOSED", "CLOSE"):
            continue

        rm = _f(r.get("r_mult", 0.0), 0.0)
        ok = int(r.get("of_confirm_ok", 0) or 0)
        ok_soft = int(r.get("of_confirm_ok_soft") or 0)
        scn = str(r.get("scenario") or r.get("scenario_v4") or "na")

        n += 1
        sum_r += rm
        rs.append(rm)
        if rm > 0:
            wins += 1
        if rm <= -1.0:
            tail_loss += 1
        if rm >= 2.0:
            bigwin += 1

        if ok_soft == 1 and ok == 0:
            ok_soft_stats["n"] += 1
            ok_soft_stats["sum_r"] += rm
            if rm > 0:
                ok_soft_stats["wins"] += 1

        by_scn[scn]["n"] += 1
        by_scn[scn]["sum_r"] += rm
        if rm > 0:
            by_scn[scn]["wins"] += 1
        if rm <= -1.0:
            by_scn[scn]["tail"] += 1
        if rm >= 2.0:
            by_scn[scn]["bigwin"] += 1

        k = f"of_confirm_ok={ok}"
        by_ok[k]["n"] += 1
        by_ok[k]["sum_r"] += rm
        if rm > 0:
            by_ok[k]["wins"] += 1
        if rm <= -1.0:
            by_ok[k]["tail"] += 1
        if rm >= 2.0:
            by_ok[k]["bigwin"] += 1

    rs_sorted = sorted(rs)
    def pct(p: float) -> float:
        """Вычисляет перцентиль из отсортированного списка R-multiple."""
        if not rs_sorted:
            return 0.0
        i = int(round((len(rs_sorted) - 1) * p))
        i = max(0, min(len(rs_sorted) - 1, i))
        return float(rs_sorted[i])

    def pack(g: dict) -> dict:
        """Упаковывает групповые метрики в словарь."""
        nn = int(g["n"])
        if nn <= 0:
            return {"n": 0, "winrate": 0.0, "meanR": 0.0, "tail_loss_rate": 0.0, "bigwin_rate": 0.0}
        return {
            "n": nn,
            "winrate": float(g["wins"] / nn),
            "meanR": float(g["sum_r"] / nn),
            "tail_loss_rate": float(g["tail"] / nn),
            "bigwin_rate": float(g["bigwin"] / nn),
        }

    out = {
        "n": n,
        "winrate": float(wins / n) if n else 0.0,
        "meanR": float(sum_r / n) if n else 0.0,
        "p10": pct(0.10),
        "p50": pct(0.50),
        "p90": pct(0.90),
        "tail_loss_rate": float(tail_loss / n) if n else 0.0,
        "bigwin_rate": float(bigwin / n) if n else 0.0,
        "by_scenario": {k: pack(v) for k, v in by_scn.items()},
        "by_of_confirm_ok": {k: pack(v) for k, v in by_ok.items()},
        "ok_soft_stats": {
            "n": ok_soft_stats["n"],
            "winrate": float(ok_soft_stats["wins"] / ok_soft_stats["n"]) if ok_soft_stats["n"] > 0 else 0.0,
            "meanR": float(ok_soft_stats["sum_r"] / ok_soft_stats["n"]) if ok_soft_stats["n"] > 0 else 0.0,
        }
    }
    return out


def send_report_redis(redis_url: str, stream: str, text: str, buttons_json: Optional[str] = None, bundle_id: Optional[str] = None) -> None:
    """
    Отправляет отчет в Redis stream notify:telegram.
    
    Поддерживает buttons для bot-nest (формат: JSON string с 2D массивом {text, callback}).
    bot-nest читает поле "buttons" и создает inline keyboard из него.
    
    Args:
        redis_url: URL Redis подключения
        stream: Имя stream (обычно "notify:telegram")
        text: Текст сообщения (HTML)
        buttons_json: Опциональный JSON string с кнопками для bot-nest
        bundle_id: Опциональный bundle_id для отслеживания
    """
    import redis  # redis-py
    r = redis.Redis.from_url(redis_url)
    fields = {
        "type": "report",
        "text": text,
        "parse_mode": "HTML",
        "ts": str(get_ny_time_millis()),
    }
    if buttons_json:
        fields["buttons"] = buttons_json
    if bundle_id:
        fields["bundle_id"] = bundle_id
    r.xadd(stream, fields, maxlen=200000, approximate=True)


def send_report_direct(token: str, chat_id: str, text: str) -> None:
    import requests
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    requests.post(url, json={"chat_id": chat_id, "text": text, "parse_mode": "HTML", "disable_web_page_preview": True}, timeout=15).raise_for_status()


def run_report(mode: str) -> int:
    # env
    redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    stream = os.getenv("OF_INPUTS_STREAM", "signals:of:inputs")
    field = os.getenv("OF_INPUTS_STREAM_FIELD", "payload")

    # Default paths for container if not overridden
    state_dir = os.getenv("STATE_DIR", "/app/of_reports_state")
    out_dir = os.getenv("OUT_DIR", "/app/of_reports_out")
    baseline_dir = os.getenv("BASELINE_DIR", "/app/of_reports_baselines")
    baseline_inputs = os.getenv("BASELINE_INPUTS", "")

    canary_symbols = os.getenv("CANARY_SYMBOLS", "").strip()
    canary_share = float(os.getenv("CANARY_SHARE", "0") or 0)
    max_records = int(os.getenv("MAX_RECORDS", "200000") or 200000)
    batch = int(os.getenv("BATCH", "2000") or 2000)

    pass_rate_min = float(os.getenv("PASS_RATE_MIN", "0.25") or 0.25)
    exec_p90_warn = float(os.getenv("EXEC_RISK_NORM_P90_WARN", "0.85") or 0.85)
    vol_cap_warn = float(os.getenv("VOL_SHOCK_CAP_HIT_WARN", "0.20") or 0.20)
    saw_miss_warn = float(os.getenv("SAW_CHOP_HARD_MISS_WARN", "0.30") or 0.30)

    ts = time.strftime("%Y%m%d_%H%M%S")
    run_dir = Path(out_dir) / f"run_{mode}_{ts}"
    run_dir.mkdir(parents=True, exist_ok=True)

    # Build inputs path:
    if mode == "regress":
        if not baseline_inputs:
            print("BASELINE_INPUTS is required for regress mode")
            return 1
        inputs_path = baseline_inputs
    else:
        # export incremental inputs
        state_file = str(Path(state_dir) / "of_inputs.state")
        inputs_raw = str(run_dir / "of_inputs_raw.ndjson")

        cmd = [
            sys.executable, "-m", "tools.export_of_inputs_ndjson_v2",
            "--redis-url", redis_url,
            "--out", inputs_raw,
            "--stream", stream,
            "--field", field,
            "--state-file", state_file,
            "--resume",
            "--batch", str(batch),
            "--max-records", str(max_records),
        ]
        subprocess.check_call(cmd)

        # canary filter
        syms = [s.strip() for s in canary_symbols.split(",") if s.strip()] if canary_symbols else None
        if (syms and len(syms) > 0) or (canary_share and canary_share > 0):
            inputs_canary = str(run_dir / "of_inputs_canary.ndjson")
            n = write_ndjson(inputs_canary, filter_inputs(iter_ndjson(inputs_raw), canary_symbols=syms, canary_share=canary_share))
            inputs_path = inputs_canary
            # guard: if empty canary -> skip
            if n <= 0:
                print(f"No canary data found for share={canary_share} syms={syms}")
                return 0
        else:
            inputs_path = inputs_raw

    # replay
    replay_out = str(run_dir / "of_replay.ndjson")
    dbg_out = str(run_dir / "of_replay_debug.ndjson")
    subprocess.check_call([
        sys.executable, "-m", "tools.of_confirm_replay_from_inputs", 
        "--inputs", inputs_path, 
        "--out", replay_out,
        "--debug-out", dbg_out
    ])

    stats = compute_replay_stats(replay_out)

    # ok=0 failure breakdown (inline, separate from ReplayStats
    # so no dataclass changes are required)
    ok_fail = compute_ok_fail_breakdown(replay_out)

    # diff in regress mode (baseline output) OR if baseline_dir provided
    diff_summary = None
    if mode == "regress":
        base_out = pick_baseline(baseline_dir, "ALL") or str(Path(baseline_dir) / "baseline.ndjson")
        if Path(base_out).exists():
            diff_summary = diff_report(base_out, replay_out)
            (run_dir / "diff_summary.json").write_text(json.dumps(diff_summary, ensure_ascii=False, indent=2), encoding="utf-8")

    # Export closed trades and analyze outcome (only in regress mode)
    closed_path = None
    outcome = None
    if mode == "regress":
        try:
            closed_path = export_closed_trades(run_dir)
        except Exception as e:
            print(f"Warning: failed to export closed trades: {e}")
            closed_path = None

        if closed_path and Path(closed_path).exists() and int(os.getenv("RECS_USE_OUTCOME", "1") or 1) == 1:
            try:
                outcome = analyze_outcome(closed_path)
                (run_dir / "outcome_summary.json").write_text(json.dumps(outcome, ensure_ascii=False, indent=2), encoding="utf-8")
            except Exception as e:
                print(f"Warning: failed to analyze outcome: {e}")
                outcome = None

    # Generate config recommendations (with outcome if available)
    recs = propose_cfg_recs(stats, mode=mode, outcome=outcome)

    # Legacy recommendations (kept for backward compatibility)
    rec = []
    if stats.ok_rate is not None and stats.ok_rate < pass_rate_min:
        rec.append(f"⚠️ ok_rate ниже порога: {stats.ok_rate:.2f} < {pass_rate_min:.2f} (проверьте пороги need/exec_risk caps)")
    if stats.exec_risk_norm_p90 >= exec_p90_warn:
        rec.append(f"⚠️ высокий exec_risk_norm p90={stats.exec_risk_norm_p90:.2f} ≥ {exec_p90_warn:.2f} (возможна 'пила' / ухудшение ликвидности)")
    if stats.vol_shock_cap_hit_rate >= vol_cap_warn:
        rec.append(f"⚠️ vol_shock cap-hit rate={stats.vol_shock_cap_hit_rate:.2f} ≥ {vol_cap_warn:.2f} (подстройка vol_shock_exec_risk_norm_max / exec_risk_ref_bps)")
    if stats.saw_chop_hard_miss_rate >= saw_miss_warn:
        rec.append(f"⚠️ saw_chop hard-evidence miss rate={stats.saw_chop_hard_miss_rate:.2f} ≥ {saw_miss_warn:.2f} (проверьте fp_edge/iceberg/ofi детекторы)")

    # telegram message
    lines = []
    lines.append(f"<b>OF Gate Report</b>  mode=<code>{html.escape(mode)}</code>  ts=<code>{html.escape(ts)}</code>")
    ok_rate_s = f"{stats.ok_rate:.3f}" if stats.ok_rate is not None else "NA"
    ok_soft_s = f"{stats.ok_soft_rate:.3f}" if stats.ok_soft_rate is not None else "NA"
    lines.append(f"records=<code>{stats.n}</code> ok_rate=<code>{ok_rate_s}</code> ok_soft=<code>{ok_soft_s}</code> exec_p50=<code>{stats.exec_risk_norm_p50:.2f}</code> exec_p90=<code>{stats.exec_risk_norm_p90:.2f}</code>")
    lines.append(f"vol_shock_cap_hit=<code>{stats.vol_shock_cap_hit_rate:.2f}</code> saw_hard_miss=<code>{stats.saw_chop_hard_miss_rate:.2f}</code>")
    lines.append("")
    lines.append("<b>By scenario</b>")
    for k, v in sorted(stats.by_scenario.items(), key=lambda kv: -kv[1]["n"]):
        _ok_s = f"{v['ok_rate']:.3f}" if v['ok_rate'] is not None else "NA"
        _ok_soft_s = f"{v.get('ok_soft_rate'):.3f}" if v.get('ok_soft_rate') is not None else "NA"
        lines.append(f"- <code>{html.escape(k)}</code>: n={v['n']} ok_rate={_ok_s} ok_soft={_ok_soft_s}")
    lines.append("")
    lines.append("<b>Top missing legs</b>")
    for name, cnt in stats.top_missing_legs[:8]:
        lines.append(f"- <code>{html.escape(str(name))}</code>: {cnt}")

    # ── ok=0 failure breakdown ──────────────────────────────────────────────
    _ok_val = stats.ok_rate if stats.ok_rate is not None else 0.0
    if stats.n > 0 and _ok_val < pass_rate_min and ok_fail:
        fail_n = stats.n - int(_ok_val * stats.n + 0.5)  # rejected rows
        lines.append("")
        lines.append("<b>❌ Почему ok=0? (breakdown условий фильтра)</b>")
        lines.append(f"Отклонено: <code>{fail_n}</code> из <code>{stats.n}</code> | ok_rate: <code>{_ok_val:.3f}</code>")
        lines.append("Условия, заблокировавшие ok (кол-во строк):")
        for cond, ccnt in sorted(ok_fail.items(), key=lambda kv: -kv[1])[:10]:
            pct = ccnt / fail_n * 100 if fail_n > 0 else 0.0
            lines.append(f"  • <code>{html.escape(cond)}</code>: {ccnt} ({pct:.1f}%)")
        lines.append("<i>Одна запись может нарушать несколько условий.</i>")

    # ── ok-soft block ────────────────────────────────────────────────────────
    ok_soft_val = stats.ok_soft_rate if stats.ok_soft_rate is not None else 0.0
    if stats.n > 0 and ok_soft_val > 0:
        n_soft = int(ok_soft_val * stats.n + 0.5)
        # Outcome metrics for ok-soft
        s_wr, s_pnl, s_share = 0.0, 0.0, ok_soft_val * 100
        if outcome and "ok_soft_stats" in outcome:
            s_wr = outcome["ok_soft_stats"]["winrate"] * 100
            s_pnl = outcome["ok_soft_stats"]["meanR"] # MeanR as proxy for PnL in R
            
        lines.append("")
        lines.append("<b>ok-soft block</b>")
        lines.append(f"WR: <code>{s_wr:.1f}%</code> | PnL: <code>{s_pnl:+.2f}R</code> | Share: <code>{s_share:.1f}%</code> (<code>{n_soft}</code>)")
        
        soft_fail = compute_ok_soft_fail_breakdown(replay_out)
        if soft_fail:
            lines.append("Условия, заблокировавшие ok для ok-soft:")
            for cond, ccnt in sorted(soft_fail.items(), key=lambda kv: -kv[1])[:6]:
                pct = ccnt / n_soft * 100 if n_soft > 0 else 0.0
                lines.append(f"  • <code>{html.escape(cond)}</code>: {ccnt} ({pct:.1f}%)")
    # ─────────────────────────────────────────────────────────────────────────

    if diff_summary:
        lines.append("")
        lines.append("<b>Regression diff</b>")
        lines.append(f"overlap_n=<code>{diff_summary.get('n',0)}</code> mismatches=<code>{diff_summary.get('mismatches',0)}</code>")
        top = diff_summary.get("mismatch_by_field", {})
        if isinstance(top, dict) and top:
            lines.append(f"mismatch_by_field=<code>{html.escape(str(top))}</code>")
    
    # Outcome block (R-multiple analysis)
    if outcome:
        lines.append("")
        lines.append("<b>Outcome (POSITION_CLOSED, R-multiple)</b>")
        lines.append(
            f"n=<code>{outcome.get('n',0)}</code> "
            f"meanR=<code>{outcome.get('meanR',0.0):.3f}</code> "
            f"winrate=<code>{outcome.get('winrate',0.0):.3f}</code> "
            f"tail(R&lt;=-1)=<code>{outcome.get('tail_loss_rate',0.0):.3f}</code> "
            f"bigwin(R&gt;=2)=<code>{outcome.get('bigwin_rate',0.0):.3f}</code>"
        )
        lines.append(f"p10=<code>{outcome.get('p10',0.0):.2f}</code> p50=<code>{outcome.get('p50',0.0):.2f}</code> p90=<code>{outcome.get('p90',0.0):.2f}</code>")

        # top scenarios
        bs = outcome.get("by_scenario", {}) or {}
        items = sorted(bs.items(), key=lambda kv: -(kv[1].get("n",0) or 0))[:6]
        if items:
            lines.append("<b>By scenario</b>")
            for k,v in items:
                lines.append(f"- <code>{html.escape(k)}</code>: n={v['n']} meanR={v['meanR']:.2f} win={v['winrate']:.2f} tail={v['tail_loss_rate']:.2f}")

    # Config recommendations with Redis commands
    # Формат для bot-nest: buttons (JSON string) вместо reply_markup
    buttons_json = None
    bundle_id = None
    if recs and int(os.getenv("RECS_ENABLE", "1") or 1) == 1:
        # Импортируем модули для работы с рекомендациями
        try:
            from core.recs_contract import RecOp, sign_bundle_id
            import redis
            
            # Подготовка данных для bundle
            prefix = os.getenv("CFG_HASH_PREFIX", "config:orderflow:")
            ttl_sec = int(os.getenv("RECS_TTL_SEC", "86400") or 86400)
            secret = os.getenv("RECS_HMAC_SECRET", "CHANGE_ME")
            rdb = redis.Redis.from_url(redis_url)
            
            buttons = []
            
            # Строим раздельные bundles для каждой рекомендации
            for r in recs:
                if r.get("scope") != "per_symbol":
                    continue
                sym = r.get("symbol", "").upper()
                if not sym:
                    continue
                kv = r.get("value") or {}
                if not isinstance(kv, dict):
                    continue
                    
                ops = []
                for field, val in kv.items():
                    ops.append(RecOp(
                        op="HSET",
                        key=f"{prefix}{sym}",
                        field=str(field),
                        value=str(val)
                    ))
            
                if ops:
                    # Создаем уникальный bundle для конкретной рекомендации
                    b_id = secrets.token_hex(6)  # 12 hex символов
                    bundle_dict = {
                        "id": b_id,
                        "created_ms": get_ny_time_millis(),
                        "ttl_sec": ttl_sec,
                        "who": "cron_of_reports",
                        "ops": [{"op": op.op, "key": op.key, "field": op.field, "value": op.value} for op in ops],
                        "meta": {"kind": "of_gate_recs", "mode": mode, "ts": ts},
                    }
                    
                    rdb.set(f"recs:bundle:{b_id}", json.dumps(bundle_dict, ensure_ascii=False, separators=(",", ":")), ex=ttl_sec)
                    rdb.set(f"recs:status:{b_id}", "PENDING", ex=ttl_sec)
                    
                    sig = sign_bundle_id(b_id, secret)
                    
                    # Кнопка для этой рекомендации
                    short_key = str(r.get('key', ''))[:15]
                    buttons.append([
                        {"text": f"✅ {sym} {short_key}", "callback": f"recs:preview2:{b_id}:{sig}"},
                        {"text": f"❌ Reject", "callback": f"recs:reject2:{b_id}:{sig}"}
                    ])
                    
            if buttons:
                buttons_json = json.dumps(buttons, ensure_ascii=False, separators=(",", ":"))
                
        except Exception as e:
            print(f"Warning: failed to create recommendation bundle: {e}", file=sys.stderr)
            # Продолжаем без bundle, но показываем рекомендации как обычно
            pass
    
    if recs:
        lines.append("")
        lines.append("<b>Config recommendations</b>")
        # показываем только actionable, не больше 8
        shown = 0
        for r in recs:
            if r.get("scope") == "info":
                continue
            lines.append(f"- <code>{html.escape(r.get('symbol',''))}</code>: {r.get('why','')}")
            if r.get("cmd"):
                lines.append(f"  <code>{html.escape(r['cmd'])}</code>")
            shown += 1
            if shown >= 8:
                break

        # diagnostics tail
        for r in recs:
            if r.get("scope") == "info":
                lines.append("")
                lines.append("<b>Diagnostics</b>")
                lines.append(f"top_missing_legs=<code>{html.escape(str(r['value'].get('top_missing_legs')))}</code>")
                break
    
    # Legacy recommendations (kept for backward compatibility)
    if rec:
        lines.append("")
        lines.append("<b>Legacy Recommendations</b>")
        for r in rec[:6]:
            lines.append(f"- {html.escape(r)}")
    elif not recs:
        lines.append("")
        lines.append("<b>Recommendations</b>")
        lines.append("- ✅ отклонений по порогам нет. Можно продолжать rollout/canary без изменений.")

    msg = "\n".join(lines)

    tg_mode = os.getenv("TELEGRAM_MODE", "redis").strip().lower()
    if tg_mode == "direct":
        token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
        chat_id = os.getenv("TELEGRAM_CHAT_ID", "").strip()
        if not token or not chat_id:
            print("TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID required for direct mode")
            return 1
        send_report_direct(token, chat_id, msg)
    else:
        nredis = os.getenv("TELEGRAM_REDIS_URL", redis_url)
        stream_out = os.getenv("TELEGRAM_NOTIFY_STREAM", "notify:telegram")
        send_report_redis(nredis, stream_out, msg, buttons_json=buttons_json, bundle_id=bundle_id)

    return 0


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", choices=["monitor", "regress"], required=True)
    args = ap.parse_args()
    
    # Run crossvenue gate calibrator every 4 hours
    try:
        import time
        from pathlib import Path
        last_run_file = Path("/tmp/cv_calibrator_last_run")
        run_calibrator = True
        if last_run_file.exists():
            # Check if 4 hours (14400 seconds) have passed
            if time.time() - last_run_file.stat().st_mtime < 14400:
                run_calibrator = False
        if run_calibrator:
            print("Triggering crossvenue_gate_calibrator.py (4h interval)", file=sys.stderr)
            subprocess.Popen([sys.executable, "tools/crossvenue_gate_calibrator.py"])
            last_run_file.touch()
    except Exception as e:
        print(f"Warning: failed to trigger crossvenue_gate_calibrator: {e}", file=sys.stderr)

    return run_report(args.mode)


if __name__ == "__main__":
    raise SystemExit(main())
