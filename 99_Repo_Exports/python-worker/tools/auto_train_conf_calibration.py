#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import time
from dataclasses import dataclass
import psycopg2

from typing import Tuple, Dict, Any, List, Iterable
import shutil
from pathlib import Path

from tools.train_confidence_calibration import train


def _env(*names: str, default: str = "") -> str:
    for n in names:
        v = os.getenv(n, "")
        if v:
            return str(v)
    return str(default)


def _read_json(path: str) -> Dict[str, Any]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return dict(json.load(f) or {})
    except Exception:
        return {}


def _safe_int(x: Any, default: int = 0) -> int:
    try:
        return int(x)
    except Exception:
        return int(default)


def _safe_float(x: Any, default: float = 0.0) -> float:
    try:
        return float(x)
    except Exception:
        return float(default)


def _isfinite(x: float) -> bool:
    try:
        return float(x) == float(x) and abs(float(x)) != float("inf")
    except Exception:
        return False


def _weighted_mean(pairs: List[Tuple[float, float]]) -> float:
    """
    pairs: [(value, weight), ...]
    """
    sw = 0.0
    s = 0.0
    for v, w in pairs:
        if w <= 0.0:
            continue
        if not _isfinite(v):
            continue
        sw += float(w)
        s += float(v) * float(w)
    if sw <= 1e-12:
        return float("nan")
    return float(s / sw)


def _weighted_quantile(pairs: List[Tuple[float, float]], q: float) -> float:
    """
    Weighted quantile of values with weights.
    q in [0..1]. Returns nan if empty.
    """
    q = 0.0 if q <= 0.0 else (1.0 if q >= 1.0 else float(q))
    data = [(float(v), float(w)) for v, w in pairs if w > 0.0 and _isfinite(v)]
    if not data:
        return float("nan")
    data.sort(key=lambda t: t[0])
    total = sum(w for _, w in data)
    if total <= 1e-12:
        return float("nan")
    target = q * total
    acc = 0.0
    for v, w in data:
        acc += w
        if acc + 1e-12 >= target:
            return float(v)
    return float(data[-1][0])


@dataclass
class State:
    trained_at: int = 0
    max_ts_epoch: float = 0.0


def _load_state(path: str) -> State:
    try:
        with open(path, "r", encoding="utf-8") as f:
            o = json.load(f) or {}
        return State(trained_at=int(o.get("trained_at", 0) or 0), max_ts_epoch=float(o.get("max_ts_epoch", 0.0) or 0.0))
    except Exception:
        return State()


def _save_state_atomic(path: str, st: State) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump({"trained_at": int(st.trained_at), "max_ts_epoch": float(st.max_ts_epoch)}, f, ensure_ascii=False, sort_keys=True, indent=2)
    os.replace(tmp, path)


def _count_new_eligible(conn, *, since_ts_epoch: float) -> int:
    # приближённо: считаем outcome'ы, которые потенциально дают лейбл (без строгой логики realized_R)
    sql = """
    SELECT COUNT(*)
    FROM signal_performance p
    JOIN signals s ON p.signal_id = s.signal_id
    WHERE EXTRACT(EPOCH FROM p.ts_signal) > %s
      AND s.final_score IS NOT NULL
      AND p.outcome IN ('target_hit','stop_hit','manual_exit','expired_no_target','breakeven')
    """
    with conn.cursor() as cur:
        cur.execute(sql, (float(since_ts_epoch),))
        r = cur.fetchone()
        return int((r[0] if r else 0) or 0)


def _read_calib_max_ts(path: str) -> float:
    try:
        with open(path, "r", encoding="utf-8") as f:
            o = json.load(f) or {}
        return float(o.get("max_ts_epoch", 0.0) or 0.0)
    except Exception:
        return 0.0


def _read_trained_at(path: str) -> int:
    o = _read_json(path)
    return _safe_int(o.get("trained_at", 0), 0)


def _ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def _history_name(prefix: str, trained_at: int) -> str:
    # confidence_calibration.1730000000.json
    return f"{prefix}.{int(trained_at)}.json"


def _list_history(dirp: str, prefix: str) -> List[str]:
    try:
        xs = []
        for fn in os.listdir(dirp):
            if fn.startswith(prefix + ".") and fn.endswith(".json"):
                xs.append(os.path.join(dirp, fn))
        xs.sort(key=lambda p: os.path.getmtime(p))
        return xs
    except Exception:
        return []


def _cleanup_keep_last(dirp: str, prefix: str, keep: int) -> None:
    keep = int(keep)
    if keep <= 0:
        return
    files = _list_history(dirp, prefix)
    if len(files) <= keep:
        return
    for p in files[: max(0, len(files) - keep)]:
        try:
            # также удаляем report рядом
            rp = p + ".report.json"
            if os.path.exists(rp):
                os.remove(rp)
            os.remove(p)
        except Exception:
            pass


def gate_decision(report: Dict[str, Any]) -> Tuple[bool, str, Dict[str, Any]]:
    """
    Gate по global + top-K kind|symbol на val-slice (по n_val).
    По умолчанию режим "soft":
      - не ухудшить brier больше чем +MAX_BRIER_UP
      - не ухудшить ece   больше чем +MAX_ECE_UP
    "strict":
      - brier должен улучшиться минимум на MIN_BRIER_IMPROVE (дельта <= -min_improve)
      - ece не ухудшать больше чем +MAX_ECE_UP
    """
    mode = str(os.getenv("CONF_CAL_GATE_MODE", "soft") or "soft").lower()

    # --- global thresholds ---
    max_brier_up = float(os.getenv("CONF_CAL_GATE_MAX_BRIER_UP", "0.002"))
    max_ece_up = float(os.getenv("CONF_CAL_GATE_MAX_ECE_UP", "0.01"))
    min_brier_improve = float(os.getenv("CONF_CAL_GATE_MIN_BRIER_IMPROVE", "0.0005"))
    min_val_global = int(os.getenv("CONF_CAL_GATE_MIN_VAL", "200"))

    # --- group gate selection ---
    topk = int(os.getenv("CONF_CAL_GATE_TOPK", "10"))
    min_val_group = int(os.getenv("CONF_CAL_GATE_MIN_VAL_GROUP", "150"))

    # group thresholds (обычно чуть мягче, т.к. дисперсия выше)
    grp_max_brier_up = float(os.getenv("CONF_CAL_GATE_GROUP_MAX_BRIER_UP", "0.004"))
    grp_max_ece_up = float(os.getenv("CONF_CAL_GATE_GROUP_MAX_ECE_UP", "0.02"))

    # --- aggregated group gate (надежнее, чем "count fails") ---
    # Включено по умолчанию.
    agg_enable = str(os.getenv("CONF_CAL_GATE_GROUP_AGG_ENABLE", "1") or "1").strip().lower() not in ("0", "false", "no", "off")
    agg_q = float(os.getenv("CONF_CAL_GATE_GROUP_AGG_Q", "0.90"))
    agg_min_total_val = int(os.getenv("CONF_CAL_GATE_GROUP_AGG_MIN_TOTAL_VAL", "2000"))
    # Пороги для weighted mean
    agg_wmean_brier_up = float(os.getenv("CONF_CAL_GATE_GROUP_WMEAN_MAX_BRIER_UP", "0.0025"))
    agg_wmean_ece_up = float(os.getenv("CONF_CAL_GATE_GROUP_WMEAN_MAX_ECE_UP", "0.012"))
    # Пороги для weighted quantile (обычно выше mean)
    agg_wq_brier_up = float(os.getenv("CONF_CAL_GATE_GROUP_WQ_MAX_BRIER_UP", "0.006"))
    agg_wq_ece_up = float(os.getenv("CONF_CAL_GATE_GROUP_WQ_MAX_ECE_UP", "0.03"))

    # hard caps: если совсем плохо — сразу reject независимо от allow-fails
    grp_hard_brier_up = float(os.getenv("CONF_CAL_GATE_GROUP_HARD_BRIER_UP", "0.02"))
    grp_hard_ece_up = float(os.getenv("CONF_CAL_GATE_GROUP_HARD_ECE_UP", "0.08"))

    # допускаем небольшой шум: 1 группа или до 20% от K
    allow_fails_abs = int(os.getenv("CONF_CAL_GATE_GROUP_ALLOW_FAILS", "1"))
    allow_fails_frac = float(os.getenv("CONF_CAL_GATE_GROUP_ALLOW_FAIL_FRAC", "0.2"))

    delta_groups = (report.get("delta_groups", {}) or {})
    cand_groups = (report.get("groups", {}) or {})

    def _get_n_val(key: str) -> int:
        g = cand_groups.get(key, {}) or {}
        try:
            return int(float(g.get("n_val", 0.0) or 0.0))
        except Exception:
            return 0

    def _get_delta(key: str) -> Tuple[float, float]:
        dg = delta_groups.get(key, {}) or {}
        return (
            _safe_float(dg.get("brier", float("nan")), float("nan")),
            _safe_float(dg.get("ece", float("nan")), float("nan")),
        )

    def _has_baseline(key: str) -> bool:
        db, de = _get_delta(key)
        return _isfinite(db) and _isfinite(de)

    def _eval_global() -> Tuple[bool, str, Dict[str, Any]]:
        n_val = _get_n_val("global")
        db, de = _get_delta("global")

        if n_val < min_val_global:
            return True, "pass_low_val_global", {"n_val": float(n_val)}
        if not (_isfinite(db) and _isfinite(de)):
            return True, "pass_no_baseline_global", {"n_val": float(n_val)}

        if mode == "strict":
            if db > (-min_brier_improve):
                return False, "reject_global_no_brier_improve", {"d_brier": db, "d_ece": de, "n_val": float(n_val)}
            if de > max_ece_up:
                return False, "reject_global_ece_regress", {"d_brier": db, "d_ece": de, "n_val": float(n_val)}
            return True, "pass_global_strict", {"d_brier": db, "d_ece": de, "n_val": float(n_val)}

        # soft
        if db > max_brier_up:
            return False, "reject_global_brier_regress", {"d_brier": db, "d_ece": de, "n_val": float(n_val)}
        if de > max_ece_up:
            return False, "reject_global_ece_regress", {"d_brier": db, "d_ece": de, "n_val": float(n_val)}
        return True, "pass_global_soft", {"d_brier": db, "d_ece": de, "n_val": float(n_val)}

    def _iter_candidate_keys() -> Iterable[str]:
        # ключи формата kind:...|symbol:...
        for k in delta_groups.keys():
            if k and k != "global":
                yield str(k)

    def _select_topk() -> List[str]:
        items: List[Tuple[int, str]] = []
        for k in _iter_candidate_keys():
            n_val = _get_n_val(k)
            if n_val < min_val_group:
                continue
            if not _has_baseline(k):
                continue
            items.append((n_val, k))
        items.sort(key=lambda t: t[0], reverse=True)
        if topk > 0:
            items = items[:topk]
        return [k for _, k in items]

    def _eval_group(key: str) -> Tuple[bool, str, Dict[str, Any]]:
        n_val = _get_n_val(key)
        db, de = _get_delta(key)
        if n_val < min_val_group:
            return True, "skip_low_val_group", {"key": key, "n_val": float(n_val)}
        if not (_isfinite(db) and _isfinite(de)):
            return True, "skip_no_baseline_group", {"key": key, "n_val": float(n_val)}

        # hard caps (немедленный reject)
        if db > grp_hard_brier_up:
            return False, "reject_group_hard_brier", {"key": key, "n_val": float(n_val), "d_brier": db, "d_ece": de}
        if de > grp_hard_ece_up:
            return False, "reject_group_hard_ece", {"key": key, "n_val": float(n_val), "d_brier": db, "d_ece": de}

        # soft caps for groups
        if db > grp_max_brier_up:
            return False, "reject_group_brier_regress", {"key": key, "n_val": float(n_val), "d_brier": db, "d_ece": de}
        if de > grp_max_ece_up:
            return False, "reject_group_ece_regress", {"key": key, "n_val": float(n_val), "d_brier": db, "d_ece": de}
        return True, "pass_group_soft", {"key": key, "n_val": float(n_val), "d_brier": db, "d_ece": de}

    # 1) global gate must pass
    ok_g, reason_g, det_g = _eval_global()
    if not ok_g:
        return False, reason_g, {"global": det_g}

    # 2) group gate top-K
    keys = _select_topk()
    group_results: List[Dict[str, Any]] = []
    hard_reject = False
    fails = 0
    # пары (delta, n_val) для агрегатных проверок
    pairs_db: List[Tuple[float, float]] = []
    pairs_de: List[Tuple[float, float]] = []
    total_val = 0.0
    for k in keys:
        ok, r, d = _eval_group(k)
        d2 = dict(d)
        d2["ok"] = bool(ok)
        d2["reason"] = str(r)
        group_results.append(d2)
        # собираем только те, у кого есть baseline и n_val >= min_val_group
        try:
            n_val = float(d.get("n_val", 0.0) or 0.0)
        except Exception:
            n_val = 0.0
        db, de = _get_delta(k)
        if n_val >= float(min_val_group) and _isfinite(db) and _isfinite(de):
            pairs_db.append((float(db), float(n_val)))
            pairs_de.append((float(de), float(n_val)))
            total_val += float(n_val)
        if not ok:
            fails += 1
            if "hard" in str(r):
                hard_reject = True

    allow = max(int(allow_fails_abs), int(float(allow_fails_frac) * float(len(keys) or 0)))

    if hard_reject:
        return False, "reject_groups_hard", {"global": det_g, "topk": keys, "fails": fails, "allow": allow, "groups": group_results}

    # 2.05) top-M worst-case gate (самые массовые группы)
    # Более жёсткий и "точный": гарантируем, что ни один из крупнейших сегментов не регрессировал заметно.
    topm_enable = str(os.getenv("CONF_CAL_GATE_GROUP_TOPM_ENABLE", "1") or "1").strip().lower() not in ("0", "false", "no", "off")
    topm = int(os.getenv("CONF_CAL_GATE_GROUP_TOPM", "3"))  # default: 3 (точнее чем 5)
    topm_min_val_group = int(os.getenv("CONF_CAL_GATE_GROUP_TOPM_MIN_VAL_GROUP", str(min_val_group)))
    topm_max_brier_up = float(os.getenv("CONF_CAL_GATE_GROUP_TOPM_MAX_BRIER_UP", "0.0035"))
    topm_max_ece_up = float(os.getenv("CONF_CAL_GATE_GROUP_TOPM_MAX_ECE_UP", "0.018"))
    topm_hard_brier_up = float(os.getenv("CONF_CAL_GATE_GROUP_TOPM_HARD_BRIER_UP", "0.02"))
    topm_hard_ece_up = float(os.getenv("CONF_CAL_GATE_GROUP_TOPM_HARD_ECE_UP", "0.08"))

    topm_details: Dict[str, Any] = {
        "enabled": bool(topm_enable),
        "topm": int(topm),
        "min_val_group": int(topm_min_val_group),
        "thr_brier": float(topm_max_brier_up),
        "thr_ece": float(topm_max_ece_up),
        "hard_brier": float(topm_hard_brier_up),
        "hard_ece": float(topm_hard_ece_up),
    }

    if topm_enable and topm > 0:
        # берём только группы, где:
        # - есть baseline (delta посчитана)
        # - n_val достаточно большой (иначе шум)
        cand = []
        for gr in group_results:
            try:
                n_val = float(gr.get("n_val", 0.0) or 0.0)
            except Exception:
                n_val = 0.0
            # _eval_group кладёт d_brier/d_ece в d (через _get_delta), мы копируем в group_results
            try:
                d_brier = float(gr.get("d_brier", float("nan")))
                d_ece = float(gr.get("d_ece", float("nan")))
            except Exception:
                d_brier, d_ece = float("nan"), float("nan")
            if n_val < float(topm_min_val_group):
                continue
            if not _isfinite(d_brier) or not _isfinite(d_ece):
                continue
            # key может быть в разных полях, поэтому аккуратно:
            k = str(gr.get("key") or gr.get("group") or gr.get("name") or gr.get("id") or "")
            cand.append((n_val, d_brier, d_ece, k))

        cand.sort(key=lambda t: t[0], reverse=True)
        picked = cand[:topm]
        topm_details["picked"] = [
            {"n_val": float(n), "d_brier": float(db), "d_ece": float(de), "key": str(k)}
            for (n, db, de, k) in picked
        ]
        if picked:
            worst_db = max(db for _, db, _, _ in picked)
            worst_de = max(de for _, _, de, _ in picked)
            topm_details["worst_d_brier"] = float(worst_db)
            topm_details["worst_d_ece"] = float(worst_de)

            # hard reject
            if worst_db > topm_hard_brier_up:
                return False, "reject_groups_topm_hard_brier", {"global": det_g, "topk": keys, "fails": fails, "allow": allow, "topm": topm_details, "groups": group_results}
            if worst_de > topm_hard_ece_up:
                return False, "reject_groups_topm_hard_ece", {"global": det_g, "topk": keys, "fails": fails, "allow": allow, "topm": topm_details, "groups": group_results}

            # soft reject (жёстче, чем allow-fails)
            if worst_db > topm_max_brier_up:
                return False, "reject_groups_topm_worst_brier", {"global": det_g, "topk": keys, "fails": fails, "allow": allow, "topm": topm_details, "groups": group_results}
            if worst_de > topm_max_ece_up:
                return False, "reject_groups_topm_worst_ece", {"global": det_g, "topk": keys, "fails": fails, "allow": allow, "topm": topm_details, "groups": group_results}
        else:
            topm_details["skipped"] = True
            topm_details["reason"] = "no_eligible_groups_for_topm"

    # 2.1) aggregated gate (weighted mean + weighted quantile)
    agg_details: Dict[str, Any] = {"enabled": bool(agg_enable), "q": float(agg_q), "total_val": float(total_val)}
    if agg_enable and total_val >= float(agg_min_total_val):
        wmean_db = _weighted_mean(pairs_db)
        wmean_de = _weighted_mean(pairs_de)
        wq_db = _weighted_quantile(pairs_db, agg_q)
        wq_de = _weighted_quantile(pairs_de, agg_q)
        agg_details.update({
            "wmean_d_brier": float(wmean_db),
            "wmean_d_ece": float(wmean_de),
            "wq_d_brier": float(wq_db),
            "wq_d_ece": float(wq_de),
            "min_total_val": float(agg_min_total_val),
            "wmean_thr_brier": float(agg_wmean_brier_up),
            "wmean_thr_ece": float(agg_wmean_ece_up),
            "wq_thr_brier": float(agg_wq_brier_up),
            "wq_thr_ece": float(agg_wq_ece_up),
        })
        # reject if aggregated regressions exceed thresholds
        if _isfinite(wmean_db) and wmean_db > agg_wmean_brier_up:
            return False, "reject_groups_weighted_mean_brier", {"global": det_g, "topk": keys, "fails": fails, "allow": allow, "topm": topm_details, "agg": agg_details, "groups": group_results}
        if _isfinite(wmean_de) and wmean_de > agg_wmean_ece_up:
            return False, "reject_groups_weighted_mean_ece", {"global": det_g, "topk": keys, "fails": fails, "allow": allow, "topm": topm_details, "agg": agg_details, "groups": group_results}
        if _isfinite(wq_db) and wq_db > agg_wq_brier_up:
            return False, "reject_groups_weighted_quantile_brier", {"global": det_g, "topk": keys, "fails": fails, "allow": allow, "topm": topm_details, "agg": agg_details, "groups": group_results}
        if _isfinite(wq_de) and wq_de > agg_wq_ece_up:
            return False, "reject_groups_weighted_quantile_ece", {"global": det_g, "topk": keys, "fails": fails, "allow": allow, "topm": topm_details, "agg": agg_details, "groups": group_results}
    else:
        agg_details["skipped"] = True
        agg_details["reason"] = "low_total_val" if total_val < float(agg_min_total_val) else "disabled"

    if fails > allow:
        return False, "reject_groups_too_many_regressions", {"global": det_g, "topk": keys, "fails": fails, "allow": allow, "topm": topm_details, "agg": agg_details, "groups": group_results}

    return True, "pass_global_plus_topk", {"global": det_g, "topk": keys, "fails": fails, "allow": allow, "topm": topm_details, "agg": agg_details, "groups": group_results}


def _weighted_mean(pairs):
    # pairs: list[(value, weight)]
    sw = 0.0
    s = 0.0
    for v, w in pairs:
        try:
            vv = float(v); ww = float(w)
        except Exception:
            continue
        if ww <= 0:
            continue
        if not _isfinite(vv) or not _isfinite(ww):
            continue
        sw += ww
        s += vv * ww
    if sw <= 1e-12:
        return float("nan")
    return s / sw


def _worst_topm_weighted_mean(pairs, m: int):
    """
    Берём top-M по наибольшей delta (хуже всего), затем weighted mean.
    M=3 по умолчанию (строже).
    """
    mm = max(1, int(m))
    clean = []
    for d, w in pairs:
        try:
            dd = float(d); ww = float(w)
        except Exception:
            continue
        if ww <= 0:
            continue
        if not _isfinite(dd) or not _isfinite(ww):
            continue
        clean.append((dd, ww))
    if not clean:
        return float("nan")
    clean.sort(key=lambda t: t[0], reverse=True)
    top = clean[:mm]
    return _weighted_mean(top)


def _backup_to_history(path: str, history_dir: str, keep: int = 50) -> str | None:
    """
    Сохраняем предыдущую версию калибровки в history/, затем ротация keep.
    Возвращает путь к backup или None.
    """
    p = Path(str(path))
    if not p.exists():
        return None
    hd = Path(str(history_dir))
    hd.mkdir(parents=True, exist_ok=True)
    ts = int(time.time())
    dst = hd / f"{ts}_confidence_calibration.json"
    try:
        shutil.copy2(str(p), str(dst))
    except Exception:
        return None
    # rotate
    try:
        files = sorted([x for x in hd.glob("*_confidence_calibration.json")], key=lambda x: x.name, reverse=True)
        for x in files[int(keep):]:
            try: x.unlink()
            except Exception: pass
    except Exception:
        pass
    return str(dst)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dsn", default=_env("PERF_PG_DSN", "TRADES_DB_DSN", default=""))
    ap.add_argument("--out", default=_env("CONF_CAL_PATH", default=""))
    ap.add_argument("--state", default=_env("CONF_CAL_STATE_PATH", default="calibration/confidence_calibration.state.json"))
    ap.add_argument("--since", default=_env("CONF_CAL_SINCE", default="2024-01-01T00:00:00Z"))
    ap.add_argument("--min-new", type=int, default=int(os.getenv("CONF_CAL_MIN_NEW_SAMPLES", "300")))
    ap.add_argument("--min-samples", type=int, default=int(os.getenv("CONF_CAL_MIN_SAMPLES", "300")))
    ap.add_argument("--val-days", type=int, default=int(os.getenv("CONF_CAL_VAL_DAYS", "14")))
    ap.add_argument("--force", action="store_true")
    ap.add_argument("--history", default=_env("CONF_CAL_HISTORY_DIR", default="calibration/history"))
    ap.add_argument("--rejects", default=_env("CONF_CAL_REJECT_DIR", default="calibration/history/rejected"))
    args = ap.parse_args()

    if not args.dsn:
        raise SystemExit("DSN empty (PERF_PG_DSN/TRADES_DB_DSN)")
    if not args.out:
        raise SystemExit("CONF_CAL_PATH/--out empty")

    st = _load_state(str(args.state))

    # если state пустой, попробуем взять max_ts_epoch из текущей калибровки
    if st.max_ts_epoch <= 0.0 and os.path.exists(str(args.out)):
        st.max_ts_epoch = float(_read_calib_max_ts(str(args.out)) or 0.0)

    conn = psycopg2.connect(str(args.dsn))
    conn.autocommit = True

    new_n = _count_new_eligible(conn, since_ts_epoch=float(st.max_ts_epoch))
    conn.close()

    if (not args.force) and int(new_n) < int(args.min_new):
        print(json.dumps({"action": "skip", "reason": "not_enough_new_samples", "new_eligible": int(new_n), "min_new": int(args.min_new)}))
        return

    # staging train (не трогаем текущий out)
    out_path = str(args.out)
    cand_path = out_path + ".cand"
    baseline_path = out_path if os.path.exists(out_path) else None

    rep = train(
        dsn=str(args.dsn),
        since=str(args.since),
        out_path=str(cand_path),
        min_samples=int(args.min_samples),
        val_days=int(args.val_days),
        mode=str(os.getenv("CONF_CAL_ISO_MODE", "linear")),
        baseline_path=baseline_path,
    )

    passed, reason, details = gate_decision(rep)

    hist_dir = str(args.history)
    rej_dir = str(args.rejects)
    _ensure_dir(hist_dir)
    _ensure_dir(rej_dir)

    cand_trained_at = _read_trained_at(cand_path)
    cand_report_path = cand_path + ".report.json"

    if not passed:
        # сохраняем rejected candidate
        rname = _history_name("confidence_calibration", cand_trained_at or int(time.time()))
        dst = os.path.join(rej_dir, rname)
        try:
            os.replace(cand_path, dst)
        except Exception:
            pass
        try:
            if os.path.exists(cand_report_path):
                os.replace(cand_report_path, dst + ".report.json")
        except Exception:
            pass
        _cleanup_keep_last(rej_dir, "confidence_calibration", int(os.getenv("CONF_CAL_REJECT_KEEP", "20")))
        print(json.dumps({"action": "reject", "reason": reason, "details": details, "new_eligible": int(new_n)}, ensure_ascii=False))
        return

    # promote: архивируем baseline -> history, затем заменяем out_path
    if baseline_path and os.path.exists(baseline_path):
        base_trained_at = _read_trained_at(baseline_path)
        base_name = _history_name("confidence_calibration", base_trained_at or int(time.time()))
        base_dst = os.path.join(hist_dir, base_name)
        try:
            os.replace(baseline_path, base_dst)
        except Exception:
            # fail-open: если rename не вышел — не блокируем промоут
            pass
        try:
            base_rep = baseline_path + ".report.json"
            if os.path.exists(base_rep):
                os.replace(base_rep, base_dst + ".report.json")
        except Exception:
            pass

    # ставим candidate как текущий
    os.replace(cand_path, out_path)
    if os.path.exists(cand_report_path):
        os.replace(cand_report_path, out_path + ".report.json")

    # сохраняем promoted в history (копией из out_path)
    new_name = _history_name("confidence_calibration", cand_trained_at or int(time.time()))
    new_dst = os.path.join(hist_dir, new_name)
    try:
        # copy bytes
        with open(out_path, "rb") as fsrc:
            with open(new_dst, "wb") as fdst:
                fdst.write(fsrc.read())
        rep_src = out_path + ".report.json"
        if os.path.exists(rep_src):
            with open(rep_src, "rb") as fsrc:
                with open(new_dst + ".report.json", "wb") as fdst:
                    fdst.write(fsrc.read())
    except Exception:
        pass

    _cleanup_keep_last(hist_dir, "confidence_calibration", int(os.getenv("CONF_CAL_HISTORY_KEEP", "60")))

    # обновляем state по факту записанного max_ts_epoch
    st.trained_at = int(rep.get("trained_at", int(time.time())) or int(time.time()))
    st.max_ts_epoch = float(rep.get("max_ts_epoch", 0.0) or 0.0)
    _save_state_atomic(str(args.state), st)

    print(json.dumps({
        "action": "promote",
        "reason": reason,
        "details": details,
        "new_eligible": int(new_n),
        "state": {"trained_at": st.trained_at, "max_ts_epoch": st.max_ts_epoch},
        "history_dir": hist_dir,
    }, ensure_ascii=False))


if __name__ == "__main__":
    # Check for loop args before calling main which has its own argparse
    import sys
    if "--loop" in sys.argv:
        # custom loop wrapper
        interval = 21600
        try:
            full_args = list(sys.argv)
            if "--interval" in full_args:
                idx = full_args.index("--interval")
                interval = int(full_args[idx+1])
                del full_args[idx:idx+2]
            full_args.remove("--loop")
            sys.argv = full_args
        except Exception:
            pass
            
        print(f"Starting auto_train_conf_calibration loop, interval={interval}s")
        while True:
            try:
                # reload sys.argv for inner argparse if needed, or just call main
                # main() uses sys.argv/argparse internally
                # we need to be careful as main() might not return or might clean up
                # standard approach: invoke main
                main()
            except SystemExit:
                pass
            except Exception as e:
                print(f"auto_train loop error: {e}")
            time.sleep(interval)
    else:
        main()