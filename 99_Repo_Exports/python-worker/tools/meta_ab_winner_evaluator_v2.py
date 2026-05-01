from __future__ import annotations
"""Stage 4 next: AB-winner evaluator v2 (deployable p_min, stratified + bootstrap CI).

Why v2:
- v1 scans thresholds per-model (great for research), but runtime is deployable at a single p_min.
- v2 compares champion vs challenger under the SAME p_min (what you actually run).
- Adds:
  - bootstrap CI for delta expR and delta tail-rate-per-candidate
  - stratified worst-case guard (symbol/scenario/session buckets)
  - optional respect of freeze/caps (META_FREEZE_FILE, if present)

Dataset expectations:
- parquet or ndjson
- at least: y, r_mult, ok (eligible: ok==1)
- plus columns required by MetaModelLR features

Outputs JSON report + optional apply to Redis dynamic config.

ENV vars (all have defaults):
  META_P_MIN              — deployable threshold (default 0.55)
  META_AB_MIN_ELIGIBLE    — min eligible rows (default 1000)
  META_AB_MIN_DELTA_EXPR  — min expR delta per candidate (default 0.002)
  META_AB_TAIL_R          — tail event r_mult threshold (default -1.0)
  META_AB_TAIL_SLACK      — max Δ tail per candidate (default 0.01)
  META_AB_BOOTSTRAP       — 1=enable bootstrap CI, 0=skip (default 1)
  META_AB_BOOT_N          — bootstrap iterations (default 400)
  META_AB_BOOT_ALPHA      — CI significance level (default 0.05)
  META_AB_BOOT_SEED       — RNG seed (default 7)
  META_AB_REQUIRE_CI_POSITIVE — 1=require CI_lo(ΔexpR)>0 (default 1)
  META_AB_STRATA          — comma-sep strata columns (default 'symbol')
  META_AB_STRATA_TOPK     — top-K strata to check (default 10)
  META_AB_APPLY           — 1=write to Redis (default 0)
  META_AB_CURRENT_SHARE   — current challenger share (default 0.0)
  META_AB_RAMP_STEP       — share step per evaluation (default 0.05)
  META_AB_MAX_SHARE       — max challenger share (default 0.50)
  REDIS_URL               — Redis connection URL
  DYNAMIC_CFG_HASH        — Redis hash for dynamic config (default 'settings:dynamic_cfg')
  NOTIFY_TELEGRAM_STREAM  — Redis stream for Telegram notifications
"""

from utils.time_utils import get_ny_time_millis

import argparse
import json
import math
import os
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

try:
    import numpy as np
    import pandas as pd
except Exception as e:
    raise SystemExit("Missing deps. Install: pip install numpy pandas") from e

from core.bootstrap_ci import bootstrap_mean_diff, bootstrap_rate_diff


def now_ms() -> int:
    """Returns current UTC time in milliseconds since epoch."""
    return get_ny_time_millis()


def _env_float(name: str, default: float) -> float:
    """Read ENV var as float with fallback to default."""
    try:
        return float(os.getenv(name, str(default)).strip())
    except Exception:
        return float(default)


def _env_int(name: str, default: int) -> int:
    """Read ENV var as int with fallback to default."""
    try:
        return int(float(os.getenv(name, str(default)).strip()))
    except Exception:
        return int(default)


def _safe_float(x: Any, default: float = 0.0) -> float:
    """Safely convert any value to float with NaN/None handling."""
    try:
        if x is None:
            return float(default)
        if isinstance(x, (int, float, np.number)):
            return float(x)
        s = str(x).strip()
        if not s:
            return float(default)
        return float(s)
    except Exception:
        return float(default)


def load_dataset(in_parquet: Optional[str], in_ndjson: Optional[str], limit_rows: int = 0) -> pd.DataFrame:
    """Load evaluation dataset from parquet or ndjson.

    Exactly one of in_parquet/in_ndjson must be provided.
    limit_rows=0 means no limit.
    """
    if bool(in_parquet) == bool(in_ndjson):
        raise SystemExit("exactly_one_input_required: provide --in-parquet OR --in-ndjson")

    if in_parquet:
        df = pd.read_parquet(in_parquet)
        if limit_rows and len(df) > limit_rows:
            df = df.iloc[:limit_rows].copy()
        return df

    rows: List[Dict[str, Any]] = []
    with open(in_ndjson, "r", encoding="utf-8") as f:
        for line in f:
            s = line.strip()
            if not s:
                continue
            try:
                rows.append(json.loads(s))
            except Exception:
                continue
            if limit_rows and len(rows) >= limit_rows:
                break
    df = pd.DataFrame(rows)

    # Auto-join with outcomes ndjson when y/r_mult are missing (confirm_train_v7 format)
    if ("r_mult" not in df.columns or "y" not in df.columns) and "sid" in df.columns:
        _dir = os.path.dirname(in_ndjson)
        outcomes_path = os.path.join(_dir, "latest_outcomes.ndjson")
        if os.path.isfile(outcomes_path):
            out_rows: List[Dict[str, Any]] = []
            with open(outcomes_path, "r", encoding="utf-8") as fo:
                for line in fo:
                    s = line.strip()
                    if s:
                        try:
                            out_rows.append(json.loads(s))
                        except Exception:
                            pass
            if out_rows:
                df_out = pd.DataFrame(out_rows)
                if "sid" in df_out.columns:
                    df_out = df_out.rename(columns={
                        c: f"outcome_{c}" for c in df_out.columns if c != "sid"
                    })
                    df = df.merge(df_out, on="sid", how="inner")
                    _pnl = pd.to_numeric(df.get("outcome_pnl", 0), errors="coerce").fillna(0.0)
                    _risk = pd.to_numeric(df.get("outcome_risk_usd", 0), errors="coerce").fillna(1.0).replace(0.0, 1.0)
                    df["r_mult"] = _pnl / _risk
                    df["y"] = (df["r_mult"] > 0).astype(int)
                    df["ok"] = 1

    return df


def _load_meta_model(path: str):
    """Load a MetaModelLR from a JSON file path."""
    from core.meta_model_lr import MetaModelLR

    return MetaModelLR.load(path)


def score_model_proba(model: Any, df: pd.DataFrame) -> np.ndarray:
    """Score all rows in df using model.predict_proba(feat_dict).

    Returns np.ndarray of shape (len(df),) with probabilities in [0, 1].
    Missing feature columns are treated as 0.0 (fail-safe default).
    """
    feats: List[str] = list(getattr(model, "features", []))
    if not feats:
        feats = [c for c in df.columns if c not in ("y", "r_mult", "ok")]

    cols_present = {c for c in feats if c in df.columns}
    p = np.zeros((len(df),), dtype=float)

    for i, row in enumerate(df.itertuples(index=False)):
        feat_dict: Dict[str, float] = {}
        for c in feats:
            if c in cols_present:
                feat_dict[c] = _safe_float(getattr(row, c), 0.0)
            else:
                # Feature not in dataset → use 0.0 (same as training imputation)
                feat_dict[c] = 0.0
        try:
            p[i] = float(model.predict_proba(feat_dict))
        except Exception:
            p[i] = 0.5  # fail-open to neutral on scoring error
    return np.clip(p, 0.0, 1.0)


@dataclass(frozen=True)
class V2Config:
    """Full configuration for evaluate_v2.

    All fields have production-safe defaults. Override via CLI args or ENV vars.
    """

    label_col: str = "y"
    r_col: str = "r_mult"
    ok_col: str = "ok"

    # Deployable threshold — MUST be same as runtime p_min to avoid false winners
    p_min: float = 0.55

    # Minimum eligible rows to produce a non-tie result
    min_n: int = 1000

    # Tail indicator: executed AND r <= tail_r (a bad trade)
    tail_r: float = -1.0
    tail_slack: float = 0.01  # Δ tail_rate per candidate must be <= slack

    # Minimum expR improvement per candidate to declare challenger winner
    min_delta_exp_r: float = 0.002

    # Bootstrap CI knobs
    bootstrap: int = 1
    boot_n: int = 400
    boot_alpha: float = 0.05
    boot_seed: int = 7
    require_ci_positive: int = 1  # 1=require CI_lo(ΔexpR) > 0

    # Stratification: top-K strata by size are checked for worst-case guard
    strata_cols: Tuple[str, ...] = ("symbol",)
    strata_topk: int = 10

    # Ramp knobs
    current_share: float = 0.0
    ramp_step: float = 0.05
    max_share: float = 0.50


def _read_freeze_max_share() -> Optional[float]:
    """Read max_ab_share cap from freeze file if present.

    Integrates with stage4 meta freeze guard (core.meta_freeze_file).
    Fail-open: returns None if module/state unavailable.
    """
    try:
        from core.meta_freeze_file import get_meta_freeze_state

        st = get_meta_freeze_state()
        if isinstance(st, dict):
            v = st.get("max_ab_share")
            if v is not None:
                return float(v)
    except Exception:
        pass
    return None


def _policy_vectors(
    r: np.ndarray,
    p: np.ndarray,
    p_min: float,
    tail_r: float,
) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
    """Compute per-candidate policy contribution vectors.

    Returns:
        contrib — r_mult where model fires (p >= p_min), else 0.0
        tail    — 1 where fires AND r <= tail_r (bad trade), else 0
        allow   — bool mask where model fires
    """
    allow = p >= float(p_min)
    contrib = np.where(allow, r, 0.0)    # per-candidate contribution
    tail = np.where(allow & (r <= float(tail_r)), 1, 0)  # per-candidate tail event
    return contrib.astype(float), tail.astype(int), allow.astype(bool)


def _metrics_from_vectors(
    contrib: np.ndarray,
    tail: np.ndarray,
    allow: np.ndarray,
) -> Dict[str, Any]:
    """Compute policy metrics from pre-computed policy vectors.

    All per-candidate rates use the full dataset as denominator (not just
    allowed trades), which ensures fair comparison independent of allow_rate.
    """
    n = int(len(contrib))
    allow_n = int(np.sum(allow))
    out: Dict[str, Any] = {
        "n": n,
        "allow_n": allow_n,
        "allow_rate": float(allow_n / n) if n > 0 else 0.0,
        # Key KPIs: denominator=n (total), not allow_n
        "exp_r_per_candidate": float(np.mean(contrib)) if n > 0 else float("nan"),
        "tail_rate_per_candidate": float(np.mean(tail)) if n > 0 else float("nan"),
    }
    # Conditional metrics (only within allowed candidates)
    if allow_n > 0:
        # contrib[allow] equals r[allow] since contrib=r when allow=True
        r_allow = contrib[allow]
        out["mean_r_allowed"] = float(np.mean(r_allow))
        out["win_rate_allowed"] = float(np.mean(r_allow > 0))
    else:
        out["mean_r_allowed"] = None
        out["win_rate_allowed"] = None
    return out


def _stratum_key(row: Any, cols: Tuple[str, ...]) -> str:
    """Build a string stratum key from named row attributes."""
    if not cols:
        return "ALL"
    parts = []
    for c in cols:
        try:
            v = getattr(row, c)
        except Exception:
            v = None
        if v is None or v == "":
            parts.append(f"{c}=NA")
        else:
            parts.append(f"{c}={str(v)}")
    return "|".join(parts)


def evaluate_v2(df: pd.DataFrame, champ_model: Any, chal_model: Any, cfg: V2Config) -> Dict[str, Any]:
    """Compare champion vs challenger at fixed p_min with CI and stratified guard.

    Decision logic (risk-aware):
      challenger wins  → exp_ok AND tail_ok AND ci_ok AND ci_tail_ok AND NOT strata_bad
      champion wins    → challenger exp_r is clearly lower OR tail is clearly worse
      tie (hold)       → everything else (insufficient evidence, CI not positive, strata bad)

    Returns a JSON-serializable report dict.
    """
    # Filter to eligible rows only (ok==1 means the trade was executed/counterfactual)
    if cfg.ok_col in df.columns:
        df_el = df[df[cfg.ok_col].astype(int) == 1].copy()
    else:
        df_el = df.copy()

    if cfg.label_col not in df_el.columns or cfg.r_col not in df_el.columns:
        raise SystemExit("missing_required_columns")

    df_el = df_el[df_el[cfg.label_col].notna() & df_el[cfg.r_col].notna()].copy()
    n = int(len(df_el))

    rep: Dict[str, Any] = {
        "ts_ms": now_ms(),
        "counts": {"n_total": int(len(df)), "n_eligible": n},
        "config": {
            "p_min": float(cfg.p_min),
            "min_n": int(cfg.min_n),
            "tail_r": float(cfg.tail_r),
            "tail_slack": float(cfg.tail_slack),
            "min_delta_exp_r": float(cfg.min_delta_exp_r),
            "bootstrap": int(cfg.bootstrap),
            "boot_n": int(cfg.boot_n),
            "boot_alpha": float(cfg.boot_alpha),
            "boot_seed": int(cfg.boot_seed),
            "require_ci_positive": int(cfg.require_ci_positive),
            "strata_cols": list(cfg.strata_cols),
            "strata_topk": int(cfg.strata_topk),
        },
    }

    if n == 0:
        rep["winner"] = "tie"
        rep["reason"] = "no_eligible_data"
        return rep

    if n < cfg.min_n:
        rep["winner"] = "tie"
        rep["reason"] = f"insufficient_data n={n} < min_n={cfg.min_n}"
        return rep

    r = df_el[cfg.r_col].astype(float).to_numpy(dtype=float)

    # Score both models on the same dataset (fixed p_min = same deployable threshold)
    p_champ = score_model_proba(champ_model, df_el)
    p_chal = score_model_proba(chal_model, df_el)

    champ_contrib, champ_tail, champ_allow = _policy_vectors(r, p_champ, cfg.p_min, cfg.tail_r)
    chal_contrib, chal_tail, chal_allow = _policy_vectors(r, p_chal, cfg.p_min, cfg.tail_r)

    champ_m = _metrics_from_vectors(champ_contrib, champ_tail, champ_allow)
    chal_m = _metrics_from_vectors(chal_contrib, chal_tail, chal_allow)

    delta_exp_r = float(chal_m["exp_r_per_candidate"] - champ_m["exp_r_per_candidate"])
    delta_tail = float(chal_m["tail_rate_per_candidate"] - champ_m["tail_rate_per_candidate"])

    rep["champion"] = champ_m
    rep["challenger"] = chal_m
    rep["delta"] = {"exp_r_per_candidate": delta_exp_r, "tail_rate_per_candidate": delta_tail}

    # --- Bootstrap CI on deltas (optional but strongly recommended) ---
    ci: Dict[str, Any] = {}
    if int(cfg.bootstrap) == 1:
        # CI for Δ expR (challenger − champion contribution vectors)
        ci_er = bootstrap_mean_diff(
            chal_contrib.tolist(),
            champ_contrib.tolist(),
            n_boot=int(cfg.boot_n),
            alpha=float(cfg.boot_alpha),
            seed=int(cfg.boot_seed),
        )
        # CI for Δ tail rate (separate seed to avoid correlation)
        ci_tail = bootstrap_rate_diff(
            chal_tail.tolist(),
            champ_tail.tolist(),
            n_boot=int(cfg.boot_n),
            alpha=float(cfg.boot_alpha),
            seed=int(cfg.boot_seed) + 1,
        )
        ci = {
            "delta_exp_r": {"mean": ci_er.mean, "lo": ci_er.lo, "hi": ci_er.hi},
            "delta_tail": {"mean": ci_tail.mean, "lo": ci_tail.lo, "hi": ci_tail.hi},
        }
    rep["ci"] = ci

    # --- Stratified worst-case checks (top strata by n) ---
    # Purpose: prevent a "global winner" that actually harms specific symbols/sessions
    strata = []
    if cfg.strata_cols:
        keys: List[str] = []
        for row in df_el.itertuples(index=False):
            keys.append(_stratum_key(row, cfg.strata_cols))
        # map key -> row indices
        by: Dict[str, List[int]] = {}
        for i, k in enumerate(keys):
            by.setdefault(k, []).append(i)

        # take largest strata by n (most data = most reliable worst-case estimate)
        items = sorted(by.items(), key=lambda kv: len(kv[1]), reverse=True)
        for k, idxs in items[: max(1, int(cfg.strata_topk))]:
            idx = np.array(idxs, dtype=int)
            c_contrib = champ_contrib[idx]
            s_contrib = chal_contrib[idx]
            c_tail = champ_tail[idx]
            s_tail = chal_tail[idx]
            d_er = float(np.mean(s_contrib) - np.mean(c_contrib))
            d_tail = float(np.mean(s_tail) - np.mean(c_tail))
            strata.append({"stratum": k, "n": int(len(idxs)), "delta_exp_r": d_er, "delta_tail": d_tail})
    rep["strata_top"] = strata

    # --- Winner decision (risk-aware, CI-aware, strata-aware) ---
    exp_ok = delta_exp_r >= float(cfg.min_delta_exp_r)
    tail_ok = delta_tail <= float(cfg.tail_slack)

    # CI gates (skipped if bootstrap disabled or require_ci_positive=0)
    ci_ok = True
    ci_tail_ok = True
    if int(cfg.bootstrap) == 1 and int(cfg.require_ci_positive) == 1 and ci:
        ci_ok = float(ci["delta_exp_r"]["lo"]) > 0.0
        ci_tail_ok = float(ci["delta_tail"]["hi"]) <= float(cfg.tail_slack)

    # Strata guard: block if any top stratum is BOTH worse in expR AND worse in tail
    strata_bad = any(
        (s["delta_exp_r"] < -float(cfg.min_delta_exp_r)) and (s["delta_tail"] > float(cfg.tail_slack))
        for s in strata
    )

    winner = "tie"
    reason = "no_strong_evidence"
    if exp_ok and tail_ok and ci_ok and ci_tail_ok and (not strata_bad):
        winner = "challenger"
        reason = "delta_exp_r_ok_tail_ok_ci_ok_strata_ok"
    elif (delta_exp_r <= -float(cfg.min_delta_exp_r)) and (delta_tail >= -float(cfg.tail_slack)):
        winner = "champion"
        reason = "champion_better_or_challenger_worse"
    elif strata_bad:
        winner = "tie"
        reason = "strata_worstcase_blocks_ramp"
    elif (int(cfg.bootstrap) == 1 and int(cfg.require_ci_positive) == 1 and (not ci_ok)):
        winner = "tie"
        reason = "ci_not_positive"

    rep["winner"] = winner
    rep["reason"] = reason
    return rep


def recommend_next_share(
    winner: str,
    current_share: float,
    cfg: V2Config,
    freeze_max_share: Optional[float],
) -> Tuple[float, str]:
    """Compute next challenger share based on winner, respecting freeze cap.

    Returns: (share_next, action) where action ∈ {increase_share, decrease_share, hold}
    """
    max_share = float(cfg.max_share)
    if freeze_max_share is not None:
        max_share = min(max_share, float(freeze_max_share))  # freeze cap takes priority
    cur = float(max(0.0, min(1.0, current_share)))
    if winner == "challenger":
        return min(cur + float(cfg.ramp_step), max_share), "increase_share"
    if winner == "champion":
        return max(cur - float(cfg.ramp_step), 0.0), "decrease_share"
    return cur, "hold"


def _apply_to_redis(
    redis_url: str,
    cfg_hash: str,
    share_key: str,
    winner_key: str,
    ts_key: str,
    share_next: float,
    winner: str,
    notify_stream: str,
    report_compact: Dict[str, Any],
) -> None:
    """Write evaluation result to Redis dynamic config hash and notify stream.

    Sets share_key, winner_key, ts_key in cfg_hash atomically (pipeline).
    Then publishes a compact event to notify_stream for Telegram/monitoring.
    """
    try:
        import redis  # type: ignore
    except Exception as e:
        raise SystemExit("missing_deps: pip install redis") from e

    rds = redis.Redis.from_url(redis_url, decode_responses=True)
    pipe = rds.pipeline()
    pipe.hset(cfg_hash, share_key, f"{share_next:.6f}")
    pipe.hset(cfg_hash, winner_key, winner)
    pipe.hset(cfg_hash, ts_key, str(now_ms()))
    pipe.execute()

    try:
        msg = {
            "kind": "meta_ab_winner_v2",
            "ts_ms": str(now_ms()),
            "winner": winner,
            "share_next": f"{share_next:.6f}",
            "summary": json.dumps(report_compact, ensure_ascii=False),
        }
        rds.xadd(notify_stream, msg, maxlen=200000, approximate=True)
    except Exception:
        pass  # notify failures are non-critical


def _compact(rep: Dict[str, Any]) -> Dict[str, Any]:
    """Extract compact subset of report for Redis stream payload."""
    return {
        "winner": rep.get("winner"),
        "reason": rep.get("reason"),
        "counts": rep.get("counts"),
        "delta": rep.get("delta"),
        "ci": rep.get("ci"),
        "champion": rep.get("champion"),
        "challenger": rep.get("challenger"),
        "strata_top": rep.get("strata_top"),
    }


def main() -> None:
    ap = argparse.ArgumentParser(description="Stage4 AB winner evaluator v2 (fixed p_min + CI + strata)")
    ap.add_argument("--in-parquet", default=None)
    ap.add_argument("--in-ndjson", default=None)
    ap.add_argument("--limit-rows", type=int, default=0)

    ap.add_argument("--champion-model", required=True, help="Path to champion MetaModelLR JSON")
    ap.add_argument("--challenger-model", required=True, help="Path to challenger MetaModelLR JSON")

    ap.add_argument("--out-json", default=None, help="Write full JSON report to this path")

    ap.add_argument("--label-col", default=os.getenv("META_AB_LABEL_COL", "y"))
    ap.add_argument("--r-col", default=os.getenv("META_AB_R_COL", "r_mult"))
    ap.add_argument("--ok-col", default=os.getenv("META_AB_OK_COL", "ok"))

    ap.add_argument("--p-min", type=float, default=_env_float("META_P_MIN", 0.55))
    ap.add_argument("--min-n", type=int, default=_env_int("META_AB_MIN_ELIGIBLE", 1000))
    ap.add_argument("--min-delta-exp-r", type=float, default=_env_float("META_AB_MIN_DELTA_EXPR", 0.002))
    ap.add_argument("--tail-r", type=float, default=_env_float("META_AB_TAIL_R", -1.0))
    ap.add_argument("--tail-slack", type=float, default=_env_float("META_AB_TAIL_SLACK", 0.01))

    ap.add_argument("--bootstrap", type=int, default=_env_int("META_AB_BOOTSTRAP", 1))
    ap.add_argument("--boot-n", type=int, default=_env_int("META_AB_BOOT_N", 400))
    ap.add_argument("--boot-alpha", type=float, default=_env_float("META_AB_BOOT_ALPHA", 0.05))
    ap.add_argument("--boot-seed", type=int, default=_env_int("META_AB_BOOT_SEED", 7))
    ap.add_argument("--require-ci-positive", type=int, default=_env_int("META_AB_REQUIRE_CI_POSITIVE", 1))

    ap.add_argument(
        "--strata",
        default=os.getenv("META_AB_STRATA", "symbol"),
        help="Comma-separated strata cols, e.g. symbol,scenario_v4,session_bucket",
    )
    ap.add_argument("--strata-topk", type=int, default=_env_int("META_AB_STRATA_TOPK", 10))

    ap.add_argument("--apply", type=int, default=_env_int("META_AB_APPLY", 0))
    ap.add_argument("--redis-url", default=os.getenv("REDIS_URL", ""))
    ap.add_argument("--dynamic-cfg-hash", default=os.getenv("DYNAMIC_CFG_HASH", "settings:dynamic_cfg"))
    ap.add_argument("--share-key", default=os.getenv("META_AB_SHARE_KEY", "meta_ab_challenger_share"))
    ap.add_argument("--winner-key", default=os.getenv("META_AB_WINNER_KEY", "meta_ab_last_winner"))
    ap.add_argument("--ts-key", default=os.getenv("META_AB_LAST_EVAL_TS_KEY", "meta_ab_last_eval_ts_ms"))
    ap.add_argument("--notify-stream", default=os.getenv("NOTIFY_TELEGRAM_STREAM", "notify:telegram"))

    ap.add_argument("--current-share", type=float, default=_env_float("META_AB_CURRENT_SHARE", 0.0))
    ap.add_argument("--ramp-step", type=float, default=_env_float("META_AB_RAMP_STEP", 0.05))
    ap.add_argument("--max-share", type=float, default=_env_float("META_AB_MAX_SHARE", 0.50))

    args = ap.parse_args()

    strata_cols = tuple([s.strip() for s in str(args.strata).split(",") if s.strip()])

    cfg = V2Config(
        label_col=str(args.label_col),
        r_col=str(args.r_col),
        ok_col=str(args.ok_col),
        p_min=float(args.p_min),
        min_n=int(args.min_n),
        tail_r=float(args.tail_r),
        tail_slack=float(args.tail_slack),
        min_delta_exp_r=float(args.min_delta_exp_r),
        bootstrap=int(args.bootstrap),
        boot_n=int(args.boot_n),
        boot_alpha=float(args.boot_alpha),
        boot_seed=int(args.boot_seed),
        require_ci_positive=int(args.require_ci_positive),
        strata_cols=strata_cols,
        strata_topk=int(args.strata_topk),
        current_share=float(args.current_share),
        ramp_step=float(args.ramp_step),
        max_share=float(args.max_share),
    )

    df = load_dataset(args.in_parquet, args.in_ndjson, limit_rows=int(args.limit_rows))
    mm_champ = _load_meta_model(args.champion_model)
    mm_chal = _load_meta_model(args.challenger_model)

    rep = evaluate_v2(df, mm_champ, mm_chal, cfg)

    freeze_max_share = _read_freeze_max_share()
    share_next, action = recommend_next_share(str(rep.get("winner")), float(cfg.current_share), cfg, freeze_max_share)
    rep["ramp"] = {
        "current_share": float(cfg.current_share),
        "share_next": float(share_next),
        "action": action,
        "freeze_max_share": freeze_max_share,
    }

    if args.out_json:
        os.makedirs(os.path.dirname(os.path.abspath(args.out_json)), exist_ok=True)
        with open(args.out_json, "w", encoding="utf-8") as f:
            json.dump(rep, f, ensure_ascii=False, indent=2)

    if int(args.apply) == 1:
        if not args.redis_url:
            raise SystemExit("apply_requires_redis_url")
        _apply_to_redis(
            redis_url=args.redis_url,
            cfg_hash=args.dynamic_cfg_hash,
            share_key=args.share_key,
            winner_key=args.winner_key,
            ts_key=args.ts_key,
            share_next=share_next,
            winner=str(rep.get("winner")),
            notify_stream=args.notify_stream,
            report_compact=_compact(rep),
        )

    print(json.dumps(rep, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
