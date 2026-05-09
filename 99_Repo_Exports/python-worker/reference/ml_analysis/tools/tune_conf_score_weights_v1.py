from __future__ import annotations

"""Tune confirmation bonus weights from historical outcomes.

Goal: produce a conservative `conf_score_weight_tuning` dict that can be placed
into `runtime.config` and consumed by `handlers.crypto_orderflow.scoring.confidence_scorer`.

This is a Phase-2 (data calibration) helper:
  - computes uplift vs baseline by confirmation key, per regime
  - clamps weights, enforces minimum sample sizes
  - optionally estimates simple synergy uplifts for selected pairs

Inputs (choose one):
  - Parquet dataset produced by `ml_analysis/tools/build_dataset_from_inputs_outcomes_v2.py`
  - NDJSON replay with keys: confirmations/evidence/indicators + outcomes (best effort)
"""

import argparse
import json
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any

import pandas as pd


def _read_ndjson(path: str) -> Iterable[dict[str, Any]]:
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except Exception:
                continue


def _safe_float(v: Any, default: float = 0.0) -> float:
    try:
        if v is None:
            return default
        return float(v)
    except Exception:
        return default


def _safe_int(v: Any, default: int = 0) -> int:
    try:
        if v is None:
            return default
        return int(v)
    except Exception:
        return default


def _parse_regime(row: dict[str, Any]) -> str:
    # Prefer explicit market_mode/market_regime
    mm = str(row.get("market_mode") or row.get("market_regime") or "").lower()
    if mm.startswith("momentum") or mm.startswith("trend"):
        return "trend"
    if mm.startswith("mean") or mm.startswith("range"):
        return "range"
    # fallback heuristic: `signal_kind`
    sk = str(row.get("signal_kind") or row.get("kind") or "").lower()
    if "breakout" in sk:
        return "trend"
    if "meanrev" in sk or "revert" in sk:
        return "range"
    return "neutral"


_ALLOW = {
    "reclaim",
    "obi_stable",
    "ice_strict",
    "iceberg_strict",
    "fp_edge_absorb",
    "rsi_agree",
    "div_match",
    "sweep",
    "sweep_eqh",
    "sweep_eql",
}


def _extract_flags_from_any(row: dict[str, Any]) -> dict[str, int]:
    """Extract confirmation flags (0/1) from a dataset/replay row."""
    flags: dict[str, int] = {}

    def _put(k: str, v: Any = 1) -> None:
        k = (k or "").strip()
        if k not in _ALLOW:
            return
        try:
            vv = _safe_int(v, 1)
            if vv != 0:
                flags[k] = 1
        except Exception:
            flags[k] = 1

        # aliases
        if k == "ice_strict":
            flags.setdefault("iceberg_strict", 1)
        if k == "iceberg_strict":
            flags.setdefault("ice_strict", 1)

    # 1) explicit columns (parquet)
    for k in _ALLOW:
        if k in row:
            _put(k, row.get(k))

    # 2) evidence dict
    ev = row.get("evidence")
    if isinstance(ev, str):
        try:
            ev = json.loads(ev)
        except Exception:
            ev = {}
    if isinstance(ev, dict):
        for k in _ALLOW:
            if k in ev:
                _put(k, ev.get(k))

    # 3) confirmations list/string
    confs = row.get("confirmations")
    if isinstance(confs, str):
        # either JSON list or comma-separated
        s = confs.strip()
        if s.startswith("["):
            try:
                confs = json.loads(s)
            except Exception:
                confs = [x.strip() for x in s.split(",") if x.strip()]
        else:
            confs = [x.strip() for x in s.split(",") if x.strip()]

    if isinstance(confs, list):
        for c in confs:
            if not isinstance(c, str):
                continue
            s = c.strip()
            if not s:
                continue
            if "=" in s:
                k, v = s.split("=", 1)
                _put(k.strip(), v.strip() or 1)
            else:
                _put(s, 1)

    return flags


@dataclass
class TuneCfg:
    min_n_key: int = 200
    min_n_regime: int = 2000
    uplift_min: float = 0.002
    max_w: float = 0.08
    base_scale: float = 0.25
    synergy_pairs: tuple[tuple[str, str], ...] = (
        ("sweep", "reclaim"),
        ("sweep_eqh", "reclaim"),
        ("sweep_eql", "reclaim"),
        ("iceberg_strict", "fp_edge_absorb"),
        ("sweep", "div_match"),
    )


def _uplift_stats(df: pd.DataFrame, mask: pd.Series) -> tuple[float, float, int]:
    """Return (mean_r, winrate, n). Expects columns: r_mult, y"""
    sub = df.loc[mask]
    n = int(len(sub))
    if n <= 0:
        return 0.0, 0.0, 0
    mean_r = float(sub["r_mult"].mean())
    winrate = float(sub["y"].mean())
    return mean_r, winrate, n


def _weight_from_uplift(uplift_r: float, uplift_wr: float, cfg: TuneCfg) -> float:
    # Conservative: combine both uplifts with small scale.
    score = (uplift_r + 0.5 * uplift_wr)
    w = cfg.base_scale * score
    return max(min(w, cfg.max_w), -cfg.max_w)


def tune(df: pd.DataFrame, cfg: TuneCfg) -> dict[str, Any]:
    """Return conf_score_weight_tuning dict."""
    # normalize
    if "r_mult" not in df.columns:
        df["r_mult"] = 0.0
    if "y" not in df.columns:
        df["y"] = (df["r_mult"] > 0).astype(int)
    df["regime"] = df.apply(lambda r: _parse_regime(r.to_dict()), axis=1)

    # Ensure flags columns exist
    for k in _ALLOW:
        if k not in df.columns:
            df[k] = 0

    # Fill missing from evidence/confirmations if present
    # (best effort, row-wise; slower but safer)
    need_fill = any(col in df.columns for col in ("evidence", "confirmations"))
    if need_fill:
        rows = []
        for _, r in df.iterrows():
            d = r.to_dict()
            flags = _extract_flags_from_any(d)
            for k in _ALLOW:
                if int(d.get(k, 0) or 0) == 0 and flags.get(k, 0) == 1:
                    d[k] = 1
            rows.append(d)
        df = pd.DataFrame(rows)

    out: dict[str, Any] = {"by_regime": {}, "meta": {}}
    out["meta"].update(
        {
            "min_n_key": cfg.min_n_key,
            "min_n_regime": cfg.min_n_regime,
            "uplift_min": cfg.uplift_min,
            "max_w": cfg.max_w,
        }
    )

    # baseline per regime
    baselines: dict[str, dict[str, Any]] = {}
    for reg in ("trend", "range", "neutral"):
        m = df["regime"] == reg
        mean_r, wr, n = _uplift_stats(df, m)
        baselines[reg] = {"mean_r": mean_r, "winrate": wr, "n": n}
        out["by_regime"].setdefault(reg, {})

    out["meta"]["baselines"] = baselines

    for reg in ("trend", "range", "neutral"):
        if baselines[reg]["n"] < cfg.min_n_regime:
            continue

        base_mean_r = float(baselines[reg]["mean_r"])
        base_wr = float(baselines[reg]["winrate"])

        for k in sorted(_ALLOW):
            m1 = (df["regime"] == reg) & (df[k] == 1)
            m0 = (df["regime"] == reg) & (df[k] == 0)
            _, _, n1 = _uplift_stats(df, m1)
            if n1 < cfg.min_n_key:
                continue
            mean_r1, wr1, _ = _uplift_stats(df, m1)
            # uplift relative to baseline (not to m0 to reduce selection artifacts)
            uplift_r = mean_r1 - base_mean_r
            uplift_wr = wr1 - base_wr

            if abs(uplift_r) < cfg.uplift_min and abs(uplift_wr) < cfg.uplift_min:
                continue

            w = _weight_from_uplift(uplift_r, uplift_wr, cfg)
            out["by_regime"][reg][f"bonus_{k}"] = float(w)

    # Synergy (pair uplift beyond sum of singles)
    synergy_by_regime: dict[str, dict[str, float]] = {}
    synergy_global: dict[str, float] = {}
    for reg in ("trend", "range", "neutral"):
        if baselines[reg]["n"] < cfg.min_n_regime:
            continue

        base_mean_r = float(baselines[reg]["mean_r"])
        base_wr = float(baselines[reg]["winrate"])

        bucket_out: dict[str, float] = {}
        for a, b in cfg.synergy_pairs:
            if a not in df.columns or b not in df.columns:
                continue
            m_ab = (df["regime"] == reg) & (df[a] == 1) & (df[b] == 1)
            _, _, n_ab = _uplift_stats(df, m_ab)
            if n_ab < max(cfg.min_n_key, 100):
                continue
            mean_r_ab, wr_ab, _ = _uplift_stats(df, m_ab)
            uplift_r = mean_r_ab - base_mean_r
            uplift_wr = wr_ab - base_wr
            if abs(uplift_r) < cfg.uplift_min and abs(uplift_wr) < cfg.uplift_min:
                continue
            w = _weight_from_uplift(uplift_r, uplift_wr, cfg)
            # synergy weights should be smaller than single bonuses
            w = max(min(w, cfg.max_w / 2), -cfg.max_w / 2)

            key = f"{a}+{b}"
            bucket_out[key] = float(w)
            synergy_global[key] = float(max(synergy_global.get(key, 0.0), w))

        if bucket_out:
            synergy_by_regime[reg] = bucket_out

    if synergy_by_regime:
        out["synergy_by_regime"] = synergy_by_regime
    if synergy_global:
        out["synergy"] = synergy_global

    return out


def _load_parquet(path: str) -> pd.DataFrame:
    # pandas requires pyarrow/fastparquet; bubble up informative error.
    return pd.read_parquet(path)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--parquet", default="", help="Input parquet dataset")
    ap.add_argument("--ndjson", default="", help="Input ndjson replay (fallback)")
    ap.add_argument("--out-json", default="", help="Output json path (optional)")

    ap.add_argument("--min-n-key", type=int, default=200)
    ap.add_argument("--min-n-regime", type=int, default=2000)
    ap.add_argument("--uplift-min", type=float, default=0.002)
    ap.add_argument("--max-w", type=float, default=0.08)
    args = ap.parse_args()

    if not args.parquet and not args.ndjson:
        raise SystemExit("Provide --parquet or --ndjson")

    if args.parquet:
        df = _load_parquet(args.parquet)
    else:
        rows = list(_read_ndjson(args.ndjson))
        df = pd.DataFrame(rows)

    cfg = TuneCfg(
        min_n_key=int(args.min_n_key),
        min_n_regime=int(args.min_n_regime),
        uplift_min=float(args.uplift_min),
        max_w=float(args.max_w),
    )
    out = tune(df, cfg)

    js = json.dumps(out, ensure_ascii=False, indent=2, sort_keys=True)
    if args.out_json:
        with open(args.out_json, "w", encoding="utf-8") as f:
            f.write(js)
    else:
        print(js)


if __name__ == "__main__":
    main()
