"""
Phase-2 scorer multipliers optimizer (stub v1).

See docs/PHASE2_SCORER_MULTIPLIERS_REDESIGN.md for the full contract.

What this script does:
  1. Loads labeled signals from edge_live JSONL window (default 7 days).
  2. Re-implements the confidence_scorer formula (pure-python, deterministic).
  3. Grid-searches multipliers + bonus coefficients against r_mult.
  4. Validates winner on a held-out chronological split.
  5. Writes suggested_weights.json only if validation gates pass.

Output schema:
  python-worker/config/suggested_weights.json (read by confidence_scorer._cfgf).

This is a STUB: the grid is small for fast iteration; expand once the script is
shown to be wired correctly. Not enabled in compose by default.
"""

from __future__ import annotations

import argparse
import glob
import json
import logging
import math
import os
import random
import time
from dataclasses import dataclass
from typing import Any, Iterable

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("phase2_optimizer")

DEFAULT_INPUT_DIR = "/var/lib/trade/of_reports/out/confidence_cal_live"
DEFAULT_OUTPUT = "python-worker/config/suggested_weights.json"

DEFAULT_WEIGHTS: dict[str, float] = {
    "z_trend_m":      1.2,
    "obi_trend_m":    1.1,
    "prog_range_m":   1.3,
    "b_reclaim":      0.05,
    "b_sweep":        0.03,
    "b_rsi":          0.02,
    "b_div":          0.03,
    "ml_fusion_alpha": 0.4,
}

WEIGHT_BOUNDS: dict[str, tuple[float, float]] = {
    "z_trend_m":      (0.5, 2.5),
    "obi_trend_m":    (0.5, 2.5),
    "prog_range_m":   (0.5, 2.5),
    "b_reclaim":      (0.0, 0.08),
    "b_sweep":        (0.0, 0.08),
    "b_rsi":          (0.0, 0.08),
    "b_div":          (0.0, 0.08),
    "ml_fusion_alpha": (0.1, 0.6),
}

# Stage-A coarse grid (kept small for stub; expand once wiring is verified).
COARSE_GRID: dict[str, list[float]] = {
    "z_trend_m":    [0.8, 1.2, 1.5, 2.0],
    "obi_trend_m":  [0.8, 1.1, 1.4, 1.8],
    "prog_range_m": [0.8, 1.3, 1.6, 2.0],
    "b_reclaim":    [0.02, 0.05, 0.08],
    "b_sweep":      [0.02, 0.03, 0.05],
    "b_rsi":        [0.01, 0.02, 0.04],
    "b_div":        [0.02, 0.03, 0.05],
}


@dataclass(frozen=True)
class Sample:
    main_z: float
    obi_z: float
    weak_ratio: float
    regime: str
    reclaim: bool
    sweep: bool
    rsi_agree: bool
    div_match: bool
    r_mult: float
    y: int
    ts_ms: int


def _f(x: Any, default: float = 0.0) -> float:
    try:
        v = float(x)
        return v if math.isfinite(v) else default
    except (TypeError, ValueError):
        return default


def _bool(x: Any) -> bool:
    if isinstance(x, bool):
        return x
    if isinstance(x, (int, float)):
        return bool(x)
    if isinstance(x, str):
        return x.strip().lower() in ("1", "true", "yes", "on")
    return False


def _has_confirmation(inds: dict[str, Any], key: str) -> bool:
    confs = inds.get("confirmations") or []
    if isinstance(confs, (list, tuple)):
        # confirmations entries may be either bare keys or "key=value" strings
        for c in confs:
            s = str(c)
            if s == key or s.startswith(key + "=") or s.startswith(key + ":"):
                return True
    ev = inds.get("evidence") or {}
    if isinstance(ev, dict) and key in ev:
        return _bool(ev.get(key)) or True
    return False


def _parse_sample(d: dict[str, Any]) -> Sample | None:
    inds = d.get("indicators") or {}
    if not isinstance(inds, dict):
        return None
    r = d.get("r_mult", d.get("r_multiple"))
    if r is None:
        return None
    main_z = _f(inds.get("main_z", inds.get("delta_z", 0.0)))
    obi_z = _f(inds.get("obi_z", 0.0))
    weak_ratio = _f(inds.get("weak_ratio", inds.get("range_vs_atr", 1.0)), default=1.0)
    regime_raw = str(inds.get("regime", "neutral")).lower()
    regime = "trend" if any(x in regime_raw for x in ("trend", "momentum")) else "range"
    y_raw = d.get("y")
    if y_raw is not None:
        try:
            y = 1 if int(y_raw) else 0
        except (TypeError, ValueError):
            y = 1 if _f(r) > 0 else 0
    else:
        y = 1 if _f(r) > 0 else 0
    return Sample(
        main_z=abs(main_z),
        obi_z=abs(obi_z),
        weak_ratio=weak_ratio,
        regime=regime,
        reclaim=_has_confirmation(inds, "reclaim"),
        sweep=_has_confirmation(inds, "sweep"),
        rsi_agree=_has_confirmation(inds, "rsi_agree"),
        div_match=_has_confirmation(inds, "div_match"),
        r_mult=_f(r),
        y=y,
        ts_ms=int(d.get("ts_ms", 0) or 0),
    )


def load_samples(input_dir: str, since_ms: int) -> list[Sample]:
    paths = sorted(glob.glob(os.path.join(input_dir, "edge_live_[0-9]*.jsonl")))
    if not paths:
        logger.warning("no JSONL files under %s", input_dir)
        return []
    out: list[Sample] = []
    for p in paths:
        # skip files that are clearly older than since_ms by name suffix
        try:
            stem = os.path.basename(p)
            ts_str = stem.replace("edge_live_", "").replace(".jsonl", "")
            file_ts_ms = int(ts_str)
            if file_ts_ms + 6 * 3600 * 1000 < since_ms:
                continue
        except ValueError:
            pass
        try:
            with open(p, encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        d = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    if int(d.get("ts_ms", 0) or 0) < since_ms:
                        continue
                    s = _parse_sample(d)
                    if s is not None:
                        out.append(s)
        except OSError as e:
            logger.warning("cannot read %s: %s", p, e)
    out.sort(key=lambda s: s.ts_ms)
    return out


def _clamp01(x: float) -> float:
    return max(0.0, min(1.0, x))


def replay_confidence(s: Sample, w: dict[str, float]) -> float:
    """Pure-python mirror of confidence_scorer._crypto_conf_factor's deterministic path.

    Must match the formula exactly: any divergence will produce biased optimum.
    Excludes ML fusion (ML_SCORING_ENABLE off in replay).
    """
    main_z = s.main_z
    obi_z = s.obi_z
    weak_ratio = s.weak_ratio

    z_core = _clamp01((main_z - 1.0) / 3.0)
    obi_persist = _clamp01((obi_z - 0.5) / 2.0)
    if weak_ratio < 0.4:
        progress = (weak_ratio - 0.2) / 0.2
    elif weak_ratio > 1.2:
        progress = (1.5 - weak_ratio) / 0.3
    else:
        progress = 1.0
    progress = _clamp01(progress)

    w_z, w_obi, w_prog = 0.4, 0.3, 0.3
    if s.regime == "trend":
        w_z *= w["z_trend_m"]
        w_obi *= w["obi_trend_m"]
    else:
        w_prog *= w["prog_range_m"]
    w_sum = w_z + w_obi + w_prog
    base = (w_z / w_sum) * z_core + (w_obi / w_sum) * obi_persist + (w_prog / w_sum) * progress

    b_raw = 0.0
    if s.reclaim:
        b_raw += w["b_reclaim"]
    if s.sweep:
        b_raw += w["b_sweep"]
    if s.rsi_agree:
        b_raw += w["b_rsi"]
    if s.div_match:
        b_raw += w["b_div"]
    if s.regime == "trend" and main_z > 3.0:
        b_raw *= 0.5
    if s.reclaim and s.sweep:
        b_raw += 0.02

    bonus = min(b_raw, 0.15)
    return _clamp01(_clamp01(base) + bonus)


def metrics_at_top5pct(samples: list[Sample], w: dict[str, float]) -> dict[str, float]:
    if not samples:
        return {"precision_top5pct": 0.0, "expectancy_r_top5pct": 0.0, "spearman_conf_r": 0.0, "n": 0}
    confs = [replay_confidence(s, w) for s in samples]
    n = len(samples)
    top_n = max(1, int(n * 0.05))
    order = sorted(range(n), key=lambda i: confs[i], reverse=True)
    top = order[:top_n]
    precision = sum(samples[i].y for i in top) / top_n
    expectancy = sum(samples[i].r_mult for i in top) / top_n
    # rough Spearman: ranks of confs vs ranks of r_mult
    rs = [s.r_mult for s in samples]
    rk_c = _rank(confs)
    rk_r = _rank(rs)
    spearman = _pearson(rk_c, rk_r)
    return {
        "precision_top5pct": precision,
        "expectancy_r_top5pct": expectancy,
        "spearman_conf_r": spearman,
        "n": n,
    }


def _rank(xs: list[float]) -> list[float]:
    order = sorted(range(len(xs)), key=lambda i: xs[i])
    rk = [0.0] * len(xs)
    for r, i in enumerate(order):
        rk[i] = float(r)
    return rk


def _pearson(a: list[float], b: list[float]) -> float:
    n = len(a)
    if n < 2:
        return 0.0
    ma = sum(a) / n
    mb = sum(b) / n
    num = sum((a[i] - ma) * (b[i] - mb) for i in range(n))
    da = math.sqrt(sum((x - ma) ** 2 for x in a))
    db = math.sqrt(sum((x - mb) ** 2 for x in b))
    if da == 0 or db == 0:
        return 0.0
    return num / (da * db)


def iter_grid(grid: dict[str, list[float]]) -> Iterable[dict[str, float]]:
    keys = list(grid.keys())
    vals = [grid[k] for k in keys]
    idx = [0] * len(keys)
    while True:
        yield {keys[i]: vals[i][idx[i]] for i in range(len(keys))}
        for j in range(len(keys) - 1, -1, -1):
            idx[j] += 1
            if idx[j] < len(vals[j]):
                break
            idx[j] = 0
            if j == 0:
                return


def random_refine(seed: dict[str, float], n: int, rng: random.Random) -> list[dict[str, float]]:
    out = []
    for _ in range(n):
        cand: dict[str, float] = dict(DEFAULT_WEIGHTS)
        for k, v in seed.items():
            lo, hi = WEIGHT_BOUNDS[k]
            jitter = v * (1.0 + rng.uniform(-0.20, 0.20))
            cand[k] = max(lo, min(hi, jitter))
        out.append(cand)
    return out


def validate_gates(
    train_m: dict[str, float],
    holdout_m: dict[str, float],
    baseline_m: dict[str, float],
    min_train_n: int,
    min_holdout_n: int,
) -> tuple[bool, list[str]]:
    fail: list[str] = []
    if int(train_m["n"]) < min_train_n:
        fail.append(f"train_n={int(train_m['n'])}<{min_train_n}")
    if int(holdout_m["n"]) < min_holdout_n:
        fail.append(f"holdout_n={int(holdout_m['n'])}<{min_holdout_n}")
    base_p = baseline_m["precision_top5pct"]
    if train_m["precision_top5pct"] < base_p + 0.05:
        fail.append(f"train_precision_gain<0.05 (got {train_m['precision_top5pct']-base_p:+.3f})")
    if holdout_m["precision_top5pct"] < base_p + 0.03:
        fail.append(f"holdout_precision_gain<0.03 (got {holdout_m['precision_top5pct']-base_p:+.3f})")
    if holdout_m["spearman_conf_r"] < 0.05:
        fail.append(f"holdout_spearman<{0.05} (got {holdout_m['spearman_conf_r']:.3f})")
    if holdout_m["expectancy_r_top5pct"] < 0.0:
        fail.append(f"holdout_expectancy<0 (got {holdout_m['expectancy_r_top5pct']:+.3f})")
    return (len(fail) == 0, fail)


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--input-dir", default=os.getenv("PHASE2_INPUT_DIR", DEFAULT_INPUT_DIR))
    p.add_argument("--output", default=os.getenv("PHASE2_OUTPUT_PATH", DEFAULT_OUTPUT))
    p.add_argument("--lookback-hours", type=int, default=int(os.getenv("PHASE2_LOOKBACK_HOURS", "168")))
    p.add_argument("--min-train-n", type=int, default=int(os.getenv("PHASE2_MIN_TRAIN_N", "3500")))
    p.add_argument("--min-holdout-n", type=int, default=int(os.getenv("PHASE2_MIN_HOLDOUT_N", "1500")))
    p.add_argument("--holdout-frac", type=float, default=float(os.getenv("PHASE2_HOLDOUT_FRAC", "0.30")))
    p.add_argument("--refine-n", type=int, default=int(os.getenv("PHASE2_REFINE_N", "50")))
    p.add_argument("--dry-run", action="store_true", help="compute but do not write output")
    args = p.parse_args()

    enabled = os.getenv("PHASE2_OPTIMIZER_ENABLE", "0").strip().lower() in ("1", "true", "yes", "on")
    if not enabled and not args.dry_run:
        logger.info("PHASE2_OPTIMIZER_ENABLE!=1 — refusing to write; use --dry-run to compute only.")
        return 0

    now_ms = int(time.time() * 1000)
    since_ms = now_ms - args.lookback_hours * 3600 * 1000
    samples = load_samples(args.input_dir, since_ms)
    logger.info("loaded %d samples (lookback %dh)", len(samples), args.lookback_hours)

    if len(samples) < args.min_train_n + args.min_holdout_n:
        out = {
            "phase": 2,
            "version": "2.0.0-stub",
            "generated_at_ms": now_ms,
            "n_samples": len(samples),
            "validation": {"pass": False, "reasons": ["insufficient_samples"]},
            "suggested_weights": dict(DEFAULT_WEIGHTS),
        }
        _write_output(out, args.output, dry_run=args.dry_run)
        return 0

    split = int(len(samples) * (1.0 - args.holdout_frac))
    train, holdout = samples[:split], samples[split:]
    baseline_train = metrics_at_top5pct(train, DEFAULT_WEIGHTS)
    baseline_holdout = metrics_at_top5pct(holdout, DEFAULT_WEIGHTS)
    logger.info("baseline train: %s", baseline_train)
    logger.info("baseline holdout: %s", baseline_holdout)

    best: tuple[float, dict[str, float], dict[str, float]] | None = None
    iters = 0
    t0 = time.time()
    for cand in iter_grid(COARSE_GRID):
        full_cand = dict(DEFAULT_WEIGHTS)
        full_cand.update(cand)
        m = metrics_at_top5pct(train, full_cand)
        score = m["precision_top5pct"]
        if best is None or score > best[0]:
            best = (score, full_cand, m)
        iters += 1
    logger.info("coarse grid: iters=%d in %.1fs; train best=%s", iters, time.time() - t0, best[2] if best else None)

    if best is not None:
        rng = random.Random(args.lookback_hours)
        for cand in random_refine(best[1], args.refine_n, rng):
            m = metrics_at_top5pct(train, cand)
            if m["precision_top5pct"] > best[0]:
                best = (m["precision_top5pct"], cand, m)
        logger.info("after refine: train best precision=%.4f", best[0])

    if best is None:
        logger.error("no candidate evaluated; aborting")
        return 1

    winner_w = best[1]
    train_m = best[2]
    holdout_m = metrics_at_top5pct(holdout, winner_w)
    logger.info("winner holdout: %s", holdout_m)

    ok, fails = validate_gates(train_m, holdout_m, baseline_holdout, args.min_train_n, args.min_holdout_n)
    out = {
        "phase": 2,
        "version": "2.0.0-stub",
        "generated_at_ms": now_ms,
        "n_samples": len(samples),
        "lookback_hours": args.lookback_hours,
        "objective": "precision_top5pct",
        "baseline": {k: v for k, v in baseline_holdout.items() if k != "n"},
        "best_train": {k: v for k, v in train_m.items() if k != "n"},
        "best_holdout": {k: v for k, v in holdout_m.items() if k != "n"},
        "validation": {"pass": ok, "reasons": fails, "holdout_split": args.holdout_frac},
        "suggested_weights": winner_w if ok else dict(DEFAULT_WEIGHTS),
    }
    _write_output(out, args.output, dry_run=args.dry_run)
    return 0


def _write_output(out: dict[str, Any], path: str, *, dry_run: bool) -> None:
    payload = json.dumps(out, indent=2, sort_keys=True, default=float)
    if dry_run:
        print(payload)
        return
    os.makedirs(os.path.dirname(os.path.abspath(path)) or ".", exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        f.write(payload)
    os.replace(tmp, path)
    logger.info("wrote %s (pass=%s, n=%d)", path, out.get("validation", {}).get("pass"), out.get("n_samples", 0))


if __name__ == "__main__":
    raise SystemExit(main())
