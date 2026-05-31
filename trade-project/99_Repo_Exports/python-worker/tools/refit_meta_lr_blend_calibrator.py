#!/usr/bin/env python3
"""Refit posterior isotonic calibrator for meta_lr_blend (2026-05-23 stop-bleed).

Background
----------
`/var/lib/trade/ml_models/meta_lr_blend_*/meta_lr_blend.json` ships with raw
LR coefficients (intercept + coef_v14 + coef_v5) but no posterior calibrator.
On production traffic where v14/v5 child probabilities saturate near 1.0,
the raw sigmoid output saturates → median p_edge=1.000 while actual WR ≈
base_rate (~3-7%). Per audit (2026-05-23, WR=5.9%) this is the root cause
of the "model exists but is unprofitable" symptom.

What this tool does
-------------------
1. Pull (p_edge_raw, win) pairs from `trades:closed` over a configurable
   look-back window (default 7d). Accepts both legacy `ml_prob` and the
   newer `p_edge_raw` field (added 2026-05-23 in services/label_joiner.py).
2. Filter by `ml_version` ∈ {meta_lr_blend, schema_id}; require n ≥ N_MIN
   (default 300).
3. Compute baseline Brier/ECE with the raw probabilities.
4. Fit an isotonic regression via `common.isotonic_calibration.fit_isotonic_pav`.
5. Compute Brier/ECE after calibration; reject if not improved by at least
   the configured deltas (default Brier −0.005, ECE −0.01).
6. Atomically write `calibrator.json` next to the model artifact and a
   report JSON to `/var/lib/trade/of_reports/`. Loader sibling discovery
   (services/ml_confirm/config_loader.py:_load_calibrator_sync, Priority 4)
   picks it up on next cfg cache refresh.

Usage
-----
    python -m tools.refit_meta_lr_blend_calibrator \\
        --model-path /var/lib/trade/ml_models/meta_lr_blend_20260516_191237/meta_lr_blend.json \\
        --lookback-hours 168 \\
        --min-n 300 \\
        --apply

By default runs in `--dry-run` mode — prints metrics, does not write.
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import tempfile
import time
from typing import Any

log = logging.getLogger("refit_meta_lr_blend_calibrator")


def _safe_float(x: Any, default: float = float("nan")) -> float:
    try:
        v = float(x)
        return v if v == v else default
    except Exception:
        return default


def _decode_field(v: Any) -> str:
    if isinstance(v, bytes):
        return v.decode("utf-8", "ignore")
    return str(v)


def _is_target_version(ml_version: str, target_substr: str) -> bool:
    if not target_substr:
        return True
    return target_substr.lower() in (ml_version or "").lower()


def _read_trades_closed(
    redis_client: Any,
    *,
    stream: str = "trades:closed",
    lookback_hours: int = 168,
    target_version_substr: str = "meta_lr_blend",
    limit: int = 100_000,
) -> list[tuple[float, int]]:
    """Read (p_edge_raw, win) pairs from trades:closed in window.

    Uses XRANGE with a `(ts_ms-` start id. Returns at most `limit` rows.
    """
    now_ms = int(time.time() * 1000)
    start_ms = now_ms - int(lookback_hours) * 3600 * 1000
    start_id = f"{start_ms}-0"
    out: list[tuple[float, int]] = []
    skipped_be = 0
    skipped_version = 0
    skipped_no_p = 0

    cursor = start_id
    fetched = 0
    while True:
        try:
            chunk = redis_client.xrange(stream, min=cursor, max="+", count=1000)
        except Exception as e:
            log.error("xrange failed: %s", e)
            break
        if not chunk:
            break
        last_id = None
        for msg_id, fields in chunk:
            last_id = _decode_field(msg_id)
            fetched += 1
            rec = {_decode_field(k): _decode_field(v) for k, v in fields.items()}
            ml_version = rec.get("ml_version") or rec.get("model_ver") or ""
            if not _is_target_version(ml_version, target_version_substr):
                skipped_version += 1
                continue
            # Prefer the explicit raw column; fall back to ml_prob (which is
            # raw while no calibrator is attached).
            p_raw_s = rec.get("p_edge_raw") or rec.get("ml_prob")
            p_raw = _safe_float(p_raw_s, float("nan"))
            if p_raw != p_raw or not (0.0 <= p_raw <= 1.0):
                skipped_no_p += 1
                continue
            result = (rec.get("result") or "").upper()
            if result not in ("WIN", "LOSS"):
                # BE / missing — excluded from binary calibration.
                if result == "BE":
                    skipped_be += 1
                continue
            win = 1 if result == "WIN" else 0
            out.append((p_raw, win))
            if len(out) >= limit:
                break
        if len(out) >= limit:
            break
        if last_id is None:
            break
        # Advance cursor: '(<id>' is exclusive form
        cursor = f"({last_id}"

    log.info(
        "fetched=%d kept=%d skipped_version=%d skipped_be=%d skipped_no_p=%d window_h=%d",
        fetched, len(out), skipped_version, skipped_be, skipped_no_p, lookback_hours,
    )
    return out


def _brier(probs: list[float], wins: list[int]) -> float:
    if not probs:
        return 0.0
    s = 0.0
    for p, w in zip(probs, wins):
        s += (p - float(w)) ** 2
    return s / len(probs)


def _ece(probs: list[float], wins: list[int], n_bins: int = 15) -> float:
    if not probs:
        return 0.0
    bins = [[0, 0.0, 0.0] for _ in range(n_bins)]  # n, conf_sum, acc_sum
    for p, w in zip(probs, wins):
        idx = min(int(p * n_bins), n_bins - 1)
        if idx < 0:
            idx = 0
        bins[idx][0] += 1
        bins[idx][1] += p
        bins[idx][2] += w
    n_total = len(probs)
    ece = 0.0
    for n, c, a in bins:
        if n <= 0:
            continue
        conf = c / n
        acc = a / n
        ece += abs(conf - acc) * (n / n_total)
    return ece


def fit_and_evaluate(
    pairs: list[tuple[float, int]],
    *,
    min_n: int = 300,
    require_brier_improvement: float = 0.005,
    require_ece_improvement: float = 0.01,
) -> dict[str, Any]:
    """Fit isotonic on (p_raw, win) and report acceptance metrics."""
    from common.isotonic_calibration import fit_isotonic_pav

    n = len(pairs)
    if n < min_n:
        return {
            "accepted": False,
            "reason": f"insufficient_n({n}<{min_n})",
            "n": n,
        }

    probs_raw = [p for p, _ in pairs]
    wins = [w for _, w in pairs]
    brier_raw = _brier(probs_raw, wins)
    ece_raw = _ece(probs_raw, wins)

    samples = [(p, float(w), 1.0) for p, w in pairs]
    cal = fit_isotonic_pav(samples)
    if not cal.x or not cal.p:
        return {
            "accepted": False,
            "reason": "isotonic_fit_returned_empty",
            "n": n,
            "brier_raw": brier_raw,
            "ece_raw": ece_raw,
        }

    probs_cal = [cal.predict(p) for p in probs_raw]
    brier_cal = _brier(probs_cal, wins)
    ece_cal = _ece(probs_cal, wins)

    brier_delta = brier_raw - brier_cal
    ece_delta = ece_raw - ece_cal

    accepted = (
        brier_delta >= require_brier_improvement
        and ece_delta >= require_ece_improvement
    )

    return {
        "accepted": accepted,
        "reason": "ok" if accepted else (
            f"insufficient_improvement(brier_delta={brier_delta:.4f}<{require_brier_improvement},"
            f" ece_delta={ece_delta:.4f}<{require_ece_improvement})"
        ),
        "n": n,
        "pos_rate": sum(wins) / n,
        "brier_raw": brier_raw,
        "brier_cal": brier_cal,
        "brier_delta": brier_delta,
        "ece_raw": ece_raw,
        "ece_cal": ece_cal,
        "ece_delta": ece_delta,
        "calibrator": cal.to_dict(),
    }


def _atomic_write_json(path: str, payload: dict[str, Any]) -> None:
    """Write JSON atomically via tempfile + os.replace.

    Ensures readers never see a partially-written calibrator artifact.
    """
    dir_ = os.path.dirname(os.path.abspath(path)) or "."
    os.makedirs(dir_, exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix=".calibrator-", suffix=".json", dir=dir_)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2, sort_keys=True)
        os.replace(tmp, path)
    except Exception:
        with open(os.devnull, "w"):
            pass
        try:
            os.remove(tmp)
        except OSError:
            pass
        raise


def _emit_metric(success: bool, reason: str, n: int) -> None:
    """Best-effort Prometheus push via metric file.

    Persisted to node_exporter textfile collector path if writable; otherwise
    silently skipped. Service uses `_metric` directly when running in-process.
    """
    try:
        textfile_dir = os.getenv("REFIT_METRIC_TEXTFILE_DIR", "/var/lib/node_exporter/textfile_collector")
        if not os.path.isdir(textfile_dir):
            return
        out_path = os.path.join(textfile_dir, "meta_lr_blend_refit.prom")
        ts = int(time.time())
        with open(out_path, "w", encoding="utf-8") as f:
            f.write("# HELP meta_lr_blend_refit_last_run_ts Unix timestamp of last refit attempt\n")
            f.write("# TYPE meta_lr_blend_refit_last_run_ts gauge\n")
            f.write(f"meta_lr_blend_refit_last_run_ts {ts}\n")
            f.write("# HELP meta_lr_blend_refit_accepted Whether the last refit was accepted\n")
            f.write("# TYPE meta_lr_blend_refit_accepted gauge\n")
            f.write(f"meta_lr_blend_refit_accepted {1 if success else 0}\n")
            f.write("# HELP meta_lr_blend_refit_n Sample count used by last refit\n")
            f.write("# TYPE meta_lr_blend_refit_n gauge\n")
            f.write(f"meta_lr_blend_refit_n {n}\n")
            f.write(f'# HELP meta_lr_blend_refit_reason_info Last refit reason (info, value=1)\n')
            f.write(f"# TYPE meta_lr_blend_refit_reason_info gauge\n")
            f.write(f'meta_lr_blend_refit_reason_info{{reason="{reason}"}} 1\n')
    except Exception:
        pass


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "--model-path", required=True,
        help="Path to meta_lr_blend.json (calibrator.json will be written next to it).",
    )
    parser.add_argument("--stream", default=os.getenv("REFIT_STREAM", "trades:closed"))
    parser.add_argument("--lookback-hours", type=int, default=int(os.getenv("REFIT_LOOKBACK_HOURS", "168")))
    parser.add_argument("--min-n", type=int, default=int(os.getenv("REFIT_MIN_N", "300")))
    parser.add_argument("--target-version", default=os.getenv("REFIT_TARGET_VERSION", "meta_lr_blend"),
                        help="Substring match against ml_version; '' to accept any.")
    parser.add_argument("--require-brier-improvement", type=float, default=0.005)
    parser.add_argument("--require-ece-improvement", type=float, default=0.01)
    parser.add_argument("--report-dir", default=os.getenv("REFIT_REPORT_DIR", "/var/lib/trade/of_reports"))
    parser.add_argument("--apply", action="store_true",
                        help="Write calibrator.json on success. Default: dry-run.")
    parser.add_argument("--log-level", default=os.getenv("LOG_LEVEL", "INFO"))
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=args.log_level,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
        stream=sys.stderr,
    )

    if not os.path.exists(args.model_path):
        log.error("model not found: %s", args.model_path)
        return 2

    from core.redis_client import get_redis
    r = get_redis()
    pairs = _read_trades_closed(
        r,
        stream=args.stream,
        lookback_hours=args.lookback_hours,
        target_version_substr=args.target_version,
    )

    result = fit_and_evaluate(
        pairs,
        min_n=args.min_n,
        require_brier_improvement=args.require_brier_improvement,
        require_ece_improvement=args.require_ece_improvement,
    )
    result["model_path"] = args.model_path
    result["lookback_hours"] = args.lookback_hours
    result["target_version"] = args.target_version
    result["ts_ms"] = int(time.time() * 1000)

    log.info("refit summary: %s", json.dumps({k: v for k, v in result.items() if k != "calibrator"}))

    # Persist report regardless of acceptance
    report_path = ""
    try:
        os.makedirs(args.report_dir, exist_ok=True)
        report_path = os.path.join(args.report_dir, f"meta_lr_blend_refit_{result['ts_ms']}.json")
        _atomic_write_json(report_path, result)
        log.info("wrote report: %s", report_path)
    except Exception as e:
        log.warning("report write failed: %s", e)

    _emit_metric(result.get("accepted", False), result.get("reason", "unknown"), result.get("n", 0))

    if not result.get("accepted"):
        log.warning("refit not accepted: %s", result.get("reason"))
        return 1

    if not args.apply:
        log.info("dry-run: not writing calibrator (use --apply to promote)")
        return 0

    cal_path = os.path.join(os.path.dirname(args.model_path), "calibrator.json")
    artifact = dict(result["calibrator"])
    artifact["meta"] = {
        "kind": "meta_lr_blend_posterior",
        "n": result["n"],
        "brier_raw": result["brier_raw"],
        "brier_cal": result["brier_cal"],
        "ece_raw": result["ece_raw"],
        "ece_cal": result["ece_cal"],
        "pos_rate": result["pos_rate"],
        "fit_ts_ms": result["ts_ms"],
        "lookback_hours": args.lookback_hours,
        "target_version": args.target_version,
    }
    _atomic_write_json(cal_path, artifact)
    log.info("wrote calibrator: %s", cal_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
