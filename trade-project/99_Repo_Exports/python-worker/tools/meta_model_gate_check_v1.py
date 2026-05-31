#!/usr/bin/env python3
"""meta_model_gate_check_v1

Pre-promotion gate check for MetaModelLR artifacts.

Validates a model JSON + paired report before allowing the meta-model gate
to be set to SHADOW or ENFORCE in cfg2.

ENFORCE is hard-blocked by this script — it can only approve SHADOW.
To reach ENFORCE use nightly_meta_enforce_propose_bundle.py after 48 h of
clean shadow metrics.

Gate criteria (defaults match train_scorer_model_v1 thresholds):
  - artifact age         ≤ MAX_AGE_DAYS  (default 30 d)
  - not a stub           version != "3.0.0-stub"
  - holdout_auc          ≥ AUC_MIN       (default 0.62)
  - expectancy_r_top5pct > 0.0
  - ECE                  ≤ ECE_MAX       (default 0.10)
  - META_MODEL_PATH      must be non-empty

Usage (dry-run):
  python -m tools.meta_model_gate_check_v1 \\
      --model /var/lib/trade/of_reports/models/meta_lr_v4_nightly.json \\
      --report /var/lib/trade/of_reports/models/scorer_model.report.json

Apply SHADOW to cfg2 if all gates pass (--apply 1):
  python -m tools.meta_model_gate_check_v1 \\
      --model /var/lib/trade/of_reports/models/meta_lr_v4_nightly.json \\
      --apply 1

Exit codes:
  0  all gates pass  (apply may have written cfg2)
  1  one or more gates fail
  2  environment / file error
"""

from __future__ import annotations

import argparse
import json
import os
import time
from pathlib import Path
from typing import Any

_MS_PER_DAY = 86_400_000


def _now_ms() -> int:
    return int(time.time() * 1000)


def _f(v: Any, default: float = 0.0) -> float:
    try:
        return float(v)
    except Exception:
        return default


def _s(v: Any, default: str = "") -> str:
    if v is None:
        return default
    return str(v)


def _find_report(model_path: str) -> str | None:
    """Locate the paired .report.json by convention."""
    p = Path(model_path)
    # 1. same stem: meta_lr_v4_nightly.json → meta_lr_v4_nightly.report.json
    candidate = p.with_suffix(".report.json")
    if candidate.exists():
        return str(candidate)
    # 2. scorer_model.report.json in the same dir
    scorer = p.parent / "scorer_model.report.json"
    if scorer.exists():
        return str(scorer)
    # 3. meta_train/meta_lr.report.json in same dir
    train_report = p.parent / "meta_train" / "meta_lr.report.json"
    if train_report.exists():
        return str(train_report)
    return None


def _load_json(path: str) -> dict[str, Any]:
    with open(path, encoding="utf-8") as f:
        d = json.load(f)
    if not isinstance(d, dict):
        raise ValueError(f"expected JSON object in {path}")
    return d


def check_model(
    model_path: str,
    report_path: str | None,
    *,
    max_age_days: int,
    auc_min: float,
    ece_max: float,
    now_ms: int,
) -> tuple[bool, list[str], dict[str, Any]]:
    """Return (pass, blockers, evidence_dict)."""
    blockers: list[str] = []
    evidence: dict[str, Any] = {
        "model_path": model_path,
        "report_path": report_path,
        "ts_ms": now_ms,
    }

    # ── 1. Model file existence ────────────────────────────────────────────
    if not os.path.isfile(model_path):
        blockers.append(f"model_file_not_found: {model_path}")
        evidence["model_exists"] = False
        return False, blockers, evidence
    evidence["model_exists"] = True

    # ── 2. Load model JSON ─────────────────────────────────────────────────
    try:
        model = _load_json(model_path)
    except Exception as e:
        blockers.append(f"model_json_load_error: {e}")
        return False, blockers, evidence

    created_ms = int(_f(model.get("created_ms", 0)))
    schema_name = _s(model.get("schema_name", model.get("schema_version", "")))
    age_ms = now_ms - created_ms if created_ms else None
    age_days = round(age_ms / _MS_PER_DAY, 1) if age_ms else None
    evidence["created_ms"] = created_ms
    evidence["age_days"] = age_days
    evidence["schema_name"] = schema_name

    # ── 3. Age ────────────────────────────────────────────────────────────
    if not created_ms:
        blockers.append("missing_created_ms: cannot verify artifact age")
    elif age_days is not None and age_days > max_age_days:
        blockers.append(f"stale_artifact: age={age_days}d > max={max_age_days}d")

    # ── 4. META_MODEL_PATH ENV ────────────────────────────────────────────
    env_path = (os.getenv("META_MODEL_PATH", "") or "").strip()
    evidence["env_META_MODEL_PATH"] = env_path if env_path else "(unset)"
    if not env_path:
        blockers.append("META_MODEL_PATH env var is unset — engine will not load the model")

    # ── 5. Report ─────────────────────────────────────────────────────────
    if not report_path:
        report_path = _find_report(model_path)
        evidence["report_path"] = report_path

    holdout: dict[str, Any] = {}
    version_str = ""

    if not report_path or not os.path.isfile(report_path):
        blockers.append("report_not_found: cannot validate holdout metrics")
    else:
        try:
            report = _load_json(report_path)
        except Exception as e:
            blockers.append(f"report_json_load_error: {e}")
            report = {}

        version_str = _s(report.get("version", ""))
        evidence["report_version"] = version_str

        # Holdout metrics live in different fields across report formats.
        hm = report.get("holdout_metrics") or {}
        if not hm:
            # flat-format (older meta_lr reports)
            hm = {
                "auc": report.get("auc", report.get("holdout_auc", None)),
                "expectancy_r_top5pct": report.get("expectancy_r_top5pct", None),
                "ece": report.get("ece", report.get("ece10", None)),
                "n": report.get("n", report.get("n_holdout", None)),
            }
        holdout = {k: v for k, v in hm.items() if v is not None}
        evidence["holdout"] = holdout

        # ── 5a. Stub guard ─────────────────────────────────────────────────
        if version_str == "3.0.0-stub":
            blockers.append(
                "stub_artifact: version=3.0.0-stub — model not fully trained "
                "(insufficient data, missing dependency, or validation failure)"
            )
            # Extract embedded reason if present
            val = report.get("validation", {})
            reasons = val.get("reasons", []) if isinstance(val, dict) else []
            if reasons:
                blockers.append(f"stub_reasons: {reasons}")

        # ── 5b. AUC ───────────────────────────────────────────────────────
        auc = holdout.get("auc")
        if auc is None:
            blockers.append("missing_holdout_auc")
        elif _f(auc) < auc_min:
            blockers.append(f"auc_below_threshold: {_f(auc):.3f} < {auc_min:.3f}")

        # ── 5c. Expectancy ────────────────────────────────────────────────
        exp = holdout.get("expectancy_r_top5pct")
        if exp is None:
            blockers.append("missing_expectancy_r_top5pct")
        elif _f(exp) <= 0.0:
            blockers.append(f"negative_expectancy: expectancy_r_top5pct={_f(exp):+.3f} ≤ 0")

        # ── 5d. ECE ───────────────────────────────────────────────────────
        ece = holdout.get("ece", holdout.get("ece10"))
        if ece is None:
            blockers.append("missing_ece")
        elif _f(ece) > ece_max:
            blockers.append(f"poor_calibration: ECE={_f(ece):.3f} > {ece_max:.3f}")

    passed = len(blockers) == 0
    return passed, blockers, evidence


def _write_cfg2(r: Any, key: str, patch: dict[str, Any]) -> None:
    m: dict[str, str] = {}
    for k, v in patch.items():
        if isinstance(v, (dict, list)):
            m[k] = json.dumps(v, ensure_ascii=False, separators=(",", ":"))
        else:
            m[k] = str(v)
    r.hset(key, mapping=m)


def main() -> int:
    ap = argparse.ArgumentParser(description="Pre-promotion gate check for MetaModelLR artifacts")
    ap.add_argument(
        "--model",
        default=os.getenv("META_MODEL_PATH", ""),
        help="Path to model JSON (default: $META_MODEL_PATH)",
    )
    ap.add_argument(
        "--report",
        default="",
        help="Path to paired report JSON (auto-detected if omitted)",
    )
    ap.add_argument(
        "--max-age-days",
        type=int,
        default=int(os.getenv("META_GATE_MAX_AGE_DAYS", "30")),
        help="Maximum artifact age in days (default 30)",
    )
    ap.add_argument(
        "--auc-min",
        type=float,
        default=float(os.getenv("META_GATE_AUC_MIN", "0.62")),
        help="Minimum holdout AUC (default 0.62)",
    )
    ap.add_argument(
        "--ece-max",
        type=float,
        default=float(os.getenv("META_GATE_ECE_MAX", "0.10")),
        help="Maximum ECE (default 0.10)",
    )
    ap.add_argument(
        "--apply",
        type=int,
        default=0,
        help="1 = write meta_model_enable=1 + meta_model_mode=SHADOW to cfg2 on PASS",
    )
    ap.add_argument(
        "--redis-url",
        default=os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0"),
    )
    ap.add_argument(
        "--cfg2-key",
        default=os.getenv("DYN_CFG_KEY", "settings:dynamic_cfg"),
    )
    args = ap.parse_args()

    if not args.model:
        print(json.dumps({
            "ok": False,
            "blockers": ["--model not specified and META_MODEL_PATH is unset"],
        }, ensure_ascii=False, indent=2))
        return 2

    passed, blockers, evidence = check_model(
        model_path=args.model,
        report_path=args.report or None,
        max_age_days=args.max_age_days,
        auc_min=args.auc_min,
        ece_max=args.ece_max,
        now_ms=_now_ms(),
    )

    result: dict[str, Any] = {
        "ok": passed,
        "blockers": blockers,
        "evidence": evidence,
        "thresholds": {
            "auc_min": args.auc_min,
            "ece_max": args.ece_max,
            "max_age_days": args.max_age_days,
        },
        "apply": bool(args.apply),
        "applied": False,
        "note": (
            "PASS — SHADOW only. Reach ENFORCE via nightly_meta_enforce_propose_bundle "
            "after ≥48 h of clean shadow metrics."
            if passed
            else "FAIL — ENFORCE blocked. Fix blockers and retrain before enabling."
        ),
    }

    if passed and args.apply:
        try:
            import redis as redis_lib  # type: ignore
        except ImportError:
            result["apply_error"] = "redis package not installed"
            print(json.dumps(result, ensure_ascii=False, indent=2))
            return 1

        try:
            r = redis_lib.Redis.from_url(args.redis_url, decode_responses=False)
            r.ping()
            patch = {
                "meta_model_enable": "1",
                "meta_model_mode": "SHADOW",
                "meta_model_path": args.model,
                "meta_gate_check_last_ms": str(_now_ms()),
            }
            _write_cfg2(r, args.cfg2_key, patch)
            result["applied"] = True
            result["applied_patch"] = patch
        except Exception as e:
            result["apply_error"] = str(e)
            print(json.dumps(result, ensure_ascii=False, indent=2))
            return 1

    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0 if passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
