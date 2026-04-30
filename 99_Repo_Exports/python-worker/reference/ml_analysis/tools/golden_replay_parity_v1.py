from __future__ import annotations

"""Golden replay parity harness (B5).

Reads captured decision records (NDJSON), re-runs OFConfirmEngine.build() deterministically
(from captured inputs + runtime snapshot) and compares with stored OFConfirmV3.

Policy guard:
  - refuse mixed dq_policy_hash / manifest_hash within one replay run (unless --allow-mixed-policy)
"""

import argparse
import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Dict, Iterable, Optional, Tuple

from ml_analysis.golden_replay.compare import (
    diff_objects
    extract_expected_ofc
    extract_policy_keys
    summarize_diffs
)


def _read_ndjson(path: Path, *, limit: int = 0) -> Iterable[Dict[str, Any]]:
    n = 0
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except Exception:
                continue
            if isinstance(obj, dict):
                yield obj
                n += 1
            if limit and n >= limit:
                break


def _pick(rec: Dict[str, Any], *keys: str) -> Optional[Any]:
    for k in keys:
        v = rec.get(k)
        if v is not None:
            return v
    return None


def _resolve_inputs(rec: Dict[str, Any]) -> Tuple[Dict[str, Any], Dict[str, Any], Dict[str, Any]]:
    inputs = rec.get("inputs") if isinstance(rec.get("inputs"), dict) else rec
    indicators = rec.get("indicators")
    if not isinstance(indicators, dict):
        indicators = inputs.get("indicators") if isinstance(inputs.get("indicators"), dict) else {}
    snap = _pick(rec, "runtime_snapshot", "runtime", "runtime_snap", "runtime_state")
    if not isinstance(snap, dict):
        snap = inputs.get("runtime_snapshot") if isinstance(inputs.get("runtime_snapshot"), dict) else {}
    return inputs, indicators, snap


def _build_runtime(engine: Any, snap: Dict[str, Any]) -> Any:
    fn = getattr(engine, "build_runtime_from_snapshot", None)
    if callable(fn) and snap:
        try:
            return fn(snap)
        except Exception:
            pass
    return SimpleNamespace(**{k: v for k, v in snap.items() if isinstance(k, str)})


def _engine_import() -> Any:
    for mod in (
        "tick_flow_full.core.of_confirm_engine"
        "core.of_confirm_engine"
        "of_confirm_engine"
    ):
        try:
            m = __import__(mod, fromlist=["OFConfirmEngine"])
            return getattr(m, "OFConfirmEngine")
        except Exception:
            continue
    raise RuntimeError("Cannot import OFConfirmEngine (check PYTHONPATH / repo layout).")


def _as_dict(obj: Any) -> Any:
    if obj is None:
        return None
    try:
        import dataclasses
        if dataclasses.is_dataclass(obj):
            return dataclasses.asdict(obj)
    except Exception:
        pass
    if isinstance(obj, dict):
        return obj
    try:
        return dict(obj.__dict__)
    except Exception:
        return obj


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True, help="NDJSON decision records path")
    ap.add_argument("--outdir", default="out_golden_replay", help="Output directory")
    ap.add_argument("--limit", type=int, default=0, help="Max rows (0 = all)")
    ap.add_argument("--abs-tol", type=float, default=1e-6)
    ap.add_argument("--rel-tol", type=float, default=1e-6)
    ap.add_argument("--evidence", default="lite", choices=("none", "lite", "all"))
    ap.add_argument("--fail-on-mismatch", action="store_true")
    ap.add_argument("--allow-mixed-policy", action="store_true")
    ap.add_argument("--compare-meta-features", action="store_true")
    args = ap.parse_args()

    in_path = Path(args.input)
    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    OFConfirmEngine = _engine_import()
    engine = OFConfirmEngine()

    policy_seen = None
    manifest_seen = None

    total = 0
    mism = 0
    mismatch_samples = []

    evidence_keys_lite = {
        "meta_enable", "meta_mode", "meta_p_min", "meta_p", "meta_veto", "meta_reason"
        "meta_schema_name", "meta_schema_version", "meta_schema_hash"
        "meta_model_schema_name", "meta_model_schema_version", "meta_model_schema_hash"
        "hard_veto", "ok_soft", "scenario_v4", "need_reason", "policy_reason"
    }

    for rec in _read_ndjson(in_path, limit=args.limit):
        total += 1
        inputs, indicators, snap = _resolve_inputs(rec)
        ph, mh = extract_policy_keys(rec)

        if not args.allow_mixed_policy:
            if policy_seen is None and ph:
                policy_seen = ph
            if manifest_seen is None and mh:
                manifest_seen = mh
            if policy_seen and ph and ph != policy_seen:
                raise SystemExit(f"Mixed dq_policy_hash: seen={policy_seen} got={ph} row={total}")
            if manifest_seen and mh and mh != manifest_seen:
                raise SystemExit(f"Mixed manifest hash: seen={manifest_seen} got={mh} row={total}")

        exp_ofc = extract_expected_ofc(rec)
        if not isinstance(exp_ofc, dict):
            continue

        cap = None
        try:
            ev = exp_ofc.get("evidence") if isinstance(exp_ofc.get("evidence"), dict) else {}
            cap = ev.get("golden_replay_inputs_v1") if isinstance(ev.get("golden_replay_inputs_v1"), dict) else None
        except Exception:
            cap = None

        if (not snap) and isinstance(cap, dict) and isinstance(cap.get("runtime_snapshot"), dict):
            snap = cap.get("runtime_snapshot") or {}

        symbol = str(
            _pick(inputs, "symbol", "sym")
            or (cap.get("symbol") if isinstance(cap, dict) else None)
            or exp_ofc.get("symbol")
            or indicators.get("symbol")
            or ""
        )
        tf = str(_pick(inputs, "tf", "timeframe") or (cap.get("tf") if isinstance(cap, dict) else None) or "1m")
        direction = str(
            _pick(inputs, "direction", "dir")
            or (cap.get("direction") if isinstance(cap, dict) else None)
            or exp_ofc.get("direction")
            or indicators.get("direction")
            or indicators.get("side")
            or ""
        )
        tick_ts_ms = int(float(
            _pick(inputs, "tick_ts_ms", "ts_ms", "tick_ts", "event_ts_ms")
            or (cap.get("tick_ts_ms") if isinstance(cap, dict) else None)
            or indicators.get("tick_ts_ms")
            or indicators.get("event_ts_ms")
            or exp_ofc.get("ts_ms")
            or 0
        ))
        price = float(
            _pick(inputs, "price", "mid", "last")
            or (cap.get("price") if isinstance(cap, dict) else None)
            or indicators.get("price")
            or indicators.get("mid")
            or 0.0
        )
        delta_z = float(_pick(inputs, "delta_z", "dz") or (cap.get("delta_z") if isinstance(cap, dict) else None) or indicators.get("delta_z") or 0.0)

        cfg = _pick(inputs, "cfg", "cfg2", "config") or rec.get("cfg2") or {}
        if not isinstance(cfg, dict):
            cfg = {}

        runtime = _build_runtime(engine, snap)

        try:
            ofc, _dec = engine.build(
                symbol=symbol
                tf=tf
                direction=direction
                tick_ts_ms=tick_ts_ms
                price=price
                delta_z=delta_z
                snap_t0=None
                snap_prev=None
                runtime=runtime
                cfg=cfg
                indicators=dict(indicators)
                absorption=inputs.get("absorption") if isinstance(inputs.get("absorption"), dict) else None
            )
        except Exception as e:
            mism += 1
            mismatch_samples.append({"row": total, "kind": "engine_error", "error": str(e)[:300]})
            continue

        got = _as_dict(ofc) if ofc is not None else None
        exp = exp_ofc
        if got is None:
            mism += 1
            mismatch_samples.append({"row": total, "kind": "missing_output"})
            continue

        if args.evidence != "all":
            if isinstance(got.get("evidence"), dict):
                got["evidence"] = {} if args.evidence == "none" else {k: got["evidence"].get(k) for k in evidence_keys_lite if k in got["evidence"]}
            if isinstance(exp.get("evidence"), dict):
                exp["evidence"] = {} if args.evidence == "none" else {k: exp["evidence"].get(k) for k in evidence_keys_lite if k in exp["evidence"]}

        if args.compare_meta_features:
            ge = got.get("evidence") if isinstance(got.get("evidence"), dict) else {}
            ee = exp.get("evidence") if isinstance(exp.get("evidence"), dict) else {}
            if not (isinstance(ge.get("meta_features_export"), dict) and isinstance(ee.get("meta_features_export"), dict)):
                mism += 1
                mismatch_samples.append({"row": total, "kind": "meta_features_missing"})
                continue

        diffs = diff_objects(exp, got, abs_tol=float(args.abs_tol), rel_tol=float(args.rel_tol), max_diffs=200)
        if diffs:
            mism += 1
            mismatch_samples.append({
                "row": total
                "kind": "diff"
                "summary": summarize_diffs(diffs)
                "policy_hash": ph
                "manifest_hash": mh
            })

    report = {
        "input": str(in_path)
        "total_rows": total
        "mismatched_rows": mism
        "policy_hash": policy_seen
        "manifest_hash": manifest_seen
        "abs_tol": float(args.abs_tol)
        "rel_tol": float(args.rel_tol)
        "evidence_mode": args.evidence
        "compare_meta_features": bool(args.compare_meta_features)
        "samples": mismatch_samples[:50]
    }
    (outdir / "golden_replay_report.json").write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")

    if mism and args.fail_on_mismatch:
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
