from __future__ import annotations

"""Feature coverage gate for edge-stack training inputs.

Checks that Registry `f_*` columns are actually present in the source rows
before training silently vectorizes missing values to 0.0.

Sources:
  - JSONL dataset rows from build_edge_stack_dataset_from_redis
  - Redis stream payloads from signals:of:inputs
"""

import argparse
import json
import math
import os
from collections.abc import Iterable, Sequence
from typing import Any

try:
    from ml_analysis.tools.schema_choices_v1 import normalize_schema_ver as _norm_schema_ver
    from ml_analysis.tools.schema_choices_v1 import schema_choices as _schema_choices
except Exception:  # pragma: no cover
    from tools.schema_choices_v1 import normalize_schema_ver as _norm_schema_ver  # type: ignore
    from tools.schema_choices_v1 import schema_choices as _schema_choices  # type: ignore


DEFAULT_CRITICAL_FEATURES_TOKEN = "__all__"


def _as_str(x: Any) -> str:
    if x is None:
        return ""
    if isinstance(x, (bytes, bytearray)):
        try:
            return x.decode("utf-8", "ignore")
        except Exception:
            return str(x)
    return str(x)


def _safe_json_loads(x: Any) -> Any:
    if x is None:
        return None
    if isinstance(x, (dict, list)):
        return x
    s = _as_str(x).strip()
    if not s:
        return None
    try:
        return json.loads(s)
    except Exception:
        return None


def _as_float_or_nan(x: Any) -> float:
    try:
        if x is None or isinstance(x, bool):
            return float("nan")
        v = float(x)
        return v if math.isfinite(v) else float("nan")
    except Exception:
        return float("nan")


def _quantile(xs: Sequence[float], q: float) -> float:
    vals = sorted(float(x) for x in xs if math.isfinite(float(x)))
    if not vals:
        return 0.0
    if len(vals) == 1:
        return float(vals[0])
    pos = max(0.0, min(1.0, float(q))) * float(len(vals) - 1)
    lo = int(math.floor(pos))
    hi = int(math.ceil(pos))
    if lo == hi:
        return float(vals[lo])
    w = pos - lo
    return float(vals[lo] * (1.0 - w) + vals[hi] * w)


def _iter_dataset_rows(path: str, *, max_rows: int) -> Iterable[dict[str, Any]]:
    n = 0
    with open(path, encoding="utf-8") as f:
        for line in f:
            if max_rows > 0 and n >= max_rows:
                break
            s = line.strip()
            if not s:
                continue
            obj = _safe_json_loads(s)
            if isinstance(obj, dict):
                n += 1
                yield obj


def _iter_redis_rows(redis_url: str, stream: str, *, count: int) -> Iterable[dict[str, Any]]:
    try:
        import redis  # type: ignore
    except Exception as e:  # pragma: no cover
        raise SystemExit(f"redis-py is required for --redis_url: {e}")
    r = redis.Redis.from_url(redis_url, decode_responses=False)
    for _msg_id, fields in r.xrevrange(stream, max="+", min="-", count=int(count)):
        if not isinstance(fields, dict):
            continue
        payload = fields.get(b"payload")
        if payload is None:
            payload = fields.get("payload")
        obj = _safe_json_loads(payload)
        if isinstance(obj, dict):
            yield obj


def _row_indicators(row: dict[str, Any]) -> dict[str, Any]:
    ind = row.get("indicators") or row.get("features") or {}
    if isinstance(ind, str):
        ind2 = _safe_json_loads(ind)
        ind = ind2 if isinstance(ind2, dict) else {}
    return ind if isinstance(ind, dict) else {}


def _row_schema(row: dict[str, Any]) -> str:
    raw = row.get("feature_schema_version") or row.get("feature_schema_ver") or ""
    if not raw:
        ind = _row_indicators(row)
        raw = ind.get("feature_schema_version") or ind.get("feature_schema_ver") or ""
    return _norm_schema_ver(_as_str(raw).strip()) or "unknown"


def feature_cols_for_schema(schema_ver: str) -> list[str]:
    from core.feature_registry import get_edge_stack_feature_spec  # type: ignore
    spec = get_edge_stack_feature_spec(_norm_schema_ver(schema_ver))
    return [str(c) for c in spec.feature_cols]


def evaluate_rows(
    rows: Sequence[dict[str, Any]],
    *,
    feature_schema_ver: str,
    min_present_rate: float,
    critical_features: Sequence[str],
    min_nonzero_sample_n: int,
    fail_on_mixed_schema: bool,
) -> dict[str, Any]:
    schema_ver = _norm_schema_ver(feature_schema_ver)
    feature_cols = feature_cols_for_schema(schema_ver)
    fkeys = [c[2:] for c in feature_cols if str(c).startswith("f_")]
    total = int(len(rows))
    critical = _normalize_critical_features(critical_features, all_features=fkeys)

    schemas: dict[str, int] = {}
    stats: dict[str, dict[str, Any]] = {}
    values: dict[str, list[float]] = {k: [] for k in fkeys}
    for k in fkeys:
        stats[k] = {"present": 0, "nan": 0, "nonzero": 0}

    for row in rows:
        sv = _row_schema(row)
        schemas[sv] = schemas.get(sv, 0) + 1
        ind = _row_indicators(row)
        for k in fkeys:
            if k not in ind or ind.get(k) is None:
                continue
            stats[k]["present"] += 1
            v = _as_float_or_nan(ind.get(k))
            if not math.isfinite(v):
                stats[k]["nan"] += 1
                continue
            values[k].append(float(v))
            if abs(float(v)) > 1e-12:
                stats[k]["nonzero"] += 1

    feature_reports: list[dict[str, Any]] = []
    violations: list[dict[str, Any]] = []
    denom = max(1, total)
    for k in fkeys:
        s = stats[k]
        present_rate = float(s["present"]) / float(denom)
        nonzero_rate = float(s["nonzero"]) / float(denom)
        nan_rate = float(s["nan"]) / float(denom)
        rep = {
            "feature": k,
            "present": int(s["present"]),
            "present_rate": round(present_rate, 6),
            "nonzero": int(s["nonzero"]),
            "nonzero_rate": round(nonzero_rate, 6),
            "nan": int(s["nan"]),
            "nan_rate": round(nan_rate, 6),
            "p50": _quantile(values[k], 0.50),
            "p95": _quantile(values[k], 0.95),
        }
        feature_reports.append(rep)
        if present_rate < float(min_present_rate):
            violations.append({"kind": "low_present_rate", **rep})
        if k in critical and total >= int(min_nonzero_sample_n) and int(s["nonzero"]) == 0:
            violations.append({"kind": "critical_all_zero", **rep})

    normalized_schemas = {k: v for k, v in schemas.items() if v > 0}
    mixed_schema = len(normalized_schemas) != 1 or (schema_ver and normalized_schemas.get(schema_ver, 0) != total)
    if fail_on_mixed_schema and mixed_schema:
        violations.append({
            "kind": "mixed_feature_schema_version",
            "expected": schema_ver,
            "counts": normalized_schemas,
        })

    feature_reports.sort(key=lambda r: (float(r["present_rate"]), float(r["nonzero_rate"]), str(r["feature"])))
    return {
        "tool": "feature_coverage_gate_v1",
        "ok": len(violations) == 0,
        "feature_schema_ver": schema_ver,
        "rows": total,
        "feature_cols_n": len(feature_cols),
        "f_features_n": len(fkeys),
        "min_present_rate": float(min_present_rate),
        "critical_features": sorted(critical),
        "min_nonzero_sample_n": int(min_nonzero_sample_n),
        "schema_version_counts": normalized_schemas,
        "violations_n": len(violations),
        "violations": violations[:100],
        "top_missing_features": feature_reports[:50],
    }


def _normalize_critical_features(features: Sequence[str], *, all_features: Sequence[str]) -> set[str]:
    out: set[str] = set()
    saw_all = False
    for c in features:
        raw = str(c or "").strip()
        if not raw:
            continue
        low = raw.lower()
        if low in ("none", "off", "disable", "disabled", "0"):
            return set()
        if low in ("all", "__all__", "*"):
            saw_all = True
            continue
        out.add(raw[2:] if raw.startswith("f_") else raw)
    if saw_all or not out:
        out.update(str(k) for k in all_features)
    return out


def _split_csv(s: str) -> list[str]:
    return [x.strip() for x in str(s or "").split(",") if x.strip()]


def main(argv: Sequence[str] | None = None) -> int:
    ap = argparse.ArgumentParser()
    src = ap.add_mutually_exclusive_group(required=True)
    src.add_argument("--data_jsonl", default="")
    src.add_argument("--redis_url", default="")
    ap.add_argument("--stream", default=os.getenv("ML_REPLAY_STREAM", "signals:of:inputs"))
    ap.add_argument("--count", type=int, default=int(os.getenv("FEATURE_COVERAGE_COUNT", "200000")))
    ap.add_argument("--feature_schema_ver", required=True, choices=_schema_choices(include_empty=False))
    ap.add_argument("--min_present_rate", type=float, default=float(os.getenv("FEATURE_COVERAGE_MIN_PRESENT_RATE", "0.995")))
    ap.add_argument(
        "--critical_features",
        default=os.getenv("FEATURE_COVERAGE_CRITICAL_FEATURES", DEFAULT_CRITICAL_FEATURES_TOKEN),
        help="Comma-separated critical f_* features. Default '__all__' checks every Registry f_* feature; use 'none' to disable all-zero checks.",
    )
    ap.add_argument("--min_nonzero_sample_n", type=int, default=int(os.getenv("FEATURE_COVERAGE_MIN_NONZERO_SAMPLE_N", "500")))
    ap.add_argument("--fail_on_mixed_schema", type=int, default=int(os.getenv("FEATURE_COVERAGE_FAIL_ON_MIXED_SCHEMA", "1")))
    ap.add_argument("--out_report_json", default="")
    args = ap.parse_args(list(argv) if argv is not None else None)

    if args.data_jsonl:
        rows = list(_iter_dataset_rows(str(args.data_jsonl), max_rows=int(args.count)))
    else:
        rows = list(_iter_redis_rows(str(args.redis_url), str(args.stream), count=int(args.count)))

    report = evaluate_rows(
        rows,
        feature_schema_ver=str(args.feature_schema_ver),
        min_present_rate=float(args.min_present_rate),
        critical_features=_split_csv(str(args.critical_features)),
        min_nonzero_sample_n=int(args.min_nonzero_sample_n),
        fail_on_mixed_schema=bool(int(args.fail_on_mixed_schema)),
    )

    if args.out_report_json:
        os.makedirs(os.path.dirname(os.path.abspath(args.out_report_json)) or ".", exist_ok=True)
        with open(args.out_report_json, "w", encoding="utf-8") as f:
            json.dump(report, f, ensure_ascii=False, indent=2, sort_keys=True)

    print(json.dumps(report, ensure_ascii=False, sort_keys=True))
    return 0 if bool(report.get("ok")) else 2


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
