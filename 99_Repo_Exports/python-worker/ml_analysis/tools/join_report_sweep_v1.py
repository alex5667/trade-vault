#!/usr/bin/env python3
from __future__ import annotations

"""
Run `build_edge_stack_dataset_from_redis` for multiple join tolerances and emit
one compact comparison report.

Intended use:
  - after 24-72h of fresh production closes
  - compare 10s / 60s / 300s join behaviour on the same source window
"""

import json
import os
import tempfile
from pathlib import Path

from core.redis_keys import RedisStreams as RS
from ml_analysis.tools import build_edge_stack_dataset_from_redis as builder


def _env(name: str, default: str) -> str:
    v = os.getenv(name)
    return v if (v is not None and str(v).strip() != "") else default


def _load_json(path: Path) -> dict:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _extract_summary(report: dict) -> dict[str, float | int]:
    ds = dict(report.get("dataset") or {})
    drop = dict(report.get("drop_stats") or {})
    total = int(ds.get("n_closes_total") or 0)
    matched = int(ds.get("n_rows") or 0)
    joined_by_sid = int(ds.get("n_joined_by_sid") or 0)
    joined_by_nearest = int(ds.get("n_joined_by_nearest") or 0)
    unmatched = max(0, total - matched)
    return {
        "n_closes_total": total,
        "n_rows": matched,
        "join_rate": (matched / total) if total > 0 else 0.0,
        "joined_by_sid": joined_by_sid,
        "joined_by_nearest": joined_by_nearest,
        "unmatched": unmatched,
        "drop_join_nearest_too_far": int(drop.get("join_nearest_too_far", 0) or 0),
        "drop_join_secondary_no_match": int(drop.get("join_secondary_no_match", 0) or 0),
    }


def main(argv: list[str] | None = None) -> int:
    del argv
    tolerances = [int(x.strip()) for x in _env("JOIN_SWEEP_TOLERANCES_MS", "10000,60000,300000").split(",") if x.strip()]
    out_dir = Path(_env("JOIN_SWEEP_OUT_DIR", "/tmp/join_report_sweep_v1")).expanduser()
    out_dir.mkdir(parents=True, exist_ok=True)

    results: dict[str, dict[str, float | int]] = {}
    for tol_ms in tolerances:
        tag = f"{tol_ms}ms"
        report_path = out_dir / f"join_{tag}.report.json"
        quarantine_path = out_dir / f"join_{tag}.quarantine.jsonl"
        with tempfile.NamedTemporaryFile(prefix=f"join_{tag}_", suffix=".jsonl", delete=False) as tmp_jsonl:
            out_jsonl = tmp_jsonl.name

        args = [
            "--redis_url", _env("REDIS_URL", "redis://localhost:6379/0"),
            "--signal_stream", _env("SIGNAL_STREAM", RS.OF_INPUTS),
            "--closed_stream", _env("TRADES_CLOSED_STREAM", RS.TRADES_CLOSED),
            "--signals_count", _env("SIGNALS_COUNT", "200000"),
            "--closes_count", _env("CLOSES_COUNT", "200000"),
            "--file_fallback", _env("FILE_FALLBACK", "0"),
            "--join_tolerance_ms", str(tol_ms),
            "--join_secondary", _env("JOIN_SECONDARY", "dir_scenario_soft"),
            "--nearest_max_scan", _env("NEAREST_MAX_SCAN", "200"),
            "--max_examples", _env("JOIN_SWEEP_MAX_EXAMPLES", "20"),
            "--out_jsonl", out_jsonl,
            "--out_report_json", str(report_path),
            "--out_quarantine_jsonl", str(quarantine_path),
        ]
        rc = int(builder.main(args))
        report = _load_json(report_path)
        summary = _extract_summary(report)
        summary["rc"] = rc
        results[tag] = summary

    summary_path = out_dir / "summary.json"
    summary_payload = {
        "tolerances_ms": tolerances,
        "join_secondary": _env("JOIN_SECONDARY", "dir_scenario_soft"),
        "nearest_max_scan": int(_env("NEAREST_MAX_SCAN", "200")),
        "results": results,
    }
    summary_path.write_text(json.dumps(summary_payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(summary_payload, ensure_ascii=False))
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
