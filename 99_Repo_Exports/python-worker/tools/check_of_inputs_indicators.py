#!/usr/bin/env python3
from __future__ import annotations
"""Проверка наличия всех необходимых индикаторов в OF inputs для legs.

Проверяет:
- Основные индикаторы из OFInputsV1 contract
- Дополнительные индикаторы для legs (OFI, FP edge)
- Конфигурация (of_score_min)
"""


import argparse
import json
import os
import sys
from pathlib import Path
from typing import Dict, Any, List, Set


REQUIRED_INDICATORS: Dict[str, List[str]] = {
    "core": [
        "delta_z",
        "weak_progress",
        "sweep_recent",
        "reclaim_recent",
        "obi_stable",
        "iceberg_strict",
        "abs_lvl_ok",
    ],
    "obi": [
        "obi",  # значение OBI
        "obi_stable_secs",  # длительность стабильности
    ],
    "iceberg": [
        "iceberg_score",  # score айсберга
    ],
    "ofi": [
        "ofi_stable",  # флаг стабильности OFI
        "ofi_dir_ok",  # флаг направления OFI
        "ofi",  # значение OFI
        "ofi_z",  # z-score OFI
        "ofi_stable_secs",  # длительность стабильности
        "ofi_stability_score",  # score стабильности
    ],
    "fp_edge": [
        "fp_edge_absorb",  # флаг поглощения на краю
        "fp_edge_absorb_strength",  # сила поглощения
    ],
    "sweep_reclaim": [
        "sweep_kind",  # тип sweep
        "reclaim_kind",  # тип reclaim
    ],
    "config": [
        "cfg",  # конфигурация
    ],
}


def check_inputs_file(inputs_path: str, symbol_filter: str = "") -> Dict[str, Any]:
    """Проверяет файл inputs на наличие всех индикаторов."""
    if not os.path.exists(inputs_path):
        return {"error": f"File not found: {inputs_path}"}
    
    results: Dict[str, Any] = {
        "total_rows": 0,
        "symbols": set(),
        "indicators_present": {},
        "indicators_missing": {},
        "config_values": {},
        "sample": None,
    }
    
    with open(inputs_path, "r", encoding="utf-8") as f:
        rows = []
        for line in f:
            if not line.strip():
                continue
            try:
                row = json.loads(line)
                rows.append(row)
            except Exception:
                continue
    
    if not rows:
        return {"error": "No valid rows found"}
    
    results["total_rows"] = len(rows)
    
    # Фильтр по символу
    if symbol_filter:
        rows = [r for r in rows if str(r.get("symbol", "")).upper() == symbol_filter.upper()]
        if not rows:
            return {"error": f"No rows for symbol {symbol_filter}"}
    
    # Собираем символы
    for row in rows:
        sym = str(row.get("symbol", "")).upper()
        if sym:
            results["symbols"].add(sym)
    
    # Проверяем индикаторы на первом sample
    sample = rows[0]
    results["sample"] = {
        "symbol": sample.get("symbol"),
        "ts_ms": sample.get("ts_ms"),
        "direction": sample.get("direction"),
        "scenario": sample.get("scenario"),
    }
    
    # Determine version from sample
    version = sample.get("v", 1)
    inputs_version = sample.get("inputs_version", "v1" if version == 1 else "v2")
    if version == 2 or inputs_version == "v2":
        version_str = "v2"
    else:
        version_str = "v1"
    
    results["version"] = version_str
    results["inputs_version"] = inputs_version
    
    # Проверка наличия индикаторов
    all_required = []
    for category, indicators in REQUIRED_INDICATORS.items():
        for ind in indicators:
            all_required.append((category, ind))
    
    # For v1, OFI and FP edge are optional (not required)
    # For v2, they should be present (but may be zero/disabled)
    for category, ind in all_required:
        # Skip OFI/FP edge requirements for v1
        if version_str == "v1" and (category == "ofi" or category == "fp_edge"):
            continue
        
        if ind in sample:
            val = sample[ind]
            if category not in results["indicators_present"]:
                results["indicators_present"][category] = {}
            results["indicators_present"][category][ind] = val
        else:
            if category not in results["indicators_missing"]:
                results["indicators_missing"][category] = []
            results["indicators_missing"][category].append(ind)
    
    # Проверка конфигурации
    cfg = sample.get("cfg", {})
    if isinstance(cfg, dict):
        results["config_values"] = {
            "of_score_min": cfg.get("of_score_min", "NOT_SET"),
            "w_exec_risk": cfg.get("w_exec_risk", "NOT_SET"),
            "exec_risk_ref_bps": cfg.get("exec_risk_ref_bps", "NOT_SET"),
        }
    
    # Статистика по всем строкам
    stats = {
        "obi_stable_count": sum(1 for r in rows if r.get("obi_stable", 0) == 1),
        "iceberg_strict_count": sum(1 for r in rows if r.get("iceberg_strict", 0) == 1),
        "sweep_recent_count": sum(1 for r in rows if r.get("sweep_recent", 0) == 1),
        "reclaim_recent_count": sum(1 for r in rows if r.get("reclaim_recent", 0) == 1),
        "weak_progress_count": sum(1 for r in rows if r.get("weak_progress", 0) == 1),
    }
    
    # Проверяем наличие OFI и FP edge (если они есть в contract)
    ofi_present = any("ofi" in str(k).lower() for k in sample.keys())
    fp_present = any("fp_edge" in str(k).lower() for k in sample.keys())
    
    # Check for missing OFI/FP in v2
    missing_inputs_ofi = 0
    missing_inputs_fp = 0
    if version_str == "v2":
        # In v2, OFI fields should be present (even if zero/disabled)
        ofi_required_fields = ["ofi", "ofi_z", "ofi_stable", "ofi_dir_ok", "ofi_age_ms"]
        if not all(f in sample for f in ofi_required_fields):
            missing_inputs_ofi = 1
        
        # In v2, FP edge fields should be present (even if zero/disabled)
        fp_required_fields = ["fp_edge_absorb", "fp_edge_age_ms"]
        if not all(f in sample for f in fp_required_fields):
            missing_inputs_fp = 1
    
    results["stats"] = stats
    results["ofi_present"] = ofi_present
    results["fp_edge_present"] = fp_present
    results["missing_inputs_ofi"] = missing_inputs_ofi
    results["missing_inputs_fp"] = missing_inputs_fp
    
    return results


def main() -> None:
    ap = argparse.ArgumentParser(description="Check OF inputs for required indicators")
    ap.add_argument("--inputs", required=True, help="Path to inputs NDJSON file")
    ap.add_argument("--symbol", default="", help="Filter by symbol (optional)")
    ap.add_argument("--json", action="store_true", help="Output as JSON")
    args = ap.parse_args()
    
    results = check_inputs_file(args.inputs, args.symbol)
    
    if "error" in results:
        print(f"❌ Error: {results['error']}", file=sys.stderr)
        sys.exit(1)
    
    if args.json:
        print(json.dumps(results, indent=2, ensure_ascii=False))
        return
    
    # Human-readable output
    print("=" * 60)
    print("OF Inputs Indicators Check")
    print("=" * 60)
    print(f"\nFile: {args.inputs}")
    print(f"Total rows: {results['total_rows']}")
    print(f"Symbols: {', '.join(sorted(results['symbols']))}")
    
    if results["sample"]:
        print(f"\nSample:")
        for k, v in results["sample"].items():
            print(f"  {k}: {v}")
    
    print(f"\n=== Indicators Status ===")
    
    # Present indicators
    if results["indicators_present"]:
        print("\n✅ Present:")
        for category, indicators in results["indicators_present"].items():
            print(f"  {category}:")
            for ind, val in indicators.items():
                print(f"    ✓ {ind} = {val}")
    
    # Missing indicators
    if results["indicators_missing"]:
        print("\n❌ Missing:")
        for category, indicators in results["indicators_missing"].items():
            print(f"  {category}:")
            for ind in indicators:
                print(f"    ✗ {ind}")
    else:
        print("\n✅ All required indicators present!")
    
    # Config
    if results["config_values"]:
        print(f"\n=== Configuration ===")
        for k, v in results["config_values"].items():
            print(f"  {k}: {v}")
    
    # Stats
    if results.get("stats"):
        print(f"\n=== Statistics ===")
        stats = results["stats"]
        total = results["total_rows"]
        for k, v in stats.items():
            pct = (v / total * 100.0) if total > 0 else 0.0
            print(f"  {k}: {v} ({pct:.1f}%)")
    
    # Version info
    print(f"\n=== Version Info ===")
    print(f"  Version: {results.get('version', 'unknown')}")
    print(f"  Inputs version: {results.get('inputs_version', 'unknown')}")
    
    # OFI/FP presence
    print(f"\n=== Advanced Indicators ===")
    print(f"  OFI indicators present: {'✅' if results.get('ofi_present') else '❌'}")
    print(f"  FP edge indicators present: {'✅' if results.get('fp_edge_present') else '❌'}")
    if results.get('version') == 'v2':
        print(f"  Missing OFI fields (v2): {'❌' if results.get('missing_inputs_ofi') else '✅'}")
        print(f"  Missing FP edge fields (v2): {'❌' if results.get('missing_inputs_fp') else '✅'}")


if __name__ == "__main__":
    main()

