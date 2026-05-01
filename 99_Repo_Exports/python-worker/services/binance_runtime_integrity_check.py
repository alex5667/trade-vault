from __future__ import annotations
"""P4: Runtime integrity checker — class-aware duplicate method scanner.

Scans each of the two runtime source files for methods that appear MORE THAN
ONCE under the same name within the SAME class.  Methods that appear once per
class (e.g. _request in BinanceFuturesREST AND in BinanceFuturesClient) are
NOT reported as duplicates — that is the correct multi-class layout.

Usage (standalone CLI):
    python services/binance_runtime_integrity_check.py
    # exits 0 → OK, exits 1 → duplicates found
"""

import re
from pathlib import Path
from typing import Dict, Iterable, List, Tuple

# Critical method names that must appear at most ONCE per class in each file.
CRITICAL_METHODS: Dict[str, List[str]] = {
    "binance_futures_client.py": [
        "_request",
        "get_account",
        "get_position_risk",
        "get_open_orders",
        "from_env",
        "get_premium_index",
        "reconcile_entry_by_client_id",
        "reconcile_protection_by_sid",
        "replace_untriggered_algo_order",
        "inspect_protection_set",
    ],
    "binance_executor.py": [
        "_resume_open_from_state",
        "_attempt_reconcile_after_exception",
        "_emergency_flatten_position",
        "_cancel_plain_order_best_effort",
        "handle_open",
        "handle_modify",
        "handle_resize",
        "_replace_position_protection",
        "_verify_protection_on_exchange",
    ],
}


def scan_duplicate_method_defs(path: Path, method_names: Iterable[str]) -> Dict[str, List[int]]:
    """Scan *path* for duplicate definitions of *method_names* within each class.

    Returns {method_name: [line_numbers, ...]} only for names that appear
    MORE THAN ONCE within the same class block.  Cross-class occurrences (e.g.
    two different classes both defining ``_request``) are not reported here.

    Algorithm:
      - Track the current class with a lightweight indent heuristic.
      - Each ``class Foo`` resets the per-class counter.
      - Counts are accumulated per (class_name, method_name).
      - After parsing, any (class, name) pair with count > 1 is a duplicate.
    """
    names = set(method_names)
    # class_name → {method_name: [linenos]}
    class_hits: Dict[str, Dict[str, List[int]]] = {}
    current_class: str = "<module>"
    class_hits[current_class] = {}

    for lineno, line in enumerate(path.read_text().splitlines(), start=1):
        # Detect class definition
        cm = re.match(r"^class\s+([A-Za-z_][A-Za-z0-9_]*)\s*[:(]", line)
        if cm:
            current_class = cm.group(1)
            class_hits.setdefault(current_class, {})
            continue
        # Detect method definition
        dm = re.match(r"^\s{4,}def\s+([A-Za-z_][A-Za-z0-9_]*)\s*\(", line)
        if dm:
            name = dm.group(1)
            if name in names:
                class_hits.setdefault(current_class, {}).setdefault(name, []).append(lineno)

    # Collect names with > 1 occurrence within ANY single class
    duplicates: Dict[str, List[int]] = {}
    for _cls, hits in class_hits.items():
        for name, linenos in hits.items():
            if len(linenos) > 1:
                existing = duplicates.get(name, [])
                duplicates[name] = existing + linenos
    return duplicates


def list_runtime_artifact_files(base_dir: Path) -> List[Path]:
    """Return sorted list of .orig/.rej/.bak files in *base_dir*."""
    return sorted(
        p for p in base_dir.iterdir()
        if p.is_file() and p.name.endswith((".orig", ".rej", ".bak"))
    )


def main() -> int:
    """CLI entry-point: scans runtime source files, returns exit code."""
    base_dir = Path(__file__).resolve().parent
    failures: List[Tuple[Path, Dict[str, List[int]]]] = []
    for filename, method_names in CRITICAL_METHODS.items():
        path = base_dir / filename
        if not path.exists():
            print(f"WARN file not found: {path}")
            continue
        duplicates = scan_duplicate_method_defs(path, method_names)
        if duplicates:
            failures.append((path, duplicates))
    if failures:
        for path, duplicates in failures:
            print(f"DUPLICATE METHODS: {path}")
            for name, lines in sorted(duplicates.items()):
                print(f"  {name}: {lines}")
        return 1
    print("binance runtime integrity OK")
    artifact_files = list_runtime_artifact_files(base_dir)
    if artifact_files:
        print("WARN runtime artifacts present:")
        for item in artifact_files:
            print(f"  {item.name}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
