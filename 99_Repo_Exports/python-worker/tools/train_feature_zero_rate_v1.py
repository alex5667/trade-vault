"""train_feature_zero_rate_v1.py — per-column zero-rate report for the
edge-stack trainer.

Closes audit-2026-05-29 item 6: when training v15_of, the registry adds
categorical one-hots (``bucket:``, ``hour:``, ``dow:``, ``session_``,
``dir:``) and ``f_*`` numeric columns. If `build_feature_row` regresses
and stops encoding a category, the trainer silently produces a model
whose entire column is zero — every prediction loses access to that
feature without any error surface.

This helper:
  1. Computes per-column zero-rate from the built feature matrix.
  2. Groups columns by family (``bucket:`` / ``hour:`` / ``dow:`` /
     ``session_`` / ``dir:`` / ``f_`` / ``other``) for low-cardinality
     Prometheus emission.
  3. Writes a deterministic JSON report (training artefact) plus best-
     effort Prometheus gauges (no-op if `prometheus_client` is missing).
  4. Raises ``CategoricalAllZeroError`` when a v15_of categorical family
     is entirely zero — the trainer wraps this as fail-fast.

The helper is import-clean (no Prometheus side-effects on import) so the
existing trainer test suite stays fast.
"""
from __future__ import annotations

import json
import logging
import os
from collections.abc import Sequence
from typing import Any

logger = logging.getLogger("train_feature_zero_rate_v1")


# ── Column family taxonomy ───────────────────────────────────────────────────

_REGISTRY_CATEGORICAL_PREFIXES: tuple[str, ...] = (
    "bucket:",
    "hour:",
    "dow:",
    "session_",
    "dir:",
)

_NUMERIC_PREFIX = "f_"


def _column_family(col: str) -> str:
    for prefix in _REGISTRY_CATEGORICAL_PREFIXES:
        if col.startswith(prefix):
            return prefix.rstrip(":_")
    if col.startswith(_NUMERIC_PREFIX):
        return "f"
    return "other"


# ── Pure computation ─────────────────────────────────────────────────────────


def compute_zero_rates(
    X: Any,
    feature_cols: Sequence[str],
    *,
    epsilon: float = 1e-12,
) -> dict[str, Any]:
    """Compute per-column and per-family zero-rate.

    Args:
        X:            row-oriented feature matrix (numpy ndarray or
                      anything with ``shape`` and indexable rows/cols).
        feature_cols: column names in the same order as ``X`` columns.
        epsilon:      |value| ≤ epsilon counts as zero.

    Returns:
        {
            "rows":          int n_rows,
            "cols":          int n_cols,
            "epsilon":       float,
            "per_column":    {col: zero_rate, ...},
            "per_family":    {family: {"cols": int, "all_zero_cols": int,
                                      "mean_zero_rate": float}},
            "all_zero_cols": [col, ...]    (sorted, deterministic),
        }
    """
    try:
        import numpy as np  # local — keeps import cheap when unused
    except Exception as e:
        raise RuntimeError("numpy required for compute_zero_rates") from e

    arr = np.asarray(X)
    if arr.ndim != 2:
        raise ValueError(f"expected 2D matrix, got shape {arr.shape}")
    n_rows, n_cols = arr.shape
    if n_cols != len(feature_cols):
        raise ValueError(
            f"feature_cols length {len(feature_cols)} != X.shape[1] {n_cols}"
        )

    # |x| ≤ epsilon counted as zero so robust-scaled values around 0 do
    # not trigger false positives on numeric f_* columns.
    zero_mask = np.abs(arr) <= float(epsilon)
    per_col_zero = zero_mask.mean(axis=0) if n_rows > 0 else np.zeros(n_cols)

    per_column: dict[str, float] = {}
    per_family_rates: dict[str, list[float]] = {}
    per_family_all_zero: dict[str, int] = {}
    per_family_cols: dict[str, int] = {}
    all_zero_cols: list[str] = []

    for i, col in enumerate(feature_cols):
        rate = float(per_col_zero[i])
        per_column[col] = round(rate, 6)
        family = _column_family(col)
        per_family_rates.setdefault(family, []).append(rate)
        per_family_cols[family] = per_family_cols.get(family, 0) + 1
        if rate >= 1.0 - float(epsilon):
            all_zero_cols.append(col)
            per_family_all_zero[family] = per_family_all_zero.get(family, 0) + 1

    per_family: dict[str, dict[str, float]] = {}
    for family, rates in per_family_rates.items():
        per_family[family] = {
            "cols": per_family_cols.get(family, 0),
            "all_zero_cols": per_family_all_zero.get(family, 0),
            "mean_zero_rate": round(sum(rates) / len(rates), 6) if rates else 0.0,
        }

    return {
        "rows": int(n_rows),
        "cols": int(n_cols),
        "epsilon": float(epsilon),
        "per_column": per_column,
        "per_family": per_family,
        "all_zero_cols": sorted(all_zero_cols),
    }


# ── Fail-fast guard ───────────────────────────────────────────────────────────


class CategoricalAllZeroError(RuntimeError):
    """Raised when a v15_of categorical family is entirely zero across the
    training matrix. The trainer treats this as fail-fast — silent zero
    columns would yield a model that can never use the category."""


_FAIL_FAST_FAMILIES: tuple[str, ...] = ("bucket", "hour", "dow", "session", "dir")


def assert_categorical_families_alive(
    report: dict[str, Any],
    *,
    schema_ver: str,
    enabled_for_schemas: tuple[str, ...] = ("v15_of",),
    families: Sequence[str] = _FAIL_FAST_FAMILIES,
) -> None:
    """Raise ``CategoricalAllZeroError`` when every column in a required
    family is all-zero for the requested schema_ver.

    Disabled (no-op) when ``schema_ver`` not in ``enabled_for_schemas``.
    """
    if schema_ver not in enabled_for_schemas:
        return
    per_family = report.get("per_family") or {}
    dead: list[str] = []
    for family in families:
        info = per_family.get(family)
        if not info:
            continue  # registry didn't include this family — fine
        if int(info.get("cols", 0)) == int(info.get("all_zero_cols", 0)):
            dead.append(family)
    if dead:
        raise CategoricalAllZeroError(
            f"schema={schema_ver}: categorical families fully zero in training "
            f"matrix: {sorted(dead)}. build_feature_row regressed — model would "
            "lose access to these features entirely."
        )


# ── Side-effects: Prometheus + JSON ──────────────────────────────────────────


def write_report_json(report: dict[str, Any], path: str) -> None:
    """Persist the report deterministically. Idempotent — caller controls path."""
    if not path:
        return
    dirpath = os.path.dirname(path)
    if dirpath:
        os.makedirs(dirpath, exist_ok=True)
    payload = {
        "tool": "train_feature_zero_rate_v1",
        **report,
    }
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2, sort_keys=True)


_PROM_GAUGE_CACHE: dict[str, Any] = {}


def emit_prometheus(report: dict[str, Any], *, schema_ver: str) -> bool:
    """Best-effort Prometheus emission.

    Emits two gauges, keyed by family (not per-column to keep cardinality
    bounded):
      * ``ml_train_feature_zero_rate{schema,family}``
      * ``ml_train_feature_all_zero_cols{schema,family}``

    Returns True when prometheus_client was available and metrics were
    set, False otherwise. Designed so trainers that don't run inside a
    Prometheus-scraped process (one-shot scripts) silently skip emission.
    """
    try:
        from prometheus_client import Gauge  # type: ignore
    except Exception:
        return False

    gauge_rate = _PROM_GAUGE_CACHE.get("zero_rate")
    if gauge_rate is None:
        try:
            gauge_rate = Gauge(
                "ml_train_feature_zero_rate",
                "Mean zero-rate per feature family observed during training",
                ["schema", "family"],
            )
        except ValueError:
            # already registered in another module — reuse via collector lookup
            from prometheus_client import REGISTRY  # type: ignore
            gauge_rate = next(
                c for c in REGISTRY.collect()  # type: ignore[attr-defined]
                if getattr(c, "name", "") == "ml_train_feature_zero_rate"
            )
        _PROM_GAUGE_CACHE["zero_rate"] = gauge_rate

    gauge_dead = _PROM_GAUGE_CACHE.get("all_zero_cols")
    if gauge_dead is None:
        try:
            gauge_dead = Gauge(
                "ml_train_feature_all_zero_cols",
                "Count of all-zero feature columns per family in training matrix",
                ["schema", "family"],
            )
        except ValueError:
            from prometheus_client import REGISTRY  # type: ignore
            gauge_dead = next(
                c for c in REGISTRY.collect()  # type: ignore[attr-defined]
                if getattr(c, "name", "") == "ml_train_feature_all_zero_cols"
            )
        _PROM_GAUGE_CACHE["all_zero_cols"] = gauge_dead

    per_family = report.get("per_family") or {}
    for family, info in per_family.items():
        try:
            gauge_rate.labels(schema=schema_ver, family=family).set(
                float(info.get("mean_zero_rate", 0.0))
            )
            gauge_dead.labels(schema=schema_ver, family=family).set(
                float(info.get("all_zero_cols", 0))
            )
        except Exception as e:
            logger.warning("prometheus emit failed for family=%s: %s", family, e)
    return True
