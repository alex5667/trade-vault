#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
test_oof_split_no_leakage.py

Тесты для проверки, что OOF splitter не допускает leakage:
  - train_ts не пересекается с [val_start-purge, val_end+embargo]
  - каждый fold имеет корректные границы
"""

from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pytest

# Add tools to path
tools_path = Path(__file__).parent.parent / "tools"
if str(tools_path) not in sys.path:
    sys.path.insert(0, str(tools_path))

from train_edge_stack_v1_oof import PurgedEmbargoTimeSeriesSplit


def test_purged_embargo_no_overlap():
    """Проверка, что train и val не пересекаются с учётом purge и embargo."""
    # Создаём синтетические timestamps (1 час данных, каждые 10 секунд)
    n = 360  # 360 точек = 1 час
    ts_ms = np.arange(0, n * 10_000, 10_000, dtype=np.int64)  # каждые 10 секунд

    splitter = PurgedEmbargoTimeSeriesSplit(
        n_splits=5,
        purge_ms=5 * 60_000,  # 5 минут
        embargo_ms=5 * 60_000,  # 5 минут
    )

    for fold_idx, (tr_idx, va_idx) in enumerate(splitter.split(ts_ms)):
        if len(tr_idx) == 0 or len(va_idx) == 0:
            continue  # пропускаем пустые folds

        # Границы validation fold
        val_start = ts_ms[va_idx].min()
        val_end = ts_ms[va_idx].max()

        # Границы train fold
        train_ts = ts_ms[tr_idx]

        # Проверка: train не должен содержать точки в [val_start - purge, val_end + embargo]
        forbidden_start = val_start - splitter.purge_ms
        forbidden_end = val_end + splitter.embargo_ms

        # Все train точки должны быть либо до forbidden_start, либо после forbidden_end
        train_before = train_ts[train_ts < forbidden_start]
        train_after = train_ts[train_ts > forbidden_end]
        train_forbidden = train_ts[(train_ts >= forbidden_start) & (train_ts <= forbidden_end)]

        assert len(train_forbidden) == 0, (
            f"Fold {fold_idx}: train содержит {len(train_forbidden)} точек в forbidden zone "
            f"[{forbidden_start}, {forbidden_end}]. "
            f"val=[{val_start}, {val_end}], purge={splitter.purge_ms}, embargo={splitter.embargo_ms}"
        )


def test_purged_embargo_time_ordering():
    """Проверка, что folds идут в хронологическом порядке."""
    n = 1000
    ts_ms = np.arange(0, n * 1000, 1000, dtype=np.int64)  # каждую секунду

    splitter = PurgedEmbargoTimeSeriesSplit(n_splits=5, purge_ms=10_000, embargo_ms=10_000)

    prev_val_end = -1
    for fold_idx, (tr_idx, va_idx) in enumerate(splitter.split(ts_ms)):
        if len(va_idx) == 0:
            continue

        val_start = ts_ms[va_idx].min()
        val_end = ts_ms[va_idx].max()

        # Каждый следующий fold должен начинаться после предыдущего
        assert val_start > prev_val_end, (
            f"Fold {fold_idx}: val_start={val_start} <= prev_val_end={prev_val_end} "
            "(folds должны идти в хронологическом порядке)"
        )

        prev_val_end = val_end


def test_purged_embargo_small_dataset():
    """Проверка на маленьком датасете (fallback на single split)."""
    n = 10  # очень маленький датасет
    ts_ms = np.arange(0, n * 1000, 1000, dtype=np.int64)

    splitter = PurgedEmbargoTimeSeriesSplit(n_splits=5, purge_ms=1000, embargo_ms=1000)

    splits = list(splitter.split(ts_ms))
    assert len(splits) > 0, "Должен быть хотя бы один split"

    # Проверяем, что все точки покрыты
    all_train = set()
    all_val = set()
    for tr_idx, va_idx in splits:
        all_train.update(tr_idx)
        all_val.update(va_idx)

    # Train и val не должны пересекаться
    assert len(all_train & all_val) == 0, "Train и val не должны пересекаться"


def test_purged_embargo_all_points_covered():
    """Проверка, что все точки покрыты (train или val)."""
    n = 500
    ts_ms = np.arange(0, n * 1000, 1000, dtype=np.int64)

    splitter = PurgedEmbargoTimeSeriesSplit(n_splits=5, purge_ms=5000, embargo_ms=5000)

    all_indices = set(range(n))
    covered = set()

    for tr_idx, va_idx in splitter.split(ts_ms):
        covered.update(tr_idx)
        covered.update(va_idx)

    # Не все точки могут быть покрыты из-за purge/embargo, но большинство должны быть
    coverage = len(covered) / len(all_indices)
    assert coverage > 0.5, f"Покрытие слишком низкое: {coverage:.2%} (ожидается >50%)"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

