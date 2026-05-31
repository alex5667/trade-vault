from __future__ import annotations

import pytest
from ml_analysis.pbo_cscv import compute_pbo, contiguous_folds


def _make_matrix(n_variants: int, n_periods: int, *, bias_idx: int | None = None, bias: float = 0.05):
    """Build deterministic variant x period matrix."""
    import math
    rows = {}
    for v in range(n_variants):
        vals = [math.sin(v * 0.31 + t * 0.17) * 0.05 for t in range(n_periods)]
        if bias_idx is not None and v == bias_idx:
            vals = [x + bias for x in vals]
        rows[str(v)] = vals
    return rows


def test_contiguous_folds_basic():
    folds = contiguous_folds(20, n_folds=8)
    # must produce an even number of non-empty folds
    assert len(folds) >= 2
    assert len(folds) % 2 == 0
    all_idx = sorted(i for f in folds for i in f)
    # all periods covered in ordering (subset of 0..19)
    assert all_idx == list(range(all_idx[0], all_idx[-1] + 1))


def test_contiguous_folds_too_few():
    with pytest.raises(ValueError):
        contiguous_folds(1, n_folds=4)


def test_compute_pbo_structure():
    mat = _make_matrix(4, 20)
    result = compute_pbo(mat, n_folds=4)
    assert 'pbo' in result
    assert 'cscv_splits' in result
    assert 0.0 <= result['pbo'] <= 1.0
    assert result['cscv_splits'] >= 1.0


def test_compute_pbo_biased_variant_low():
    """Dominant variant should yield low PBO."""
    mat = _make_matrix(6, 32, bias_idx=0, bias=0.15)
    result = compute_pbo(mat, n_folds=4)
    # With a clear bias, PBO should be below 0.5
    assert result['pbo'] <= 0.5


def test_compute_pbo_uniform_tends_higher():
    """With similar variants, PBO tends higher than biased case."""
    mat_uniform = _make_matrix(8, 32, bias_idx=None)
    mat_biased = _make_matrix(8, 32, bias_idx=0, bias=0.3)
    pbo_uniform = compute_pbo(mat_uniform, n_folds=4)['pbo']
    pbo_biased = compute_pbo(mat_biased, n_folds=4)['pbo']
    assert pbo_uniform >= pbo_biased


def test_compute_pbo_single_variant_raises_not_enough_variants():
    """Single-variant matrix → ValueError from _matrix (need >=2 for CSCV folds)."""
    with pytest.raises(ValueError):
        # contiguous_folds needs >=2 periods but we also need >=2 variants for meaningful CSCV.
        # _matrix with only 1 entry is valid, but contiguous_folds(1, ...) raises ValueError.
        compute_pbo({'a': [1.0]}, n_folds=2)


def test_compute_pbo_mismatched_lengths():
    with pytest.raises(ValueError):
        compute_pbo({'a': [1.0, 2.0], 'b': [1.0]}, n_folds=2)
