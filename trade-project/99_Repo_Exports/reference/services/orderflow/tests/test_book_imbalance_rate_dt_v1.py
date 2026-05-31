# -*- coding: utf-8 -*-
"""Unit tests for book_imbalance_rate_10 dt guards (A2).

We validate the dependency-free helper in `core.book_derivatives_v1`.
"""

from __future__ import annotations

import sys
from pathlib import Path

# Ensure we import SoT (tick_flow_full/...) as top-level packages: core.*, services.*
sys.path.insert(0, str(Path(__file__).resolve().parents[3]))  # .../tick_flow_full

from core.book_derivatives_v1 import compute_book_imbalance_rate_10


def test_first_observation_no_prev_is_safe():
    # First observation: prev state is None. Must return 0.0, bad_dt=0 (no error, just init).
    rate, bad = compute_book_imbalance_rate_10(prev_imb10=None, prev_ts_ms=None, cur_imb10=0.1, cur_ts_ms=1_000)
    assert rate == 0.0
    assert bad == 0


def test_dt_positive_computes_derivative():
    # Delta imb=0.20 over 1000ms (1s) => 0.20 1/s
    rate, bad = compute_book_imbalance_rate_10(prev_imb10=0.20, prev_ts_ms=1_000, cur_imb10=0.40, cur_ts_ms=2_000)
    assert abs(rate - 0.20) < 1e-12
    assert bad == 0


def test_dt_zero_is_bad_time_guarded():
    # Same timestamp = duplicate snapshot: must not compute rate, flag bad_dt=1.
    rate, bad = compute_book_imbalance_rate_10(prev_imb10=0.10, prev_ts_ms=1_000, cur_imb10=0.20, cur_ts_ms=1_000)
    assert rate == 0.0
    assert bad == 1


def test_dt_negative_is_bad_time_guarded():
    # cur_ts < prev_ts = out-of-order snapshot: must not compute rate, flag bad_dt=1.
    rate, bad = compute_book_imbalance_rate_10(prev_imb10=0.10, prev_ts_ms=1_000, cur_imb10=0.20, cur_ts_ms=900)
    assert rate == 0.0
    assert bad == 1


def test_clip_prevents_explosions_on_tiny_dt():
    # Delta imb=1.0 over 1ms => 1000 1/s, but default clip is 50.0.
    rate, bad = compute_book_imbalance_rate_10(prev_imb10=0.0, prev_ts_ms=1_000, cur_imb10=1.0, cur_ts_ms=1_001)
    assert rate == 50.0
    assert bad == 0


def test_clip_negative_side():
    # Delta imb=-1.0 over 1ms => -1000 1/s, clipped to -50.0.
    rate, bad = compute_book_imbalance_rate_10(prev_imb10=1.0, prev_ts_ms=1_000, cur_imb10=0.0, cur_ts_ms=1_001)
    assert rate == -50.0
    assert bad == 0


def test_custom_clip_respected():
    # Custom clip=10.0: delta 0.5 over 10ms => 50 1/s clipped to 10.0.
    rate, bad = compute_book_imbalance_rate_10(
        prev_imb10=0.0, prev_ts_ms=1_000, cur_imb10=0.5, cur_ts_ms=1_010, clip_abs_per_s=10.0
    )
    assert rate == 10.0
    assert bad == 0


def test_normal_rate_within_clip_unaffected():
    # Delta imb=0.03 over 500ms => 0.06 1/s, well within 50.0 clip.
    rate, bad = compute_book_imbalance_rate_10(prev_imb10=0.10, prev_ts_ms=1_000, cur_imb10=0.13, cur_ts_ms=1_500)
    assert abs(rate - 0.06) < 1e-12
    assert bad == 0


def test_only_prev_ts_none_is_safe():
    # prev_ts_ms=None with prev_imb10 provided still returns safe defaults.
    rate, bad = compute_book_imbalance_rate_10(prev_imb10=0.5, prev_ts_ms=None, cur_imb10=0.6, cur_ts_ms=2_000)
    assert rate == 0.0
    assert bad == 0


def test_only_prev_imb_none_is_safe():
    # prev_imb10=None with prev_ts_ms provided still returns safe defaults.
    rate, bad = compute_book_imbalance_rate_10(prev_imb10=None, prev_ts_ms=1_000, cur_imb10=0.6, cur_ts_ms=2_000)
    assert rate == 0.0
    assert bad == 0
