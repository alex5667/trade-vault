# -*- coding: utf-8 -*-
"""Unit tests for derived flow features (A4).

We test the dependency-free helper:
  core.flow_derived_features_v1.compute_liquidity_pressure_and_info_flow
"""

from __future__ import annotations

from core.flow_derived_features_v1 import compute_liquidity_pressure_and_info_flow


def test_info_flow_is_in_0_1_and_pressure_non_negative():
    lp, info = compute_liquidity_pressure_and_info_flow(
        taker_buy_rate_ema=2.0
        taker_sell_rate_ema=1.0
        depth_total_10=100.0
    )
    assert lp >= 0.0
    assert 0.0 <= info <= 1.0
    # 3 / 100 = 0.03
    assert abs(lp - 0.03) < 1e-12
    # |2-1| / 3 = 0.333...
    assert abs(info - (1.0 / 3.0)) < 1e-12


def test_zero_rates_produce_zero_features():
    lp, info = compute_liquidity_pressure_and_info_flow(
        taker_buy_rate_ema=0.0
        taker_sell_rate_ema=0.0
        depth_total_10=50.0
    )
    assert lp == 0.0
    assert info == 0.0


def test_missing_depth_fail_open_to_zero_pressure():
    lp, info = compute_liquidity_pressure_and_info_flow(
        taker_buy_rate_ema=10.0
        taker_sell_rate_ema=5.0
        depth_total_10=0.0
    )
    assert lp == 0.0
    assert 0.0 <= info <= 1.0


def test_negative_rates_are_clamped():
    lp, info = compute_liquidity_pressure_and_info_flow(
        taker_buy_rate_ema=-10.0
        taker_sell_rate_ema=5.0
        depth_total_10=100.0
    )
    # buy clamped to 0 => sum=5 => lp=0.05
    assert abs(lp - 0.05) < 1e-12
    # info = |0-5|/5 = 1
    assert info == 1.0
