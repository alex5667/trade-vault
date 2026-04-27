# -*- coding: utf-8 -*-
"""
Regression: Zombie Janitor Age Check Math (Before Canary 3.5)

Tests that the _get_position_age_sec safely parses different time representations
(ms vs sec, missing fields, malformed floats) without throwing exceptions.
"""
from __future__ import annotations
from utils.time_utils import get_ny_time_millis

import pytest
import time
from unittest.mock import MagicMock

from services.zombie_position_janitor import _get_position_age_sec

class MockRedisForAge:
    def __init__(self, mapping: dict):
        self._mapping = mapping
        
    def hmget(self, key, *fields):
        # returns list of values matching fields
        return [self._mapping.get(f) for f in fields]


def test_get_position_age_sec_milliseconds() -> None:
    now_ms = get_ny_time_millis()
    # 5 minutes ago
    ts_ms = now_ms - (5 * 60 * 1000)
    
    r = MockRedisForAge({"entry_ts_ms": str(ts_ms)})
    age_sec = _get_position_age_sec(r, "pos_1")
    
    # approx 300 seconds
    assert age_sec is not None
    assert 299 <= age_sec <= 301
    
def test_get_position_age_sec_seconds() -> None:
    now_sec = time.time()
    # 10 minutes ago
    ts_sec = now_sec - (10 * 60)
    
    r = MockRedisForAge({"created_at_ms": str(ts_sec)}) # despite the 'ms' name, let's say it holds secs
    age = _get_position_age_sec(r, "pos_2")
    
    assert age is not None
    assert 599 <= age <= 601
    
def test_get_position_age_missing_all() -> None:
    r = MockRedisForAge({})
    age = _get_position_age_sec(r, "pos_3")
    assert age is None
    
def test_get_position_age_malformed() -> None:
    r = MockRedisForAge({"entry_ts_ms": "invalid_float", "open_ts_ms": None})
    age = _get_position_age_sec(r, "pos_4")
    assert age is None

def test_get_position_age_fallback_order() -> None:
    # Multiple fields are present, it should pick the first valid one in priority
    now_ms = get_ny_time_millis()
    
    r = MockRedisForAge({
        "entry_ts_ms": "invalid",
        "open_ts_ms": str(now_ms - (5 * 60 * 1000)), # 5 mins
        "ts_ms": str(now_ms - (10 * 60 * 1000))       # 10 mins 
    })
    
    age = _get_position_age_sec(r, "pos_5")
    assert age is not None
    assert 299 <= age <= 301 # It should pick open_ts_ms over ts_ms
