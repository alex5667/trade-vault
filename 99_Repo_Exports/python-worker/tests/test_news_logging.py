# python-worker/tests/test_news_logging.py
from __future__ import annotations

import json
import logging

from common.stable_hash import sample_pct, stable_hash64
from contexts import NewsFeatures, OrderflowSignalContext
from news_pipeline.news_logging import NewsFullDebugFetcher, add_news_minilog, normalize_analysis_key


class FakeRedis:
    def __init__(self, mapping):
        self.mapping = dict(mapping)

    def get(self, key):
        return self.mapping.get(key)


def test_normalize_analysis_key():
    assert normalize_analysis_key("news:analysis:abc") == "news:analysis:abc"
    assert normalize_analysis_key("abc") == "news:analysis:abc"
    assert normalize_analysis_key("") == ""


def test_add_news_minilog_only_compact_fields():
    ctx = OrderflowSignalContext(symbol="BTCUSDT", ts=123)
    ctx.news = NewsFeatures(
        ref="abc",
        news_risk=0.9,
        surprise_score=0.7,
        news_grade_id=3,
        event_tminus_sec=120,
        event_grade_id=2,
        tags_mask=5,
        primary_tag_id=2,
        confidence=0.8,
        horizon_sec=3600,
        asof_ts_ms=111,
    )

    ev = {"kind": "signal", "symbol": "BTCUSDT", "ts_ms": 123}
    add_news_minilog(ev, ctx)

    # Must include the requested fields
    assert ev["news_risk"] == 0.9
    assert ev["news_tminus_sec"] == 120
    assert ev["news_grade_id"] == 3
    assert ev["news_tags_mask"] == 5

    # Must NOT include full ctx or nested objects
    assert "ctx" not in ev
    assert "news" not in ev  # we flatten

    # Still compact extras:
    assert ev["news_ref"] == "abc"


def test_stable_hash_is_deterministic():
    h1 = stable_hash64("abc", 123)
    h2 = stable_hash64("abc", 123)
    assert h1 == h2
    assert isinstance(h1, int)


def test_sample_pct_bounds():
    assert sample_pct("x", pct=0) is False
    assert sample_pct("x", pct=100) is True


def test_full_debug_fetcher_process_one_logs(caplog):
    # Force-enable by setting attributes directly.
    fr = FakeRedis({"news:analysis:abc": '{"uid":"abc","x":1}'})
    f = NewsFullDebugFetcher(redis=fr)
    f.enabled = True
    f.sample_pct = 100  # always
    f.max_bytes = 10_000

    caplog.set_level(logging.INFO, logger="news_full_debug")

    # run internal without thread
    f._process_one(ref="abc", symbol="BTCUSDT", ts_ms=123)

    assert any("news_full" in rec.message for rec in caplog.records)
    msg = caplog.records[-1].message
    obj = json.loads(msg)
    assert obj["ok"] is True
    assert obj["key"] == "news:analysis:abc"
    assert obj["symbol"] == "BTCUSDT"
