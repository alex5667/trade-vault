"""Tests for tools.preflight_reader_flip."""
from __future__ import annotations

import json
import time
from unittest.mock import MagicMock, patch

import pytest

from tools.preflight_reader_flip import check_adaptive_ttl, check_ensemble_weights


_NOW_MS = int(time.time() * 1000)


def _fresh_payload(n_recs=2, degen=False):
    rec = lambda d: dict(
        symbol="BTCUSDT", regime="momentum", direction=1,
        n=60, win_rate=0.55,
        tp_r=0 if d else 1.2,
        sl_r=0.8,
    )
    return json.dumps(
        dict(
            v=1,
            generated_at_ms=_NOW_MS - 60_000,  # 1 min old
            n=n_recs,
            recs=[rec(degen and i == 0) for i in range(n_recs)],
        )
    )


# ─── adaptive_ttl ────────────────────────────────────────────────────────────


@patch("tools.preflight_reader_flip._fetch_metrics", return_value={})
@patch("tools.preflight_reader_flip._redis")
def test_adaptive_ttl_pass_when_fresh_and_valid(rc_factory, _mx):
    rc = MagicMock()
    rc.exists.return_value = 1
    rc.get.return_value = _fresh_payload()
    rc_factory.return_value = rc
    r = check_adaptive_ttl()
    assert r.passed is True
    assert all(c.passed for c in r.checks)


@patch("tools.preflight_reader_flip._fetch_metrics", return_value={})
@patch("tools.preflight_reader_flip._redis")
def test_adaptive_ttl_fail_when_key_missing(rc_factory, _mx):
    rc = MagicMock()
    rc.exists.return_value = 0
    rc_factory.return_value = rc
    r = check_adaptive_ttl()
    assert r.passed is False
    assert r.checks[0].name == "key_exists"
    assert r.checks[0].passed is False


@patch("tools.preflight_reader_flip._fetch_metrics", return_value={})
@patch("tools.preflight_reader_flip._redis")
def test_adaptive_ttl_fail_when_stale(rc_factory, _mx, monkeypatch):
    monkeypatch.setenv("PREFLIGHT_MAX_AGE_MIN", "5")
    payload = json.dumps(
        dict(v=1, generated_at_ms=_NOW_MS - 600_000, n=1, recs=[
            dict(symbol="BTC", regime="r", direction=1, n=60, tp_r=1, sl_r=1)
        ])
    )
    rc = MagicMock()
    rc.exists.return_value = 1
    rc.get.return_value = payload
    rc_factory.return_value = rc
    r = check_adaptive_ttl()
    assert r.passed is False
    fr = next(c for c in r.checks if c.name == "freshness")
    assert fr.passed is False


@patch("tools.preflight_reader_flip._fetch_metrics", return_value={})
@patch("tools.preflight_reader_flip._redis")
def test_adaptive_ttl_fail_on_degenerate_barriers(rc_factory, _mx):
    rc = MagicMock()
    rc.exists.return_value = 1
    rc.get.return_value = _fresh_payload(n_recs=2, degen=True)
    rc_factory.return_value = rc
    r = check_adaptive_ttl()
    assert r.passed is False
    s = next(c for c in r.checks if c.name == "recs_sanity")
    assert s.passed is False


@patch("tools.preflight_reader_flip._fetch_metrics", return_value={})
@patch("tools.preflight_reader_flip._redis")
def test_adaptive_ttl_fail_on_bad_json(rc_factory, _mx):
    rc = MagicMock()
    rc.exists.return_value = 1
    rc.get.return_value = "garbage"
    rc_factory.return_value = rc
    r = check_adaptive_ttl()
    assert r.passed is False


# ─── ensemble ────────────────────────────────────────────────────────────────


@patch("tools.preflight_reader_flip._fetch_metrics", return_value={})
@patch("tools.preflight_reader_flip._redis")
def test_ensemble_pass(rc_factory, _mx):
    rc = MagicMock()
    rc.scan_iter.return_value = iter(["ensemble:weights:BTC"])
    rc.hgetall.return_value = {"of": "0.6", "ml": "0.4"}
    rc.ttl.return_value = 600_000
    rc_factory.return_value = rc
    r = check_ensemble_weights()
    assert r.passed is True


@patch("tools.preflight_reader_flip._fetch_metrics", return_value={})
@patch("tools.preflight_reader_flip._redis")
def test_ensemble_fail_when_no_symbols(rc_factory, _mx):
    rc = MagicMock()
    rc.scan_iter.return_value = iter([])
    rc_factory.return_value = rc
    r = check_ensemble_weights()
    assert r.passed is False


@patch("tools.preflight_reader_flip._fetch_metrics", return_value={})
@patch("tools.preflight_reader_flip._redis")
def test_ensemble_fail_when_weights_dont_sum_to_1(rc_factory, _mx):
    rc = MagicMock()
    rc.scan_iter.return_value = iter(["ensemble:weights:BTC"])
    rc.hgetall.return_value = {"of": "0.3", "ml": "0.3"}  # sum=0.6
    rc.ttl.return_value = 600_000
    rc_factory.return_value = rc
    r = check_ensemble_weights()
    assert r.passed is False
    s = next(c for c in r.checks if c.name == "weights_sanity")
    assert s.passed is False


@patch("tools.preflight_reader_flip._fetch_metrics", return_value={})
@patch("tools.preflight_reader_flip._redis")
def test_ensemble_fail_when_too_few_sources(rc_factory, _mx, monkeypatch):
    monkeypatch.setenv("PREFLIGHT_MIN_SOURCES", "2")
    rc = MagicMock()
    rc.scan_iter.return_value = iter(["ensemble:weights:BTC"])
    rc.hgetall.return_value = {"of": "1.0"}  # 1 source, sum=1
    rc.ttl.return_value = 600_000
    rc_factory.return_value = rc
    r = check_ensemble_weights()
    assert r.passed is False


@patch("tools.preflight_reader_flip._fetch_metrics", return_value={})
@patch("tools.preflight_reader_flip._redis")
def test_ensemble_fail_when_no_ttl(rc_factory, _mx):
    rc = MagicMock()
    rc.scan_iter.return_value = iter(["ensemble:weights:BTC"])
    rc.hgetall.return_value = {"of": "0.6", "ml": "0.4"}
    rc.ttl.return_value = -1  # no TTL
    rc_factory.return_value = rc
    r = check_ensemble_weights()
    assert r.passed is False


@patch("tools.preflight_reader_flip._fetch_metrics")
@patch("tools.preflight_reader_flip._redis")
def test_adaptive_ttl_publisher_metrics_unhealthy(rc_factory, mx):
    # Reachable but no published cycles
    mx.return_value = {
        'adaptive_ttl_cycle_total{status="error"}': 5.0,
        'adaptive_ttl_recs_total': 0.0,
    }
    rc = MagicMock()
    rc.exists.return_value = 1
    rc.get.return_value = _fresh_payload()
    rc_factory.return_value = rc
    r = check_adaptive_ttl()
    ph = next(c for c in r.checks if c.name == "publisher_health")
    assert ph.passed is False
