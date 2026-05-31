"""Smoke tests for the periodic preflight service wrapper.

Validate orchestration only — the per-reader logic is covered by
tests/test_preflight_reader_flip.py.
"""
from __future__ import annotations

from unittest.mock import MagicMock, patch

from tools.preflight_reader_flip import Check, Report


def _make_report(reader: str, passed: bool, fail_check: str | None = None) -> Report:
    checks = [Check(name="key_exists", passed=True, detail="ok")]
    if fail_check:
        checks.append(Check(name=fail_check, passed=False, detail="bad"))
    return Report(reader=reader, passed=passed, checks=checks)


def _drive_one_cycle(monkeypatch, *, adaptive_passes: bool, ensemble_passes: bool):
    """Patch metrics + sleep, run one cycle, return collected gauge/counter values."""
    monkeypatch.setenv("PREFLIGHT_INTERVAL_SEC", "1")
    monkeypatch.setenv("PREFLIGHT_PORT", "0")
    monkeypatch.setenv("PREFLIGHT_READERS", "adaptive_ttl,ensemble")

    gauges: dict = {}
    counters: dict = {}

    def fake_gauge(name, _desc, labelnames=()):
        class G:
            def labels(self, **kw):
                gauges[(name, tuple(sorted(kw.items())))] = self
                return self
            def set(self, v):
                gauges[("set", name, getattr(self, "_lbl", ()))] = v
        g = G()
        return g

    def fake_counter(name, _desc, labelnames=()):
        class C:
            def labels(self, **kw):
                self._lbl = tuple(sorted(kw.items()))
                return self
            def inc(self, n=1.0):
                key = ("inc", name, self._lbl)
                counters[key] = counters.get(key, 0.0) + n
        return C()

    def fake_start_http_server(_port):
        return None

    # Stop after one cycle by raising on second sleep call
    calls = {"sleeps": 0}

    def fake_sleep(_n):
        calls["sleeps"] += 1
        if calls["sleeps"] >= 2:
            raise KeyboardInterrupt()

    rep_adaptive = _make_report("adaptive_ttl", adaptive_passes, None if adaptive_passes else "freshness")
    rep_ensemble = _make_report("ensemble", ensemble_passes, None if ensemble_passes else "weights_sanity")

    with patch("prometheus_client.start_http_server", fake_start_http_server), \
         patch("prometheus_client.Gauge", fake_gauge), \
         patch("prometheus_client.Counter", fake_counter), \
         patch("tools.preflight_reader_flip.check_adaptive_ttl", return_value=rep_adaptive), \
         patch("tools.preflight_reader_flip.check_ensemble_weights", return_value=rep_ensemble), \
         patch("time.sleep", side_effect=fake_sleep):
        from services.preflight_reader_flip_service import main
        try:
            main()
        except KeyboardInterrupt:
            pass

    return counters


def test_cycle_records_pass_when_both_readers_pass(monkeypatch):
    counters = _drive_one_cycle(monkeypatch, adaptive_passes=True, ensemble_passes=True)
    keys = [k for k in counters if k[0] == "inc" and k[1] == "preflight_reader_check_total"]
    # Each reader logged once with status="pass"
    pass_keys = [k for k in keys if ("status", "pass") in k[2]]
    assert len(pass_keys) == 2


def test_cycle_records_fail_with_specific_check(monkeypatch):
    counters = _drive_one_cycle(monkeypatch, adaptive_passes=False, ensemble_passes=True)
    # Failure-by-check counter must show freshness specifically for adaptive_ttl
    fail_keys = [
        k for k in counters
        if k[0] == "inc" and k[1] == "preflight_reader_check_failure_total"
    ]
    assert any(
        ("reader", "adaptive_ttl") in k[2] and ("check", "freshness") in k[2]
        for k in fail_keys
    )


def test_cycle_independent_readers(monkeypatch):
    # adaptive fails, ensemble passes — must record both independently
    counters = _drive_one_cycle(monkeypatch, adaptive_passes=False, ensemble_passes=True)
    total_keys = [k for k in counters if k[1] == "preflight_reader_check_total"]
    adaptive_fail = [k for k in total_keys
                     if ("reader", "adaptive_ttl") in k[2] and ("status", "fail") in k[2]]
    ensemble_pass = [k for k in total_keys
                     if ("reader", "ensemble") in k[2] and ("status", "pass") in k[2]]
    assert adaptive_fail and ensemble_pass
