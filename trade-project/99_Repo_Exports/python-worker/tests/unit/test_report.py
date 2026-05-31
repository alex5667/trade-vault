from __future__ import annotations

from replay.report import build_report


def test_build_report_counts_and_percentiles() -> None:
    signals = [
        {"kind": "breakout", "final_score": 1.0},
        {"kind": "breakout", "final_score": 2.0},
        {"kind": "breakout", "final_score": 3.0},
        {"kind": "absorption", "final_score": -1.0},
    ]
    rep = build_report(signals)
    assert rep.counts_by_kind["breakout"] == 3
    assert rep.counts_by_kind["absorption"] == 1
    assert rep.score_p50_by_kind["breakout"] in (2.0,)
    assert rep.score_p95_by_kind["breakout"] in (3.0,)
