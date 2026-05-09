from __future__ import annotations

from core.crypto_orderflow_detectors import OBIDetector


def test_obi_stable_and_features():
    det = OBIDetector(depth=3, threshold=0.2, hold_secs=1.0, z_alpha=0.5)
    # book timestamps in ms
    book0 = {"ts_ms": 1000, "bids": [[100,5],[99,6],[98,7]], "asks": [[101,1],[102,1],[103,1]]}
    book1 = {"ts_ms": 2000, "bids": [[100,5],[99,6],[98,7]], "asks": [[101,1],[102,1],[103,1]]}

    # First push: initializes state, checks threshold (OBI ~ (18-3)/21 ~ 0.7 > 0.2)
    ev0 = det.push(book0)
    assert ev0 is None  # not yet stable (just started)

    # Second push: 1 sec later, same state -> stable
    ev1 = det.push(book1)
    assert ev1 is not None, "Should be stable after 1.0s"
    assert "stable_secs" in ev1 and ev1["stable_secs"] >= 1.0

    # Check new metrics
    assert "stacking" in ev1
    # Bids: 5->6->7 (all increase) -> score 1.0
    # Asks: 1->1->1 (all equal/increase) -> score 1.0
    # Diff = 0.0? Wait:
    # _stacking_score: sizes[i+1] >= sizes[i].
    # Bids: 5,6,7. 6>=5(ok), 7>=6(ok). 2/2 = 1.0.
    # Asks: 1,1,1. 1>=1(ok), 1>=1(ok). 2/2 = 1.0.
    # stacking = 1.0 - 1.0 = 0.0
    assert ev1["stacking"] == 0.0

    assert "concentration" in ev1
    # Bids: top=5, total=18 -> 5/18 ~ 0.27
    # Asks: top=1, total=3 -> 1/3 ~ 0.33
    # conc = 0.27 - 0.33 = -0.05
    assert abs(ev1["concentration"] - (5/18 - 1/3)) < 1e-6

    assert "obi_z" in ev1
    # Z-score updates.
    # 1st push: mu updates, var updates.
    # 2nd push: mu updates, var updates, then z calculated.
    # It shouldn't crash.
    assert isinstance(ev1["obi_z"], float)
