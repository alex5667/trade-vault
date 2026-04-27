from __future__ import annotations

import math

from dataclasses import dataclass

from handlers.confirmations.l2_common import sanitize_book
from handlers.crypto_orderflow.types.crypto_orderflow_handler_types import L2Level


def test_sanitize_book_filters_nan_inf_and_nonpositive():
    levels = [
        L2Level(price=float("nan"), size=1.0, notional=100.0),
        L2Level(price=float("inf"), size=1.0, notional=100.0),
        L2Level(price=-1.0, size=1.0, notional=100.0),
        L2Level(price=100.0, size=1.0, notional=float("nan")),
        L2Level(price=100.0, size=1.0, notional=float("inf")),
        L2Level(price=100.0, size=1.0, notional=-5.0),
        L2Level(price=100.0, size=1.0, notional=100.0),
    ]
    out = sanitize_book(levels, max_levels=50)
    assert len(out) == 1
    assert out[0].price == 100.0
    assert out[0].notional == 100.0
