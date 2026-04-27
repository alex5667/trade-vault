from __future__ import annotations

from datetime import datetime, timezone
import os
import sys

# Ensure both trees are importable:
# - services.* (mirror)
# - core.* (from tick_flow_full which exposes top-level 'core' package)
# [AUTOGRAVITY CLEANUP] sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../..'))
# [AUTOGRAVITY CLEANUP] sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../../tick_flow_full'))

from core.calendar_flags import calendar_flags_utc, is_end_of_month_utc, is_end_of_quarter_utc


def _ts_ms(y: int, m: int, d: int, hh: int = 0, mm: int = 0, ss: int = 0) -> int:
    return int(datetime(y, m, d, hh, mm, ss, tzinfo=timezone.utc).timestamp() * 1000)


def test_calendar_flags_eom_eoq_quick() -> None:
    # 2026-12-31 is both EOM and EOQ.
    ts = _ts_ms(2026, 12, 31, 23, 59, 59)
    f = calendar_flags_utc(ts)
    assert f["cal_eom_utc"] == 1
    assert f["cal_eoq_utc"] == 1
    assert is_end_of_month_utc(ts) == 1
    assert is_end_of_quarter_utc(ts) == 1
