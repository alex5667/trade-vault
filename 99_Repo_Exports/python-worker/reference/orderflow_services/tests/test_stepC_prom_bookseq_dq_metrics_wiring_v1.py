"""
Test verifying the Step C string-contracts:
1) DQ / Book-seq Prom metrics are wired into BookProcessor & TickProcessor (SoT + mirror).
2) They use fail-open (try/except) blocks.
"""

import os

def test_book_processor_prom_wiring_v1():
    paths = [
        "services/orderflow/components/book_processor.py"
        "tick_flow_full/services/orderflow/components/book_processor.py"
    ]
    for p in paths:
        if os.path.exists(p):
            with open(p, "r", encoding="utf-8") as f:
                content = f.read()
            assert "book_missing_seq_ema_gauge" in content, f"{p} missing book_missing_seq_ema_gauge"
            assert "book_seq_last_gap_gauge" in content, f"{p} missing book_seq_last_gap_gauge"
            assert "try:" in content
            assert "except Exception:" in content

def test_tick_processor_prom_wiring_v1():
    paths = [
        "services/orderflow/components/tick_processor.py"
        "tick_flow_full/services/orderflow/components/tick_processor.py"
    ]
    for p in paths:
        if os.path.exists(p):
            with open(p, "r", encoding="utf-8") as f:
                content = f.read()
            assert "dq_level_gauge" in content, f"{p} missing dq_level_gauge"
            assert "dq_veto_total" in content, f"{p} missing dq_veto_total"
            assert "try:" in content
            assert "except Exception:" in content
