import os
import types
import pytest

from handlers.crypto_orderflow_handler import CryptoOrderFlowHandler
from handlers.crypto_orderflow.config.runtime_config import _RuntimeCfg

class Dummy:
    pass

def test_build_manual_audit_does_not_call_getenv(monkeypatch):
    # Create handler without running its real __init__ (avoid needing full deps).
    h = CryptoOrderFlowHandler.__new__(CryptoOrderFlowHandler)
    # Manually inject cached cfg.
    h._cfg = _RuntimeCfg(
        qf_pack_u16=True,
        strict_reason_codes=False,
        audit_compact=True,
        candidate_log_every_ms=5000,
        signal_log_every_ms=0,
        pack_soft_u16=True,
    )

    # Patch getenv to explode if called.
    monkeypatch.setattr(os, "getenv", lambda *a, **k: (_ for _ in ()).throw(RuntimeError("getenv called")))

    ctx = Dummy(); ctx.symbol="BTCUSDT"; ctx.ts=123; ctx.price=100.0
    cand = Dummy(); cand.kind="breakout"; cand.side=1; cand.raw_score=1.0
    parts = {"x": 1}

    out = h._build_manual_audit(ctx, cand, parts=parts)
    assert out["kind"] == "breakout"
    assert "parts" not in out  # compact mode
