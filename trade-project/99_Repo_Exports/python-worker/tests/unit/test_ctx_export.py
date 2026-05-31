from __future__ import annotations

from dataclasses import dataclass

from replay.ctx_export import export_ctx


@dataclass
class DummyCtx:
    ts: int = 1700000000000
    symbol: str = "BTCUSDT"
    price: float = 43000.0
    z_delta: float = 3.5
    obi_avg: float = float("nan")
    spread_bps: float = float("inf")
    data_quality_flags: list[str] = None  # type: ignore
    huge_obj: object = object()


def test_export_ctx_compact_sanitizes_nan_inf_and_skips_heavy(monkeypatch) -> None:
    monkeypatch.setenv("REPLAY_RECORD_CTX_MODE", "compact")
    ctx = DummyCtx()
    ctx.data_quality_flags = ["l3_missing", "htf_missing"]

    d = export_ctx(ctx)
    assert d["ts"] == 1700000000000
    assert d["symbol"] == "BTCUSDT"
    assert d["price"] == 43000.0
    assert d["z_delta"] == 3.5
    assert d["obi_avg"] is None
    assert d["spread_bps"] is None
    assert "huge_obj" not in d
    assert d["data_quality_flags"] == ["l3_missing", "htf_missing"]


def test_export_ctx_full_exports_more(monkeypatch) -> None:
    monkeypatch.setenv("REPLAY_RECORD_CTX_MODE", "full")
    ctx = DummyCtx()
    ctx.data_quality_flags = ["x"]
    d = export_ctx(ctx)
    # full exports public primitives
    assert d["symbol"] == "BTCUSDT"
    # still sanitizes
    assert d["obi_avg"] is None
