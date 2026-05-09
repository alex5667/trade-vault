from types import SimpleNamespace

from signals.empirical_levels import EmpiricalLevels, EmpiricalLevelStats
from signals.level_enricher import attach_trade_levels_to_ctx


class _FakeProvider:
    def __init__(self, table):
        self.table = table

    def get(self, *, symbol: str, kind: str, regime: str):
        return self.table.get((symbol, kind, regime))

    def get_level_stats(self, *, symbol: str, kind: str, regime: str, samples: int = 0):
        return self.get(symbol=symbol, kind=kind, regime=regime)


def test_empirical_levels_override_applied(monkeypatch):
    monkeypatch.setenv("LEVELS_EMPIRICAL_ENABLED", "1")
    monkeypatch.setenv("LEVELS_EMPIRICAL_MIN_SAMPLES", "10")
    monkeypatch.setenv("LEVELS_EMPIRICAL_BLEND_ALPHA", "1.0")  # pure empirical
    monkeypatch.setenv("LEVELS_EMPIRICAL_TP1_MIN_RR", "1.0")
    monkeypatch.setenv("LEVELS_EMPIRICAL_TP1_BPS_MIN", "1")
    monkeypatch.setenv("LEVELS_EMPIRICAL_TP1_BPS_MAX", "1000")
    monkeypatch.setenv("LEVELS_EMPIRICAL_STOP_BPS_MIN", "1")
    monkeypatch.setenv("LEVELS_EMPIRICAL_STOP_BPS_MAX", "1000")

    table = {
        ("BTCUSDT", "breakout", "trend"): EmpiricalLevelStats(
            samples=100,
            mfe_tp1_bps_q60=50.0,    # 50 bps => tp1_dist=0.50 at entry=100
            mae_to_tp1_bps_q80=30.0, # 30 bps => stop_dist=0.30
            ttd_tp1_ms_median=120000,
        )
    }
    empirical = EmpiricalLevels.from_env(provider=_FakeProvider(table))

    ctx = SimpleNamespace()
    ctx.of = SimpleNamespace(price=100.0, atr=1.0, regime="trend")
    ctx.price = 100.0
    ctx.atr = 1.0

    cfg = {"STOP_MODE": "ATR", "STOP_ATR_MULT": 0.6, "TP_MODE": "RR", "TP_RR": "1,2,3"}

    attach_trade_levels_to_ctx(
        ctx,
        side="LONG",
        symbol="BTCUSDT",
        cfg=cfg,
        kind="breakout",
        regime="trend",
        empirical=empirical,
        overwrite=True,
        logger=None,
    )

    assert abs(ctx.sl_price - 99.70) < 1e-6
    assert abs(ctx.tp1_price - 100.50) < 1e-6
    assert getattr(ctx, "levels_source", "") == "empirical_blend"
    assert int(getattr(ctx, "levels_samples", 0)) == 100
    assert int(getattr(ctx, "levels_ttd_tp1_ms", 0)) == 120000
