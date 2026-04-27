class DummyEnricher:
    def __init__(self):
        self.calls = 0
    def attach(self, ctx, asset_class=""):
        self.calls += 1
        ctx.news = getattr(ctx, "news") or None


def test_dummy_enricher_attach_is_called():
    from contexts import OrderflowSignalContext
    ctx = OrderflowSignalContext(symbol="BTCUSDT", asset_class="crypto")
    e = DummyEnricher()
    e.attach(ctx, asset_class=ctx.asset_class)
    assert e.calls == 1
