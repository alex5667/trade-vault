from news_pipeline.calendar_mapping import map_calendar_asset_classes


def test_us_high_includes_metals_and_crypto():
    out = map_calendar_asset_classes(country="United States", currency="USD", title="FOMC Rate Decision", importance=3)
    assert out == ["forex", "metals", "crypto"]

def test_eur_medium_includes_crypto():
    out = map_calendar_asset_classes(country="Eurozone", currency="EUR", title="ECB Press Conference", importance=2)
    assert "forex" in out
    assert "crypto" in out

def test_low_only_forex():
    out = map_calendar_asset_classes(country="Japan", currency="JPY", title="CPI", importance=1)
    assert out == ["forex"]
