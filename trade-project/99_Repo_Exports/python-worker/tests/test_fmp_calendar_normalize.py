from news_pipeline.sources.fmp_calendar import normalize_calendar_events


def test_normalize_fanout_usd_high():
    rows = [{
        "date": "2026-01-03 13:30:00",
        "country": "United States",
        "currency": "USD",
        "event": "Nonfarm Payrolls",
        "impact": "High",
        "forecast": "200K",
        "previous": "150K",
        "id": "nfp-2026-01"
    }]
    out = normalize_calendar_events(rows)
    asset = sorted(set([x["asset_class"] for x in out]))
    assert asset == ["crypto", "forex", "metals"]
    assert len(set([x["uid"] for x in out])) == 3

def test_normalize_requires_title_and_time():
    rows = [{"date": "2026-01-03"}]
    assert normalize_calendar_events(rows) == []
