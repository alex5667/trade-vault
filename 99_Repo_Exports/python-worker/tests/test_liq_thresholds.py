from core.liq_thresholds import classify_symbol, get_thresholds


def test_classify_symbol():
    assert classify_symbol("BTCUSDT") == "majors"
    assert classify_symbol("ETHUSDT") == "majors"
    assert classify_symbol("SOLUSDT") == "large"
    assert classify_symbol("XRPUSDT") == "mid"
    assert classify_symbol("1000BONKUSDT") == "memes"


def test_thresholds_differ_by_class():
    t_btc = get_thresholds("BTCUSDT")
    t_meme = get_thresholds("1000BONKUSDT")
    # majors should expect higher book update rate than memes
    assert t_btc.book_rate_good_hz > t_meme.book_rate_good_hz
    # memes should tolerate wider spreads
    assert t_meme.spread_bad_bp > t_btc.spread_bad_bp
