

# Add project root to sys.path
# [AUTOGRAVITY CLEANUP] sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from services.l3_queue_events_proxy import L3BucketStats, L3QueueEventsProxy


def test_l3_lite_reconciliation_trades_only():
    """Test that trades without L2 level changes result in zero cancellation rate (only added)."""
    proxy = L3QueueEventsProxy(bucket_ms=1000, alpha=1.0)
    proxy.on_l2_totals(bid_total=100.0, ask_total=100.0)
    proxy.on_bucket_advance(bucket_id=1)
    proxy.on_trade(side=1, qty=10.0)
    proxy.on_trade(side=-1, qty=5.0)
    proxy.on_l2_totals(bid_total=100.0, ask_total=100.0)
    stats = proxy.on_bucket_advance(bucket_id=2)
    assert stats.cancel_bid_rate_ema == 0.0
    assert stats.cancel_ask_rate_ema == 0.0
    assert stats.taker_buy_qty == 10.0
    assert stats.taker_sell_qty == 5.0

def test_l3_lite_reconciliation_pulled_only():
    """Test that L2 level decreases without trades result in positive cancellation rate."""
    proxy = L3QueueEventsProxy(bucket_ms=1000, alpha=1.0)
    proxy.on_l2_totals(bid_total=100.0, ask_total=100.0)
    proxy.on_bucket_advance(bucket_id=1)
    proxy.on_l2_totals(bid_total=90.0, ask_total=80.0)
    stats = proxy.on_bucket_advance(bucket_id=2)
    assert stats.cancel_bid_rate_ema == 10.0
    assert stats.cancel_ask_rate_ema == 20.0

def test_l3_lite_reconciliation_mixed():
    """Test mixed scenario with trades and L2 level changes."""
    proxy = L3QueueEventsProxy(bucket_ms=1000, alpha=1.0)
    proxy.on_l2_totals(bid_total=100.0, ask_total=100.0)
    proxy.on_bucket_advance(bucket_id=1)
    proxy.on_trade(side=-1, qty=30.0)
    proxy.on_l2_totals(bid_total=90.0, ask_total=100.0)
    proxy.on_trade(side=1, qty=10.0)
    proxy.on_l2_totals(bid_total=90.0, ask_total=50.0)
    stats = proxy.on_bucket_advance(bucket_id=2)
    assert stats.cancel_bid_rate_ema == 0.0
    assert stats.cancel_ask_rate_ema == 40.0

def test_l3_lite_reconciliation_overflow():
    """Test that trades exceeding L2 depth are handled and don't cause negative Pulled."""
    proxy = L3QueueEventsProxy(bucket_ms=1000, alpha=1.0)
    proxy.on_l2_totals(bid_total=10.0, ask_total=10.0)
    proxy.on_bucket_advance(bucket_id=1)
    proxy.on_trade(side=1, qty=50.0)
    proxy.on_l2_totals(bid_total=10.0, ask_total=0.0)
    stats = proxy.on_bucket_advance(bucket_id=2)
    assert stats.cancel_ask_rate_ema == 0.0
    assert stats.taker_buy_qty == 50.0

def test_l3_lite_missing_baseline():
    """Test that reconciliation is skipped if baseline L2 totals are missing."""
    proxy = L3QueueEventsProxy(bucket_ms=1000, alpha=1.0)
    proxy.on_bucket_advance(bucket_id=1)
    proxy.on_trade(side=1, qty=10.0)
    proxy.on_l2_totals(bid_total=100.0, ask_total=100.0)
    stats = proxy.on_bucket_advance(bucket_id=2)
    assert stats.cancel_bid_rate_ema == 0.0
    assert stats.cancel_ask_rate_ema == 0.0


# ---------------------------------------------------------------------------
# New tests: VPIN toxicity + added rates (v7 features)
# ---------------------------------------------------------------------------

def test_l3_vpin_tox_balanced_flow():
    """Balanced buy/sell => toxicity EMA near 0."""
    proxy = L3QueueEventsProxy(bucket_ms=1000, alpha=1.0)
    proxy.on_l2_totals(bid_total=100.0, ask_total=100.0)
    proxy.on_bucket_advance(bucket_id=1)
    proxy.on_trade(side=1, qty=50.0)
    proxy.on_trade(side=-1, qty=50.0)
    proxy.on_l2_totals(bid_total=100.0, ask_total=100.0)
    stats = proxy.on_bucket_advance(bucket_id=2)
    assert stats.vpin_tox_ema is not None
    assert 0.0 <= float(stats.vpin_tox_ema) <= 1.0


def test_l3_vpin_tox_one_sided_flow():
    """One-sided buy flow => toxicity EMA > 0."""
    proxy = L3QueueEventsProxy(bucket_ms=1000, alpha=1.0)
    proxy.on_l2_totals(bid_total=100.0, ask_total=100.0)
    proxy.on_bucket_advance(bucket_id=1)
    proxy.on_trade(side=1, qty=100.0)  # only buy
    proxy.on_l2_totals(bid_total=100.0, ask_total=0.0)
    stats = proxy.on_bucket_advance(bucket_id=2)
    # raw tox = 1.0, EMA alpha=0.10 => vpin_tox_ema > 0
    assert float(stats.vpin_tox_ema) > 0.0


def test_l3_added_bid_rate_ema_positive():
    """L2 bid depth increase (stacking) → positive added_bid_rate_ema."""
    proxy = L3QueueEventsProxy(bucket_ms=1000, alpha=1.0)
    proxy.on_l2_totals(bid_total=50.0, ask_total=50.0)
    proxy.on_bucket_advance(bucket_id=1)
    # No trades, bid depth increased => stacking
    proxy.on_l2_totals(bid_total=70.0, ask_total=50.0)
    stats = proxy.on_bucket_advance(bucket_id=2)
    # added_bid = 70 - 50 = 20 => rate = 20/1.0 = 20.0
    assert float(stats.added_bid_rate_ema) > 0.0
    assert stats.cancel_bid_rate_ema == 0.0  # nothing pulled


def test_l3_no_added_when_pulled():
    """No adds when only pulling liquidity (depth decreases with no trades)."""
    proxy = L3QueueEventsProxy(bucket_ms=1000, alpha=1.0)
    proxy.on_l2_totals(bid_total=100.0, ask_total=100.0)
    proxy.on_bucket_advance(bucket_id=1)
    # Depth decreased without trades => pulled, not added
    proxy.on_l2_totals(bid_total=80.0, ask_total=80.0)
    stats = proxy.on_bucket_advance(bucket_id=2)
    assert stats.added_bid_rate_ema == 0.0
    assert stats.added_ask_rate_ema == 0.0
    assert stats.cancel_bid_rate_ema > 0.0


def test_l3_snapshot_contains_new_keys():
    """proxy.snapshot() must contain all new v7 keys."""
    proxy = L3QueueEventsProxy(bucket_ms=1000)
    snap = proxy.snapshot()
    for key in ("added_bid_rate_ema", "added_ask_rate_ema", "vpin_tox_ema", "vpin_tox_z",
                "cancel_bid_rate_ema", "cancel_ask_rate_ema"):
        assert key in snap, f"snapshot() missing key: {key}"


def test_l3_bucket_stats_has_new_fields():
    """L3BucketStats dataclass must have added/vpin fields."""
    s = L3BucketStats()
    assert hasattr(s, "added_bid_qty")
    assert hasattr(s, "added_ask_qty")
    assert hasattr(s, "added_bid_rate_ema")
    assert hasattr(s, "added_ask_rate_ema")
    assert hasattr(s, "vpin_tox_ema")
    assert hasattr(s, "vpin_tox_z")
