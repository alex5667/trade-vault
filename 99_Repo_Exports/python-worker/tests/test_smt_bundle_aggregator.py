from __future__ import annotations

from services.smt_bundle_aggregator import BundleSpec, SmtAggregatorConfig, SmtBundleAggregator
from tests.fake_redis import FakeRedis


def test_smt_aggregator_writes_bundle_state(monkeypatch):
    r = FakeRedis()

    # create deterministic config
    cfg = SmtAggregatorConfig(
        window_n=64,
        max_lag=3,
        leader_confirm_min_bps=2.0,
        leader_dir_window=6,
        coh_min_corr=0.1,
        price_stale_ms=999999999,
        write_key_prefix="smt:bundle:v1:",
    )
    b = BundleSpec(bundle_id="btc_eth_sol", symbols=["BTCUSDT", "ETHUSDT", "SOLUSDT"])
    agg = SmtBundleAggregator(redis_client=r, bundles=[b], cfg=cfg)

    # feed prices (BTC leads)
    t0 = 1700000000000
    for i in range(1, 50):
        # BTC up first, then ETH/SOL follow slightly
        r.hset("price:latest:BTCUSDT", mapping={"mid": str(100 + i * 0.10), "ts_ms": str(t0 + i * 1000)})
        r.hset("price:latest:ETHUSDT", mapping={"mid": str(50 + max(0, i - 1) * 0.06), "ts_ms": str(t0 + i * 1000)})
        r.hset("price:latest:SOLUSDT", mapping={"mid": str(20 + max(0, i - 1) * 0.04), "ts_ms": str(t0 + i * 1000)})
        agg.tick_once()

    st = r.hgetall("smt:bundle:v1:btc_eth_sol")
    assert st, "bundle state must be written"
    # FakeRedis returns bytes/str depending on implementation; cast to str
    leader = (st.get("leader") or st.get(b"leader") or b"").decode() if isinstance(st.get("leader") or st.get(b"leader"), (bytes, bytearray)) else str(st.get("leader") or st.get(b"leader"))
    assert leader in ("BTCUSDT", "ETHUSDT", "SOLUSDT")

    coh = float((st.get("coh") or st.get(b"coh") or b"0").decode() if isinstance(st.get("coh") or st.get(b"coh"), (bytes, bytearray)) else (st.get("coh") or "0"))
    assert 0.0 <= coh <= 1.0, f"coherence should be 0..1, got {coh}"


def test_smt_aggregator_no_write_without_prices(monkeypatch):
    r = FakeRedis()

    cfg = SmtAggregatorConfig(
        window_n=64,
        max_lag=3,
        leader_confirm_min_bps=2.0,
        leader_dir_window=6,
        coh_min_corr=0.1,
        price_stale_ms=999999999,
        write_key_prefix="smt:bundle:v1:",
    )
    b = BundleSpec(bundle_id="btc_eth_sol", symbols=["BTCUSDT", "ETHUSDT", "SOLUSDT"])
    agg = SmtBundleAggregator(redis_client=r, bundles=[b], cfg=cfg)

    # no prices written
    n = agg.tick_once()
    assert n == 0, "should not write without prices"

    st = r.hgetall("smt:bundle:v1:btc_eth_sol")
    assert not st, "should not write bundle state without prices"


def test_smt_aggregator_leader_detection():
    r = FakeRedis()

    cfg = SmtAggregatorConfig(
        window_n=180,
        max_lag=5,
        leader_confirm_min_bps=12.0,
        leader_dir_window=6,
        coh_min_corr=0.55,
        price_stale_ms=7000,
        write_key_prefix="smt:bundle:v1:",
    )
    b = BundleSpec(bundle_id="test_bundle", symbols=["LDRUSDT", "FLWUSDT"])
    agg = SmtBundleAggregator(redis_client=r, bundles=[b], cfg=cfg)

    # simulate leader going up strongly
    t0 = 1700000000000
    for i in range(1, 200):  # enough samples
        # leader moves up 20bps per step
        ldr_price = 100 * (1 + i * 0.002)
        # follower lags by 2 steps, moves up 10bps per step
        flw_price = 50 * (1 + max(0, i - 2) * 0.001)

        r.hset("price:latest:LDRUSDT", mapping={"mid": str(ldr_price), "ts_ms": str(t0 + i * 1000)})
        r.hset("price:latest:FLWUSDT", mapping={"mid": str(flw_price), "ts_ms": str(t0 + i * 1000)})
        agg.tick_once()

    st = r.hgetall("smt:bundle:v1:test_bundle")
    assert st, "bundle state must be written"

    leader = (st.get("leader") or st.get(b"leader") or b"").decode() if isinstance(st.get("leader") or st.get(b"leader"), (bytes, bytearray)) else str(st.get("leader") or st.get(b"leader"))
    leader_dir = (st.get("leader_dir") or st.get(b"leader_dir") or b"").decode() if isinstance(st.get("leader_dir") or st.get(b"leader_dir"), (bytes, bytearray)) else str(st.get("leader_dir") or st.get(b"leader_dir"))
    leader_confirm = int(float((st.get("leader_confirm") or st.get(b"leader_confirm") or b"0").decode() if isinstance(st.get("leader_confirm") or st.get(b"leader_confirm"), (bytes, bytearray)) else (st.get("leader_confirm") or "0")))

    assert leader == "LDRUSDT", f"expected leader LDRUSDT, got {leader}"
    assert leader_dir == "UP", f"expected direction UP, got {leader_dir}"
    assert leader_confirm == 1, f"expected confirmed=1, got {leader_confirm}"
