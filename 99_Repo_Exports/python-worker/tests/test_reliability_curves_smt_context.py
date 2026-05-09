import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tests.fake_redis import FakeRedis


def test_reliability_curve_writes_global_and_smt_context_keys():
    os.environ["RELIABILITY_TARGETS"] = "tp1"
    os.environ["RELIABILITY_BUCKET_STEP"] = "5"
    os.environ["RELIABILITY_SMT_COH_THR"] = "0.65"

    from services.reliability_curves import make_reliability_key, make_reliability_key_v3, update_reliability_curve

    r = FakeRedis()

    # PositionState.__dict__ shape (important): signal_payload.ctx holds smt_* and confidence.
    pos = {
        "strategy": "CryptoOrderFlow",
        "symbol": "BTCUSDT",
        "tf": "1m",
        "signal_payload": {
            "kind": "absorption",
            "entry_regime": "trending_bull",
            "venue": "binance_futures",
            "ctx": {
                "final_score": 0.70,
                "smt_leader_confirm": 1,
                "smt_coh": 0.80,
                "smt_leader_dir": "UP",
                "smt_bundle": "btc_eth_sol",
                "smt_leader": "BTCUSDT",
            }
        },
    }
    closed = {
        "strategy": "CryptoOrderFlow",
        "symbol": "BTCUSDT",
        "tf": "1m",
        "direction": "LONG",
        "tp1_hit": 1,
        "exit_ts_ms": 1_700_000_000_000,
    }

    update_reliability_curve(r, closed=closed, pos=pos)

    venue = "binance_futures"
    # v4 keys (preferred)
    k4_global = make_reliability_key_v4(
        target="tp1",
        strategy="CryptoOrderFlow",
        symbol="BTCUSDT",
        tf="1m",
        venue=venue,
        kind="absorption",
        regime="trending_bull",
        ctx_key="na",
    )
    k4_ctx = make_reliability_key_v4(
        target="tp1",
        strategy="CryptoOrderFlow",
        symbol="BTCUSDT",
        tf="1m",
        venue=venue,
        kind="absorption",
        regime="trending_bull",
        ctx_key="smtc1_coh1_al1",
    )

    # v3 keys (legacy-but-still-written)
    k3_global = make_reliability_key_v3(
        target="tp1",
        strategy="CryptoOrderFlow",
        symbol="BTCUSDT",
        tf="1m",
        kind="absorption",
        regime="trending_bull",
        ctx_key="na",
    )
    k3_ctx = make_reliability_key_v3(
        target="tp1",
        strategy="CryptoOrderFlow",
        symbol="BTCUSDT",
        tf="1m",
        kind="absorption",
        regime="trending_bull",
        ctx_key="smtc1_coh1_al1",
    )

    d4g = r.hgetall(k4_global)
    d4c = r.hgetall(k4_ctx)
    assert d4g, "v4 global curve must be written"
    assert d4c, "v4 smt context curve must be written"

    d3g = r.hgetall(k3_global)
    d3c = r.hgetall(k3_ctx)
    assert d3g, "v3 global curve must be written"
    assert d3c, "v3 smt context curve must be written"

    # base_conf=0.70 -> bucket=70 (step=5)
    k_global = make_reliability_key(
        target="tp1",
        strategy="CryptoOrderFlow",
        symbol="BTCUSDT",
        tf="1m",
        ctx_key="na",
    )
    # SMT context:
    # leader_confirm=1, coh_hi=1 (0.8>=0.65), align=1 (LONG->UP matches leader_dir UP)
    k_ctx = make_reliability_key(
        target="tp1",
        strategy="CryptoOrderFlow",
        symbol="BTCUSDT",
        tf="1m",
        ctx_key="smtc1_coh1_al1",
    )

    d0 = r.hgetall(k_global)  # v2 still written for compatibility
    d1 = r.hgetall(k_ctx)
    assert d0, "global curve must be written"
    assert d1, "smt context curve must be written"
    assert int(d0.get(b"samples") or d0.get("samples")) == 1
    assert int(d1.get(b"samples") or d1.get("samples")) == 1
    # bucket counters
    assert int(d0.get(b"n:70") or d0.get("n:70")) == 1
    assert int(d0.get(b"h:70") or d0.get("h:70")) == 1
    assert int(d1.get(b"n:70") or d1.get("n:70")) == 1
    assert int(d1.get(b"h:70") or d1.get("h:70")) == 1
