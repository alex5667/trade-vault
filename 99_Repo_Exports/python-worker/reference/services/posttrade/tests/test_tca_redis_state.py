import unittest


class TestTcaRedisState(unittest.TestCase):
    def test_make_key(self):
        from services.posttrade.tca_redis_state import TcaKeyDims, make_key

        dims = TcaKeyDims(sym="btcusdt", venue="BINANCE", session="eu", tf="1m", kind="breakout", side="long")
        k = make_key("is", "p95", dims)
        self.assertEqual(k, "tca:is_p95_bps:BTCUSDT:binance:eu:1m:breakout:LONG")

        k2 = make_key("perm_impact", "p95", dims, delta_sec=1)
        self.assertEqual(k2, "tca:perm_impact_p95_bps:1:BTCUSDT:binance:eu:1m:breakout:LONG")

    def test_make_key_no_delta(self):
        """Key without delta_sec should omit delta segment."""
        from services.posttrade.tca_redis_state import TcaKeyDims, make_key

        dims = TcaKeyDims(sym="ETHUSDT", venue="binance", session="us", tf="5m", kind="pullback", side="SHORT")
        k = make_key("eff_spread", "p95", dims)
        self.assertEqual(k, "tca:eff_spread_p95_bps:ETHUSDT:binance:us:5m:pullback:SHORT")

    def test_norm_enforced(self):
        """make_key normalizes sym to upper, venue to lower, side to upper."""
        from services.posttrade.tca_redis_state import TcaKeyDims, make_key

        dims = TcaKeyDims(sym="solusdt", venue="BINANCE", session="as", tf="1m", kind="mo", side="buy")
        k = make_key("is", "p50", dims)
        self.assertIn("SOLUSDT", k)
        self.assertIn("binance", k)
        self.assertIn("BUY", k)


if __name__ == "__main__":
    unittest.main()
