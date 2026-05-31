from core.atr_sanity import ATRSanity


class TestATRSanityRepro:
    def test_mixed_timeframes_bug(self):
        """
        Reproduce the bug where mixing timeframes in the same ATRSanity instance
        causes false positives because history is shared per-symbol, ignoring TF.
        """
        sanity = ATRSanity(window=10)
        symbol = "BTCUSDT"

        # 1. Warm up with consistent 1m data (low ATR)
        # avg ~ 10.0
        for _ in range(20):
            res = sanity.update(
                atr=10.0, px=50000.0, age_ms=0, now_ms=1000,
                symbol=symbol, tf="1m"
            )
            assert res.bad == 0

        # 2. Switch to 15m data (higher ATR naturally)
        # avg ~ 40.0 (4x 1m)
        # This SHOULD be treated as a new stream or separate history.
        # But in the buggy version, it compares 40.0 against 1m median (10.0) -> Jump > 300% -> BAD
        res = sanity.update(
            atr=40.0, px=50000.0, age_ms=0, now_ms=2000,
            symbol=symbol, tf="15m"
        )

        # In FIXED version, this should be 0 (fresh history for 15m).
        # We assert that the fix works.
        print(f"Result for 15m switch: bad={res.bad}, reason={res.reason}")

        # Expect NO alert (bad=0) because 15m history is now separate from 1m.
        assert res.bad == 0
        assert res.reason == ""

    def test_zero_price_handling(self):
        """
        Ensure zero price doesn't crash calculations
        """
        sanity = ATRSanity(window=10)
        # px=0 -> atr_bps calculation div/0 protection
        res = sanity.update(
            atr=10.0, px=0.0, age_ms=0, now_ms=1000,
            symbol="ETHUSDT", tf="1m"
        )
        # Should rely on safe division
        assert res.atr_bps == 0.0
        # If atr_bps=0 and min_bps=2.0 (default), it might be bad depending on logic.
        # Logic: if atr_bps <= 0 ... -> bad, reason="atr_bps_oob:0.00"
        assert res.bad == 1
        assert "atr_bps_oob" in str(res.reason)

    def test_zero_atr_handling(self):
        """
        Ensure zero ATR is handled gracefully
        """
        sanity = ATRSanity(window=10)
        res = sanity.update(
            atr=0.0, px=50000.0, age_ms=0, now_ms=1000,
            symbol="ETHUSDT", tf="1m"
        )
        assert res.bad == 1
        assert "atr<=0" in str(res.reason)
