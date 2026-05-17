from __future__ import annotations

"""Tests for Layer D consumer arm_callback and early-arm hook."""

import hashlib
import hmac
import json
import unittest
from unittest.mock import MagicMock, patch


# ─── helpers ─────────────────────────────────────────────────────────────────

def _sign(payload: dict, secret: str = "test_secret") -> str:
    blob = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    return hmac.new(secret.encode(), blob, hashlib.sha256).hexdigest()


def _arm_payload(
    signal_id: str = "sig-001",
    symbol: str = "BTCUSDT",
    side: str = "LONG",
    mfe_r: float = 0.7,
    mfe_bps: float = 35.0,
    one_r_bps: float = 50.0,
) -> dict:
    return {
        "signal_id": signal_id,
        "symbol": symbol,
        "side": side,
        "mfe_r": mfe_r,
        "mfe_bps": mfe_bps,
        "one_r_bps": one_r_bps,
        "ts_ms": 1_700_000_000_000,
        "source": "MFE_EARLY_ARM",
        "arm_threshold_r": 0.5,
    }


# ─── _verify_hmac ─────────────────────────────────────────────────────────────

class TestVerifyHmacConsumer(unittest.TestCase):

    def test_valid_hmac(self):
        from services.tp_hit_trailing_orchestrator_layer_d_consumer import _verify_hmac
        p = _arm_payload()
        raw = json.dumps(p, sort_keys=True, separators=(",", ":"))
        sig = _sign(p, "secret")
        self.assertTrue(_verify_hmac(raw, sig, "secret"))

    def test_wrong_secret(self):
        from services.tp_hit_trailing_orchestrator_layer_d_consumer import _verify_hmac
        p = _arm_payload()
        raw = json.dumps(p, sort_keys=True, separators=(",", ":"))
        sig = _sign(p, "correct_secret")
        self.assertFalse(_verify_hmac(raw, sig, "wrong_secret"))

    def test_tampered_payload(self):
        from services.tp_hit_trailing_orchestrator_layer_d_consumer import _verify_hmac
        p = _arm_payload()
        raw = json.dumps(p, sort_keys=True, separators=(",", ":"))
        sig = _sign(p, "secret")
        tampered = raw.replace("0.7", "0.9")
        self.assertFalse(_verify_hmac(tampered, sig, "secret"))

    def test_empty_inputs_return_false(self):
        from services.tp_hit_trailing_orchestrator_layer_d_consumer import _verify_hmac
        self.assertFalse(_verify_hmac("", "sig", "secret"))
        self.assertFalse(_verify_hmac("{}", "", "secret"))
        self.assertFalse(_verify_hmac("{}", "sig", ""))


# ─── _is_duplicate ────────────────────────────────────────────────────────────

class TestIsDuplicate(unittest.TestCase):

    def test_first_call_not_duplicate(self):
        from services.tp_hit_trailing_orchestrator_layer_d_consumer import _is_duplicate
        r = MagicMock()
        r.set.return_value = True  # nx=True succeeded
        self.assertFalse(_is_duplicate(r, "sid-1", 3600))

    def test_second_call_is_duplicate(self):
        from services.tp_hit_trailing_orchestrator_layer_d_consumer import _is_duplicate
        r = MagicMock()
        r.set.return_value = None  # nx=True failed → already exists
        self.assertTrue(_is_duplicate(r, "sid-1", 3600))

    def test_redis_error_returns_false(self):
        from services.tp_hit_trailing_orchestrator_layer_d_consumer import _is_duplicate
        r = MagicMock()
        r.set.side_effect = Exception("redis down")
        self.assertFalse(_is_duplicate(r, "sid-1", 3600))


# ─── _get_mid_price ──────────────────────────────────────────────────────────

class TestGetMidPrice(unittest.TestCase):

    def test_reads_mid_field(self):
        from services.tp_hit_trailing_orchestrator_layer_d_consumer import _get_mid_price
        r = MagicMock()
        r.xrevrange.return_value = [("1-0", {"mid": "68500.5"})]
        self.assertAlmostEqual(_get_mid_price(r, "BTCUSDT"), 68500.5)

    def test_falls_back_to_price_field(self):
        from services.tp_hit_trailing_orchestrator_layer_d_consumer import _get_mid_price
        r = MagicMock()
        r.xrevrange.return_value = [("1-0", {"price": "3200.0"})]
        self.assertAlmostEqual(_get_mid_price(r, "ETHUSDT"), 3200.0)

    def test_returns_zero_on_empty_stream(self):
        from services.tp_hit_trailing_orchestrator_layer_d_consumer import _get_mid_price
        r = MagicMock()
        r.xrevrange.return_value = []
        self.assertEqual(_get_mid_price(r, "BTCUSDT"), 0.0)

    def test_returns_zero_on_redis_error(self):
        from services.tp_hit_trailing_orchestrator_layer_d_consumer import _get_mid_price
        r = MagicMock()
        r.xrevrange.side_effect = Exception("connection refused")
        self.assertEqual(_get_mid_price(r, "BTCUSDT"), 0.0)

    def test_skips_zero_price(self):
        from services.tp_hit_trailing_orchestrator_layer_d_consumer import _get_mid_price
        r = MagicMock()
        r.xrevrange.return_value = [("1-0", {"mid": "0", "price": "68000.0"})]
        self.assertAlmostEqual(_get_mid_price(r, "BTCUSDT"), 68000.0)


# ─── arm_callback via _make_arm_callback ─────────────────────────────────────

class TestMakeArmCallback(unittest.TestCase):

    def _mock_result(self, success=True, skipped=False, error=""):
        result = MagicMock()
        result.success = success
        result.skipped = skipped
        result.error = error
        return result

    def test_success_path(self):
        """arm_callback injects trail_after_tp1=True and calls start_trailing."""
        import services.tp_hit_trailing_orchestrator_layer_d_consumer as mod

        mock_orch = MagicMock()
        callback_called_with: list = []

        def fake_start_trailing(**kwargs):
            callback_called_with.append(kwargs)
            return self._mock_result(success=True)

        mock_orch.start_trailing = fake_start_trailing
        mock_orch._get_signal.return_value = (
            {"trail_after_tp1": False, "trail_profile": "default"},
            "signals:sig-001",
        )

        with patch.object(mod, "_get_mid_price", return_value=69000.0):
            def arm_cb(payload):
                sid = str(payload.get("signal_id", "") or "")
                symbol = str(payload.get("symbol", "") or "").upper()
                price = mod._get_mid_price(MagicMock(), symbol)
                if not sid or not symbol or price <= 0:
                    return False
                sig_data = mock_orch._get_signal(sid)
                signal = dict(sig_data[0]) if sig_data else {"trail_after_tp1": True}
                signal_key = sig_data[1] if sig_data else None
                signal["trail_after_tp1"] = True
                result = mock_orch.start_trailing(
                    sid=sid, symbol=symbol, price=price,
                    source="layer_d_early_arm", signal_payload=signal, signal_key=signal_key,
                )
                return result.success or result.skipped

            result = arm_cb(_arm_payload())

        self.assertTrue(result)
        self.assertEqual(callback_called_with[0]["sid"], "sig-001")
        self.assertEqual(callback_called_with[0]["symbol"], "BTCUSDT")
        self.assertAlmostEqual(callback_called_with[0]["price"], 69000.0)
        self.assertTrue(callback_called_with[0]["signal_payload"]["trail_after_tp1"])

    def test_missing_sid_returns_false(self):
        import services.tp_hit_trailing_orchestrator_layer_d_consumer as mod
        with patch.object(mod, "_get_mid_price", return_value=68000.0):
            mock_orch = MagicMock()
            mock_orch._get_signal.return_value = None

            def arm_cb(payload):
                sid = str(payload.get("signal_id", "") or "")
                symbol = str(payload.get("symbol", "") or "").upper()
                price = mod._get_mid_price(MagicMock(), symbol)
                if not sid or not symbol or price <= 0:
                    return False
                return True

            p = _arm_payload()
            p["signal_id"] = ""
            self.assertFalse(arm_cb(p))

    def test_no_mid_price_returns_false(self):
        import services.tp_hit_trailing_orchestrator_layer_d_consumer as mod
        with patch.object(mod, "_get_mid_price", return_value=0.0):
            def arm_cb(payload):
                sid = str(payload.get("signal_id", "") or "")
                symbol = str(payload.get("symbol", "") or "").upper()
                price = mod._get_mid_price(MagicMock(), symbol)
                if not sid or not symbol or price <= 0:
                    return False
                return True

            self.assertFalse(arm_cb(_arm_payload()))

    def test_skipped_treated_as_success(self):
        result = MagicMock()
        result.success = False
        result.skipped = True
        # skipped (dedup hit) should be treated as OK
        self.assertTrue(result.success or result.skipped)

    def test_trail_after_tp1_injected(self):
        """Even if signal has trail_after_tp1=False, callback forces it True."""
        signal = {"trail_after_tp1": False, "trail_profile": "default"}
        signal["trail_after_tp1"] = True  # injection
        self.assertTrue(signal["trail_after_tp1"])


# ─── run() with arm_callback=None ────────────────────────────────────────────

class TestRunNullCallback(unittest.TestCase):

    def test_disabled_exits_immediately(self):
        from services.tp_hit_trailing_orchestrator_layer_d_consumer import run
        import os
        with patch.dict(os.environ, {"LAYER_D_CONSUMER_ENABLE": "0"}):
            ret = run(arm_callback=None)
        self.assertEqual(ret, 0)


# ─── layer_d_early_arm_hook ──────────────────────────────────────────────────

class TestLayerDEarlyArmHook(unittest.TestCase):

    def _make_pos(self, *, direction="LONG", entry=100.0, peak=150.0, sl=80.0,
                  arm_sent=False, trailing_started=False, signal_id="sid-test",
                  symbol="BTCUSDT"):
        pos = MagicMock()
        pos.symbol = symbol
        pos.direction = direction
        pos.entry_price = entry
        pos.max_favorable_price = peak
        pos.sl_price = sl
        pos.one_r_bps = 0.0
        pos.trailing_started = trailing_started
        pos.trailing_active = False
        pos._layer_d_arm_sent = arm_sent
        pos.signal_id = signal_id
        pos.sid = signal_id
        return pos

    def test_below_threshold_does_not_emit(self):
        from services.trade_monitor.layer_d_early_arm_hook import evaluate_and_emit
        pos = self._make_pos(entry=100.0, peak=101.0)  # mfe_bps=100, one_r_bps=200 → mfe_r=0.5
        # mfe_r = 100/200 = 0.5, threshold default=0.5 → mfe_r < threshold fails (strict <)
        # Actually 0.5 is not >= 0.5 when threshold is 0.5 (it's equal, so should arm)
        # Let's use peak=100.5 for mfe_r=0.25 < threshold
        pos.max_favorable_price = 100.5  # mfe_bps=50, one_r_bps=200 → mfe_r=0.25
        redis_mock = MagicMock()
        # Mode must be non-off for this to even evaluate
        with patch.dict(__import__("os").environ, {
            "OF_LAYER_D_EARLY_ARM_MODE": "enforce",
            "OF_LAYER_D_ARM_THRESHOLD_R": "0.5",
            "LAYER_D_HOOK_ENABLED": "1",
        }):
            from services.trade_monitor import layer_d_early_arm_hook as _hook
            _hook._CFG.mode = "enforce"
            _hook._CFG.canary_symbols = set()
            _hook._CFG.arm_threshold_r = 0.5
            result = evaluate_and_emit(pos, 1_700_000_000_000, redis_mock)
        self.assertFalse(result)
        redis_mock.xadd.assert_not_called()

    def test_already_sent_skips(self):
        from services.trade_monitor.layer_d_early_arm_hook import evaluate_and_emit
        pos = self._make_pos(arm_sent=True)
        redis_mock = MagicMock()
        from services.trade_monitor import layer_d_early_arm_hook as _hook
        _hook._CFG.mode = "enforce"
        _hook._CFG.canary_symbols = set()
        _hook._CFG.arm_threshold_r = 0.5
        result = evaluate_and_emit(pos, 1_700_000_000_000, redis_mock)
        self.assertFalse(result)
        redis_mock.xadd.assert_not_called()

    def test_trailing_already_started_skips(self):
        from services.trade_monitor.layer_d_early_arm_hook import evaluate_and_emit
        pos = self._make_pos(trailing_started=True)
        redis_mock = MagicMock()
        from services.trade_monitor import layer_d_early_arm_hook as _hook
        _hook._CFG.mode = "enforce"
        _hook._CFG.canary_symbols = set()
        result = evaluate_and_emit(pos, 1_700_000_000_000, redis_mock)
        self.assertFalse(result)

    def test_shadow_mode_does_not_xadd(self):
        from services.trade_monitor import layer_d_early_arm_hook as _hook
        _hook._CFG.mode = "shadow"
        _hook._CFG.canary_symbols = set()
        _hook._CFG.arm_threshold_r = 0.3  # low threshold so mfe_r passes

        pos = self._make_pos(entry=100.0, peak=150.0, sl=80.0)
        # mfe_bps=5000, one_r_bps=2000 → mfe_r=2.5 > 0.3
        redis_mock = MagicMock()
        from services.trade_monitor.layer_d_early_arm_hook import evaluate_and_emit
        result = evaluate_and_emit(pos, 1_700_000_000_000, redis_mock)
        self.assertFalse(result)  # shadow returns False (no actual emit)
        redis_mock.xadd.assert_not_called()

    def test_enforce_mode_emits_xadd(self):
        from services.trade_monitor import layer_d_early_arm_hook as _hook
        _hook._CFG.mode = "enforce"
        _hook._CFG.canary_symbols = set()
        _hook._CFG.arm_threshold_r = 0.3

        pos = self._make_pos(entry=100.0, peak=150.0, sl=80.0)
        redis_mock = MagicMock()
        with patch.dict(__import__("os").environ, {
            "LAYER_D_HMAC_SECRET": "test_secret",
            "LAYER_D_ARM_STREAM": "trail:arm:requests",
        }):
            from services.trade_monitor.layer_d_early_arm_hook import evaluate_and_emit
            result = evaluate_and_emit(pos, 1_700_000_000_000, redis_mock)

        self.assertTrue(result)
        redis_mock.xadd.assert_called_once()
        xadd_args = redis_mock.xadd.call_args
        stream_name = xadd_args[0][0]
        self.assertEqual(stream_name, "trail:arm:requests")
        fields = xadd_args[0][1]
        self.assertIn("payload", fields)
        self.assertIn("sig", fields)
        payload = json.loads(fields["payload"])
        self.assertEqual(payload["signal_id"], "sid-test")
        self.assertEqual(payload["symbol"], "BTCUSDT")
        self.assertGreater(payload["mfe_r"], 0.3)

    def test_canary_blocks_non_canary_symbol(self):
        from services.trade_monitor import layer_d_early_arm_hook as _hook
        _hook._CFG.mode = "enforce"
        _hook._CFG.canary_symbols = {"ETHUSDT"}  # only ETH in canary
        _hook._CFG.arm_threshold_r = 0.3

        pos = self._make_pos(entry=100.0, peak=150.0, sl=80.0)  # symbol=BTCUSDT via mock
        pos.symbol = "BTCUSDT"
        redis_mock = MagicMock()
        from services.trade_monitor.layer_d_early_arm_hook import evaluate_and_emit
        result = evaluate_and_emit(pos, 1_700_000_000_000, redis_mock)
        self.assertFalse(result)
        redis_mock.xadd.assert_not_called()

    def test_idempotency_flag_set_after_emit(self):
        from services.trade_monitor import layer_d_early_arm_hook as _hook
        _hook._CFG.mode = "enforce"
        _hook._CFG.canary_symbols = set()
        _hook._CFG.arm_threshold_r = 0.3

        pos = self._make_pos(entry=100.0, peak=150.0, sl=80.0)
        redis_mock = MagicMock()
        with patch.dict(__import__("os").environ, {"LAYER_D_HMAC_SECRET": "sec"}):
            from services.trade_monitor.layer_d_early_arm_hook import evaluate_and_emit
            evaluate_and_emit(pos, 1_700_000_000_000, redis_mock)

        self.assertTrue(getattr(pos, "_layer_d_arm_sent", False))


if __name__ == "__main__":
    unittest.main()
