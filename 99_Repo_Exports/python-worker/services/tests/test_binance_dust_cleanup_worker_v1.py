from __future__ import annotations

"""Tests for binance_dust_cleanup_worker (sequential patch v1).

Covers:
  1. confirm-before-act — sweep_once must accumulate confirm_passes before acting.
  2. exact qty close    — MARKET order is placed with live positionAmt quantity.
  3. skip non-dust      — positions above dust thresholds are never touched.
  4. error path         — cleanup errors surface as 'error' status in acted list.
"""

from services.binance_dust_cleanup_worker import BinanceDustCleanupWorker

# ---------------------------------------------------------------------------
# Fake infrastructure
# ---------------------------------------------------------------------------

class FakeRedis:
    """Minimal Redis stub that records xadd calls and simulates KV + TTL + Set operations."""
    def __init__(self):
        self.events = []
        self.kv = {}
        self.expiry_ms = {}
        self.sets = {}
        self.now_ms = 0

    def _purge(self, key=None):
        """Expire entries whose TTL has elapsed based on simulated clock."""
        if key is not None:
            exp = self.expiry_ms.get(key)
            if exp is not None and exp <= self.now_ms:
                self.kv.pop(key, None)
                self.expiry_ms.pop(key, None)
            return
        for k in list(self.expiry_ms.keys()):
            self._purge(k)

    def advance(self, sec: float):
        """Advance the simulated clock by sec seconds, triggering TTL expiry."""
        self.now_ms += int(sec * 1000)
        self._purge()

    def xadd(self, stream, fields, **kwargs):
        self.events.append((stream, dict(fields), dict(kwargs)))
        return f"{len(self.events)}-0"

    def setex(self, key, ttl_sec, value):
        self.kv[str(key)] = str(value)
        self.expiry_ms[str(key)] = self.now_ms + (int(ttl_sec) * 1000)
        return True

    def get(self, key):
        key = str(key)
        self._purge(key)
        return self.kv.get(key)

    def delete(self, key):
        self.kv.pop(str(key), None)
        self.expiry_ms.pop(str(key), None)
        return 1

    def pttl(self, key):
        key = str(key)
        self._purge(key)
        if key not in self.kv:
            return -2
        exp = self.expiry_ms.get(key)
        if exp is None:
            return -1
        return max(0, exp - self.now_ms)

    def sadd(self, key, *values):
        bucket = self.sets.setdefault(str(key), set())
        for v in values:
            bucket.add(str(v)),
        return len(values),

    def sismember(self, key, value):
        return str(value) in self.sets.get(str(key), set()),


class FakeClient:
    """Stub exchange client that plays back per-symbol position sequences.,

    APTUSDT: starts as 0.1 qty / 0.10 notional (dust) → becomes flat after first close.,
    BTCUSDT: 0.005 qty / 350 notional (NOT dust) — should never be touched.,
    """
    def __init__(self):
        # Each symbol maps to an ordered list of positionRisk rows.
        # current_idx tracks how many times post_plain_order advanced the row index.
        self.position_rows = {
            'APTUSDT': [
                # row 0 — dust tail
                {'symbol': 'APTUSDT', 'positionAmt': '0.1', 'notional': '0.10', 'isolatedMargin': '0.00'},
                # row 1 — flat (after close order fills)
                {'symbol': 'APTUSDT', 'positionAmt': '0.0', 'notional': '0.0', 'isolatedMargin': '0.0'}],
            'BTCUSDT': [
                {'symbol': 'BTCUSDT', 'positionAmt': '0.005', 'notional': '350.0', 'isolatedMargin': '18.0'}],
            'SUIUSDT': [
                {'symbol': 'SUIUSDT', 'positionAmt': '0.1', 'notional': '0.11', 'isolatedMargin': '0.01'},
                {'symbol': 'SUIUSDT', 'positionAmt': '0.0', 'notional': '0.0', 'isolatedMargin': '0.0'}]
        }
        self.current_idx = {'APTUSDT': 0, 'BTCUSDT': 0, 'SUIUSDT': 0}
        # Mutable order books (reset to [] after cancel_all_orders)
        self.plain_orders = {'APTUSDT': [{'orderId': 1, 'clientOrderId': 'plain-1'}], 'BTCUSDT': [], 'SUIUSDT': []}
        self.algo_orders = {'APTUSDT': [{'algoId': 11, 'clientAlgoId': 'algo-11'}], 'BTCUSDT': [], 'SUIUSDT': []}
        self.cancel_all_calls = []
        self.post_plain_calls = []

    def get_exchange_info(self):
        return {
            'symbols': [
                {
                    'symbol': 'APTUSDT',
                    'filters': [{'filterType': 'LOT_SIZE', 'stepSize': '0.1', 'minQty': '0.1'}]
                },
                {
                    'symbol': 'BTCUSDT',
                    'filters': [{'filterType': 'LOT_SIZE', 'stepSize': '0.001', 'minQty': '0.001'}],
                },
                {
                    'symbol': 'SUIUSDT',
                    'filters': [{'filterType': 'LOT_SIZE', 'stepSize': '0.1', 'minQty': '0.1'}],
                }
            ]
        }

    def get_position_risk(self):
        """Return the current row for each symbol."""
        out = []
        for symbol, rows in self.position_rows.items():
            idx = min(self.current_idx.get(symbol, 0), len(rows) - 1)
            out.append(dict(rows[idx]))
        return out

    def get_symbol_position_risk(self, symbol, position_side=None):
        rows = self.position_rows[symbol.upper()]
        idx = min(self.current_idx.get(symbol.upper(), 0), len(rows) - 1)
        return dict(rows[idx])

    def get_open_orders(self, symbol=None):
        return list(self.plain_orders.get(symbol.upper(), []))  # type: ignore

    def get_open_algo_orders(self, symbol=None):
        return list(self.algo_orders.get(symbol.upper(), []))  # type: ignore

    def cancel_all_orders(self, symbol):
        self.cancel_all_calls.append(symbol.upper())
        # Clear orders so verify loop sees empty books
        self.plain_orders[symbol.upper()] = []
        self.algo_orders[symbol.upper()] = []
        return {'status': 'ok'}

    def cancel_plain_order(self, symbol, order_id=None, client_order_id=None):
        self.plain_orders[symbol.upper()] = []
        return {'status': 'canceled'}

    def cancel_algo_order(self, symbol, algo_id=None, client_algo_id=None):
        self.algo_orders[symbol.upper()] = []
        return {'status': 'canceled'}

    def post_plain_order(self, params):
        self.post_plain_calls.append(dict(params))
        symbol = str(params['symbol']).upper()
        # Advance position row to simulate fill
        self.current_idx[symbol] = min(
            self.current_idx.get(symbol, 0) + 1,
            len(self.position_rows[symbol]) - 1,
        )
        return {'orderId': 999, 'status': 'FILLED'}


# ---------------------------------------------------------------------------
# Test 1 — confirm_passes gate + exact qty close
# ---------------------------------------------------------------------------

def test_dust_worker_confirms_then_cleans_up_exact_qty():
    """Worker must wait for confirm_passes before acting;
    then the MARKET close must use the live positionAmt quantity."""
    fake_client = FakeClient()
    fake_redis = FakeRedis()
    worker = BinanceDustCleanupWorker(
        client=fake_client,  # type: ignore
        redis_client=fake_redis,
        confirm_passes=2,          # requires 2 consecutive dust observations
        close_retries=2,
        verify_timeout_ms=50,
        verify_poll_ms=10,
        allowlist={'APTUSDT', 'BTCUSDT'},
        cooldown_sec=0,  # disable cooldown so second sweep acts immediately
    )

    # ---- First sweep: confirmation pass 1 — must NOT place any orders ----
    first = worker.sweep_once()
    assert first['candidates'] == ['APTUSDT'], "Only APTUSDT should be a dust candidate"
    assert first['pending'] == ['APTUSDT'], "APTUSDT must be pending on first pass (needs 2 confirms)"
    assert fake_client.post_plain_calls == [], "No orders must be placed on first pass"

    # ---- Second sweep: confirmation pass 2 — must close the position ----
    second = worker.sweep_once()
    assert len(fake_client.post_plain_calls) == 1, "Exactly one MARKET order expected"
    req = fake_client.post_plain_calls[0]
    assert req['symbol'] == 'APTUSDT'
    assert req['side'] == 'SELL', "LONG dust → SELL to exit"
    # Qty must match live positionAmt (0.1)
    assert float(req['quantity']) == 0.1, f"Expected quantity=0.1, got {req['quantity']}"
    assert req['reduceOnly'] is True, "One-way mode must use reduceOnly=True"
    # Outcome stored in acted list
    assert second['acted'][0]['status'] == 'closed', second['acted'][0]
    # Event written to exec stream
    assert any(
        fields.get('event_type') == 'dust_cleanup_worker'
        for _, fields, _ in fake_redis.events
    ), "dust_cleanup_worker event must be emitted"


# ---------------------------------------------------------------------------
# Test 2 — non-dust position must never be touched
# ---------------------------------------------------------------------------

def test_dust_worker_skips_non_dust_position():
    """BTCUSDT with notional=350 is clearly not dust (> default 3 USDT threshold).
    The worker must not touch it even with confirm_passes=1."""
    fake_client = FakeClient()
    worker = BinanceDustCleanupWorker(
        client=fake_client,  # type: ignore
        redis_client=FakeRedis(),
        confirm_passes=1,
        allowlist={'BTCUSDT'},  # only BTCUSDT in scope
        cooldown_sec=0,  # cooldown irrelevant for non-dust; clear for test clarity
    )
    res = worker.sweep_once()
    assert res['candidates'] == [], "BTCUSDT with large notional is NOT dust"
    assert fake_client.post_plain_calls == [], "No orders must be placed for non-dust position"


# ---------------------------------------------------------------------------
# Test 3 — cleanup error surfaces correctly
# ---------------------------------------------------------------------------

def test_dust_worker_reports_error_when_cleanup_fails():
    """If post_plain_order raises, the acted entry must show status='error'
    and an error event must appear in the exec stream."""
    class ErrorClient(FakeClient):
        def post_plain_order(self, params):
            raise RuntimeError('submit_failed')

    fake_client = ErrorClient()
    fake_redis = FakeRedis()
    worker = BinanceDustCleanupWorker(
        client=fake_client,  # type: ignore
        redis_client=fake_redis,
        confirm_passes=1,   # act immediately on first pass
        close_retries=1,
        verify_timeout_ms=50,
        verify_poll_ms=10,
        allowlist={'APTUSDT'},
        error_cooldown_sec=0,  # disable error cooldown so it doesn't interfere
    )
    res = worker.sweep_once()
    assert res['acted'][0]['status'] == 'error', res['acted'][0]
    assert any(
        fields.get('status') == 'error'
        for _, fields, _ in fake_redis.events
    ), "error event must be written to exec stream"


# ---------------------------------------------------------------------------
# Test 4 — already_flat and self-healed position paths
# ---------------------------------------------------------------------------

def test_dust_worker_already_flat_skips_close():
    """_cleanup_symbol returns 'already_flat' (no close orders) when the initial
    _build_live_exposure call using the sweep row itself shows qty=0.

    We get that by overriding get_symbol_position_risk to return flat AND
    having get_position_risk return a dust row for the sweep scan — but then the
    initial re-read inside _cleanup_symbol calls get_open_orders (empty) and the
    qty from the passed row is 0.1... so the already_flat branch is NOT hit there.

    The REAL already_flat path is tested via a direct _cleanup_symbol call.
    This test verifies that self-healed positions (flat by time cleanup runs)
    produce 'closed' status with zero attempts and no close order.
    """

    class AlreadyFlatClient(FakeClient):
        """get_symbol_position_risk always returns a flat row for any symbol."""
        def get_symbol_position_risk(self, symbol, position_side=None):
            return {
                'symbol': symbol.upper(),
                'positionAmt': '0.0',
                'notional': '0.0',
                'isolatedMargin': '0.0',
            }

    flat_client = AlreadyFlatClient()
    # No open orders → is_flat=True on every live re-fetch
    flat_client.plain_orders['APTUSDT'] = []
    flat_client.algo_orders['APTUSDT'] = []
    worker = BinanceDustCleanupWorker(
        client=flat_client,  # type: ignore
        redis_client=FakeRedis(),
        confirm_passes=1,
        allowlist={'APTUSDT'},
    )
    res = worker.sweep_once()
    # The position self-healed: cleanup detects flat on first live re-fetch in the loop
    # and breaks without placing any close order → status 'closed' with 0 attempts.
    assert res['acted'], "APTUSDT was a dust candidate so acted must not be empty"
    act = res['acted'][0]
    assert act['status'] in {'closed', 'already_flat'}, (
        f"Expected closed or already_flat, got: {act}"
    )
    assert flat_client.post_plain_calls == [], "No close orders must be placed when already flat"


def test_cleanup_symbol_already_flat_via_direct_call():
    """_cleanup_symbol must return 'already_flat' with no attempts when the passed
    sweep row itself decodes to qty=0 (position is flat going into the cleanup call)."""
    fake_client = FakeClient()
    fake_client.plain_orders['APTUSDT'] = []
    fake_client.algo_orders['APTUSDT'] = []
    # Override so every live read also returns flat
    fake_client.position_rows['APTUSDT'] = [
        {'symbol': 'APTUSDT', 'positionAmt': '0.0', 'notional': '0.0', 'isolatedMargin': '0.0'}]
    fake_client.current_idx['APTUSDT'] = 0
    worker = BinanceDustCleanupWorker(
        client=fake_client,  # type: ignore
        redis_client=FakeRedis(),
        confirm_passes=1,
        allowlist={'APTUSDT'},
    )
    # Call _cleanup_symbol directly with a flat row — should short-circuit immediately
    flat_row = {'symbol': 'APTUSDT', 'positionAmt': '0.0', 'notional': '0.0', 'isolatedMargin': '0.0'}
    result = worker._cleanup_symbol('APTUSDT', flat_row)
    assert result['status'] == 'already_flat', f"Expected already_flat, got: {result['status']}"
    assert result['attempts'] == [], "No close attempts expected when already flat"
    assert fake_client.post_plain_calls == [], "No close orders must be placed"


# ---------------------------------------------------------------------------
# Test 5 — static + dynamic denylist
# ---------------------------------------------------------------------------

def test_dust_worker_respects_static_and_dynamic_denylist():
    """Worker must skip symbols on the static env denylist (denylist= kwarg) and on
    the dynamic Redis set (SISMEMBER orders:dust_cleanup:denylist <symbol>).
    Both APTUSDT (static) and SUIUSDT (dynamic via Redis SADD) must appear in
    skipped with skip_reason='denylist', and no close orders must be placed."""
    fake_client = FakeClient()
    fake_redis = FakeRedis()
    # Seed SUIUSDT into the dynamic Redis denylist set.
    fake_redis.sadd('orders:dust_cleanup:denylist', 'SUIUSDT')
    worker = BinanceDustCleanupWorker(
        client=fake_client,  # type: ignore
        redis_client=fake_redis,
        confirm_passes=1,
        allowlist={'APTUSDT', 'SUIUSDT'},
        denylist={'APTUSDT'},  # static denylist
        cooldown_sec=0,
    )
    res = worker.sweep_once()
    assert sorted(res['candidates']) == ['APTUSDT', 'SUIUSDT']
    assert res['acted'] == []
    reasons = sorted(item['skip_reason'] for item in res['skipped'])
    assert reasons == ['denylist', 'denylist']
    assert fake_client.post_plain_calls == []


# ---------------------------------------------------------------------------
# Test 6 — per-symbol cleanup cooldown
# ---------------------------------------------------------------------------

def test_dust_worker_applies_cleanup_cooldown_per_symbol():
    """After a successful cleanup, worker must not act on the same symbol again
    until the cooldown has expired. The second sweep must return a skipped entry
    with skip_reason='cooldown' and a positive cooldown_remaining_sec."""
    fake_client = FakeClient()
    fake_redis = FakeRedis()
    worker = BinanceDustCleanupWorker(
        client=fake_client,  # type: ignore
        redis_client=fake_redis,
        confirm_passes=1,
        allowlist={'APTUSDT'},
        cooldown_sec=60,  # 60-second cooldown after each cleanup
        verify_timeout_ms=50,
        verify_poll_ms=10,
    )
    # First sweep: APTUSDT is dust — should be cleaned up successfully.
    first = worker.sweep_once()
    assert first['acted'][0]['status'] == 'closed'
    assert len(fake_client.post_plain_calls) == 1

    # Simulate that the symbol became dusty again immediately after the close.
    fake_client.current_idx['APTUSDT'] = 0
    fake_client.plain_orders['APTUSDT'] = []
    fake_client.algo_orders['APTUSDT'] = []

    # Second sweep: cooldown is active — must skip without placing another order.
    second = worker.sweep_once()
    assert second['acted'] == []
    assert second['skipped'][0]['skip_reason'] == 'cooldown'
    assert second['skipped'][0]['cooldown_remaining_sec'] > 0
    assert len(fake_client.post_plain_calls) == 1  # no new orders


# ---------------------------------------------------------------------------
# Test 7 — _is_network_error helper classification
# ---------------------------------------------------------------------------

def test_is_network_error_detects_dns_payload():
    """_is_network_error returns True for BinanceAPIError with code=dns_resolve_failed."""
    from services.binance_dust_cleanup_worker import _is_network_error
    from services.binance_futures_client import BinanceAPIError

    dns_err = BinanceAPIError(0, {'code': 'dns_resolve_failed', 'msg': 'dns error', 'ambiguous': False})
    assert _is_network_error(dns_err) is True

    conn_err = BinanceAPIError(0, {'code': 'connection_refused', 'msg': 'refused', 'ambiguous': False})
    assert _is_network_error(conn_err) is True

    # Transport timeout is NOT a network error (it's ambiguous)
    timeout_err = BinanceAPIError(0, {'code': 'transport_timeout', 'msg': 'timed out', 'ambiguous': True})
    assert _is_network_error(timeout_err) is False

    # Regular API error is NOT a network error
    api_err = BinanceAPIError(400, {'code': -1000, 'msg': 'bad request'})
    assert _is_network_error(api_err) is False


def test_is_network_error_detects_raw_gaierror():
    """_is_network_error returns True when the exception chain includes socket.gaierror."""
    import socket as _socket

    from services.binance_dust_cleanup_worker import _is_network_error

    gaierror = _socket.gaierror(-3, 'Temporary failure in name resolution')
    assert _is_network_error(gaierror) is True

    conn_ref = ConnectionRefusedError('Connection refused')
    assert _is_network_error(conn_ref) is True

    conn_rst = ConnectionResetError('Connection reset by peer')
    assert _is_network_error(conn_rst) is True

    # Plain RuntimeError is not a network error
    assert _is_network_error(RuntimeError('some error')) is False


# ---------------------------------------------------------------------------
# Test 8 — _is_1021_error helper classification
# ---------------------------------------------------------------------------

def test_is_1021_error_detects_timestamp_drift():
    """_is_1021_error returns True for BinanceAPIError with code=-1021."""
    from services.binance_dust_cleanup_worker import _is_1021_error
    from services.binance_futures_client import BinanceAPIError

    ts_err = BinanceAPIError(400, {'code': -1021, 'msg': 'Timestamp for this request is outside of the recvWindow.'})
    assert _is_1021_error(ts_err) is True

    # Regular API error is NOT a -1021 error
    api_err = BinanceAPIError(400, {'code': -1000, 'msg': 'bad request'})
    assert _is_1021_error(api_err) is False

    # 429 rate limit is NOT a -1021 error
    rate_err = BinanceAPIError(429, {'code': -1015, 'msg': 'Too many requests'})
    assert _is_1021_error(rate_err) is False

    # Plain RuntimeError is not a -1021 error
    assert _is_1021_error(RuntimeError('some error')) is False


if __name__ == '__main__':
    import pytest
    pytest.main([__file__, '-v'])
