from __future__ import annotations

from services.crypto_orderflow_service import CryptoOrderflowService


def test_parse_book_adds_ts_ms():
    # mimic payload structure
    payload = {"ts": 1700000000000, "bids": [[1,2]], "asks": [[1,2]]}

    # We can use the static method logic or instantiate service if needed.
    # Since _parse_book_payload is an instance method that doesn't use self much (except config which it doesn't use here), we can try to call it.

    # However, CryptoOrderflowService init requires redis stuff.
    # Let's mock or just replicate the logic if we want to test usage, OR clearer:
    # sub-class it or mock it.
    # Actually, the method is "almost" static.

    class MockService(CryptoOrderflowService):
        def __init__(self):
            pass

    service = MockService()
    book = service._parse_book_payload(payload, "BTCUSDT")

    assert "ts_ms" in book
    assert book["ts_ms"] == payload["ts"]
    assert book["ts"] == payload["ts"]

    # Test with event_time
    payload2 = {"event_time": 1700000000123, "bids": [], "asks": []}
    book2 = service._parse_book_payload(payload2, "ETHUSDT")
    assert book2["ts_ms"] == 1700000000123
