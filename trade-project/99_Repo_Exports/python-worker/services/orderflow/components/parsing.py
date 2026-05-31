from collections.abc import Callable
from typing import Any

from services.orderflow.utils import _parse_book_payload as _utils_parse_book_payload
from services.orderflow.utils import _parse_tick_payload as _utils_parse_tick_payload


class OrderFlowParsing:
    """
    Component responsible for parsing raw Redis messages into typed payloads.
    Extracted from OrderFlowStrategy to improve separation of concerns.
    """

    @staticmethod
    def parse_tick_payload(raw_data: Any, default_symbol="", log: Callable | None = None) -> dict[str, Any] | None:
        """
        Wrapper around utils._parse_tick_payload to maintain strategy interface if needed,
        or we can use the utils one directly. keeping this for compatibility/extension.
        """
        return _utils_parse_tick_payload(raw_data, default_symbol=default_symbol, log=log)

    @staticmethod
    def parse_book_payload(raw_data: Any, symbol: str) -> dict[str, Any] | None:
        """
        Wrapper around utils._parse_book_payload.
        """
        return _utils_parse_book_payload(raw_data, symbol)
