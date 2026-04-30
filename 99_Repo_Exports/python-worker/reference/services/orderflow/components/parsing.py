from typing import Any, Dict, Optional
from services.orderflow.utils import _parse_tick_payload as _utils_parse_tick_payload
from services.orderflow.utils import _parse_book_payload as _utils_parse_book_payload

class OrderFlowParsing:
    """
    Component responsible for parsing raw Redis messages into typed payloads.
    Extracted from OrderFlowStrategy to improve separation of concerns.
    """
    
    @staticmethod
    def parse_tick_payload(raw_data: Any) -> Optional[Dict[str, Any]]:
        """
        Wrapper around utils._parse_tick_payload to maintain strategy interface if needed
        or we can use the utils one directly. keeping this for compatibility/extension.
        """
        return _utils_parse_tick_payload(raw_data)

    @staticmethod
    def parse_book_payload(raw_data: Any) -> Optional[Dict[str, Any]]:
        """
        Wrapper around utils._parse_book_payload.
        """
        return _utils_parse_book_payload(raw_data)
