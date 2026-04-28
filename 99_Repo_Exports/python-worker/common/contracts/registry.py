from __future__ import annotations
from pydantic import BaseModel, Field, ConfigDict
from typing import List, Dict, Optional, Any, Union
from common.enums.trading import Direction, Side

class ContractBase(BaseModel):
    model_config = ConfigDict(extra="ignore")

class SignalV1(ContractBase):
    schema_version: str = "v1"
    signal_id: str
    id_algo: str = "v1"

    symbol: str
    venue: str = "binance_usdm"
    source_service: str = "crypto_orderflow"
    strategy: str = "orderflow"

    kind: str = "crypto-of"
    scenario: str = "unknown"
    direction: Direction
    side: Side
    side_int: int = 0

    entry_price: float
    sl_price: float
    tp1_price: Optional[float] = None
    tp_levels: List[float] = Field(default_factory=list)

    confidence: float = 0.0
    ts_event_ms: int
    ts_publish_ms: int

    ok: int = 0
    ok_soft: int = 0
    reason: str = ""
    gate_bits: int = 0

    meta: Dict[str, Any] = Field(default_factory=dict)
    indicators: Dict[str, Any] = Field(default_factory=dict)
    evidence: Dict[str, Any] = Field(default_factory=dict)

class OrderIntentV1(ContractBase):
    intent_id: str
    signal_id: str
    symbol: str
    ts_ms: int
    side: Side
    order_type: str = "LIMIT"
    price: float
    qty: float
    leverage: int = 1
    reduce_only: bool = False
    client_order_id: Optional[str] = None
    side_int: int = 0
    meta: Dict[str, Any] = Field(default_factory=dict)

class ExecutionEventV1(BaseModel):
    model_config = ConfigDict(extra="allow")

    exec_id: str
    order_id: str
    client_order_id: Optional[str] = None
    symbol: str
    ts_ms: int
    side: Side
    price: float
    qty: float
    fee: float = 0.0
    fee_asset: str = "USDT"
    realized_pnl: float = 0.0
    status: str = "FILLED"
    side_int: int = 0
    meta: Dict[str, Any] = Field(default_factory=dict)

class OFInputsV1(ContractBase):
    ts_ms: int
    symbol: str
    features: Dict[str, float]

class OFInputsV2(OFInputsV1):
    session_asia: int = 0
    session_eu: int = 0
    session_us: int = 0
    session_off: int = 0
    regime: Optional[str] = None

class OFConfirmV3(ContractBase):
    signal_id: str
    ok: int
    ok_soft: int
    p_edge: float
    p_win: float
    expected_value: float
    model_version: str
    threshold_used: float

class TelegramNotificationV1(ContractBase):
    ts_ms: int
    level: str = "INFO"
    topic: str = "general"
    message: str
    meta: Dict[str, Any] = Field(default_factory=dict)
