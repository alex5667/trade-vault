from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field, model_validator

from common.enums.trading import Direction, Side


class ContractBase(BaseModel):
    model_config = ConfigDict(extra="ignore", protected_namespaces=())

class ExecutionContractBase(BaseModel):
    """Strict base for execution-path contracts — rejects unknown fields."""
    model_config = ConfigDict(extra="forbid", protected_namespaces=())

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
    tp1_price: float | None = None
    tp_levels: list[float] = Field(default_factory=list)

    confidence: float = 0.0
    ts_event_ms: int
    ts_publish_ms: int

    ok: int = 0
    ok_soft: int = 0
    reason: str = ""
    gate_bits: int = 0

    meta: dict[str, Any] = Field(default_factory=dict)
    indicators: dict[str, Any] = Field(default_factory=dict)
    evidence: dict[str, Any] = Field(default_factory=dict)

class OrderIntentV1(ExecutionContractBase):
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
    client_order_id: str | None = None
    side_int: int = 0
    meta: dict[str, Any] = Field(default_factory=dict)

class ExecutionEventV1(ExecutionContractBase):
    exec_id: str
    order_id: str
    client_order_id: str | None = None
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
    meta: dict[str, Any] = Field(default_factory=dict)

# ---------------------------------------------------------------------------
# OF Input contracts (feature-map variant — generic ML/analytics use only).
# Canonical replay contracts live in core/of_inputs_contract.py.
# ---------------------------------------------------------------------------

class OFInputsFeatureMapV1(ContractBase):
    """Generic OF feature-map payload for ML/analytics consumers."""
    ts_ms: int
    symbol: str
    features: dict[str, float]

class OFInputsFeatureMapV2(OFInputsFeatureMapV1):
    session_asia: int = 0
    session_eu: int = 0
    session_us: int = 0
    session_off: int = 0
    regime: str | None = None

# Backward-compat aliases (deprecated — use OFInputsFeatureMapV1/V2 in new code)
OFInputsV1 = OFInputsFeatureMapV1
OFInputsV2 = OFInputsFeatureMapV2

# ---------------------------------------------------------------------------
# OF Confirm ML score contract.
# Canonical gate-bits contract lives in core/of_confirm_contract.py.
# ---------------------------------------------------------------------------

class OFConfirmMlScoreV1(ContractBase):
    """ML-score output payload from the confirm gate (p_edge, p_win, EV)."""
    signal_id: str
    ok: int
    ok_soft: int
    p_edge: float
    p_win: float
    expected_value: float
    model_version: str
    threshold_used: float

# Backward-compat alias (deprecated — use OFConfirmMlScoreV1 in new code)
OFConfirmV3 = OFConfirmMlScoreV1

# ---------------------------------------------------------------------------
# Telegram notification contract.
# ---------------------------------------------------------------------------

class TelegramNotificationV1(ContractBase):
    ts_ms: int
    level: str = "INFO"
    topic: str = "general"
    text: str                           # canonical field (bot reads this)
    message: str | None = None      # legacy alias — mapped to text if text absent
    meta: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="before")
    @classmethod
    def _map_legacy_message(cls, values: Any) -> Any:
        if isinstance(values, dict):
            if not values.get("text") and values.get("message"):
                values = dict(values)
                values["text"] = values["message"]
        return values
