from enum import Enum


class VetoReason(str, Enum):
    """
    Centralized registry of all veto / fail-open reason codes across the Signal Generation Pipeline.

    Rules:
      1. Every gate MUST use a member of this enum instead of a raw string literal.
      2. Dynamic f-string codes (e.g. f"dq_{bucket}") are FORBIDDEN in veto_total() calls;
         map them to the appropriate canonical member before emitting.
      3. New codes require a PR review — cardinality in signals_veto_total is bounded by this enum.

    Using (str, Enum) guarantees JSON serializability and downstream string matching.
    """

    # ------------------------------------------------------------------
    # Absorption signals
    # ------------------------------------------------------------------
    VETO_ABS_NO_REFILL_TAG = "VETO_ABS_NO_REFILL_TAG"
    VETO_ABSORPTION_NO_WEAK_PROGRESS = "VETO_ABSORPTION_NO_WEAK_PROGRESS"
    VETO_ABSORPTION_TOUCH_RHO_LOW = "VETO_ABSORPTION_TOUCH_RHO_LOW"
    VETO_ABSORPTION_TOUCH_STALE = "VETO_ABSORPTION_TOUCH_STALE"
    VETO_ABSORPTION_TOUCH_TAG_MISMATCH = "VETO_ABSORPTION_TOUCH_TAG_MISMATCH"
    VETO_ABSORPTION_TOUCH_TRADED_W_LOW = "VETO_ABSORPTION_TOUCH_TRADED_W_LOW"
    VETO_ABSORPTION_Z_TOO_LOW = "VETO_ABSORPTION_Z_TOO_LOW"
    VETO_ABS_REFILL_RHO_LOW = "VETO_ABS_REFILL_RHO_LOW"
    VETO_ABS_TOUCH_STALE = "VETO_ABS_TOUCH_STALE"
    VETO_ABS_WEAK_PROGRESS_FALSE = "VETO_ABS_WEAK_PROGRESS_FALSE"
    VETO_ABS_Z_LOW = "VETO_ABS_Z_LOW"

    # ------------------------------------------------------------------
    # Adverse selection
    # ------------------------------------------------------------------
    VETO_ADVERSE_SELECTION = "VETO_ADVERSE_SELECTION"
    VETO_EXEC_ADVERSE_SELECTION = "VETO_EXEC_ADVERSE_SELECTION"
    VETO_ON_ADVERSE = "VETO_ON_ADVERSE"
    VETO_ON_ADVERSE_CROSS = "VETO_ON_ADVERSE_CROSS"
    VETO_TRADE_ADVERSE_CROSS = "VETO_TRADE_ADVERSE_CROSS"

    # ------------------------------------------------------------------
    # ATR / volatility
    # ------------------------------------------------------------------
    VETO_ATR_Q14_OUT_OF_RANGE = "VETO_ATR_Q14_OUT_OF_RANGE"
    VETO_ATR_STALE = "VETO_ATR_STALE"
    VETO_ATR_TS_MISSING = "VETO_ATR_TS_MISSING"
    VETO_DAILY_ATR_BPS_OUT_OF_RANGE = "VETO_DAILY_ATR_BPS_OUT_OF_RANGE"
    VETO_MISSING_ATR_Q14 = "VETO_MISSING_ATR_Q14"
    VETO_MISSING_ATR_TS = "VETO_MISSING_ATR_TS"
    VETO_MISSING_DAILY_ATR_BPS = "VETO_MISSING_DAILY_ATR_BPS"

    # ------------------------------------------------------------------
    # Book / order-book quality
    # ------------------------------------------------------------------
    VETO_BOOK_STALE = "VETO_BOOK_STALE"
    VETO_BOOK_CHURN = "VETO_BOOK_CHURN"
    VETO_BOOK_CROSS = "VETO_BOOK_CROSS"
    VETO_BOOK_NAN = "VETO_BOOK_NAN"
    VETO_BOOK_NEG_QTY = "VETO_BOOK_NEG_QTY"
    VETO_BOOK_SANITY = "VETO_BOOK_SANITY"
    VETO_BOOK_STALE_ADVERSE_CROSS = "VETO_BOOK_STALE_ADVERSE_CROSS"
    VETO_ON_STALE_BOOK = "VETO_ON_STALE_BOOK"
    VETO_L2_BAD = "VETO_L2_BAD"
    VETO_L2_MISSING = "VETO_L2_MISSING"
    VETO_L2_STALE = "VETO_L2_STALE"

    # ------------------------------------------------------------------
    # Breakout pattern signals
    # ------------------------------------------------------------------
    VETO_BREAKOUT_MICROSHIFT_LOW = "VETO_BREAKOUT_MICROSHIFT_LOW"
    VETO_BREAKOUT_MICROSHIFT_TOO_LOW = "VETO_BREAKOUT_MICROSHIFT_TOO_LOW"
    VETO_BREAKOUT_OBI20_LOW = "VETO_BREAKOUT_OBI20_LOW"
    VETO_BREAKOUT_OBI_LOW = "VETO_BREAKOUT_OBI_LOW"
    VETO_BREAKOUT_OBI_SIGN_MISMATCH = "VETO_BREAKOUT_OBI_SIGN_MISMATCH"
    VETO_BREAKOUT_OBI_TOO_WEAK = "VETO_BREAKOUT_OBI_TOO_WEAK"
    VETO_BREAKOUT_TOUCH_RHO_LOW = "VETO_BREAKOUT_TOUCH_RHO_LOW"
    VETO_BREAKOUT_TOUCH_STALE = "VETO_BREAKOUT_TOUCH_STALE"
    VETO_BREAKOUT_TOUCH_TAG_MISMATCH = "VETO_BREAKOUT_TOUCH_TAG_MISMATCH"
    VETO_BREAKOUT_TOUCH_TRADED_W_LOW = "VETO_BREAKOUT_TOUCH_TRADED_W_LOW"
    VETO_BREAKOUT_Z_LOW = "VETO_BREAKOUT_Z_LOW"
    VETO_BREAKOUT_Z_TOO_LOW = "VETO_BREAKOUT_Z_TOO_LOW"
    VETO_FOR_BREAKOUT = "VETO_FOR_BREAKOUT"

    # ------------------------------------------------------------------
    # Burst / flip
    # ------------------------------------------------------------------
    VETO_BURST_FLIP = "VETO_BURST_FLIP"
    VETO_BURST_FLIP_HIGH = "VETO_BURST_FLIP_HIGH"
    VETO_MISSING_BURST_FLIP = "VETO_MISSING_BURST_FLIP"
    VETO_RS_BURST_FLIP = "VETO_RS_BURST_FLIP"

    # ------------------------------------------------------------------
    # Cancellation spike (L3-lite)
    # ------------------------------------------------------------------
    VETO_C2T = "VETO_C2T"
    VETO_CANCEL_SPIKE = "VETO_CANCEL_SPIKE"      # canonical for cancel_gate reasons
    VETO_EXTREME_CANCEL_TO_TRADE_HIGH = "VETO_EXTREME_CANCEL_TO_TRADE_HIGH"
    VETO_L3_C2T = "VETO_L3_C2T"
    VETO_L3_SPOOF_RISK = "VETO_L3_SPOOF_RISK"
    VETO_LAYERING = "VETO_LAYERING"
    VETO_QUOTE_STUFFING = "VETO_QUOTE_STUFFING"
    VETO_OTR_SPIKE = "VETO_OTR_SPIKE"

    # ------------------------------------------------------------------
    # Confidence
    # ------------------------------------------------------------------
    VETO_CONF = "VETO_CONF"
    VETO_CONF_BELOW_MIN = "VETO_CONF_BELOW_MIN"
    VETO_CONF_FACTOR_MIN = "VETO_CONF_FACTOR_MIN"
    VETO_CONF_MIN = "VETO_CONF_MIN"
    VETO_CONFIRM = "VETO_CONFIRM"
    VETO_CONFIRMATIONS_NONE = "VETO_CONFIRMATIONS_NONE"
    VETO_NO_BLOCKING_CONFIRM = "VETO_NO_BLOCKING_CONFIRM"

    # ------------------------------------------------------------------
    # Consistency gate
    # ------------------------------------------------------------------
    VETO_CONSISTENCY = "VETO_CONSISTENCY"

    # ------------------------------------------------------------------
    # Cooldown / entry policy
    # ------------------------------------------------------------------
    VETO_COOLDOWN = "VETO_COOLDOWN"
    VETO_ENTRY_COOLDOWN_ARMED = "VETO_ENTRY_COOLDOWN_ARMED"
    VETO_ENTRY_POLICY = "VETO_ENTRY_POLICY"
    VETO_ENTRY_RECHECK_ARMED = "VETO_ENTRY_RECHECK_ARMED"
    VETO_ENTRY_RECHECK_COOLDOWN = "VETO_ENTRY_RECHECK_COOLDOWN"
    VETO_ENTRY_TS_INVALID = "VETO_ENTRY_TS_INVALID"
    VETO_SESSION_NOT_ALLOWED = "VETO_SESSION_NOT_ALLOWED"

    # ------------------------------------------------------------------
    # Cost / edge
    # ------------------------------------------------------------------
    VETO_BPS = "VETO_BPS"
    VETO_COST = "VETO_COST"
    VETO_COST_BAD_INPUT = "VETO_COST_BAD_INPUT"
    VETO_COST_LT_REQUIRED = "VETO_COST_LT_REQUIRED"
    VETO_COST_NO_EDGE = "VETO_COST_NO_EDGE"
    VETO_EDGE_COST = "VETO_EDGE_COST"
    VETO_EDGE_COST_ERROR = "VETO_EDGE_COST_ERROR"
    VETO_EDGE_COST_MISSING_LEVELS = "VETO_EDGE_COST_MISSING_LEVELS"
    VETO_EDGE_COST_PRECONDITION = "VETO_EDGE_COST_PRECONDITION"
    VETO_EDGE_COST_UNKNOWN = "VETO_EDGE_COST_UNKNOWN"
    VETO_EDGE_THIN_COST = "VETO_EDGE_THIN_COST"
    VETO_EDGE_TOO_SMALL = "VETO_EDGE_TOO_SMALL"
    VETO_IMPL_SHORTFALL_P95 = "VETO_IMPL_SHORTFALL_P95"
    VETO_INLINE_IMPL_SHORTFALL_P95 = "VETO_INLINE_IMPL_SHORTFALL_P95"
    VETO_WITHOUT_TCA = "VETO_WITHOUT_TCA"

    # ------------------------------------------------------------------
    # Countertrend
    # ------------------------------------------------------------------
    VETO_COUNTERTREND = "VETO_COUNTERTREND"
    VETO_SMT_COUNTERTREND = "VETO_SMT_COUNTERTREND"
    VETO_SMT_LEADER_CT = "VETO_SMT_LEADER_CT"

    # ------------------------------------------------------------------
    # Data quality / timestamps
    # ------------------------------------------------------------------
    VETO_BAD_NUMERIC = "VETO_BAD_NUMERIC"
    VETO_BAD_TS_NOT_EPOCH = "VETO_BAD_TS_NOT_EPOCH"
    VETO_DATA_FLAGS = "VETO_DATA_FLAGS"
    VETO_DQ_BUCKET = "VETO_DQ_BUCKET"   # canonical for dq_{bucket} dynamic codes
    VETO_EVENT_LAG = "VETO_EVENT_LAG"
    VETO_FLAGS = "VETO_FLAGS"
    VETO_FUTURE_TS = "VETO_FUTURE_TS"
    VETO_NON_EPOCH_TS = "VETO_NON_EPOCH_TS"
    VETO_ON_SCHEMA_CHANGE = "VETO_ON_SCHEMA_CHANGE"
    VETO_OUT_OF_ORDER = "VETO_OUT_OF_ORDER"
    VETO_PRECISION = "VETO_PRECISION"
    VETO_QUALITY = "VETO_QUALITY"
    VETO_QUALITY_FLAG = "VETO_QUALITY_FLAG"
    VETO_SCHEMA_DRIFT = "VETO_SCHEMA_DRIFT"
    VETO_TIME_QUARANTINE = "VETO_TIME_QUARANTINE"

    # ------------------------------------------------------------------
    # Depth
    # ------------------------------------------------------------------
    VETO_DEPTH = "VETO_DEPTH"
    VETO_DEPTH_TOO_LOW = "VETO_DEPTH_TOO_LOW"
    VETO_MISSING_DEPTH = "VETO_MISSING_DEPTH"

    # ------------------------------------------------------------------
    # Duplicate / rate
    # ------------------------------------------------------------------
    VETO_DUP_RATE = "VETO_DUP_RATE"
    VETO_RATE = "VETO_RATE"

    # ------------------------------------------------------------------
    # Execution health / sizing
    # ------------------------------------------------------------------
    VETO_EXEC_HEALTH = "VETO_EXEC_HEALTH"
    VETO_EXEC_HEALTH_AUTO_FREEZE = "VETO_EXEC_HEALTH_AUTO_FREEZE"
    VETO_SIZING = "VETO_SIZING"
    VETO_AFTER_TP1 = "VETO_AFTER_TP1"

    # ------------------------------------------------------------------
    # Feature drift
    # ------------------------------------------------------------------
    VETO_FEATURE_DRIFT = "VETO_FEATURE_DRIFT"

    # ------------------------------------------------------------------
    # Flow / taker toxicity
    # ------------------------------------------------------------------
    VETO_FLOW_TOXIC = "VETO_FLOW_TOXIC"
    VETO_STREAM_INTEGRITY = "VETO_STREAM_INTEGRITY"
    VETO_SEQ_GAP_RATE = "VETO_SEQ_GAP_RATE"
    VETO_SEQ_GAP_WINDOW = "VETO_SEQ_GAP_WINDOW"
    VETO_TAKER = "VETO_TAKER"
    VETO_TAKER_RATE_LOW = "VETO_TAKER_RATE_LOW"
    VETO_TRADE_OUTSIDE_BBO = "VETO_TRADE_OUTSIDE_BBO"

    # ------------------------------------------------------------------
    # Internal / generic
    # ------------------------------------------------------------------
    VETO_ENABLE = "VETO_ENABLE"
    VETO_ENABLED = "VETO_ENABLED"
    VETO_GENERIC = "VETO_GENERIC"
    VETO_HITS = "VETO_HITS"
    VETO_IF_NO_STATS = "VETO_IF_NO_STATS"
    VETO_IF_NOT_SUSTAINED = "VETO_IF_NOT_SUSTAINED"
    VETO_INTERNAL_ERROR = "VETO_INTERNAL_ERROR"
    VETO_KINDS = "VETO_KINDS"
    VETO_LEVELS = "VETO_LEVELS"
    VETO_LEVELS_ERROR = "VETO_LEVELS_ERROR"
    VETO_MIN = "VETO_MIN"
    VETO_REQUIRE_BOTH_IS_AND_IMPACT = "VETO_REQUIRE_BOTH_IS_AND_IMPACT"
    VETO_TOTAL = "VETO_TOTAL"
    VETO_UNKNOWN = "VETO_UNKNOWN"

    # ------------------------------------------------------------------
    # Liquidation map gate  (P15)
    # ------------------------------------------------------------------
    VETO_LIQMAP_RR = "VETO_LIQMAP_RR"   # canonical for liqmap_{reason} dynamic codes

    # ------------------------------------------------------------------
    # Missing features
    # ------------------------------------------------------------------
    VETO_MISSING_MICROSHIFT = "VETO_MISSING_MICROSHIFT"
    VETO_MISSING_OBI = "VETO_MISSING_OBI"
    VETO_MISSING_OBI20 = "VETO_MISSING_OBI20"
    VETO_MISSING_OBI_SUSTAINED = "VETO_MISSING_OBI_SUSTAINED"
    VETO_MISSING_REGIME = "VETO_MISSING_REGIME"
    VETO_MISSING_SPREAD = "VETO_MISSING_SPREAD"
    VETO_MISSING_TOUCH_RHO = "VETO_MISSING_TOUCH_RHO"
    VETO_MISSING_TOUCH_STALE = "VETO_MISSING_TOUCH_STALE"
    VETO_MISSING_TOUCH_TAG = "VETO_MISSING_TOUCH_TAG"
    VETO_MISSING_TOUCH_TRADED_W = "VETO_MISSING_TOUCH_TRADED_W"
    VETO_MISSING_WEAK_PROGRESS = "VETO_MISSING_WEAK_PROGRESS"
    VETO_MISSING_Z_DELTA = "VETO_MISSING_Z_DELTA"

    # ------------------------------------------------------------------
    # Market pressure (pressure gate)
    # ------------------------------------------------------------------
    VETO_MP_CONTRA = "VETO_MP_CONTRA"
    # Canonical replacements for PRESSURE_VETO_* raw strings:
    VETO_PRESSURE_SPREAD_Z = "VETO_PRESSURE_SPREAD_Z"       # was PRESSURE_VETO_SPREAD_Z
    VETO_PRESSURE_BOOK_STALE = "VETO_PRESSURE_BOOK_STALE"   # was PRESSURE_VETO_BOOK_STALE
    VETO_PRESSURE_BOOK_CHURN = "VETO_PRESSURE_BOOK_CHURN"   # was PRESSURE_VETO_BOOK_CHURN

    # ------------------------------------------------------------------
    # News gate  (P16)
    # ------------------------------------------------------------------
    VETO_NEWS_RECO_HARD = "VETO_NEWS_RECO_HARD"  # canonical for "news_reco_hard"
    VETO_SMT_NEWS_GATE = "VETO_SMT_NEWS_GATE"

    # ------------------------------------------------------------------
    # OBI / OFI / microstructure
    # ------------------------------------------------------------------
    VETO_EXTREME_Z_LOW = "VETO_EXTREME_Z_LOW"
    VETO_MISSING_OBI_SUSTAINED_alias = "VETO_OBI_SPIKE_NOT_SUSTAINED"  # preferred alias
    VETO_OBI_SPIKE_NOT_SUSTAINED = "VETO_OBI_SPIKE_NOT_SUSTAINED"
    VETO_OBI_SPIKE_WEAK = "VETO_OBI_SPIKE_WEAK"

    # ------------------------------------------------------------------
    # Regime
    # ------------------------------------------------------------------
    VETO_REGIME = "VETO_REGIME"
    VETO_REGIME_BREAKOUT_BLOCK = "VETO_REGIME_BREAKOUT_BLOCK"
    VETO_REGIME_EXTREME_BLOCK = "VETO_REGIME_EXTREME_BLOCK"
    VETO_REGIME_NOT_ALLOWED = "VETO_REGIME_NOT_ALLOWED"
    VETO_REGIME_RANGE_BREAKOUT = "VETO_REGIME_RANGE_BREAKOUT"
    VETO_RS_DENY_RULE = "VETO_RS_DENY_RULE"
    VETO_RS_DEPTH = "VETO_RS_DEPTH"
    VETO_RS_DEPTH20 = "VETO_RS_DEPTH20"
    VETO_RS_REGIME_NOT_ALLOWED = "VETO_RS_REGIME_NOT_ALLOWED"
    VETO_RS_SPREAD = "VETO_RS_SPREAD"

    # ------------------------------------------------------------------
    # SMT
    # ------------------------------------------------------------------
    VETO_SMT = "VETO_SMT"
    VETO_SMT_NA_SIDE = "VETO_SMT_NA_SIDE"

    # ------------------------------------------------------------------
    # Spread
    # ------------------------------------------------------------------
    VETO_SPREAD = "VETO_SPREAD"
    VETO_SPREAD_SHOCK = "VETO_SPREAD_SHOCK"
    VETO_SPREAD_TOO_WIDE = "VETO_SPREAD_TOO_WIDE"
    VETO_SPREAD_WIDE = "VETO_SPREAD_WIDE"
    VETO_SPREAD_Z = "VETO_SPREAD_Z"
    VETO_MISSING_SPREAD_alias = "VETO_SPREAD_MISSING"  # alias if needed

    # ------------------------------------------------------------------
    # Statistics / stats guard
    # ------------------------------------------------------------------
    VETO_NO_STATS = "VETO_NO_STATS"
    VETO_NO_WALL_OR_REFILL = "VETO_NO_WALL_OR_REFILL"
    VETO_TOUCH_STALE = "VETO_TOUCH_STALE"
    VETO_TOUCH_SUPPRESSED = "VETO_TOUCH_SUPPRESSED"
    VETO_TP1_TOO_CLOSE = "VETO_TP1_TOO_CLOSE"
    VETO_WALL_NEAR = "VETO_WALL_NEAR"

    # ------------------------------------------------------------------
    # Fail-open sentinel codes (not real vetoes, used for DQ telemetry)
    # All fail-open codes are PASS-through; gate did not block but
    # signals_veto_total must record them with kind="fail_open".
    # ------------------------------------------------------------------
    FAIL_OPEN_QUALITY = "FAIL_OPEN_QUALITY"
    FAIL_OPEN_CONSISTENCY = "FAIL_OPEN_CONSISTENCY"
    FAIL_OPEN_EDGE_COST = "FAIL_OPEN_EDGE_COST"
    FAIL_OPEN_SMT = "FAIL_OPEN_SMT"

    def __str__(self) -> str:
        return self.value

    @property
    def is_veto(self) -> bool:
        """Returns True if this code represents an actual blocking veto."""
        return self.value.startswith("VETO_")

    @property
    def is_fail_open(self) -> bool:
        """Returns True if this code is a pass-through DQ telemetry code."""
        return self.value.startswith("FAIL_OPEN_")

    @classmethod
    def is_veto_code(cls, code: str) -> bool:
        """Helper to safely check if a raw string code is a blocking veto."""
        return bool(code and code.startswith("VETO_"))

    @classmethod
    def is_fail_open_code(cls, code: str) -> bool:
        """Helper to safely check if a raw string code is a fail-open sentinel."""
        return bool(code and code.startswith("FAIL_OPEN_"))

    @classmethod
    def is_registered(cls, code: str) -> bool:
        """Return True if `code` is a registered member value."""
        return code in cls._value2member_map_  # type: ignore[attr-defined]

    @classmethod
    def normalize(cls, code: str, fallback: "VetoReason" = None) -> "VetoReason":
        """
        Return the enum member for `code`, or `fallback` (default VETO_UNKNOWN).
        Use this in veto_total() call-sites to enforce registry membership.
        """
        if fallback is None:
            fallback = cls.VETO_UNKNOWN
        try:
            return cls(code)
        except ValueError:
            return fallback
