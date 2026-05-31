from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Literal

_models_logger = logging.getLogger(__name__)

Side = Literal["LONG", "SHORT"]

# #14: Canonical schema versions — cross-validate on any startup/test boundary.
# Bump these when the payload contract changes and update consumers accordingly.
EXPECTED_SCHEMA_VERSIONS: dict[str, int] = {
    "SignalNorm": 1,
    "TradeClosed": 2,
}


def cross_validate_schema_versions() -> list[str]:
    """Return list of mismatches between declared and expected schema versions.

    Call at service startup or in tests:
        assert not cross_validate_schema_versions(), ...
    """
    issues: list[str] = []
    # Deferred import to avoid circular import at module level
    sn_ver = SignalNorm.__dataclass_fields__["schema_version"].default  # type: ignore[attr-defined]
    tc_ver = TradeClosed.__dataclass_fields__["schema_version"].default  # type: ignore[attr-defined]
    if sn_ver != EXPECTED_SCHEMA_VERSIONS["SignalNorm"]:
        issues.append(f"SignalNorm.schema_version={sn_ver} != expected {EXPECTED_SCHEMA_VERSIONS['SignalNorm']}")
    if tc_ver != EXPECTED_SCHEMA_VERSIONS["TradeClosed"]:
        issues.append(f"TradeClosed.schema_version={tc_ver} != expected {EXPECTED_SCHEMA_VERSIONS['TradeClosed']}")
    if issues:
        _models_logger.warning("schema_version cross-validation failures: %s", issues)
    return issues

@dataclass(slots=True)
class SignalNorm:
    sid: str
    strategy: str
    source: str
    symbol: str
    tf: str
    direction: Side
    entry_price: float
    entry_ts_ms: int
    lot: float
    qty: float
    quantity: float
    sl: float
    tp_levels: list[float]
    trail_profile: str = ""
    payload: dict[str, Any] = field(default_factory=dict)
    payload_schema_version: int = 1
    entry_tag: str = ""
    schema_version: int = 1

    # ── OutboxEnvelope contract fields ────────────────────────────────────────
    # Optional: populated by the envelope builder; kept here so SignalNorm
    # fully carries the observability contract and callsites don't need to
    # reconstruct these from separate state.
    event_id: str | None = None        # UUID4 assigned at signal origination
    ingest_time_ms: int | None = None  # wall-clock ms when signal entered the pipeline
    trace_id: str | None = None        # propagated from tick/kline source if available
    quality_flags: int | None = None   # bitmask: 0 = fully clean

    def __post_init__(self) -> None:  # #13: qty/quantity consistency guard
        """Sync qty ↔ quantity and warn on divergence.

        If one is zero and the other isn't, sync the zero one.
        If both non-zero and diverge by more than 1e-9, log a warning.
        This does NOT raise — production path is fail-open.
        """
        try:
            q, qu = float(self.qty or 0.0), float(self.quantity or 0.0)
            if q == 0.0 and qu != 0.0:
                object.__setattr__(self, "qty", qu)
            elif qu == 0.0 and q != 0.0:
                object.__setattr__(self, "quantity", q)
            elif q != 0.0 and qu != 0.0 and abs(q - qu) > 1e-9:
                _models_logger.warning(
                    "SignalNorm qty/quantity diverge: qty=%.9f quantity=%.9f sid=%s",
                    q, qu, self.sid,
                )
        except Exception:
            pass


@dataclass(slots=True)
class Tick:
    symbol: str
    ts_ms: int
    bid: float = 0.0
    ask: float = 0.0
    last: float = 0.0
    price: float = 0.0  # криптовалюта
    mid: float = 0.0    # расчетная (mid)


@dataclass(slots=True)
class TradeEvent:
    event_type: str  # OPEN, TP_HIT, SL_HIT, TRAILING_MOVE, CLOSE
    order_id: str
    sid: str
    strategy: str
    source: str
    symbol: str
    tf: str
    direction: Side
    ts_ms: int
    v: int = 1
    payload: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class TradeClosed:
    schema_version: int = 2

    # --- identity / slicing ---
    trade_id: str | None = None
    signal_id: str | None = None
    symbol: str | None = None
    regime: str | None = None
    session: str | None = None
    scenario: str | None = None          # reversal / continuation
    entry_reason: str | None = None

    # --- execution ---
    side: str | None = None              # LONG/SHORT
    qty: float | None = None
    quantity: float | None = None
    entry_px: float | None = None
    exit_px: float | None = None

    def __post_init__(self) -> None:  # #13: qty/quantity consistency guard
        """Sync qty ↔ quantity and warn on divergence.

        If one is None/zero and the other has a value, copy it over.
        If both non-zero and diverge by more than 1e-9, log a warning.
        Fail-open: never raises.
        """
        try:
            q = self.qty
            qu = self.quantity
            q_val = float(q) if q is not None else 0.0
            qu_val = float(qu) if qu is not None else 0.0
            if q is None and qu is not None:
                object.__setattr__(self, "qty", qu)
            elif qu is None and q is not None:
                object.__setattr__(self, "quantity", q)
            elif q_val == 0.0 and qu_val != 0.0:
                object.__setattr__(self, "qty", qu)
            elif qu_val == 0.0 and q_val != 0.0:
                object.__setattr__(self, "quantity", q)
            elif q_val != 0.0 and qu_val != 0.0 and abs(q_val - qu_val) > 1e-9:
                _models_logger.warning(
                    "TradeClosed qty/quantity diverge: qty=%.9f quantity=%.9f trade_id=%s",
                    q_val, qu_val, self.trade_id,
                )
        except Exception:
            pass

    fees_usd: float | None = None
    spread_bps_at_entry: float | None = None
    slippage_bps_est: float | None = None
    book_age_ms: int | None = None

    # --- excursions / timing ---
    mae_bps: float | None = None
    mfe_bps: float | None = None
    time_to_mfe_ms: int | None = None
    hold_ms: int | None = None

    # features snapshot (trimmed)
    features: dict[str, Any] | None = None

    # existing identity / legacy compatibility
    order_id: str = ""
    sid: str = ""

    # existing dims
    strategy: str = ""
    source: str = ""
    tf: str = ""
    direction: Side = "LONG"

    # times/prices
    entry_ts_ms: int = 0
    exit_ts_ms: int = 0
    # signal emit time (ts_emit_ms from signal_payload); entry fill latency = fill_ts_ms - entry_ts_ms
    fill_ts_ms: int = 0
    signal_ts_ms: int = 0        # alias: signal event time (= entry_ts_ms)
    adverse_ms_first_touch: int = 0  # ms from entry_ts_ms to first adverse price move
    entry_price: float = 0.0
    exit_price: float = 0.0
    lot: float = 0.0
    notional_usd: float = 0.0

    # User Req 4.3: Turnover variants
    turnover_entry: float = 0.0
    turnover_roundtrip: float = 0.0

    # pnl
    pnl_net: float = 0.0
    pnl_gross: float = 0.0
    fees: float = 0.0
    pnl_pct: float = 0.0

    # execution path
    tp1_hit: bool = False
    tp2_hit: bool = False
    tp3_hit: bool = False
    tp_hits: int = 0
    tp_before_sl: int = 0

    trailing_started: bool = False
    trailing_active: bool = False
    trailing_moves: int = 0

    # excursions
    mfe_pnl: float = 0.0
    mae_pnl: float = 0.0
    giveback: float = 0.0
    missed_profit: float = 0.0
    one_r_money: float = 0.0
    r_multiple: float = 0.0
    risk_usd: float = 0.0
    r_mult: float = 0.0
    p0_slippage_bps_est: float = 0.0

    # analytics/execution (realized)
    realized_slippage_bps: float = 0.0
    realized_spread_bps: float = 0.0

    # p41/p0 enrichment
    tp1_hit_ts_ms: int = 0
    mfe_pnl_at_tp1: float = 0.0
    mae_pnl_before_tp1: float = 0.0
    mfe_price_at_tp1: float = 0.0
    mfe_ts_at_tp1: int = 0
    mae_price_before_tp1: float = 0.0
    mae_ts_before_tp1: int = 0
    ab_arm: str = "A"
    entry_regime: str = ""

    # nosl_after_tp1 analytics
    nosl_after_tp1_applicable: int = 0
    sl_after_tp1_elapsed_ms: int = 0
    sl_within_tp1_t500: int = 0
    nosl_after_tp1_t500: int = 0
    sl_within_tp1_t2000: int = 0
    nosl_after_tp1_t2000: int = 0

    duration_ms: int = 0
    pnl_if_fixed_exit: float = 0.0
    pnl_net_baseline: float = 0.0 # explicit alias for clarity
    mgmt_edge: float = 0.0        # pnl_net - pnl_net_baseline

    # reasons (both!)
    close_reason: str = ""       # нормализованная причина: TP1/TP2/TP3/SL/TRAILING_STOP
    close_reason_raw: str = ""   # сырая причина: SL_AFTER_TP2 и т.д.
    close_reason_detail: str = ""  # например TRAILING_PROFIT / TRAILING_STOP / ""
    baseline_exit_reason: str = ""
    baseline_exit_ts_ms: int = 0
    baseline_exit_price: float = 0.0
    entry_tag: str = ""

    # helpful debug
    max_favorable_price: float = 0.0
    max_favorable_ts: int = 0

    is_final_close: bool = True
    remaining_qty: float = 0.0
    status: str = "CLOSED"
    trailing_profile: str = ""  # профиль трейлинга (rocket_v1 / ...)
    trailing_min_lock_r: float = 0.0  # минимальная фиксация в R (например 0.25)
    min_lock_price: float = 0.0  # уровень цены, ниже которого SL не опускаем (для LONG выше; для SHORT ниже)

    # explicit events (auditable boolean flags)
    tp1_touched: bool = False
    tp2_touched: bool = False
    tp3_touched: bool = False
    trail_armed: bool = False
    sl_moved_to_be: bool = False
    lock_r: float = 0.0

    # -------------------------------------------------------------------------
    # NEW: ATR and levels persistence (for metrics/reporting)
    # -------------------------------------------------------------------------
    atr: float = 0.0
    sl: float = 0.0
    tp_levels: list[float] = field(default_factory=list)
    tp1_price: float = 0.0
    signal_payload: dict[str, Any] = field(default_factory=dict)

    # Signal & Shadow Analytics
    is_virtual: bool = False
    v_gate_status: str = "na"  # na / passed / failed
    v_gate_reason: str = ""

    # -------------------------------------------------------------------------
    # Phase 3.1: runtime adoption
    # -------------------------------------------------------------------------
    live_surface_applied: bool = False
    live_surface_reason_code: str = ""
    baseline_sl_price: float = 0.0
    baseline_tp1_price: float = 0.0
    selected_sl_price: float = 0.0
    selected_tp1_price: float = 0.0
    live_surface_policy_level: str = ""
    trailing_policy_level: str = ""

    # -------------------------------------------------------------------------
    # Phase 5: runtime policy provenance
    # -------------------------------------------------------------------------
    atr_policy_ver: int = 0
    atr_policy_tag: str = ""
    atr_policy_source: str = ""
    atr_policy_scenario: str = ""
    atr_policy_regime: str = ""
    atr_policy_bucket: str = ""
    atr_stop_ttl_mode: str = ""
    atr_trailing_mode: str = ""
    atr_recovery_run_id: str = ""
    atr_restore_cert_id: str = ""
    atr_restore_cert_status: str = ""
    atr_policy_snapshot_json: dict[str, Any] = field(default_factory=dict)

    # Phase 5: ATR selection metadata for post-trade analytics
    atr_sel_tf: str = ""
    atr_sel_src: str = ""
    atr_sel_age_ms: int = 0

    # -------------------------------------------------------------------------
    # Horizon-aware ATR scalars (stamped from PositionState by
    # stamp_closed_trade_horizon_from_position). Without these slots on the
    # slots=True dataclass, setattr silently fails.
    # -------------------------------------------------------------------------
    contract_ver: int = 0
    hold_target_ms: int = 0
    alpha_half_life_ms: int = 0
    max_signal_age_ms: int = 0
    risk_horizon_bucket: str = ""
    horizon_profile_source: str = ""
    horizon_profile_conf: float = 0.0
    horizon_reason_code: str = ""
    atr_mode: str = ""
    atr_value: float = 0.0
    atr_tf_ms: int = 0
    atr_window_n: int = 0
    atr_age_ms: int = 0
    atr_source: str = ""
    atr_pct: float = 0.0
    vol_ratio_fast_slow: float = 1.0
    vol_ratio_z: float = 0.0
    atr_regime_value: float = 0.0
    atr_trail_value: float = 0.0
    atr_regime_tf_ms: int = 0
    atr_trail_tf_ms: int = 0

    # Trailing surface A/B (set in trade_monitor close path)
    trailing_surface_applied: bool = False
    trailing_surface_reason_code: str = ""
    baseline_trailing_offset_atr: float = 0.0
    selected_trailing_offset_atr: float = 0.0

    # P41 Native Meta Fields
    meta_enforce_cov_bucket: str = ""
    meta_enforce_applied: int = -1
    meta_enforce_key: str = ""
    meta_enforce_salt: str = ""
    meta_veto: int = 0

    # EdgeCostGate directional p_min bias provenance (P0 fix 2026-05-30).
    # Set by EdgeCostGate._apply_directional_bias when bias > 0 so the
    # edge_directional_bias_autocal_v1 service can split baseline (bias=0)
    # from applied (bias>0) buckets on the realized R distribution.
    edge_directional_bias_value: float = 0.0
    edge_directional_bias_countertrend: bool = False
    edge_directional_bias_source: str = "none"  # "none" | "env" | "autocal"

    # telemetry/health
    _health_snapshot: dict[str, Any] | None = None

    # dynamic enrichment
    kind: str = ""
    venue: str = ""
    confidence: float = 0.0

    # ── Orphan cleanup metadata ────────────────────────────────────────────
    # is_orphan_cleanup=True → position removed by housekeep, not by executor fill.
    # exclude_from_ml_labels=True → exclude from ML training/evaluation datasets.
    is_orphan_cleanup: bool = False
    exclude_from_ml_labels: bool = False

    # ── Max-hold timeout close metadata ───────────────────────────────────
    timeout_age_ms: int = 0
    timeout_max_hold_ms: int = 0
    timeout_request_ts_ms: int = 0
    timeout_close_latency_ms: int = 0
    exit_order_ref: str = ""
    closed_trade_id: str = ""


@dataclass(slots=True)
class PositionState:
    id: str
    sid: str
    strategy: str
    source: str
    symbol: str
    tf: str
    direction: Side

    entry_price: float
    entry_ts_ms: int
    lot: float
    qty: float
    quantity: float
    remaining_qty: float

    sl: float
    tp_levels: list[float]
    atr: float = 0.0

    tp_hits: int = 0
    tp1_hit: bool = False
    tp2_hit: bool = False
    tp3_hit: bool = False
    tp_fill_prices: dict[int, float] = field(default_factory=dict)
    tp_fill_times: dict[int, int] = field(default_factory=dict)

    closed: bool = False
    exit_ts_ms: int = 0
    exit_price: float = 0.0

    # pnl components
    realized_pnl_gross: float = 0.0
    fees: float = 0.0
    # Idempotency guard: set True once realized_pnl_gross has been finalized for this position.
    # Subsequent `realized_pnl_gross += ...` sites MUST check this flag to avoid double-counting
    # (bug found 2026-05-14: SL handler added pnl_rest, then finalize_trade's defensive remaining_qty
    # branch added it AGAIN because remaining_qty wasn't zeroed before the call → -2R loss reported).
    _pnl_finalized: bool = False

    # trailing
    trailing_started: bool = False
    trailing_active: bool = False
    trailing_moves_count: int = 0
    trailing_distance: float = 0.0
    trailing_point: float = 0.0

    # Отслеживание MFE/MAE (в ценовом пространстве)
    max_price_seen: float = 0.0
    min_price_seen: float = 0.0
    max_favorable_price: float = 0.0
    max_favorable_ts: int = 0
    max_adverse_price: float = 0.0
    max_adverse_ts: int = 0
    max_favorable_ts_ms: int = 0
    max_adverse_ts_ms: int = 0

    mfe_pnl: float = 0.0
    mae_pnl: float = 0.0
    one_r_money: float = 0.0

    # --- track when extremes happened (epoch ms) (P0) ---
    max_price_seen_ts_ms: int | None = None
    min_price_seen_ts_ms: int | None = None

    # --- entry metadata (P0) ---
    # some fields like entry_ts_ms/entry_px already exist above with same/similar names;
    # we'll use these specific names for the PnL decomposition contract.
    p0_entry_ts_ms: int | None = None
    p0_entry_px: float | None = None
    p0_side: str | None = None
    p0_qty: float | None = None
    p0_signal_id: str | None = None
    p0_regime: str | None = None
    p0_session: str | None = None
    p0_scenario: str | None = None
    p0_entry_reason: str | None = None
    p0_spread_bps_at_entry: float | None = None
    p0_slippage_bps_est: float | None = None
    p0_book_age_ms: int | None = None
    p0_features_snapshot: dict[str, Any] | None = None

    # --- AB attribution (optional, used by winner suggester) ---
    ab_arm: str = "A"
    ab_group: str = "default"
    ab_key: str = ""
    arm_ver: int = 0
    risk_usd: float = 0.0
    entry_regime: str = "na"
    regime: str = ""
    entry_zone_id: str = ""

    # сырой сигнал для отладки/аудита
    signal_payload: dict[str, Any] = field(default_factory=dict)

    # Signal & Shadow Analytics
    is_virtual: bool = False
    v_gate_status: str = "na"  # na / passed / failed
    v_gate_reason: str = ""

    def is_long(self) -> bool:
        return self.direction == "LONG"

    def is_short(self) -> bool:
        return self.direction == "SHORT"

    # симуляция baseline-выхода (edge split)
    baseline_mode: str = "tp_sl"
    baseline_horizon_ms: int = 0
    baseline_sl: float = 0.0
    baseline_tp1: float = 0.0
    baseline_tp2: float = 0.0
    baseline_tp3: float = 0.0
    baseline_closed: bool = False
    baseline_exit_price: float = 0.0
    baseline_exit_reason: str = ""
    baseline_exit_ts_ms: int = 0
    pnl_if_fixed_exit: float = 0.0
    entry_tag: str = ""
    trail_profile: str = ""  # профиль трейлинга (rocket_v1 / ...)
    trailing_min_lock_r: float = 0.0  # минимальная фиксация в R (например 0.25)
    min_lock_price: float = 0.0  # уровень цены, ниже которого SL не опускаем (для LONG выше; для SHORT ниже)

    # explicit events
    tp1_touched: bool = False
    tp2_touched: bool = False
    tp3_touched: bool = False
    trail_armed: bool = False
    sl_moved_to_be: bool = False
    lock_r: float = 0.0

    # -------------------------------------------------------------------------
    # NEW: time-bucket snapshots (money) for strict MFE@T / MAE@T.
    # Keys are bucket_ms (int), values are pnl in quote currency (float).
    # These dicts are later serialized into TradeClosed.{mfe_pnl_t,mae_pnl_t}
    # as JSON and written into Redis by infra/redis_repo.save_closed().
    # -------------------------------------------------------------------------
    mfe_pnl_t: dict[int, float] = field(default_factory=dict)
    mae_pnl_t: dict[int, float] = field(default_factory=dict)

    # dynamic enrichment
    kind: str = ""
    venue: str = ""
    confidence: float = 0.0

    # analytics/excursions snapshots
    tp1_hit_ts_ms: int = 0
    mfe_pnl_at_tp1: float = 0.0
    mae_pnl_before_tp1: float = 0.0
    mfe_price_at_tp1: float = 0.0
    mfe_ts_at_tp1: int = 0
    mae_price_before_tp1: float = 0.0
    mae_ts_before_tp1: int = 0
    ab_arm: str = "A"

    # ------------------------------------------------------------------
    # NEW: Conditional trailing control.
    #
    # If TRAIL_COND_ENABLED=1:
    #   - after TP1 we start trailing ONLY if trail_after_tp1 == True
    #   - otherwise we keep fixed TP2/TP3 without aggressive tightening.
    #
    # These fields should be propagated from signal payload (ctx.trail_after_tp1).
    # Fail-open default is True (preserves legacy behavior).
    # ------------------------------------------------------------------
    trail_after_tp1: bool = True
    trail_after_tp1_reason: str = ""
    trail_after_tp_level: int = 0  # 0=immediate, 1=TP1, 2=TP2 (from TradeProfileRouter)
    trailing_skip_reason: str = ""

    # If trailing is explicitly NOT started after TP1, we record it for audit.
    trailing_skipped_after_tp1: bool = False
    trailing_skipped_reason: str = ""

    # When trailing is armed after TP1, record time and reason.
    trailing_armed_ts_ms: int = 0
    trailing_start_reason: str = ""

    # -------------------------------------------------------------------------
    # NEW: execution-quality probes (for "slippage model by fact").
    #
    # Почему это в PositionState:
    #   - значения считаются в момент тика (в process_tick) и относятся к конкретной сделке,
    #     поэтому удобнее держать их рядом с состоянием позиции;
    #   - далее они переносятся в TradeClosed (finalize_trade) и пишутся в Redis статистикой.
    #
    # Примечание:
    #   - exit_mid_price / exit_spread_bps — "срез рынка" на момент финального закрытия,
    #     достаточный для оценки realized_slippage_bps и realized_spread_bps.
    #   - Эти поля fail-open: если данных нет, остаются 0 и downstream пропускает запись.
    # -------------------------------------------------------------------------
    exit_mid_price: float = 0.0           # mid на тике, где сделка закрылась (для slippage)
    exit_spread_bps: float = 0.0          # spread в bps на тике закрытия (если bid/ask доступны)

    # -------------------------------------------------------------------------
    # NEW: adverse_bps@T (survival/impact probe)
    #
    # Что измеряем:
    #   adverse_bps_t[bucket_ms] = MAX неблагоприятное движение (в bps) против позиции
    #                              на интервале [entry, entry+bucket_ms].
    #
    # Пример:
    #   buckets: 500ms, 2000ms
    #   LONG: adverse_bps = max(0, (entry - mid)/entry*1e4)
    #   SHORT: adverse_bps = max(0, (mid - entry)/entry*1e4)
    #
    # Зачем:
    #   - помогает калибровать "adverse_bps@T" и учитывать реальный impact/проскальзывание,
    #     а не фиксированный "0.5*spread".
    #
    # Реализация:
    #   - adverse_bps_running держит текущее running-max до "фиксации" бакета.
    #   - adverse_bps_t фиксируется один раз, когда elapsed >= bucket.
    # -------------------------------------------------------------------------
    adverse_bps_running: dict[int, float] = field(default_factory=dict)
    adverse_bps_t: dict[int, float] = field(default_factory=dict)

    # first time price went adverse against position (epoch ms; 0 = not yet touched)
    first_adverse_ts_ms: int = 0

    # -- tick activity tracking (orphan guard) --
    # Обновляются при каждом тике on_tick. Используются _is_orphan_expired(),
    # чтобы не выгонять позиции, которые получали тики, но были long-running.
    last_tick_ts_ms: int = 0
    last_update_ts_ms: int = 0

    # P41 Native Meta Fields
    meta_enforce_cov_bucket: str = ""
    meta_enforce_applied: int = -1
    meta_enforce_key: str = ""
    meta_enforce_salt: str = ""
    meta_veto: int = 0

    # -------------------------------------------------------------------------
    # Phase 0.2: Horizon-Aware Contract (convenience attributes)
    # -------------------------------------------------------------------------
    horizon_contract_ver: int = 0
    hold_target_ms: int = 0
    alpha_half_life_ms: int = 0
    max_signal_age_ms: int = 0
    risk_horizon_bucket: str = "unknown"
    horizon_profile_source: str = ""
    horizon_profile_conf: float = 0.0
    horizon_reason_code: str = ""
    horizon_reason_details: dict[str, Any] = field(default_factory=dict)

    atr_mode: str = ""
    atr_tf_ms: int = 0
    atr_window_n: int = 0
    atr_age_ms: int = 0
    atr_source: str = ""
    atr_regime_value: float = 0.0
    atr_trail_value: float = 0.0
    atr_regime_tf_ms: int = 0
    atr_trail_tf_ms: int = 0
    atr_pct: float = 0.0
    vol_ratio_fast_slow: float = 1.0
    vol_ratio_z: float = 0.0

    horizon_contract: dict[str, Any] = field(default_factory=dict)


    # -------------------------------------------------------------------------
    # P1-9: explicit FSM status (string mirror of PositionFSM._status).
    # Kept as str for backward-compatible Redis serialisation.
    # Set exclusively by PositionFSM.transition(); do NOT assign directly.
    # -------------------------------------------------------------------------
    fsm_status: str = "PENDING"

