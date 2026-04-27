import sys
sys.path.append('.')
from datetime import datetime

class DummyClosed:
    def __init__(self):
        self.order_id = "test"
        self.sid = "test"
        self.strategy = "test"
        self.source = "test"
        self.symbol = "test"
        self.tf = "test"
        self.direction = "test"
        self.entry_ts_ms = 0
        self.exit_ts_ms = 0
        self.entry_price = 0.0
        self.exit_price = 0.0
        self.lot = 0.0
        self.notional_usd = 0.0
        self.pnl_net = 0.0
        self.pnl_gross = 0.0
        self.fees = 0.0
        self.pnl_pct = 0.0
        self.pnl_if_fixed_exit = 0.0
        self.tp1_hit = False
        self.tp2_hit = False
        self.tp3_hit = False
        self.tp_hits = 0
        self.tp_before_sl = False
        self.trailing_started = False
        self.trailing_active = False
        self.trailing_moves = 0
        self.mfe_pnl = 0.0
        self.mae_pnl = 0.0
        self.giveback = 0.0
        self.missed_profit = 0.0
        self.one_r_money = 0.0
        self.r_multiple = 0.0
        self.duration_ms = 0
        self.close_reason = ""
        self.close_reason_raw = ""

def Json(x):
    return x
import json

closed = DummyClosed()
baseline_exit_reason = ""
baseline_exit_ts_ms = 0
baseline_exit_price = 0.0
trailing_profile = ""
entry_tag = ""
max_favorable_price = 0.0
max_favorable_ts = 0
is_final_close = True
remaining_qty = 0.0
status = ""
health_l2_stale_ratio_tick = 0.0
health_l2_stale_ratio_now = 0.0
health_avg_l2_age_ms = 0.0
health_avg_l2_age_tick_ms = 0.0
health_signal_emit_rate = 0.0
health_dlq_rate = 0.0
config_snapshot = {}
horizon_contract = {}
horizon_bucket = ""
atr_tf_ms_val = 0

params = (
    closed.order_id, closed.sid, closed.strategy, closed.source, closed.symbol, closed.tf, closed.direction,
    closed.entry_ts_ms, closed.exit_ts_ms, closed.entry_price, closed.exit_price, closed.lot, closed.notional_usd,
    closed.pnl_net, closed.pnl_gross, closed.fees, closed.pnl_pct,
    closed.pnl_if_fixed_exit, baseline_exit_reason, baseline_exit_ts_ms, baseline_exit_price,
    closed.tp1_hit, closed.tp2_hit, closed.tp3_hit, closed.tp_hits, closed.tp_before_sl,
    closed.trailing_started, closed.trailing_active, closed.trailing_moves, trailing_profile,
    closed.mfe_pnl, closed.mae_pnl, closed.giveback, closed.missed_profit,
    closed.one_r_money, closed.r_multiple, closed.duration_ms,
    closed.close_reason, getattr(closed, 'close_reason_raw', ''),
    entry_tag, max_favorable_price, max_favorable_ts,
    is_final_close, remaining_qty, status,
    getattr(closed, "contract_ver", None) or getattr(closed, "horizon_contract_ver", 2),
    getattr(closed, "risk_horizon_bucket", "") or "",
    getattr(closed, "hold_target_ms", 0) or 0,
    getattr(closed, "alpha_half_life_ms", 0) or 0,
    getattr(closed, "max_signal_age_ms", 0) or 0,
    getattr(closed, "atr_age_ms", 0) or 0,
    getattr(closed, "atr_source", "") or "",
    getattr(closed, "atr_pct", 0.0) or 0.0,
    getattr(closed, "vol_ratio_fast_slow", 1.0) if getattr(closed, "vol_ratio_fast_slow", None) is not None else 1.0,
    getattr(closed, "vol_ratio_z", 0.0) or 0.0,
    health_l2_stale_ratio_tick, health_l2_stale_ratio_now,
    health_avg_l2_age_ms, health_avg_l2_age_tick_ms,
    health_signal_emit_rate, health_dlq_rate,
    json.dumps(config_snapshot, ensure_ascii=False, sort_keys=True),
    Json(horizon_contract) if Json is not None else json.dumps(horizon_contract, ensure_ascii=False),
    horizon_bucket or None,
    atr_tf_ms_val or None,
    getattr(closed, "live_surface_applied", False),
    getattr(closed, "live_surface_reason_code", ""),
    getattr(closed, "baseline_sl_price", 0.0),
    getattr(closed, "baseline_tp1_price", 0.0),
    getattr(closed, "selected_sl_price", 0.0),
    getattr(closed, "selected_tp1_price", 0.0),
    getattr(closed, "is_virtual", False),
    getattr(closed, "meta_enforce_cov_bucket", ""),
    getattr(closed, "meta_enforce_applied", -1),
    getattr(closed, "atr_policy_ver", 0),
    getattr(closed, "atr_policy_tag", ""),
    getattr(closed, "atr_policy_source", ""),
    getattr(closed, "atr_policy_scenario", ""),
    getattr(closed, "atr_policy_regime", ""),
    getattr(closed, "atr_policy_bucket", ""),
    getattr(closed, "atr_stop_ttl_mode", ""),
    getattr(closed, "atr_trailing_mode", ""),
    getattr(closed, "atr_recovery_run_id", ""),
    getattr(closed, "atr_restore_cert_id", ""),
    getattr(closed, "atr_restore_cert_status", ""),
    Json(getattr(closed, "atr_policy_snapshot_json", {})) if Json is not None else getattr(closed, "atr_policy_snapshot_json", {})
)
print("Length of params is:", len(params))
