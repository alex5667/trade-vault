from __future__ import annotations

import json
import os
import time
import math
import psycopg2
import psycopg2.extras
import redis

def _dsn():
    return (
        os.getenv("ANALYTICS_DB_DSN")
        or os.getenv("TRADES_DB_DSN")
        or "postgresql://postgres:12345@postgres:5432/scanner_analytics"
    )

def _redis():
    return redis.Redis.from_url(os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0"), decode_responses=True)

def _relu(x: float) -> float:
    return max(0.0, float(x))

def _clip(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))

def _cert_mult(status: str) -> float:
    status = str(status or "")
    if status == "passed":
        return 1.0
    if status in {"failed", "stale"}:
        return 0.0
    return 0.7

def _rollout_mult(stage: str) -> float:
    return {
        "shadow": 0.0
        "canary_5": 0.35
        "canary_25": 0.70
        "live_100": 1.00
        "frozen": 0.0
        "rolled_back": 0.0
    }.get(stage, 0.0)

def _scope_key(row: dict, layer: str) -> str:
    return f"policy:{row.get('source') or 'CryptoOrderFlow'}:{row.get('symbol', 'unknown')}:{row.get('scenario', 'unknown')}:{row.get('regime', 'unknown')}:{row.get('risk_horizon_bucket', 'unknown')}:{layer}:{int(row.get('atr_policy_ver', 0))}"

def run_once() -> int:
    conn = psycopg2.connect(_dsn(), connect_timeout=5, application_name="atr_policy_capital_allocator")
    r = _redis()
    written = 0
    try:
        with conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT
                  symbol
                  source
                  scenario
                  regime
                  risk_horizon_bucket
                  atr_policy_ver
                  atr_restore_cert_status
                  avg_pnl_bps
                  avg_slippage_bps
                  avg_mae_pct
                  win_rate
                  stop_rate
                  tp1_rate
                  n_trades
                FROM v_atr_policy_allocator_inputs
            """)
            rows = cur.fetchall()

        enriched = []
        for row in rows:
            # stop_ttl and trailing layers
            for layer in ("stop_ttl", "trailing"):
                rollout_stage = r.get(
                    f"cfg:atr_rollout_stage:{row['symbol']}:{row['scenario']}:{row['regime']}:{row['risk_horizon_bucket']}:{layer}:{int(row['atr_policy_ver'] or 0)}"
                ) or "shadow"

                slip = float(row.get("avg_slippage_bps") or 0.0)
                pnl = float(row.get("avg_pnl_bps") or 0.0)
                mae = float(row.get("avg_mae_pct") or 0.0)
                stop_rate = float(row.get("stop_rate") or 0.0)
                tp1_rate = float(row.get("tp1_rate") or 0.0)

                spread_cost_bps = float(r.get(f"spread_ema_half_bps:{row['symbol']}") or 0.0)
                fee_bps = float(os.getenv("TAKER_FEE_BPS", "4.0"))
                scope = _scope_key(row, layer)
                dd_pen = float(r.get(f"state:atr_drawdown_pen_bps:{scope}") or 0.0)
                instability_pen = float(r.get(f"state:atr_instability_pen_bps:{scope}") or 0.0)

                edge_net_bps = pnl - slip - spread_cost_bps - fee_bps - dd_pen - instability_pen
                
                # Portfolio Concentration Control
                cluster = str(r.get(f"cfg:atr_symbol_cluster:{row['symbol']}") or "unclassified")
                cluster_open = float(r.get(f"state:atr_portfolio:open_risk_pct:factor:{cluster}") or 0.0)
                cluster_cap = float(r.get(f"cfg:atr_portfolio:max_factor_cluster_risk_pct:factor:{cluster}") or 0.0)
                
                concentration_mult = 1.0
                if cluster_cap > 0.0:
                    util = cluster_open / cluster_cap
                    concentration_mult = _clip(1.0 - util, 0.10, 1.00)

                # Regime/Stress multiplier logic
                regime = str(r.get(f"state:atr_regime:{row['symbol']}") or row.get('regime') or "unknown")
                stress = str(r.get(f"state:atr_stress:{row['symbol']}") or "normal")
                regime_mult = float(r.get(f"cfg:atr_regime_risk_mult:{regime}:{stress}:{layer}:{rollout_stage}") or 1.0)

                score = _relu(edge_net_bps) \
                    * _cert_mult(row.get("atr_restore_cert_status", "")) \
                    * _rollout_mult(rollout_stage) \
                    * _clip(1.0 - mae, 0.25, 1.0) \
                    * _clip(1.0 - stop_rate, 0.25, 1.0) \
                    * _clip(0.5 + tp1_rate, 0.25, 1.5) \
                    * concentration_mult \
                    * regime_mult

                enriched.append({
                    **row
                    "layer": layer
                    "rollout_stage": rollout_stage
                    "alloc_score": float(score)
                })

        total_score = sum(x["alloc_score"] for x in enriched if x["alloc_score"] > 0.0)
        if total_score <= 0:
            return 0

        global_open_risk_budget_pct = float(os.getenv("ATR_ALLOC_GLOBAL_OPEN_RISK_BUDGET_PCT", "3.0"))
        global_daily_trades_budget = int(os.getenv("ATR_ALLOC_GLOBAL_DAILY_TRADES_BUDGET", "40"))
        min_mult = float(os.getenv("ATR_ALLOC_MIN_RISK_MULT", "0.25"))
        max_mult = float(os.getenv("ATR_ALLOC_MAX_RISK_MULT", "1.50"))
        max_share = float(os.getenv("ATR_ALLOC_MAX_SHARE", "0.40"))
        
        observe_only = os.getenv("ATR_POLICY_ALLOCATOR_OBSERVE_ONLY", "1") == "1"

        with conn, conn.cursor() as cur:
            cur.execute("UPDATE atr_policy_allocator_states SET is_current=false WHERE is_current=true")

            for x in enriched:
                if x["alloc_score"] <= 0.0:
                    weight = 0.0
                else:
                    weight = _clip(x["alloc_score"] / total_score, 0.0, max_share)

                risk_mult = _clip(weight / max(1e-9, 1.0 / max(1, len(enriched))), min_mult, max_mult)
                target_open_risk = global_open_risk_budget_pct * weight
                target_daily_trades = int(round(global_daily_trades_budget * weight))

                state = {
                    "rollout_stage": x["rollout_stage"]
                    "alloc_score": x["alloc_score"]
                    "alloc_weight": weight
                    "risk_pct_mult": risk_mult
                    "target_max_open_risk_pct": target_open_risk
                    "target_max_daily_trades": target_daily_trades
                    "restore_cert_status": x.get("atr_restore_cert_status")
                    "n_trades": int(x.get("n_trades") or 0)
                }

                cur.execute("""
                    INSERT INTO atr_policy_allocator_states (
                      source, venue, symbol, scenario, regime, risk_horizon_bucket
                      layer, policy_ver, rollout_stage, restore_cert_status
                      alloc_score, alloc_weight, risk_pct_mult
                      target_max_open_risk_pct, target_max_daily_trades
                      state_json, is_current, created_at_ms, updated_at_ms
                    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb,true,%s,%s)
                """, (
                    x.get("source") or "CryptoOrderFlow"
                    "default"
                    x["symbol"], x["scenario"], x["regime"], x["risk_horizon_bucket"]
                    x["layer"], int(x["atr_policy_ver"] or 0)
                    x["rollout_stage"], x.get("atr_restore_cert_status") or ""
                    float(x["alloc_score"]), float(weight), float(risk_mult)
                    float(target_open_risk), int(target_daily_trades)
                    json.dumps(state, ensure_ascii=False, sort_keys=True)
                    int(time.time() * 1000), int(time.time() * 1000)
                ))

                scope = _scope_key(x, x["layer"])
                
                # Update Redis config keys, but avoid if we're entirely disabled (though OBSERVE_ONLY means we might still write to redis, but gate ignores it. Actually, wait. 
                # If observe_only, we can perhaps write with a different prefix, or the gate handles observe_only? 
                # The user wrote "budget gate ignores allocator keys" for observe_only. Let's write them normally, and the gate can check observe_only.
                r.set(f"cfg:atr_alloc:risk_pct_mult:{scope}", str(float(risk_mult)))
                r.set(f"cfg:atr_alloc:max_open_risk_pct:{scope}", str(float(target_open_risk)))
                r.set(f"cfg:atr_alloc:max_daily_trades:{scope}", str(int(target_daily_trades)))

                cur.execute("""
                    INSERT INTO atr_policy_allocator_events (
                      source, venue, symbol, scenario, regime, risk_horizon_bucket
                      layer, policy_ver, action, reason_code, event_json
                    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb)
                """, (
                    x.get("source") or "CryptoOrderFlow"
                    "default"
                    x["symbol"], x["scenario"], x["regime"], x["risk_horizon_bucket"]
                    x["layer"], int(x["atr_policy_ver"] or 0)
                    "rebalance", "ATR_POLICY_ALLOC_REBALANCE"
                    json.dumps(state, ensure_ascii=False, sort_keys=True)
                ))
                written += 1
                
            conn.commit()
            
        return written
    finally:
        conn.close()

if __name__ == "__main__":
    if os.getenv("ATR_POLICY_ALLOCATOR_ENABLE", "1") == "1":
        print(f"Allocated capital for {run_once()} cohorts.")
    else:
        print("Allocator disabled via ATR_POLICY_ALLOCATOR_ENABLE.")
