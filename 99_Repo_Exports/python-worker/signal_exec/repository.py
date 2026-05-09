"""
SignalRepository: TimescaleDB operations for signal execution data.

Handles signals, execution plans, performance metrics, and TTD configurations.
Uses psycopg2 ThreadedConnectionPool for sync multi-threaded access.
"""

from __future__ import annotations

import contextlib
from dataclasses import asdict, is_dataclass
from typing import Any

from psycopg2.extras import RealDictCursor as dict_row

from .models import ExecutionPlan

# SignalPerformance imported locally to avoid circular import


class SignalRepository:
    """
    Thin layer over TimescaleDB using psycopg2 ThreadedConnectionPool.
    Safe for sync multi-threaded callers; not safe to call from within
    an asyncio coroutine without run_in_executor.
    """

    def __init__(self, dsn: str, minconn: int = 1, maxconn: int = 10):
        self._dsn = dsn
        from psycopg2.pool import ThreadedConnectionPool
        self._pool = ThreadedConnectionPool(minconn, maxconn, dsn)

    # --- Helpers ---

    @contextlib.contextmanager
    def _conn(self):
        conn = self._pool.getconn()
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            self._pool.putconn(conn)

    # --- Insert raw signals ---

    def insert_signal(self, ctx: Any, extra_json: dict[str, Any] | None = None) -> None:
        """
        Store raw SignalContext in signals table.
        ctx should serialize to dict (.to_dict() method assumed).
        """
        extra_json = extra_json or {}

        # Priority: .to_dict() -> asdict() if dataclass -> .__dict__
        if hasattr(ctx, "to_dict"):
            ctx_dict = ctx.to_dict()
        elif is_dataclass(ctx):
            ctx_dict = asdict(ctx)
        else:
            ctx_dict = getattr(ctx, "__dict__", {})

        with self._conn() as conn, conn.cursor(cursor_factory=dict_row) as cur:
            cur.execute(
                """
                INSERT INTO signals (
                    signal_id,
                    ts_signal,
                    symbol,
                    setup_type,
                    side,
                    price_at_signal,
                    atr_1m,
                    final_score,
                    experiment_id,
                    experiment_variant,
                    raw_ctx
                ) VALUES (
                    %(signal_id)s,
                    %(ts_signal)s,
                    %(symbol)s,
                    %(setup_type)s,
                    %(side)s,
                    %(price_at_signal)s,
                    %(atr_1m)s,
                    %(final_score)s,
                    %(experiment_id)s,
                    %(experiment_variant)s,
                    %(raw_ctx)s
                )
                ON CONFLICT (signal_id) DO NOTHING;
                """,
                {
                    "signal_id": ctx.signal_id,
                    "ts_signal": ctx.ts_signal,
                    "symbol": ctx.symbol,
                    "setup_type": ctx.setup_type,
                    "side": str(ctx.side),
                    "price_at_signal": ctx.price_at_signal,
                    "atr_1m": getattr(ctx, "atr_1m", 0.0),
                    "final_score": getattr(ctx, "final_score", 0.0),
                    "experiment_id": getattr(ctx, "experiment_id", None),
                    "experiment_variant": getattr(ctx, "experiment_variant", None),
                    "raw_ctx": ctx_dict,
                },
            )

    # --- Insert execution plan ---

    def insert_execution_plan(self, plan: ExecutionPlan) -> None:
        with self._conn() as conn, conn.cursor(cursor_factory=dict_row) as cur:
#             cur.execute(,
                """
                INSERT INTO signal_execution_plan (
                    signal_id,
                    ts_signal,
                    symbol,
                    setup_type,
                    side,
                    entry_zone_low,
                    entry_zone_high,
                    stop_price,
                    tp_levels,
                    partials,
                    pos_risk_R,
                    risk_usd,
                    position_size,
                    expiry_bars,
                    created_at,
                    meta
                ) VALUES (
                    %(signal_id)s,
                    %(ts_signal)s,
                    %(symbol)s,
                    %(setup_type)s,
                    %(side)s,
                    %(entry_zone_low)s,
                    %(entry_zone_high)s,
                    %(stop_price)s,
                    %(tp_levels)s,
                    %(partials)s,
                    %(pos_risk_R)s,
                    %(risk_usd)s,
                    %(position_size)s,
                    %(expiry_bars)s,
                    %(created_at)s,
                    %(meta)s
                )
                ON CONFLICT (signal_id) DO UPDATE
                SET
                    entry_zone_low = EXCLUDED.entry_zone_low,
                    entry_zone_high = EXCLUDED.entry_zone_high,
                    stop_price = EXCLUDED.stop_price,
                    tp_levels = EXCLUDED.tp_levels,
                    partials = EXCLUDED.partials,
                    pos_risk_R = EXCLUDED.pos_risk_R,
                    risk_usd = EXCLUDED.risk_usd,
                    position_size = EXCLUDED.position_size,
                    expiry_bars = EXCLUDED.expiry_bars,
                    created_at = EXCLUDED.created_at,
                    meta = EXCLUDED.meta;
                """,
                {
                    "signal_id": plan.signal_id,
                    "ts_signal": plan.ts_signal,
                    "symbol": plan.symbol,
                    "setup_type": plan.setup_type,
                    "side": str(plan.side),
                    "entry_zone_low": plan.entry_zone_low,
                    "entry_zone_high": plan.entry_zone_high,
                    "stop_price": plan.stop_price,
                    "tp_levels": plan.tp_levels,
                    "partials": plan.partials,
                    "pos_risk_R": plan.pos_risk_R,
                    "risk_usd": plan.risk_usd,
                    "position_size": plan.position_size,
                    "expiry_bars": plan.expiry_bars,
                    "created_at": plan.created_at,
                    "meta": plan.meta,
                },
#             )

    # --- Insert performance ---

    def insert_signal_performance(self, perf: SignalPerformance) -> None:

        with self._conn() as conn, conn.cursor(cursor_factory=dict_row) as cur:
            cur.execute(
#                 """
#                 INSERT INTO signal_performance (
                    signal_id,
                    ts_signal,
                    symbol,
                    setup_type,
                    side,
                    ts_entry,
                    ts_exit,
                    price_at_signal,
                    entry_price,
                    exit_price,
                    stop_price,
                    realized_R,
                    mfe_R,
                    mae_R,
                    ttd_bars,
                    ttd_seconds,
                    bars_to_entry,
                    bars_to_exit,
                    outcome,
                    notes,
#                     extra
#                 ) VALUES (
#                     %(signal_id)s,
#                     %(ts_signal)s,
#                     %(symbol)s,
#                     %(setup_type)s,
#                     %(side)s,
#                     %(ts_entry)s,
#                     %(ts_exit)s,
#                     %(price_at_signal)s,
#                     %(entry_price)s,
#                     %(exit_price)s,
#                     %(stop_price)s,
#                     %(realized_R)s,
#                     %(mfe_R)s,
#                     %(mae_R)s,
#                     %(ttd_bars)s,
#                     %(ttd_seconds)s,
#                     %(bars_to_entry)s,
#                     %(bars_to_exit)s,
#                     %(outcome)s,
#                     %(notes)s,
#                     %(extra)s
                )
#                 ON CONFLICT (signal_id) DO UPDATE
#                 SET
#                     ts_entry = EXCLUDED.ts_entry,
#                     ts_exit = EXCLUDED.ts_exit,
#                     entry_price = EXCLUDED.entry_price,
#                     exit_price = EXCLUDED.exit_price,
#                     stop_price = EXCLUDED.stop_price,
#                     realized_R = EXCLUDED.realized_R,
#                     mfe_R = EXCLUDED.mfe_R,
#                     mae_R = EXCLUDED.mae_R,
#                     ttd_bars = EXCLUDED.ttd_bars,
#                     ttd_seconds = EXCLUDED.ttd_seconds,
#                     bars_to_entry = EXCLUDED.bars_to_entry,
#                     bars_to_exit = EXCLUDED.bars_to_exit,
#                     outcome = EXCLUDED.outcome,
#                     notes = EXCLUDED.notes,
#                     extra = EXCLUDED.extra;
#                 """
#                 {
#                     "signal_id": perf.signal_id,
#                     "ts_signal": perf.ts_signal,
#                     "symbol": perf.symbol,
#                     "setup_type": perf.setup_type,
#                     "side": str(perf.side),
#                     "ts_entry": perf.ts_entry,
#                     "ts_exit": perf.ts_exit,
#                     "price_at_signal": perf.price_at_signal,
#                     "entry_price": perf.entry_price,
#                     "exit_price": perf.exit_price,
#                     "stop_price": perf.stop_price,
#                     "realized_R": perf.realized_R,
#                     "mfe_R": perf.mfe_R,
#                     "mae_R": perf.mae_R,
#                     "ttd_bars": perf.ttd_bars,
#                     "ttd_seconds": perf.ttd_seconds,
#                     "bars_to_entry": perf.bars_to_entry,
#                     "bars_to_exit": perf.bars_to_exit,
#                     "outcome": str(perf.outcome),
#                     "notes": perf.notes,
#                     "extra": perf.extra,
#                 },
#             )

    # --- Load setup configs for ExecutionPlanner ---

    def load_setup_configs(self) -> dict[tuple[str, str], Any]:
        """
        Load signal_ttd_config + apply defaults.
        Return dict[((symbol, setup_type), SymbolSetupConfig)] for ExecutionPlanner.
#         Skeleton — adapt with your defaults/logic.
        """
        from .execution_planner import SymbolSetupConfig

        configs: dict[tuple[str, str], SymbolSetupConfig] = {}
        with self._conn() as conn, conn.cursor(cursor_factory=dict_row) as cur:
            cur.execute(
                """
                SELECT
                    symbol,
                    setup_type,
                    ttd_target_R,
                    ttd_median_bars,
                    ttd_p75_bars,
                    recommended_expiry_bars
                FROM signal_ttd_config;
#                 """
            )
            rows = cur.fetchall()

        for r in rows:
            symbol = r["symbol"]
            setup_type = r["setup_type"]
            expiry_bars = int(r["recommended_expiry_bars"])

            # Hardcoded defaults — adapt to your logic
            cfg = SymbolSetupConfig(
                symbol=symbol,
                setup_type=setup_type,
                expiry_bars=expiry_bars,
                min_stop_ticks=10,
                max_stop_R=3.0,
                atr_buffer_ratio=0.15,
                entry_zone_min_R=0.3,
                entry_zone_max_R=0.7,
                default_tp_R=(1.0, 2.0, 3.0),
                score_buckets=(0.4, 0.7, 0.85),
                risk_multipliers=(0.5, 1.0, 1.5, 2.0),
                max_risk_R_per_trade=1.0,
                max_portfolio_risk_pct=5.0,
            )
            configs[(symbol, setup_type)] = cfg

        return configs
