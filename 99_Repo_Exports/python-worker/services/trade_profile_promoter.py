"""
services/trade_profile_promoter.py
====================================
Phase 4: Auto-promotion профилей по статистике из Timescale.

Логика:
  - Читает агрегат из trade_profile_decisions + virtual_trades
  - Считает LCB(net_R) для каждого профиля
  - Если LCB(new) > LCB(baseline) + threshold — обновляет Redis active_arm
  - Hold-down: не чаще N минут

ENV:
  TRADE_PROFILE_PROMOTER_ENABLED=0          — отключён по умолчанию (Phase 4)
  TRADE_PROFILE_PROMOTER_POLL_SEC=300       — интервал опроса (5 мин)
  TRADE_PROFILE_PROMOTER_MIN_N=100          — мин. сделок для ликвидных пар
  TRADE_PROFILE_PROMOTER_MIN_N_LOW_FREQ=50  — мин. сделок для низкочастотных
  TRADE_PROFILE_PROMOTER_LCB_DELTA=0.05    — LCB должен превышать baseline на 0.05R
  TRADE_PROFILE_PROMOTER_MIN_PF=1.15       — минимальный profit factor
  TRADE_PROFILE_PROMOTER_MAX_DD_WORSE=0.10 — max ухудшение drawdown относительно baseline
  TRADE_PROFILE_PROMOTER_HOLD_DOWN_MIN=60  — hold-down в минутах
  TRADE_PROFILE_PROMOTER_DRY_RUN=1         — dry-run по умолчанию
"""
from __future__ import annotations

import asyncio
import json
import logging
import math
import os
from dataclasses import dataclass
from typing import Any

logger = logging.getLogger("trade_profile_promoter")

# ---------------------------------------------------------------------------
# Stats DTO
# ---------------------------------------------------------------------------

@dataclass
class ProfileStats:
    profile: str
    regime_bucket: str
    n_trades: int
    win_rate: float          # 0..1
    profit_factor: float
    avg_net_r: float
    lcb_net_r: float         # lower confidence bound (Wilson/normal approx)
    max_drawdown: float      # positive number, fraction (0.25 = 25%)
    slippage_residual_p95: float
  # type: ignore
    @property  # type: ignore
    def is_promotable(
        self,
        *,
        baseline_lcb: float,
        min_n: int = 100,
        lcb_delta: float = 0.05,
        min_pf: float = 1.15,
        max_dd_worse: float = 0.10,
        baseline_max_dd: float = 0.0,
    ) -> bool:
        if self.n_trades < min_n:
            return False
        if self.profit_factor < min_pf:
            return False
        if self.lcb_net_r < baseline_lcb + lcb_delta:
            return False
        if self.max_drawdown > baseline_max_dd * (1.0 + max_dd_worse):
            return False
        return True


# ---------------------------------------------------------------------------
# LCB calculation
# ---------------------------------------------------------------------------

def compute_lcb_normal(values: list[float], z: float = 1.645) -> float:
    """
    Normal approximation lower confidence bound for mean.
    LCB = mean - z * (std / sqrt(n))
    z=1.645 → 95% one-sided CI.
    Returns -inf on empty / degenerate input.
    """
    n = len(values)
    if n == 0:
        return float("-inf")
    if n == 1:
        return values[0]
    mean = sum(values) / n
    variance = sum((v - mean) ** 2 for v in values) / (n - 1)
    std = math.sqrt(max(0.0, variance))
    return mean - z * std / math.sqrt(n)


# ---------------------------------------------------------------------------
# Promoter service
# ---------------------------------------------------------------------------

class TradeProfilePromoter:
    """
    Background service for auto-promotion of trade profiles.

    Run as an async task alongside the signal pipeline.
    All DB/Redis I/O is fail-open: any exception logs and continues.
    """

    def __init__(self, db_dsn: str, redis_client: Any) -> None:
        self._db_dsn = db_dsn
        self._redis = redis_client

        self._enabled = os.getenv("TRADE_PROFILE_PROMOTER_ENABLED", "0").lower() in {"1", "true", "yes", "on"}
        self._poll_sec = int(os.getenv("TRADE_PROFILE_PROMOTER_POLL_SEC", "300") or 300)
        self._min_n = int(os.getenv("TRADE_PROFILE_PROMOTER_MIN_N", "100") or 100)
        self._min_n_low = int(os.getenv("TRADE_PROFILE_PROMOTER_MIN_N_LOW_FREQ", "50") or 50)
        self._lcb_delta = float(os.getenv("TRADE_PROFILE_PROMOTER_LCB_DELTA", "0.05") or 0.05)
        self._min_pf = float(os.getenv("TRADE_PROFILE_PROMOTER_MIN_PF", "1.15") or 1.15)
        self._max_dd_worse = float(os.getenv("TRADE_PROFILE_PROMOTER_MAX_DD_WORSE", "0.10") or 0.10)
        self._hold_down_min = int(os.getenv("TRADE_PROFILE_PROMOTER_HOLD_DOWN_MIN", "60") or 60)
        self._dry_run = os.getenv("TRADE_PROFILE_PROMOTER_DRY_RUN", "1").lower() in {"1", "true", "yes", "on"}

        self._last_promotion_ts: dict[str, float] = {}   # profile → epoch_sec

    async def run_forever(self) -> None:
        if not self._enabled:
            logger.info("🔇 TradeProfilePromoter disabled (TRADE_PROFILE_PROMOTER_ENABLED=0). Phase 4 not active.")
            return

        logger.info(
            "🚀 TradeProfilePromoter started | poll=%ds min_n=%d lcb_delta=%.2f dry_run=%s",
            self._poll_sec, self._min_n, self._lcb_delta, self._dry_run
        )
        while True:
            try:
                await self._run_cycle()
            except Exception as e:
                logger.error("❌ TradeProfilePromoter cycle error: %s", e)
            await asyncio.sleep(self._poll_sec)

    async def _run_cycle(self) -> None:
        stats = await self._fetch_stats()
        if not stats:
            return

        baseline = self._find_baseline(stats)
        if baseline is None:
            logger.debug("TradeProfilePromoter: no baseline profile found")
            return

        for s in stats:
            if s.profile == baseline.profile:
                continue
            await self._maybe_promote(s, baseline)

    async def _fetch_stats(self) -> list[ProfileStats]:
        """
        Fetch per-profile stats from Timescale trade_profile_decisions
        joined with virtual_trades for net_R.

        Returns empty list on any failure (fail-open).
        """
        try:
            import asyncpg  # type: ignore
        except ImportError:
            logger.warning("asyncpg not installed — TradeProfilePromoter cannot query DB")
            return []

        query = """
            WITH profile_trades AS (
                SELECT
                    tpd.profile,
                    tpd.regime_bucket,
                    tpd.signal_id,
                    -- join net_R from virtual_trades if available
                    COALESCE(vt.net_r, 0.0) AS net_r,
                    COALESCE(vt.pnl_usd, 0.0) AS pnl_usd,
                    COALESCE(vt.slippage_bps_realized - vt.slippage_bps_expected, 0.0) AS slip_residual
                FROM trade_profile_decisions tpd
                LEFT JOIN virtual_trades vt ON vt.signal_id = tpd.signal_id
                WHERE tpd.ts > NOW() - INTERVAL '30 days'
                  AND tpd.decision = 'ALLOW'
                  AND tpd.profile_mode = 'LIVE'
            ),
            agg AS (
                SELECT
                    profile,
                    regime_bucket,
                    COUNT(*) AS n,
                    AVG(net_r) AS avg_net_r,
                    STDDEV(net_r) AS std_net_r,
                    SUM(CASE WHEN net_r > 0 THEN net_r ELSE 0 END) /
                        NULLIF(SUM(CASE WHEN net_r <= 0 THEN ABS(net_r) ELSE 0 END), 0) AS profit_factor,
                    SUM(CASE WHEN net_r > 0 THEN 1 ELSE 0 END)::float / NULLIF(COUNT(*), 0) AS win_rate,
                    PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY slip_residual) AS slip_p95
                FROM profile_trades
                GROUP BY profile, regime_bucket
                HAVING COUNT(*) >= 10
            )
            SELECT
                profile,
                regime_bucket,
                n,
                avg_net_r,
                COALESCE(std_net_r, 0.0) AS std_net_r,
                COALESCE(profit_factor, 0.0) AS profit_factor,
                COALESCE(win_rate, 0.0) AS win_rate,
                COALESCE(slip_p95, 0.0) AS slip_p95,
                -- LCB = mean - 1.645 * std / sqrt(n)
                avg_net_r - 1.645 * COALESCE(std_net_r, 0.0) / NULLIF(SQRT(n), 0) AS lcb_net_r
            FROM agg
            ORDER BY lcb_net_r DESC
        """
        try:
            conn = await asyncpg.connect(self._db_dsn, timeout=10.0)
            try:
                rows = await conn.fetch(query)
            finally:
                await conn.close()
        except Exception as e:
            logger.warning("TradeProfilePromoter: DB query failed: %s", e)
            return []

        result = []
        for row in rows:
            result.append(ProfileStats(
                profile=str(row["profile"]),
                regime_bucket=str(row["regime_bucket"]),
                n_trades=int(row["n"]),
                win_rate=float(row["win_rate"] or 0.0),
                profit_factor=float(row["profit_factor"] or 0.0),
                avg_net_r=float(row["avg_net_r"] or 0.0),
                lcb_net_r=float(row["lcb_net_r"] or float("-inf")),
                max_drawdown=0.0,       # TODO: calculate from equity curve
                slippage_residual_p95=float(row["slip_p95"] or 0.0),
            ))
        return result

    @staticmethod
    def _find_baseline(stats: list[ProfileStats]) -> ProfileStats | None:
        """Baseline = default_v1 if present, else profile with highest n_trades."""
        for s in stats:
            if s.profile == "default_v1":
                return s
        if stats:
            return max(stats, key=lambda s: s.n_trades)
        return None

    async def _maybe_promote(self, candidate: ProfileStats, baseline: ProfileStats) -> None:
        import time
        now = time.time()
        key = f"{candidate.profile}:{candidate.regime_bucket}"
        last = self._last_promotion_ts.get(key, 0.0)
        hold_sec = self._hold_down_min * 60
        if now - last < hold_sec:
            return
  # type: ignore
        promotable = candidate.is_promotable(  # type: ignore
            baseline_lcb=baseline.lcb_net_r,
            min_n=self._min_n,
            lcb_delta=self._lcb_delta,
            min_pf=self._min_pf,
            max_dd_worse=self._max_dd_worse,
            baseline_max_dd=baseline.max_drawdown,
        )

        if not promotable:
            return

        redis_key = f"cfg:trade_profile:active_profile:{candidate.regime_bucket}"
        action = "DRY_RUN" if self._dry_run else "PROMOTE"
        logger.info(
            "🏆 [%s] Profile %s for bucket=%s | lcb=%.3f baseline_lcb=%.3f pf=%.2f n=%d",
            action, candidate.profile, candidate.regime_bucket,
            candidate.lcb_net_r, baseline.lcb_net_r,
            candidate.profit_factor, candidate.n_trades
        )

        if not self._dry_run:
            try:
                await self._redis.set(redis_key, candidate.profile, ex=self._hold_down_min * 60 * 3)
                self._last_promotion_ts[key] = now
                logger.info("✅ Promoted %s → Redis %s", candidate.profile, redis_key)
            except Exception as e:
                logger.error("❌ Redis promotion write failed: %s", e)
        else:
            self._last_promotion_ts[key] = now
