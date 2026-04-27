# liquidity_geometry.py
"""
Liquidity and geometry analysis functionality.
Extracted from handlers/base_orderflow_handler.py to improve modularity.
"""

from __future__ import annotations

from typing import Optional, List, Literal, TYPE_CHECKING
import math

from contexts import LiquidityContext, GeoZoneHit, LiquidityPattern, ZoneType, ClusterVol

if TYPE_CHECKING:
    from contexts import OrderflowSignalContext, SimpleL2Snapshot


class LiquidityGeometryAnalyzer:
    """Analyzer for liquidity and geometry patterns."""

    def __init__(self, liq_max_age_ms: int = 5000):
        self._liq_max_age_ms = liq_max_age_ms

    def _push_l2_snapshot_for_liquidity(
        self,
        ts_ms: int,
        snapshot: "SimpleL2Snapshot",
        cluster_vol: Optional["ClusterVol"] = None
    ) -> tuple[int, "SimpleL2Snapshot", Optional["ClusterVol"]]:
        """
        Формирует snapshot для liquidity buffer.
        Возвращает tuple для добавления в deque.
        """
        return (ts_ms, snapshot, cluster_vol)

    def _update_geometry_liquidity_context(
        self,
        ctx: "OrderflowSignalContext",
        ts_ms: int,
        snapshots: List[tuple[int, "SimpleL2Snapshot", Optional["ClusterVol"]]]
    ) -> None:
        """
        Обновляет geometry и liquidity контекст в OrderflowSignalContext.
        """
        # Find recent snapshots within age limit
        recent_snapshots = [
            (snap_ts, snap, cluster)
            for snap_ts, snap, cluster in snapshots
            if ts_ms - snap_ts <= self._liq_max_age_ms
        ]

        if not recent_snapshots:
            return

        # Use most recent snapshot
        _, latest_snapshot, latest_cluster = recent_snapshots[-1]

        # Build liquidity context
        liquidity_ctx = self._build_liquidity_context(latest_snapshot, latest_cluster)

        # Build geometry hits
        geometry_hits = self._build_geometry_hits(ctx, latest_snapshot)

        # Update context
        ctx.liquidity_context = liquidity_ctx
        ctx.geometry_hits = geometry_hits

    def _build_liquidity_context(
        self,
        snapshot: "SimpleL2Snapshot",
        cluster_vol: Optional["ClusterVol"] = None
    ) -> "LiquidityContext":
        """
        Строит LiquidityContext из L2 snapshot и cluster volume.
        """
        # Calculate basic metrics
        bid_vol_5 = snapshot.depth_bid_5
        ask_vol_5 = snapshot.depth_ask_5

        # Detect patterns
        pattern = LiquidityPattern.NONE

        # Wall detection (simplified)
        wall_threshold = 2.0  # bids/asks ratio threshold
        if bid_vol_5 > ask_vol_5 * wall_threshold:
            pattern = LiquidityPattern.WALL_BUY
        elif ask_vol_5 > bid_vol_5 * wall_threshold:
            pattern = LiquidityPattern.WALL_SELL

        # Cluster analysis if available
        aggr_buy_at_wall = 0.0
        aggr_sell_at_wall = 0.0

        if cluster_vol:
            # Simplified cluster analysis
            total_buy = sum(cluster_vol.buy_vol_by_price.values())
            total_sell = sum(cluster_vol.sell_vol_by_price.values())

            if total_buy > 0 or total_sell > 0:
                aggr_buy_at_wall = total_buy
                aggr_sell_at_wall = total_sell

                # Cluster pattern detection
                if aggr_buy_at_wall > aggr_sell_at_wall * 1.5:
                    pattern = LiquidityPattern.CLUSTER_BUY
                elif aggr_sell_at_wall > aggr_buy_at_wall * 1.5:
                    pattern = LiquidityPattern.CLUSTER_SELL

        return LiquidityContext(
            aggr_buy_at_wall=aggr_buy_at_wall,
            aggr_sell_at_wall=aggr_sell_at_wall,
            aggr_to_rest_ratio=max(aggr_buy_at_wall, aggr_sell_at_wall) / max(bid_vol_5 + ask_vol_5, 1.0),
            pattern=pattern,
            cluster=cluster_vol
        )

    def _find_near_liquidity_wall(
        self,
        snapshot: "SimpleL2Snapshot",
        price: float,
        direction: Literal["bid", "ask"]
    ) -> Optional[tuple[str, float, float, float]]:
        """
        Находит ближайшую liquidity wall.
        Returns: (side, wall_price, distance_bps, strength)
        """
        if direction == "bid":
            levels = snapshot.bids
            side = "bid"
        else:
            levels = snapshot.asks
            side = "ask"

        if not levels:
            return None

        # Find largest level within reasonable distance
        max_distance_bps = 50.0  # 50 bps max distance
        best_wall = None
        best_strength = 0.0

        for level in levels[:10]:  # Check top 10 levels
            distance_bps = abs(level.price - price) / price * 10000.0

            if distance_bps <= max_distance_bps:
                strength = level.size
                if strength > best_strength:
                    best_strength = strength
                    best_wall = (side, level.price, distance_bps, strength)

        return best_wall

    def _build_liquidity_context(
        self,
        snapshot: "SimpleL2Snapshot",
        cluster_vol: Optional["ClusterVol"] = None
    ) -> "LiquidityContext":
        """
        Строит LiquidityContext из L2 snapshot и cluster volume.
        """
        # This is a simplified version - the full implementation would be more complex
        return LiquidityContext(
            aggr_buy_at_wall=snapshot.depth_bid_5,
            aggr_sell_at_wall=snapshot.depth_ask_5,
            aggr_to_rest_ratio=1.0,
            pattern=LiquidityPattern.NONE,
            cluster=cluster_vol
        )

    def _detect_liquidity_pattern(
        self,
        snapshot: "SimpleL2Snapshot",
        cluster_vol: Optional["ClusterVol"] = None
    ) -> "LiquidityPattern":
        """
        Определяет паттерн ликвидности на основе L2 данных.
        """
        bid_vol = snapshot.depth_bid_5
        ask_vol = snapshot.depth_ask_5

        ratio = bid_vol / max(ask_vol, 0.1)  # Avoid division by zero

        if ratio > 3.0:
            return LiquidityPattern.WALL_BUY
        elif ratio < 0.33:
            return LiquidityPattern.WALL_SELL

        # Cluster-based patterns would be more sophisticated
        return LiquidityPattern.NONE

    def _score_liquidity(self, lc: "LiquidityContext") -> float:
        """
        Считает liquidity score [0..1] для confidence scoring.
        """
        if lc.pattern == LiquidityPattern.NONE:
            return 0.5  # Neutral

        # Higher score for stronger patterns
        pattern_strength = {
            LiquidityPattern.WALL_BUY: 0.8,
            LiquidityPattern.WALL_SELL: 0.8,
            LiquidityPattern.CLUSTER_BUY: 0.9,
            LiquidityPattern.CLUSTER_SELL: 0.9,
        }.get(lc.pattern, 0.5)

        # Adjust based on volume ratios
        volume_ratio = lc.aggr_to_rest_ratio
        volume_bonus = min(volume_ratio / 2.0, 0.3)  # Cap at 0.3

        return min(pattern_strength + volume_bonus, 1.0)

    def _build_geometry_hits(
        self,
        ctx: "OrderflowSignalContext",
        snapshot: "SimpleL2Snapshot"
    ) -> List["GeoZoneHit"]:
        """
        Строит список geometry hits для данного контекста.
        """
        hits = []

        # This is a simplified implementation
        # Real implementation would check pivots, levels, etc.

        # Example: check if near daily levels
        if ctx.pivots:
            for level_name, level_price in ctx.pivots.items():
                if abs(ctx.price - level_price) / ctx.price < 0.001:  # Within 10 bps
                    zone_type = f"DAILY_{level_name.upper()}"
                    dist_bps = abs(ctx.price - level_price) / ctx.price * 10000.0

                    hits.append(GeoZoneHit(
                        zone_type=zone_type,  # type: ignore
                        zone_price=level_price,
                        dist_bps=dist_bps,
                        atr_htf_bps=ctx.atr_14_bps if ctx.atr_14_bps > 0 else 20.0,
                        dist_rel_atr=dist_bps / max(ctx.atr_14_bps, 20.0),
                        strength=0.7  # Daily levels are strong
                    ))

        return hits

    def _score_geometry(self, ctx: "OrderflowSignalContext") -> float:
        """
        Считает geometry score [0..1] для confidence scoring.
        """
        if not ctx.geometry_hits:
            return 0.0

        # Score based on strongest hit
        max_strength = max(hit.strength for hit in ctx.geometry_hits)

        # Adjust based on proximity (closer = higher score)
        proximity_bonus = 0.0
        for hit in ctx.geometry_hits:
            if hit.dist_rel_atr < 0.5:  # Within 0.5 ATR
                proximity_bonus = max(proximity_bonus, 1.0 - hit.dist_rel_atr)

        return min(max_strength + proximity_bonus * 0.2, 1.0)

    def _attach_geometry_context(
        self,
        ctx: "OrderflowSignalContext",
        bar: "BarSample"
    ) -> None:
        """
        Прикрепляет geometry context к сигналу.
        """
        # This would integrate with HTF levels and pivots
        # Simplified implementation
        pass

    def _attach_liquidity_context(
        self,
        ctx: "OrderflowSignalContext",
        snapshot: Optional["SimpleL2Snapshot"] = None
    ) -> None:
        """
        Прикрепляет liquidity context к сигналу.
        """
        if snapshot:
            ctx.liquidity_context = self._build_liquidity_context(snapshot)
