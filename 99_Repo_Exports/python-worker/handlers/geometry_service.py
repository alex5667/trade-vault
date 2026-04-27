# geometry_service.py
"""
Geometry and liquidity analysis functionality extracted from base_orderflow_handler.py
"""

from __future__ import annotations

from typing import Optional, List, TYPE_CHECKING, Any, Tuple
import math

from contexts import GeoZoneHit, LiquidityContext, ZoneType, SimpleL2Snapshot, L2Level, OrderflowSignalContext

if TYPE_CHECKING:
    from contexts import BarSample


class GeometryLiquidityService:
    """
    Service for geometry analysis and liquidity context building.
    """

    def __init__(self, symbol: str, config: Any):
        self.symbol = symbol
        self.config = config

    def _find_near_liquidity_wall(
        self,
        l2: SimpleL2Snapshot,
        max_levels: int = 10,
        max_dist_bps: float = 15.0,
        size_z_thr: float = 1.5,
    ) -> Tuple[Optional[str], Optional[L2Level], Optional[float], Optional[float]]:
        """
        Find nearest significant liquidity wall.
        Returns (side, level, distance_bps)
        """

        def process_side(levels: List[L2Level], side: str):
            """Process one side of the book."""
            mid = float(getattr(l2, "mid", 0.0) or 0.0)
            if mid <= 0.0:
                bb = float(getattr(l2, "best_bid", 0.0) or 0.0)
                ba = float(getattr(l2, "best_ask", 0.0) or 0.0)
                if bb > 0.0 and ba > 0.0 and ba >= bb:
                    mid = 0.5 * (bb + ba)
            if mid <= 0.0:
                return None, None, None, None

            # crude z-score over first N sizes (fallback, лучше чем raw compare)
            sizes = [float(x.size) for x in levels[:max_levels] if getattr(x, "size", 0) and x.size > 0]
            if len(sizes) >= 3:
                mu = sum(sizes) / len(sizes)
                var = sum((s - mu) ** 2 for s in sizes) / max(len(sizes) - 1, 1)
                sd = math.sqrt(max(var, 1e-12))
            else:
                mu, sd = 0.0, 0.0

            for level in levels[:max_levels]:
                if level.price <= 0 or level.size <= 0:
                    continue

                # Calculate distance in bps from mid
                dist_bps = abs(level.price - mid) / mid * 10000

                if dist_bps > max_dist_bps:
                    continue

                # Check if size is significant
                # size_z_thr трактуем как z-score threshold (fallback z-score)
                if sd > 0.0:
                    z = (float(level.size) - mu) / sd
                else:
                    z = 0.0
                if z >= size_z_thr:
                    return side, level, dist_bps, float(z)

            return None, None, None, None

        # Check bid side
        side, level, dist, z = process_side(l2.bids, "bid")
        if side:
            return side, level, dist, z

        # Check ask side
        side, level, dist, z = process_side(l2.asks, "ask")
        if side:
            return side, level, dist, z

        return None, None, None, None

    def _build_liquidity_context(
        self,
        *,
        l2_snapshot: "SimpleL2Snapshot",
        cluster_vol: Optional[Any] = None,
    ) -> LiquidityContext:
        """Build liquidity context from L2 snapshot and cluster volume."""

        # Find walls
        wall_side, wall_level, wall_dist, wall_size_z = self._find_near_liquidity_wall(l2_snapshot)

        # Calculate aggregated volumes at wall
        aggr_buy_at_wall = 0.0
        aggr_sell_at_wall = 0.0

        if cluster_vol:
            aggr_buy_at_wall = cluster_vol.buy_cluster_total
            aggr_sell_at_wall = cluster_vol.sell_cluster_total

        # Calculate ratio to rest of book (improved: compare to total depth around price)
        bids = getattr(l2_snapshot, "bids", []) or []
        asks = getattr(l2_snapshot, "asks", []) or []

        # Use depth metrics if available (more representative of liquidity around current price)
        depth_bid_5 = float(getattr(l2_snapshot, "depth_bid_5", 0.0) or 0.0)
        depth_ask_5 = float(getattr(l2_snapshot, "depth_ask_5", 0.0) or 0.0)

        if depth_bid_5 > 0.0 and depth_ask_5 > 0.0:
            # Use depth_5 as proxy for liquidity around price
            total_near_price_volume = depth_bid_5 + depth_ask_5
        else:
            # Fallback to raw levels sum
            total_near_price_volume = (
                sum(float(level.size) for level in bids[:5] if getattr(level, "size", 0) and level.size > 0) +
                sum(float(level.size) for level in asks[:5] if getattr(level, "size", 0) and level.size > 0)
            )

        wall_volume = aggr_buy_at_wall + aggr_sell_at_wall
        # Compare cluster volume to liquidity around the price (more meaningful)
        aggr_to_rest_ratio = wall_volume / max(total_near_price_volume, 1e-9)

        # Detect pattern
        pattern = self._detect_liquidity_pattern(
            aggr_buy_at_wall=aggr_buy_at_wall,
            aggr_sell_at_wall=aggr_sell_at_wall,
            aggr_to_rest_ratio=aggr_to_rest_ratio,
        )

        return LiquidityContext(
            aggr_buy_at_wall=aggr_buy_at_wall,
            aggr_sell_at_wall=aggr_sell_at_wall,
            aggr_to_rest_ratio=aggr_to_rest_ratio,
            pattern=pattern,
            cluster=cluster_vol,
            # Legacy fields for compatibility
            near_wall_side=wall_side,
            near_wall_price=wall_level.price if wall_level else None,
            near_wall_size=wall_level.size if wall_level else None,
            near_wall_size_z=wall_size_z,
            depth_5_vol=float(
                (getattr(l2_snapshot, "depth_bid_5", 0.0) or 0.0)
                + (getattr(l2_snapshot, "depth_ask_5", 0.0) or 0.0)
            ),
            aggr_vol_at_wall=float(wall_volume),
        )

    def _detect_liquidity_pattern(
        self,
        *,
        aggr_buy_at_wall: float,
        aggr_sell_at_wall: float,
        aggr_to_rest_ratio: float,
    ) -> str:
        """Detect liquidity pattern."""
        from contexts import LiquidityPattern

        cfg = self.config.liquidity if hasattr(self.config, 'liquidity') else None
        min_aggr_to_rest_ratio = getattr(cfg, 'min_aggr_to_rest_ratio', 0.1) if cfg else 0.1

        # If cluster is weak, no pattern
        if aggr_to_rest_ratio < min_aggr_to_rest_ratio:
            return LiquidityPattern.NONE

        # Determine dominant side
        buy_vs_sell_ratio = aggr_buy_at_wall / max(aggr_sell_at_wall, 1e-9)
        sell_vs_buy_ratio = aggr_sell_at_wall / max(aggr_buy_at_wall, 1e-9)

        min_side_dominance = getattr(cfg, 'min_side_domination_ratio', 1.5) if cfg else 1.5

        if buy_vs_sell_ratio >= min_side_dominance:
            return LiquidityPattern.BUY_AGGR_CLUSTER
        elif sell_vs_buy_ratio >= min_side_dominance:
            return LiquidityPattern.SELL_AGGR_CLUSTER
        else:
            return LiquidityPattern.BOTH_SIDES_CLUSTER

    def _score_liquidity(self, lc: LiquidityContext) -> float:
        """Score liquidity context."""
        from contexts import LiquidityPattern

        base_no_wall_score = 0.1
        max_score = 1.0

        # Pattern-based scoring
        pattern_multiplier = 1.0
        if lc.pattern == LiquidityPattern.BUY_AGGR_CLUSTER:
            pattern_multiplier = 1.2
        elif lc.pattern == LiquidityPattern.SELL_AGGR_CLUSTER:
            pattern_multiplier = 1.2
        elif lc.pattern == LiquidityPattern.BOTH_SIDES_CLUSTER:
            pattern_multiplier = 1.1

        return min(base_no_wall_score * pattern_multiplier, max_score)

    def _compute_htf_level_distance(self, price: float | None, htf_levels: Any) -> float | None:
        """Compute distance to HTF levels."""
        if price is None or not htf_levels:
            return None

        # Find nearest HTF level
        min_dist = float('inf')
        for level_name, level_price in htf_levels.items():
            if isinstance(level_price, (int, float)):
                dist = abs(price - level_price)
                min_dist = min(min_dist, dist)

        return min_dist / price * 10000 if min_dist != float('inf') else None  # bps

    def _get_htf_levels(self, symbol: str) -> Optional[Any]:
        """Get HTF levels for symbol."""
        # Placeholder - would load from Redis or external source
        return None

    def _build_geo_zone_hits(
        self,
        ctx: OrderflowSignalContext,
        htf_levels: Any,
    ) -> list[GeoZoneHit]:
        """Build geometry zone hits."""
        hits = []
        price = ctx.price

        def add_level(level_price: float, zone_type: ZoneType, strength: float) -> None:
            """Add level hit if within range."""
            dist_bps = abs(price - level_price) / price * 10000
            atr_htf = ctx.atr_htf_bps or ctx.atr_14_bps
            dist_rel_atr = dist_bps / max(atr_htf, 1e-6)

            # Check if within near/far range
            geometry_cfg = getattr(self.config, 'geometry', None)
            near_mult = getattr(geometry_cfg, 'near_mult', 0.25) if geometry_cfg else 0.25
            far_mult = getattr(geometry_cfg, 'far_mult', 1.0) if geometry_cfg else 1.0

            if dist_rel_atr <= far_mult:
                hits.append(GeoZoneHit(
                    zone_type=zone_type,
                    zone_price=level_price,
                    dist_bps=dist_bps,
                    atr_htf_bps=atr_htf,
                    dist_rel_atr=dist_rel_atr,
                    strength=strength,
                ))

        # Add HTF levels
        if htf_levels:
            for level_name, level_price in htf_levels.items():
                if isinstance(level_price, (int, float)):
                    zone_type_map = {
                        'h1_pivot': 'HTF_OB',
                        'h4_pivot': 'HTF_OB',
                        'd1_pivot': 'HTF_OB',
                        'resistance': 'HTF_FVG',
                        'support': 'HTF_FVG',
                    }
                    # Use ZoneType literals with fallback
                    zone_type = zone_type_map.get(level_name, 'HTF_OB')  # type: ignore
                    strength = 0.8 if 'h1' in level_name else 0.9
                    add_level(level_price, zone_type, strength)

        return hits

    def _attach_geometry_context(self, ctx: OrderflowSignalContext, bar: "BarSample") -> None:
        """Attach geometry context to signal context."""
        # Get HTF levels
        htf_levels = self._get_htf_levels(ctx.symbol)

        # Build zone hits
        geo_zone_hits = self._build_geo_zone_hits(ctx, htf_levels)

        # Calculate geometry score
        geometry_score = None
        if geo_zone_hits:
            # Simple scoring based on nearest hit
            nearest_hit = min(geo_zone_hits, key=lambda h: h.dist_rel_atr)
            geometry_score = max(0.0, 1.0 - nearest_hit.dist_rel_atr)

        # Attach to context
        ctx.geometry_hits = geo_zone_hits
        # Note: geometry_score and htf_level_dist_bps might be computed elsewhere or be legacy fields

    def _build_liquidity_context_from_ctx(
        self,
        ctx: OrderflowSignalContext,
        cluster_vol: Optional[Any] = None,
    ) -> LiquidityContext:
        """Build liquidity context from existing signal context (preferred method)."""

        # Use wall detection results from L2MicrostructureEngine
        wall_side = None
        wall_level = None
        wall_dist = 0.0

        if ctx.wall_bid and not ctx.wall_ask:
            wall_side = "bid"
            wall_dist = ctx.wall_bid_dist_bps
        elif ctx.wall_ask and not ctx.wall_bid:
            wall_side = "ask"
            wall_dist = ctx.wall_ask_dist_bps
        elif ctx.wall_bid and ctx.wall_ask:
            # Both sides have walls - pick closer one
            if ctx.wall_bid_dist_bps <= ctx.wall_ask_dist_bps:
                wall_side = "bid"
                wall_dist = ctx.wall_bid_dist_bps
            else:
                wall_side = "ask"
                wall_dist = ctx.wall_ask_dist_bps

        # Calculate aggregated volumes at wall (from cluster_vol if available)
        aggr_buy_at_wall = 0.0
        aggr_sell_at_wall = 0.0

        if cluster_vol:
            aggr_buy_at_wall = cluster_vol.buy_cluster_total
            aggr_sell_at_wall = cluster_vol.sell_cluster_total

        # Calculate ratio using depth metrics from ctx (more accurate)
        depth_bid_5 = ctx.depth_bid_5 or 0.0
        depth_ask_5 = ctx.depth_ask_5 or 0.0
        total_near_price_volume = depth_bid_5 + depth_ask_5

        wall_volume = aggr_buy_at_wall + aggr_sell_at_wall
        aggr_to_rest_ratio = wall_volume / max(total_near_price_volume, 1e-9)

        # Detect pattern
        pattern = self._detect_liquidity_pattern(
            aggr_buy_at_wall=aggr_buy_at_wall,
            aggr_sell_at_wall=aggr_sell_at_wall,
            aggr_to_rest_ratio=aggr_to_rest_ratio,
        )

        return LiquidityContext(
            aggr_buy_at_wall=aggr_buy_at_wall,
            aggr_sell_at_wall=aggr_sell_at_wall,
            aggr_to_rest_ratio=aggr_to_rest_ratio,
            pattern=pattern,
            cluster=cluster_vol,
            # Legacy fields for compatibility
            near_wall_side=wall_side,
            near_wall_price=None,  # Not available from ctx
            near_wall_size=None,   # Not available from ctx
            depth_5_vol=total_near_price_volume,
        )

    def _attach_liquidity_context(
        self,
        ctx: OrderflowSignalContext,
        l2: SimpleL2Snapshot | None = None,
        cluster_vol: Optional[Any] = None,
    ) -> None:
        """Attach liquidity context to signal context."""
        # Prefer building from existing context data (more accurate and consistent)
        if hasattr(ctx, 'wall_bid') and hasattr(ctx, 'depth_bid_5'):
            liquidity_ctx = self._build_liquidity_context_from_ctx(ctx, cluster_vol)
        elif l2:
            # Fallback to L2-based calculation
            liquidity_ctx = self._build_liquidity_context(
                l2_snapshot=l2,
                cluster_vol=cluster_vol,
            )
        else:
            return  # No data available

        ctx.liquidity_context = liquidity_ctx
        # Score is stored in liquidity_context.liquidity_context_score if needed
        score = self._score_liquidity(liquidity_ctx)
        if hasattr(ctx.liquidity_context, 'liquidity_context_score'):
            ctx.liquidity_context.liquidity_context_score = score

    def _update_geometry_liquidity_context(
        self,
        ctx: OrderflowSignalContext,
        price: float,
        ts: float,
    ) -> None:
        """Update geometry and liquidity context."""
        # This would be called from the main handler
        # For now, just delegate to attach methods
        bar_sample = type('BarSample', (), {'ts': ts, 'high': price, 'low': price, 'close': price, 'volume': 0.0})()
        self._attach_geometry_context(ctx, bar_sample)

        # Liquidity would need L2 snapshot
        # self._attach_liquidity_context(ctx)
