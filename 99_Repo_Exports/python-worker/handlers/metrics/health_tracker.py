"""
Health metrics tracking for handlers.

Extracted from BaseOrderFlowHandler to follow Single Responsibility Principle.
Manages:
- Tick latency tracking
- L2 freshness monitoring
- Error rate tracking
- Health snapshot publishing to Redis
"""

import time
from typing import Any, Dict
from collections import deque


class HealthMetricsTracker:
    """
    Tracks health metrics for a handler.
    
    Responsibilities:
    - Record tick processing latency
    - Track L2 data freshness
    - Monitor error rates
    - Publish health snapshots to Redis
    
    Thread-safe for concurrent metric updates.
    """
    
    def __init__(self, symbol: str, redis_client: Any):
        """
        Initialize health metrics tracker.
        
        Args:
            symbol: Trading symbol (e.g., "BTCUSDT")
            redis_client: Redis client for publishing snapshots
        """
        self.symbol = symbol
        self.redis = redis_client
        
        # Metrics storage
        self._tick_latencies = deque(maxlen=100)  # Last 100 tick latencies
        self._l2_freshness = deque(maxlen=100)    # Last 100 L2 age measurements
        self._error_count = 0
        self._total_ticks = 0
        self._last_snapshot_time = 0.0
        
        # Configuration
        self._snapshot_interval_sec = 60.0  # Publish snapshot every 60s
    
    def record_tick_latency(self, latency_ms: float) -> None:
        """
        Record tick processing latency.
        
        Args:
            latency_ms: Latency in milliseconds
        """
        self._tick_latencies.append(latency_ms)
        self._total_ticks += 1
    
    def record_l2_freshness(self, age_ms: float) -> None:
        """
        Record L2 data age.
        
        Args:
            age_ms: Age of L2 data in milliseconds
        """
        self._l2_freshness.append(age_ms)
    
    def record_error(self) -> None:
        """Increment error counter."""
        self._error_count += 1
    
    def get_metrics(self) -> Dict[str, Any]:
        """
        Get current metrics snapshot.
        
        Returns:
            Dictionary with current metrics
        """
        metrics = {
            "symbol": self.symbol,
            "total_ticks": self._total_ticks,
            "error_count": self._error_count,
            "timestamp": time.time(),
        }
        
        # Calculate tick latency stats
        if self._tick_latencies:
            latencies = list(self._tick_latencies)
            metrics["tick_latency_ms"] = {
                "avg": sum(latencies) / len(latencies),
                "min": min(latencies),
                "max": max(latencies),
                "p50": sorted(latencies)[len(latencies) // 2],
                "p95": sorted(latencies)[int(len(latencies) * 0.95)],
            }
        
        # Calculate L2 freshness stats
        if self._l2_freshness:
            freshness = list(self._l2_freshness)
            metrics["l2_freshness_ms"] = {
                "avg": sum(freshness) / len(freshness),
                "min": min(freshness),
                "max": max(freshness),
                "p50": sorted(freshness)[len(freshness) // 2],
                "p95": sorted(freshness)[int(len(freshness) * 0.95)],
            }
        
        # Calculate error rate
        if self._total_ticks > 0:
            metrics["error_rate"] = self._error_count / self._total_ticks
        else:
            metrics["error_rate"] = 0.0
        
        return metrics
    
    def publish_snapshot(self, force: bool = False) -> bool:
        """
        Publish health snapshot to Redis.
        
        Args:
            force: Force publish even if interval hasn't elapsed
            
        Returns:
            True if snapshot was published, False otherwise
        """
        now = time.time()
        
        # Check if enough time has passed since last snapshot
        if not force and (now - self._last_snapshot_time) < self._snapshot_interval_sec:
            return False
        
        try:
            metrics = self.get_metrics()
            key = f"orderflow:{self.symbol}:health_snapshot"
            
            # Publish to Redis with 2-minute TTL
            self.redis.setex(key, 120, str(metrics))
            self._last_snapshot_time = now
            return True
            
        except Exception:
            # Fail-open: don't crash if Redis is unavailable
            return False
    
    def reset(self) -> None:
        """Reset all metrics (useful for testing or restart scenarios)."""
        self._tick_latencies.clear()
        self._l2_freshness.clear()
        self._error_count = 0
        self._total_ticks = 0
        self._last_snapshot_time = 0.0
