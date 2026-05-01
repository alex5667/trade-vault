from __future__ import annotations
"""
L2GPUProcessor - GPU-accelerated L2 orderbook processing.

This module provides GPU-accelerated processing for Level 2 (L2) orderbook data,
including batch processing of orderbook snapshots and real-time updates.
"""

from utils.time_utils import get_ny_time_millis

import time
from typing import Optional, List, Dict, Any, Tuple
import threading
import logging

from common.gpu_service import get_gpu_service, is_gpu_available


class L2GPUProcessor:
    """
    GPU-accelerated processor for L2 orderbook data.

    Handles batch processing of L2 orderbook snapshots and updates using GPU acceleration
    when available, with automatic fallback to CPU processing.
    """

    def __init__(self, symbol: str, batch_size: int = 1000, buffer_timeout_ms: int = 1000):
        """
        Initialize the L2 GPU processor.

        Args:
            symbol: Trading symbol (e.g., 'BTCUSDT')
            batch_size: Maximum batch size for GPU processing
            buffer_timeout_ms: Buffer timeout in milliseconds before processing
        """
        self.symbol = symbol
        self.batch_size = batch_size
        self.buffer_timeout_ms = buffer_timeout_ms

        # GPU availability check
        self.gpu_available = is_gpu_available()
        self.gpu_service = get_gpu_service() if self.gpu_available else None

        # Setup logging
        self.logger = logging.getLogger(f"L2GPUProcessor:{symbol}")
        self.logger.info(f"Initialized L2GPUProcessor for {symbol}, GPU available: {self.gpu_available}")

        # Processing buffers
        self._buffer: List[Dict[str, Any]] = []
        self._buffer_lock = threading.Lock()
        self._last_process_time = get_ny_time_millis()  # ms

        # Processing stats
        self.processed_count = 0
        self.batch_count = 0
        self.avg_batch_size = 0.0
        self.avg_processing_time_ms = 0.0

        # GPU arrays (initialized on first use)
        self._gpu_initialized = False
        self._price_array = None
        self._size_array = None
        self._side_array = None  # 0 for bid, 1 for ask

    def _ensure_gpu_arrays(self, max_size: int) -> None:
        """Ensure GPU arrays are initialized and sized appropriately."""
        if not self.gpu_available:
            return

        # Return if arrays are already initialized and large enough
        if self._gpu_initialized and self._price_array is not None and self._price_array.size >= max_size:
            return

        try:
            import cupy as cp

            # Allocate with a buffer to prevent frequent resizing
            target_size = max(self.batch_size, int(max_size * 1.5))

            # Initialize or resize GPU arrays for L2 data
            self._price_array = cp.zeros(target_size, dtype=cp.float64)
            self._size_array = cp.zeros(target_size, dtype=cp.float64)
            self._side_array = cp.zeros(target_size, dtype=cp.int32)

            self._gpu_initialized = True
            self.logger.info(f"GPU arrays initialized/resized for max size {target_size}")

        except ImportError:
            self.logger.warning("CuPy not available, GPU processing disabled")
            self.gpu_available = False
        except Exception as e:
            self.logger.error(f"Failed to initialize/resize GPU arrays: {e}")
            self.gpu_available = False

    def _process_batch_gpu(self, batch: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Process a batch of L2 data using GPU acceleration.

        Args:
            batch: List of L2 orderbook entries

        Returns:
            Dictionary with processing results
        """
        if not self.gpu_available or not batch:
            return self._process_batch_cpu(batch)

        start_time = time.time()

        try:
            import cupy as cp

            batch_size = len(batch)
            self._ensure_gpu_arrays(max_size=batch_size)

            # Prepare data for GPU
            prices = []
            sizes = []
            sides = []

            for entry in batch:
                prices.append(float(entry.get('price', 0.0)))
                sizes.append(float(entry.get('size', 0.0)))
                sides.append(1 if entry.get('side', '').lower() == 'ask' else 0)

            # Transfer to GPU
            self._price_array[:batch_size] = cp.array(prices)
            self._size_array[:batch_size] = cp.array(sizes)
            self._side_array[:batch_size] = cp.array(sides)

            # GPU computations (example: calculate spread, depth, imbalance)
            bid_mask = self._side_array[:batch_size] == 0
            ask_mask = self._side_array[:batch_size] == 1

            bid_prices = self._price_array[:batch_size][bid_mask]
            ask_prices = self._price_array[:batch_size][ask_mask]
            bid_sizes = self._size_array[:batch_size][bid_mask]
            ask_sizes = self._size_array[:batch_size][ask_mask]

            # Calculate basic metrics
            best_bid = cp.max(bid_prices) if len(bid_prices) > 0 else cp.array(0.0)
            best_ask = cp.min(ask_prices) if len(ask_prices) > 0 else cp.array(float('inf'))
            spread = best_ask - best_bid

            # Calculate depth (sum of sizes)
            bid_depth = cp.sum(bid_sizes)
            ask_depth = cp.sum(ask_sizes)

            # Calculate imbalance
            total_depth = bid_depth + ask_depth
            imbalance = (bid_depth - ask_depth) / total_depth if total_depth > 0 else cp.array(0.0)

            # --- Microprice ---
            # Microprice = (BestAsk * BidQty + BestBid * AskQty) / (BidQty + AskQty)
            # using best bid/ask sizes (slices)
            best_bid_idx = cp.argmax(bid_prices) if len(bid_prices) > 0 else -1
            best_ask_idx = cp.argmin(ask_prices) if len(ask_prices) > 0 else -1

            best_bid_size = bid_sizes[best_bid_idx] if best_bid_idx != -1 else cp.array(0.0)
            best_ask_size = ask_sizes[best_ask_idx] if best_ask_idx != -1 else cp.array(0.0)
            
            mp_denom = best_bid_size + best_ask_size
            microprice = (best_ask * best_bid_size + best_bid * best_ask_size) / mp_denom if mp_denom > 0 else (best_bid + best_ask) * 0.5 

            # --- Wall Detection ---
            # Wall = order size > mult * average_size (global avg for batch)
            wall_mult = 4.0
            
            avg_bid_size = cp.mean(bid_sizes) if len(bid_sizes) > 0 else cp.array(0.0)
            avg_ask_size = cp.mean(ask_sizes) if len(ask_sizes) > 0 else cp.array(0.0)
            
            bid_wall_mask = bid_sizes > (avg_bid_size * wall_mult)
            ask_wall_mask = ask_sizes > (avg_ask_size * wall_mult)
            
            has_bid_wall = cp.any(bid_wall_mask)
            has_ask_wall = cp.any(ask_wall_mask)
            
            wall_bid_price = cp.array(0.0)
            wall_bid_size = cp.array(0.0)
            
            if has_bid_wall:
                # Get indices of walls
                wall_idxs = cp.where(bid_wall_mask)[0]
                # Pick the one with largest size
                best_idx_in_walls = cp.argmax(bid_sizes[wall_idxs])
                best_idx = wall_idxs[best_idx_in_walls]
                wall_bid_price = bid_prices[best_idx]
                wall_bid_size = bid_sizes[best_idx]

            wall_ask_price = cp.array(0.0)
            wall_ask_size = cp.array(0.0)

            if has_ask_wall:
                wall_idxs = cp.where(ask_wall_mask)[0]
                best_idx_in_walls = cp.argmax(ask_sizes[wall_idxs])
                best_idx = wall_idxs[best_idx_in_walls]
                wall_ask_price = ask_prices[best_idx]
                wall_ask_size = ask_sizes[best_idx]

            # Transfer results back to CPU
            result = {
                'batch_size': batch_size,
                'best_bid': float(best_bid),
                'best_ask': float(best_ask),
                'spread': float(spread),
                'bid_depth': float(bid_depth),
                'ask_depth': float(ask_depth),
                'imbalance': float(imbalance),
                'microprice': float(microprice),
                'wall_bid_price': float(wall_bid_price),
                'wall_bid_size': float(wall_bid_size),
                'wall_ask_price': float(wall_ask_price),
                'wall_ask_size': float(wall_ask_size),
                'processing_time_ms': (time.time() - start_time) * 1000,
                'gpu_used': True
            }

        except Exception as e:
            self.logger.warning(f"GPU processing failed, falling back to CPU: {e}")
            self.gpu_available = False
            return self._process_batch_cpu(batch)

        # Update stats
        processing_time = (time.time() - start_time) * 1000
        self._update_stats(batch_size, processing_time)

        return result

    def _process_batch_cpu(self, batch: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Process a batch of L2 data using CPU (fallback).

        Args:
            batch: List of L2 orderbook entries

        Returns:
            Dictionary with processing results
        """
        start_time = time.time()

        if not batch:
            return {
                'batch_size': 0,
                'best_bid': 0.0,
                'best_ask': float('inf'),
                'spread': 0.0,
                'bid_depth': 0.0,
                'ask_depth': 0.0,
                'imbalance': 0.0,
                'microprice': 0.0,
                'wall_bid_price': 0.0,
                'wall_bid_size': 0.0,
                'wall_ask_price': 0.0,
                'wall_ask_size': 0.0,
                'processing_time_ms': 0.0,
                'gpu_used': False
            }

        batch_size = len(batch)

        # Separate bids and asks
        bids = [(entry['price'], entry['size']) for entry in batch
                if entry.get('side', '').lower() in ('bid', 'bids')]
        asks = [(entry['price'], entry['size']) for entry in batch
                if entry.get('side', '').lower() in ('ask', 'asks')]

        # Calculate metrics
        # For microprice/walls, we need sorted books logic, but 'batch' is just a list of updates 
        # or snapshot entries. We'll proceed with "best available in batch" logic.
        bids.sort(key=lambda x: x[0], reverse=True) # Descending for bids
        asks.sort(key=lambda x: x[0])               # Ascending for asks

        best_bid, best_bid_sz = bids[0] if bids else (0.0, 0.0)
        best_ask, best_ask_sz = asks[0] if asks else (float('inf'), 0.0)
        
        spread = best_ask - best_bid if best_ask != float('inf') and best_bid > 0 else 0.0

        bid_depth = sum(size for _, size in bids)
        ask_depth = sum(size for _, size in asks)

        total_depth = bid_depth + ask_depth
        imbalance = (bid_depth - ask_depth) / total_depth if total_depth > 0 else 0.0
        
        # Microprice
        mp_denom = best_bid_sz + best_ask_sz
        if mp_denom > 0 and best_bid > 0 and best_ask != float('inf'):
             microprice = (best_ask * best_bid_sz + best_bid * best_ask_sz) / mp_denom
        else:
             microprice = (best_bid + best_ask) * 0.5 if (best_bid > 0 and best_ask != float('inf')) else 0.0

        # Walls (largest size > 4x mean)
        wall_mult = 4.0
        avg_bid_s = bid_depth / len(bids) if bids else 0.0
        avg_ask_s = ask_depth / len(asks) if asks else 0.0
        
        wall_bid_price, wall_bid_size = 0.0, 0.0
        for p, s in bids:
            if s > avg_bid_s * wall_mult:
                if s > wall_bid_size: # Max size wall
                    wall_bid_size = s
                    wall_bid_price = p
        
        wall_ask_price, wall_ask_size = 0.0, 0.0
        for p, s in asks:
            if s > avg_ask_s * wall_mult:
                if s > wall_ask_size:
                    wall_ask_size = s
                    wall_ask_price = p

        processing_time = (time.time() - start_time) * 1000
        self._update_stats(batch_size, processing_time)

        return {
            'batch_size': batch_size,
            'best_bid': best_bid,
            'best_ask': best_ask,
            'spread': spread,
            'bid_depth': bid_depth,
            'ask_depth': ask_depth,
            'imbalance': imbalance,
            'microprice': microprice,
            'wall_bid_price': wall_bid_price,
            'wall_bid_size': wall_bid_size,
            'wall_ask_price': wall_ask_price,
            'wall_ask_size': wall_ask_size,
            'processing_time_ms': processing_time,
            'gpu_used': False
        }

    def _update_stats(self, batch_size: int, processing_time_ms: float) -> None:
        """Update processing statistics."""
        self.processed_count += batch_size
        self.batch_count += 1

        # Update running averages
        alpha = 0.1  # smoothing factor
        self.avg_batch_size = alpha * batch_size + (1 - alpha) * self.avg_batch_size
        self.avg_processing_time_ms = alpha * processing_time_ms + (1 - alpha) * self.avg_processing_time_ms

    def add_l2_data(self, l2_entries: List[Dict[str, Any]]) -> None:
        """
        Add L2 orderbook entries to the processing buffer.

        Args:
            l2_entries: List of L2 entries with keys: price, size, side
        """
        with self._buffer_lock:
            self._buffer.extend(l2_entries)

            # Check if we should process the buffer
            current_time = get_ny_time_millis()
            should_process = (
                len(self._buffer) >= self.batch_size or
                (current_time - self._last_process_time) >= self.buffer_timeout_ms
            )

            if should_process:
                batch = self._buffer.copy()
                self._buffer.clear()
                self._last_process_time = current_time

                # Process batch asynchronously
                threading.Thread(
                    target=self._process_batch_async,
                    args=(batch,),
                    daemon=True
                ).start()

    def _process_batch_async(self, batch: List[Dict[str, Any]]) -> None:
        """Process a batch asynchronously."""
        try:
            result = self._process_batch_gpu(batch)
            self.logger.debug(
                f"Processed batch: size={result['batch_size']}, "
                f"spread={result['spread']:.6f}, gpu={result['gpu_used']}"
            )
        except Exception as e:
            self.logger.error(f"Failed to process L2 batch: {e}")

    def process_l2_snapshot(self, bids: List[Tuple[float, float]],
                           asks: List[Tuple[float, float]]) -> Dict[str, Any]:
        """
        Process a complete L2 orderbook snapshot.

        Args:
            bids: List of (price, size) tuples for bid side
            asks: List of (price, size) tuples for ask side

        Returns:
            Processing results dictionary
        """
        # Convert to standard format
        l2_entries = []

        for price, size in bids:
            l2_entries.append({'price': price, 'size': size, 'side': 'bid'})

        for price, size in asks:
            l2_entries.append({'price': price, 'size': size, 'side': 'ask'})

        # Process immediately (not buffered)
        return self._process_batch_gpu(l2_entries)

    def get_stats(self) -> Dict[str, Any]:
        """
        Get processing statistics.

        Returns:
            Dictionary with current statistics
        """
        return {
            'symbol': self.symbol,
            'gpu_available': self.gpu_available,
            'gpu_initialized': self._gpu_initialized,
            'batch_size': self.batch_size,
            'buffer_timeout_ms': self.buffer_timeout_ms,
            'processed_count': self.processed_count,
            'batch_count': self.batch_count,
            'avg_batch_size': self.avg_batch_size,
            'avg_processing_time_ms': self.avg_processing_time_ms,
            'buffer_size': len(self._buffer)
        }

    def flush_buffer(self) -> Dict[str, Any]:
        """
        Force processing of any remaining data in the buffer.

        Returns:
            Processing results for the flushed batch
        """
        with self._buffer_lock:
            if not self._buffer:
                return {'batch_size': 0, 'flushed': True}

            batch = self._buffer.copy()
            self._buffer.clear()

        result = self._process_batch_gpu(batch)
        result['flushed'] = True
        return result

    def __del__(self):
        """Cleanup GPU resources."""
        try:
            if self._gpu_initialized and self.gpu_available:
                # Free GPU memory
                if self._price_array is not None:
                    self._price_array = None
                if self._size_array is not None:
                    self._size_array = None
                if self._side_array is not None:
                    self._side_array = None
        except Exception:
            pass  # Ignore cleanup errors
