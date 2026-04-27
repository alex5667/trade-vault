
import time
import numpy as np
import sys
import os

# Try importing cupy
try:
    import cupy as cp
    HAS_CUPY = True
except ImportError:
    HAS_CUPY = False
    print("WARNING: CuPy not found. GPU benchmarks will be skipped.")

def generate_book(depth=20, spread=10.0, mid=1000.0):
    """Generates a synthetic orderbook (bids/asks)."""
    # Simple linear generation
    offsets = np.arange(1, depth + 1) * (spread / 2.0 / depth) # naive
    bids_p = mid - offsets
    asks_p = mid + offsets
    
    # Random sizes
    bids_s = np.random.rand(depth) * 1.0 + 0.1
    asks_s = np.random.rand(depth) * 1.0 + 0.1
    
    return bids_p, bids_s, asks_p, asks_s

def cpu_OBI(bids_p, bids_s, asks_p, asks_s):
    """
    Standard Order Book Imbalance (OBI) on CPU.
    OBI = (sum(bid_sz) - sum(ask_sz)) / (sum(bid_sz) + sum(ask_sz))
    """
    bid_vol = np.sum(bids_s)
    ask_vol = np.sum(asks_s)
    denom = bid_vol + ask_vol
    if denom == 0:
        return 0.0
    return (bid_vol - ask_vol) / denom

def cpu_Microprice(bids_p, bids_s, asks_p, asks_s):
    """
    Microprice = (BestAsk_P * BestBid_Q + BestBid_P * BestAsk_Q) / (BestBid_Q + BestAsk_Q)
    """
    # Assuming sorted arrays, index 0 is best
    best_bid_p = bids_p[0]
    best_bid_s = bids_s[0]
    best_ask_p = asks_p[0]
    best_ask_s = asks_s[0]
    
    denom = best_bid_s + best_ask_s
    if denom == 0:
        return (best_bid_p + best_ask_p) / 2.0
        
    return (best_ask_p * best_bid_s + best_bid_p * best_ask_s) / denom

def cpu_WallSearch(bids_p, bids_s, asks_p, asks_s, mult=4.0):
    """
    Detects large orders (walls) > mult * average_size.
    Returns (bid_wall_idx, ask_wall_idx)
    """
    avg_bid = np.mean(bids_s)
    avg_ask = np.mean(asks_s)
    
    bid_walls = np.where(bids_s > avg_bid * mult)[0]
    ask_walls = np.where(asks_s > avg_ask * mult)[0]
    
    top_bid = bid_walls[0] if len(bid_walls) > 0 else -1
    top_ask = ask_walls[0] if len(ask_walls) > 0 else -1
    return top_bid, top_ask

def gpu_ProcessBatch(bids_p_gpu, bids_s_gpu, asks_p_gpu, asks_s_gpu):
    """
    Combined GPU kernel mockup.
    """
    if not HAS_CUPY:
        return {}
        
    # Synchronization for accurate timing (in real app we might skip this)
    cp.cuda.Stream.null.synchronize()
    
    # 1. OBI
    bid_vol = cp.sum(bids_s_gpu)
    ask_vol = cp.sum(asks_s_gpu)
    denom = bid_vol + ask_vol
    obi = (bid_vol - ask_vol) / denom if denom > 0 else 0.0
    
    # 2. Microprice (slice first element)
    # Note: accessing single element on GPU array is slow if we bring it back to CPU individually. 
    # Better to keep it on GPU or do batched extract.
    # checking index 0 access:
    bb_p = bids_p_gpu[0]
    bb_s = bids_s_gpu[0]
    ba_p = asks_p_gpu[0]
    ba_s = asks_s_gpu[0]
    
    mp_denom = bb_s + ba_s
    microprice = (ba_p * bb_s + bb_p * ba_s) / mp_denom if mp_denom > 0 else (bb_p + ba_p)*0.5
    
    # 3. Walls
    avg_bid = cp.mean(bids_s_gpu)
    avg_ask = cp.mean(asks_s_gpu)
    
    # This boolean masking + nonzero checking can be heavy if not careful
    bw = bids_s_gpu > (avg_bid * 4.0)
    aw = asks_s_gpu > (avg_ask * 4.0)
    
    # finding first index is tricky efficiently without argmax on mask
    # argmax on boolean returns index of first True
    # check if any
    has_bw = cp.any(bw)
    has_aw = cp.any(aw)
    
    bw_idx = cp.argmax(bw) if has_bw else -1
    aw_idx = cp.argmax(aw) if has_aw else -1
    
    cp.cuda.Stream.null.synchronize()
    
    return obi, microprice, bw_idx, aw_idx


def run_benchmark():
    depths = [20, 50, 100, 500, 1000]
    iterations = 100
    
    print(f"{'Depth':<10} | {'CPU (ms)':<10} | {'GPU (ms)':<10} | {'Speedup':<10}")
    print("-" * 50)
    
    for d in depths:
        # Prepare Data
        bp, bs, ap, as_ = generate_book(d)
        
        # --- CPU Benchmark ---
        start = time.perf_counter()
        for _ in range(iterations):
            _ = cpu_OBI(bp, bs, ap, as_)
            _ = cpu_Microprice(bp, bs, ap, as_)
            _ = cpu_WallSearch(bp, bs, ap, as_)
        cpu_time = (time.perf_counter() - start) * 1000 / iterations
        
        # --- GPU Benchmark ---
        gpu_time = -1.0
        if HAS_CUPY:
            # Transfer overhead IS included in real-world usage usually, 
            # but for "kernel speed" we might exclude it. 
            # Ideally we want end-to-end latency including transfer for a fair comparison of "offloading".
            
            # Reset data for GPU to ensure inputs are ready
            # We include transfer time in the loop because for each tick we HAVE to transfer new data.
            # (Unless we maintain state on GPU, but L2 updates usually imply new/modified levels).
            
            # Pre-allocate arrays to be fair (in app we reuse buffers)
            # but here we just convert simple numpy arrays
            
            start = time.perf_counter()
            for _ in range(iterations):
                # Transfer
                bp_g = cp.asarray(bp)
                bs_g = cp.asarray(bs)
                ap_g = cp.asarray(ap)
                as_g = cp.asarray(as_)
                
                # Compute
                _ = gpu_ProcessBatch(bp_g, bs_g, ap_g, as_g)
                
            gpu_time = (time.perf_counter() - start) * 1000 / iterations
            
        speedup = f"{cpu_time / gpu_time:.1f}x" if gpu_time > 0 else "N/A"
        print(f"{d:<10} | {cpu_time:<10.3f} | {gpu_time:<10.3f} | {speedup:<10}")

if __name__ == "__main__":
    run_benchmark()
