
import time
import numpy as np
import pandas as pd
import cupy as cp

# Mocking the heavy feature logic from core/meta_features_v1.py
# Simplified to represent the workload (dict lookups + arithmetic + casting)

def build_meta_features_loop(records):
    # Simulates row-by-row python logic
    X = []
    for row in records:
        f = []
        # Simulate accessing ~50 features
        # Arithmetic
        have = float(row.get("have", 0.0))
        need = float(row.get("need", 0.0))
        f.append(have / max(1.0, need))
        f.append(float(row.get("base_score", 0.0)))
        f.append(float(row.get("score_final_raw", 0.0)))
        f.append(float(row.get("score_final_01", 0.0)))

        # Branching
        agg = str(row.get("agg", "")).lower()
        f.append(1.0 if agg == "sum" else 0.0)

        # More arithmetic
        f.append(float(row.get("delta_z", 0.0)) * 2)
        f.append(float(row.get("obi", 0.0)))
        f.append(float(row.get("obi_stable", 0.0)))
        f.append(float(row.get("ofi", 0.0)))
        f.append(float(row.get("ofi_z", 0.0)))

        # More fields (simulate 20-30 total ops)
        for k in ["a", "b", "c", "d", "e", "f", "g", "h", "i", "j"]:
            f.append(float(row.get(k, 0.0)))

        X.append(f)
    return np.array(X)

def build_meta_features_vectorized_cpu(df):
    # Pandas/Numpy vectorized
    f_list = []

    # Arithmetic
    have = df["have"].fillna(0.0).values
    need = df["need"].fillna(0.0).values
    # Avoiding div by zero
    denom = np.maximum(1.0, need)
    f_list.append(have / denom)

    f_list.append(df["base_score"].fillna(0.0).values)
    f_list.append(df["score_final_raw"].fillna(0.0).values)
    f_list.append(df["score_final_01"].fillna(0.0).values)

    # Branching
    # agg is string column
    agg = df["agg"].fillna("").astype(str).str.lower()
    f_list.append((agg == "sum").astype(float).values)

    f_list.append(df["delta_z"].fillna(0.0).values * 2)
    f_list.append(df["obi"].fillna(0.0).values)
    f_list.append(df["obi_stable"].fillna(0.0).values)
    f_list.append(df["ofi"].fillna(0.0).values)
    f_list.append(df["ofi_z"].fillna(0.0).values)

    for k in ["a", "b", "c", "d", "e", "f", "g", "h", "i", "j"]:
         f_list.append(df[k].fillna(0.0).values)

    return np.column_stack(f_list)

def build_meta_features_vectorized_gpu(df):
    # CuPy vectorized
    # Overhead: Transfer DF to Device
    # We assume we transfer dict of arrays or matrix

    # Convert numericals to GPU
    cols = ["have", "need", "base_score", "score_final_raw", "score_final_01",
            "delta_z", "obi", "obi_stable", "ofi", "ofi_z"]
    cols += ["a", "b", "c", "d", "e", "f", "g", "h", "i", "j"]

    # Transfer columns as a matrix for speed? Or individual arrays?
    # Individual is easier for logic map.
    d_gpu = {}
    for c in cols:
        d_gpu[c] = cp.asarray(df[c].fillna(0.0).values)

    # Handle string 'agg' manually or skip (strings hard on GPU)
    # usually we encode strings to int on CPU first.
    # Let's assume pre-encoded for GPU test or do simple boolean check if possible (cupy doesn't support strings well)
    # So we do encoding on CPU
    agg_cpu = df["agg"].fillna("").astype(str).str.lower() == "sum"
    d_gpu["agg_is_sum"] = cp.asarray(agg_cpu.astype(float))

    f_list = []

    denom = cp.maximum(1.0, d_gpu["need"])
    f_list.append(d_gpu["have"] / denom)

    f_list.append(d_gpu["base_score"])
    f_list.append(d_gpu["score_final_raw"])
    f_list.append(d_gpu["score_final_01"])
    f_list.append(d_gpu["agg_is_sum"])

    f_list.append(d_gpu["delta_z"] * 2)
    f_list.append(d_gpu["obi"])
    f_list.append(d_gpu["obi_stable"])
    f_list.append(d_gpu["ofi"])
    f_list.append(d_gpu["ofi_z"])

    for k in ["a", "b", "c", "d", "e", "f", "g", "h", "i", "j"]:
        f_list.append(d_gpu[k])

    return cp.column_stack(f_list)

def benchmark():
    N = 100_000 # 100k rows
    print(f"Generating {N} rows...")

    data = {
        "have": np.random.rand(N),
        "need": np.random.rand(N),
        "base_score": np.random.rand(N),
        "score_final_raw": np.random.rand(N),
        "score_final_01": np.random.rand(N),
        "agg": np.random.choice(["sum", "avg", "min"], N),
        "delta_z": np.random.randn(N),
        "obi": np.random.randn(N),
        "obi_stable": np.random.randn(N),
        "ofi": np.random.randn(N),
        "ofi_z": np.random.randn(N),
    }
    for k in ["a", "b", "c", "d", "e", "f", "g", "h", "i", "j"]:
        data[k] = np.random.randn(N)

    df = pd.DataFrame(data)
    records = df.to_dict("records")

    print("Starting Benchmarks...")

    # 1. Loop
    start = time.time()
    _ = build_meta_features_loop(records)
    t_loop = time.time() - start
    print(f"1. Python Loop: {t_loop:.4f} s")

    # 2. Vectorized CPU
    start = time.time()
    _ = build_meta_features_vectorized_cpu(df)
    t_cpu = time.time() - start
    print(f"2. Vectorized CPU: {t_cpu:.4f} s")

    # 3. Vectorized GPU
    # measure transfer time too as it's part of the task
    start = time.time()
    build_meta_features_vectorized_gpu(df)
    cp.cuda.Stream.null.synchronize() # ensure completion
    t_gpu = time.time() - start
    print(f"3. Vectorized GPU: {t_gpu:.4f} s")

    print("-" * 20)
    print(f"Speedup CPU vs Loop: {t_loop / t_cpu:.2f}x")
    print(f"Speedup GPU vs Loop: {t_loop / t_gpu:.2f}x")
    print(f"Speedup GPU vs CPU:  {t_cpu / t_gpu:.2f}x")

if __name__ == "__main__":
    benchmark()
