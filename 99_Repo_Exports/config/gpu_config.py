import os
from typing import Literal

Backend = Literal["auto", "cpu", "gpu"]

GPU_ENABLE = bool(int(os.getenv("GPU_ENABLE", "0")))
GPU_MIN_N = int(os.getenv("ROBUST_Z_GPU_MIN_N", "4096"))  # порог для GPU: 4096, маленькие окна всегда на CPU
GPU_BACKEND: Backend = os.getenv("GPU_BACKEND", "auto")  # "auto", "cpu", "gpu"
